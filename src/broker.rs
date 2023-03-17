use async_trait::async_trait;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::{wrappers::BroadcastStream, StreamMap};
use tokio_util::codec::Framed;

use crate::{codec, TopicMatcher};

pub struct Broker {
    listener: TcpListener,
    client_tx: mpsc::Sender<ConnectionRequest>,
    client_rx: mpsc::Receiver<ConnectionRequest>,
    subscribers: TopicMatcher<broadcast::Sender<codec::Packet>>,
    retained: TopicMatcher<codec::Publish>,
}

#[async_trait]
pub trait SubscriptionAction {
    async fn on_publish(&mut self, publish: codec::Publish);
}

struct BrokerSubscription {
    client_tx: mpsc::Sender<ConnectionRequest>,
}

#[async_trait]
pub trait Subscription {
    async fn publish(&self, topic: &str, payload: Bytes) -> Result<(), PublishError>;
}

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("topic parse error: {0}")]
    InvalidTopic(#[from] codec::TopicParseError),
    #[error("send error")]
    SendError,
}

#[async_trait]
impl Subscription for BrokerSubscription {
    async fn publish(&self, topic: &str, payload: Bytes) -> Result<(), PublishError> {
        let publish = codec::Publish {
            dup: false,
            qos: codec::QoS::AtLeastOnce,
            retain: false,
            topic: codec::Topic::try_from(topic)?,
            pid: None,
            payload,
        };
        let req = ConnectionRequest::Publish(publish);
        self.client_tx
            .send(req)
            .await
            .map_err(|_| PublishError::SendError)
    }
}

impl Broker {
    pub async fn new(address: &str) -> Self {
        let listener = TcpListener::bind(address).await.unwrap();
        let (client_tx, client_rx) = mpsc::channel(32);
        Self {
            listener,
            client_tx,
            client_rx,
            subscribers: TopicMatcher::new(),
            retained: TopicMatcher::new(),
        }
    }

    pub fn address(&self) -> String {
        self.listener.local_addr().unwrap().to_string()
    }

    pub async fn subscription<T, A>(&self, topic_filter: T, mut actor: A) -> impl Subscription
    where
        A: SubscriptionAction + Send + Sync + 'static,
        T: TryInto<codec::TopicFilter>,
        <T as TryInto<codec::TopicFilter>>::Error: std::fmt::Debug,
    {
        let topic_filter = topic_filter.try_into().unwrap();
        let client_tx = self.client_tx.clone();

        tokio::spawn(async move {
            let (res_tx, res_rx) = oneshot::channel();
            let subscribe = codec::Subscribe {
                pid: 1,
                subscription_topics: vec![codec::SubscriptionTopic {
                    topic_filter,
                    qos: codec::QoS::AtLeastOnce,
                }],
            };
            let req = ConnectionRequest::Subscribe(SubscribeRequest { subscribe, res_tx });
            client_tx.send(req).await.unwrap();
            let res = res_rx.await.unwrap();
            let (_, mut stream) = res.into_iter().next().unwrap();
            while let Some(packet) = stream.next().await {
                if let Ok(codec::Packet::Publish(publish)) = packet {
                    actor.on_publish(publish).await;
                }
            }
        });

        BrokerSubscription {
            client_tx: self.client_tx.clone(),
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                result = self.listener.accept() => {
                    let (socket, _) = result.unwrap();
                    let client_tx = self.client_tx.clone();
                    let mut client = Connection::new(socket, client_tx);
                    tokio::spawn(async move {
                        client.run().await;
                    });
                }
                req = self.client_rx.recv() => {
                    match req.unwrap() {
                        ConnectionRequest::Publish(publish) => self.handle_publish(publish),
                        ConnectionRequest::Subscribe(subscribe) => self.handle_subscribe(subscribe),
                        ConnectionRequest::Unsubscribe(unsubscribe) => self.handle_unsubscribe(unsubscribe),
                    }
                }
            }
        }
    }

    fn handle_publish(&mut self, publish: codec::Publish) {
        for (_, tx) in self.subscribers.matches(publish.topic.as_ref()) {
            use broadcast::error::SendError;
            let publish = codec::Packet::Publish(publish.clone());
            if let Err(SendError(err)) = tx.send(publish) {
                println!("{:?}", err);
            }
        }

        if publish.retain {
            if publish.payload.is_empty() {
                // [MQTT-3.3.1-6] Remove retained message
                self.retained.remove(publish.topic.as_ref());
            } else {
                // [MQTT-3.3.1-5] Store retained message
                let topic = publish.topic.clone();
                self.retained.insert(topic.as_ref(), publish.clone());
            }
        }
    }

    fn handle_subscribe(&mut self, subscribe_req: SubscribeRequest) {
        let SubscribeRequest { subscribe, res_tx } = subscribe_req;
        // TODO: have insert-or-update method instead of this
        let res = subscribe
            .subscription_topics
            .iter()
            .map(|topic| {
                if let Some(sub_tx) = self.subscribers.get(topic.topic_filter.as_ref()) {
                    let sub_rx = sub_tx.subscribe();
                    (topic.topic_filter.to_owned(), sub_rx.into())
                } else {
                    let (sub_tx, sub_rx) = broadcast::channel(32);
                    self.subscribers.insert(topic.topic_filter.as_ref(), sub_tx);
                    (topic.topic_filter.to_owned(), sub_rx.into())
                }
            })
            .collect();
        res_tx.send(res).unwrap();

        // Send retained messages
        for topic in &subscribe.subscription_topics {
            for (topic, publish) in self.retained.matches(topic.topic_filter.as_ref()) {
                let sub_tx = self.subscribers.get(topic).unwrap();
                sub_tx
                    .send(codec::Packet::Publish(publish.clone()))
                    .unwrap();
            }
        }
    }

    fn handle_unsubscribe(&mut self, unsubscribe: codec::Unsubscribe) {
        // Remove subscriptions that are no longer used
        for topic in &unsubscribe.topics {
            if let Some(sub_tx) = self.subscribers.get(topic.as_ref()) {
                if sub_tx.receiver_count() == 0 {
                    self.subscribers.remove(topic.as_ref());
                }
            }
        }
    }
}

struct Connection {
    framed: Framed<TcpStream, codec::MqttCodec>,
    client_tx: mpsc::Sender<ConnectionRequest>,
    subscription_streams: StreamMap<String, BroadcastStream<codec::Packet>>,
}

#[derive(Debug)]
struct SubscribeRequest {
    subscribe: codec::Subscribe,
    res_tx: oneshot::Sender<Vec<(codec::TopicFilter, BroadcastStream<codec::Packet>)>>,
}

#[derive(Debug)]
enum ConnectionRequest {
    Publish(codec::Publish),
    Subscribe(SubscribeRequest),
    Unsubscribe(codec::Unsubscribe),
}

impl Connection {
    fn new(socket: TcpStream, client_tx: mpsc::Sender<ConnectionRequest>) -> Self {
        Self {
            framed: Framed::new(socket, codec::MqttCodec),
            client_tx,
            subscription_streams: StreamMap::new(),
        }
    }

    async fn run(&mut self) {
        let connect = match self.framed.next().await {
            Some(Ok(codec::Packet::Connect(connect))) => connect,
            Some(Ok(packet)) => {
                println!("Expected connect packet, got {:?}", packet);
                return;
            }
            Some(Err(err)) => {
                println!("Error reading packet: {:?}", err);
                return;
            }
            None => return, // Client disconnected
        };

        if connect.protocol != codec::Protocol::V311 {
            let connack = codec::Packet::Connack(codec::Connack {
                session_present: false,
                code: codec::ConnectCode::UnacceptableProtocol,
            });
            self.framed.send(connack).await.unwrap();
            return;
        }

        let connack = codec::Packet::Connack(codec::Connack {
            session_present: false, // TODO: support clean session
            code: codec::ConnectCode::Accepted,
        });
        self.framed.send(connack).await.unwrap();

        loop {
            tokio::select! {
                Some(
                    (_, packet)
                ) = self.subscription_streams.next() => {
                    let packet = packet.unwrap();
                    self.framed.send(packet).await.unwrap();
                }
                packet = self.framed.next() => {
                    let packet = match packet {
                        Some(Ok(packet)) => packet,
                        Some(Err(err)) => {
                            println!("Error reading packet: {:?}", err);
                            return;
                        }
                        None => return, // Client disconnected
                    };
                    match self.handle_packet(packet).await {
                        Some(()) => (),
                        None => return,
                    }
                }
                else => return,
            }
        }
    }

    async fn handle_packet(&mut self, packet: codec::Packet) -> Option<()> {
        match packet {
            codec::Packet::Disconnect => {
                return None;
            }
            codec::Packet::Publish(publish) => {
                match publish.qos {
                    codec::QoS::AtMostOnce => (),
                    codec::QoS::AtLeastOnce => {
                        let pid = publish.pid.unwrap();
                        let puback = codec::Puback { pid };
                        let puback = codec::Packet::Puback(puback);
                        self.framed.send(puback).await.unwrap();
                    }
                    codec::QoS::ExactlyOnce => {
                        // We do not support QoS 2, disconnect the client
                        return None;
                    }
                }
                if !publish.dup {
                    // Send to broker
                    let req = ConnectionRequest::Publish(publish);
                    self.client_tx.send(req).await.unwrap();
                } else {
                    println!("Ignoring duplicate publish packet");
                }
            }
            codec::Packet::Subscribe(subscribe) => {
                let topics = &subscribe.subscription_topics;

                if topics.is_empty() {
                    // [MQTT-3.8.3-3] Invalid subscribe packet, disconnect the client
                    return None;
                }

                // Send to broker
                let (req_tx, req_rx) = oneshot::channel();
                let subscribe_req = SubscribeRequest {
                    subscribe: subscribe.clone(),
                    res_tx: req_tx,
                };
                let req = ConnectionRequest::Subscribe(subscribe_req);
                self.client_tx.send(req).await.unwrap();

                // Wait for the broker to ack the subscription
                let res = req_rx.await.unwrap();
                for (topic, stream) in res {
                    self.subscription_streams.insert(topic.to_string(), stream);
                }

                // Send suback
                let pid = subscribe.pid;
                let return_codes = topics
                    .iter()
                    .map(|_| codec::SubscribeAckReason::GrantedQoSOne)
                    .collect();
                let suback = codec::Suback { pid, return_codes };
                let suback = codec::Packet::Suback(suback);
                self.framed.send(suback).await.unwrap();
            }
            codec::Packet::Unsubscribe(unsubscribe) => {
                let topics = &unsubscribe.topics;

                if topics.is_empty() {
                    // [MQTT-3.10.3-2] Invalid unsubscribe packet, disconnect the client
                    return None;
                }

                // Remove the subscription streams
                for topic in topics {
                    // let topic: &str = topic;
                    self.subscription_streams.remove(topic.as_ref());
                }

                // Send to broker
                let req = ConnectionRequest::Unsubscribe(unsubscribe.to_owned());
                self.client_tx.send(req).await.unwrap();

                // Send unsuback
                let pid = unsubscribe.pid;
                let unsuback = codec::Unsuback { pid };
                let unsuback = codec::Packet::Unsuback(unsuback);
                self.framed.send(unsuback).await.unwrap();
            }
            codec::Packet::Pingreq => {
                let pingresp = codec::Packet::Pingresp;
                self.framed.send(pingresp).await.unwrap();
            }
            p => {
                // We do not support other incoming packets, disconnect the client
                println!("Unsupported packet: {:?}", p);
                return None;
            }
        }
        Some(())
    }
}
