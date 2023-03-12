mod codec;
mod topic;

use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::{wrappers::BroadcastStream, StreamMap};
use tokio_util::codec::Framed;
pub use topic::TopicMatcher;

pub struct Broker {
    listener: TcpListener,
    client_tx: mpsc::Sender<ConnectionRequest>,
    client_rx: mpsc::Receiver<ConnectionRequest>,
    subscribers: TopicMatcher<broadcast::Sender<codec::Packet>>,
    retained: TopicMatcher<codec::Publish>,
}

impl Broker {
    pub async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:1883").await.unwrap();
        let (client_tx, client_rx) = mpsc::channel(32);
        Self {
            listener,
            client_tx,
            client_rx,
            subscribers: TopicMatcher::new(),
            retained: TopicMatcher::new(),
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
                        ConnectionRequest::Publish(publish) => {
                            for (_, tx) in self.subscribers.matches(&publish.topic) {
                                use broadcast::error::SendError;
                                let publish = codec::Packet::Publish(publish.clone());
                                if let Err(SendError(err)) = tx.send(publish) {
                                    println!("{:?}", err);
                                }
                            }

                            if publish.retain {
                                if publish.payload.is_empty() {
                                    // [MQTT-3.3.1-6] Remove retained message
                                    self.retained.remove(&publish.topic);
                                } else {
                                    // [MQTT-3.3.1-5] Store retained message
                                    let topic = publish.topic.clone();
                                    self.retained.insert(topic, publish.clone());
                                }
                            }
                        }
                        ConnectionRequest::Subscribe(subscribe, res_tx) => {
                            // TODO: have insert-or-update method instead of this
                            let res = subscribe.subscription_topics.iter().map(|topic| {
                                if let Some(sub_tx) = self.subscribers.get(&topic.topic_path) {
                                    let sub_rx = sub_tx.subscribe();
                                    (topic.topic_path.to_owned(), sub_rx.into())
                                } else {
                                    let (sub_tx, sub_rx) = broadcast::channel(32);
                                    self.subscribers.insert(topic.topic_path.to_owned(), sub_tx);
                                    (topic.topic_path.to_owned(), sub_rx.into())
                                }
                            }).collect();
                            res_tx.send(res).unwrap();

                            // Send retained messages
                            for topic in &subscribe.subscription_topics {
                                for (topic, publish) in self.retained.matches(&topic.topic_path) {
                                    let sub_tx = self.subscribers.get(topic).unwrap();
                                    sub_tx.send(codec::Packet::Publish(publish.clone())).unwrap();
                                }
                            }
                        }
                        ConnectionRequest::Unsubscribe(unsubscribe) => {
                            // Remove subscriptions that are no longer used
                            for topic in &unsubscribe.topics {
                                if let Some(sub_tx) = self.subscribers.get(topic) {
                                    if sub_tx.receiver_count() == 0 {
                                        self.subscribers.remove(topic);
                                    }
                                }
                            }
                        }
                    }
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
enum ConnectionRequest {
    Publish(codec::Publish),
    Subscribe(
        codec::Subscribe,
        oneshot::Sender<Vec<(String, BroadcastStream<codec::Packet>)>>,
    ),
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
            None => {
                println!("Client disconnected");
                return;
            }
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
                        None => {
                            println!("Client disconnected");
                            return;
                        }
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
                let req = ConnectionRequest::Subscribe(subscribe.clone(), req_tx);
                self.client_tx.send(req).await.unwrap();

                // Wait for the broker to ack the subscription
                let res = req_rx.await.unwrap();
                for (topic, stream) in res {
                    self.subscription_streams.insert(topic, stream);
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
                    let topic: &str = topic;
                    self.subscription_streams.remove(topic);
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
