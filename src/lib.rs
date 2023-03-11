mod codec;
mod newcodec;
mod topic;

use bytes::BytesMut;
use newcodec::codec::MqttCodec;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::{wrappers::BroadcastStream, StreamExt, StreamMap};
use tokio_util::codec::{Decoder, Encoder};
pub use topic::TopicMatcher;

pub struct Broker {
    listener: TcpListener,
    client_tx: mpsc::Sender<ConnectionRequest>,
    client_rx: mpsc::Receiver<ConnectionRequest>,
    subscribers: TopicMatcher<broadcast::Sender<newcodec::types::Packet>>,
    retained: TopicMatcher<newcodec::types::Publish>,
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
                            for (_, tx) in self.subscribers.matches(publish.topic.topic_name()) {
                                use broadcast::error::SendError;
                                let publish = newcodec::types::Packet::Publish(publish.clone());
                                if let Err(SendError(err)) = tx.send(publish) {
                                    println!("{:?}", err);
                                }
                            }

                            if publish.retain {
                                if publish.payload.is_empty() {
                                    // [MQTT-3.3.1-6] Remove retained message
                                    self.retained.remove(publish.topic.topic_name());
                                } else {
                                    // [MQTT-3.3.1-5] Store retained message
                                    self.retained.insert(publish.topic.topic_name(), publish.clone());
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
                                    sub_tx.send(newcodec::types::Packet::Publish(publish.clone())).unwrap();
                                }
                            }
                        }
                        ConnectionRequest::Unsubscribe(unsubscribe) => {
                            // Remove subscriptions that are no longer used
                            for topic in &unsubscribe.topics {
                                if let Some(sub_tx) = self.subscribers.get(topic.topic_name()) {
                                    if sub_tx.receiver_count() == 0 {
                                        self.subscribers.remove(topic.topic_name());
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
    socket: TcpStream,
    client_tx: mpsc::Sender<ConnectionRequest>,
    subscription_streams: StreamMap<String, BroadcastStream<newcodec::types::Packet>>,
}

#[derive(Debug)]
enum ConnectionRequest {
    Publish(newcodec::types::Publish),
    Subscribe(
        newcodec::types::Subscribe,
        oneshot::Sender<Vec<(String, BroadcastStream<newcodec::types::Packet>)>>,
    ),
    Unsubscribe(newcodec::types::Unsubscribe),
}

impl Connection {
    fn new(socket: TcpStream, client_tx: mpsc::Sender<ConnectionRequest>) -> Self {
        let subscription_streams = StreamMap::new();
        Self {
            socket,
            client_tx,
            subscription_streams,
        }
    }

    async fn run(&mut self) {
        let mut buffer = BytesMut::new();
        let mut codec = MqttCodec::new();

        match self.socket.read_buf(&mut buffer).await {
            Ok(0) => {
                println!("Client disconnected");
                return;
            }
            Ok(_) => {}
            Err(err) => {
                println!("Error reading from socket: {:?}", err);
                return;
            }
        }
        let packet = match codec.decode(&mut buffer) {
            Ok(Some(packet)) => packet,
            Ok(None) => {
                println!("Error decoding packet");
                return;
            }
            Err(err) => {
                println!("Error decoding packet: {:?}", err);
                return;
            }
        };
        let newcodec::types::Packet::Connect(_connect) = &packet else {
            // This is not a connect packet, disconnect the client
            return;
        };
        let connack = newcodec::types::Packet::Connack(newcodec::types::Connack {
            session_present: false, // TODO: support clean session
            code: newcodec::types::ConnectReason::Success,
        });
        self.newsend_packet(connack).await;

        loop {
            tokio::select! {
                Some(
                    (_, packet)
                ) = self.subscription_streams.next() => {
                    let packet = packet.unwrap();
                    self.newsend_packet(packet).await;
                }
                n = self.socket.read_buf(&mut buffer) => {
                    match self.socket.read_buf(&mut buffer).await {
                        Ok(0) => {
                            println!("Client disconnected");
                            return;
                        }
                        Ok(_) => {}
                        Err(err) => {
                            println!("Error reading from socket: {:?}", err);
                            return;
                        }
                    }
                    let packet = match codec.decode(&mut buffer) {
                        Ok(Some(packet)) => packet,
                        Ok(None) => continue, // The packet is incomplete, wait for more data
                        Err(err) => {
                            println!("Error decoding packet: {:?}", err);
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

    async fn handle_packet(&mut self, packet: newcodec::types::Packet) -> Option<()> {
        match packet {
            newcodec::types::Packet::Disconnect => {
                return None;
            }
            newcodec::types::Packet::Publish(publish) => {
                match publish.qos {
                    newcodec::types::QoS::AtMostOnce => (),
                    newcodec::types::QoS::AtLeastOnce => {
                        let pid = publish.pid.unwrap();
                        let puback = newcodec::types::Puback { pid };
                        let puback = newcodec::types::Packet::Puback(puback);
                        self.newsend_packet(puback).await;
                    }
                    newcodec::types::QoS::ExactlyOnce => {
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
            newcodec::types::Packet::Subscribe(subscribe) => {
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
                    .map(|topic| newcodec::types::SubscribeAckReason::GrantedQoSOne)
                    .collect();
                let suback = newcodec::types::Suback { pid, return_codes };
                let suback = newcodec::types::Packet::Suback(suback);
                self.newsend_packet(suback).await;
            }
            newcodec::types::Packet::Unsubscribe(unsubscribe) => {
                let topics = &unsubscribe.topics;

                if topics.is_empty() {
                    // [MQTT-3.10.3-2] Invalid unsubscribe packet, disconnect the client
                    return None;
                }

                // Remove the subscription streams
                for topic in topics {
                    self.subscription_streams.remove(topic.topic_name());
                }

                // Send to broker
                let req = ConnectionRequest::Unsubscribe(unsubscribe.to_owned());
                self.client_tx.send(req).await.unwrap();

                // Send unsuback
                let pid = unsubscribe.pid;
                let unsuback = newcodec::types::Unsuback { pid };
                let unsuback = newcodec::types::Packet::Unsuback(unsuback);
                self.newsend_packet(unsuback).await;
            }
            newcodec::types::Packet::Pingreq => {
                let pingresp = newcodec::types::Packet::Pingresp;
                self.newsend_packet(pingresp).await;
            }
            p => {
                // We do not support other incoming packets, disconnect the client
                println!("Unsupported packet: {:?}", p);
                return None;
            }
        }
        Some(())
    }

    async fn newsend_packet(&mut self, packet: newcodec::types::Packet) {
        let mut codec = MqttCodec::new();
        let mut buffer = BytesMut::new();
        codec.encode(packet, &mut buffer).unwrap();
        self.socket.write_all(&buffer).await.unwrap();
    }
}
