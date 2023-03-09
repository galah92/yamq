mod codec;
mod topic;

use bytes::{Buf, BytesMut};
use codec::{decode_slice, encode_slice, Packet};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use topic::TopicMatcher;

#[tokio::main]
async fn main() {
    let mut broker = Broker::new().await;
    broker.run().await;
}

struct Broker {
    listener: TcpListener,
    broker_tx: tokio::sync::broadcast::Sender<Packet<'static>>,
    client_tx: tokio::sync::mpsc::Sender<Packet<'static>>,
    client_rx: tokio::sync::mpsc::Receiver<Packet<'static>>,
    subscribers: TopicMatcher<tokio::sync::mpsc::Sender<Packet<'static>>>,
}

impl Broker {
    pub async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:1883").await.unwrap();
        let (broker_tx, _) = tokio::sync::broadcast::channel(32);
        let (client_tx, client_rx) = tokio::sync::mpsc::channel(32);
        let subscribers = TopicMatcher::new();
        Self {
            listener,
            broker_tx,
            client_tx,
            client_rx,
            subscribers,
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                result = self.listener.accept() => {
                    let (socket, _) = result.unwrap();
                    let mut client = Client::new(socket, self.client_tx.clone(), self.broker_tx.subscribe());
                    tokio::spawn(async move {
                        client.run().await;
                    });
                }
                packet = self.client_rx.recv() => {
                    match &packet {
                        Some(Packet::Subscribe(subscribe)) => {
                            for topic in &subscribe.topics {
                                self.subscribers.insert(topic.topic_path.clone(), self.client_tx.clone());
                            }
                        }
                        Some(Packet::Unsubscribe(unsubscribe)) => {
                            for topic in &unsubscribe.topics {
                                self.subscribers.remove(topic);
                            }
                        }
                        Some(Packet::Publish(publish)) => {
                            let subscribers = self.subscribers.matches(&publish.topic_name);
                            for (_, subscriber) in subscribers {
                                let packet = packet.clone().unwrap();
                                subscriber.send(packet).await.unwrap();
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

struct Client {
    socket: TcpStream,
    client_tx: tokio::sync::mpsc::Sender<Packet<'static>>,
    broker_rx: tokio::sync::broadcast::Receiver<Packet<'static>>,
}

impl Client {
    pub fn new(
        socket: TcpStream,
        client_tx: tokio::sync::mpsc::Sender<Packet<'static>>,
        broker_rx: tokio::sync::broadcast::Receiver<Packet<'static>>,
    ) -> Self {
        Self {
            socket,
            broker_rx,
            client_tx,
        }
    }

    pub async fn run(&mut self) {
        const MAX_PACKET_SIZE: usize = std::mem::size_of::<Packet>();
        let mut buffer = BytesMut::with_capacity(MAX_PACKET_SIZE);

        let n = self.socket.read_buf(&mut buffer).await;
        let packet = decode_slice(&buffer);
        let packet = match packet {
            Ok(packet) => packet,
            Err(e) => {
                println!("{:?}", e);
                return;
            }
        };
        if let Some(Packet::Connect(connect)) = &packet {
            let accepted = connect.username == Some("user");
            let code = if accepted {
                codec::ConnectReturnCode::Accepted
            } else {
                codec::ConnectReturnCode::BadUsernamePassword
            };
            let connack = Packet::Connack(codec::Connack {
                session_present: false, // TODO: support clean session
                code,
            });
            self.send_packet(&connack).await;
        } else {
            // This is not a connect packet, disconnect the client
            return;
        }
        buffer.advance(n.unwrap());

        loop {
            tokio::select! {
                packet = self.broker_rx.recv() => {
                    let packet = packet.unwrap();
                    self.send_packet(&packet).await;
                }
                n = self.socket.read_buf(&mut buffer) => {
                    let packet = decode_slice(&buffer);
                    let packet = match packet {
                        Ok(packet) => packet,
                        Err(e) => {
                            println!("{:?}", e);
                            return;
                        }
                    };
                    let packet = match packet {
                        Some(packet) => packet,
                        None => return,
                    };
                    println!("{:?}", packet);
                    match self.handle_packet(&packet).await {
                        Some(()) => (),
                        None => return,
                    }
                    buffer.advance(n.unwrap());
                }
            }
        }
    }

    async fn handle_packet(&mut self, packet: &Packet<'_>) -> Option<()> {
        match packet {
            Packet::Disconnect => {
                return None;
            }
            Packet::Publish(publish) => {
                let qospid = publish.qospid;
                match qospid.qos() {
                    codec::QoS::AtMostOnce => (),
                    codec::QoS::AtLeastOnce => {
                        let pid = qospid.pid().unwrap();
                        let puback = Packet::Puback(pid);
                        self.send_packet(&puback).await;
                    }
                    codec::QoS::ExactlyOnce => {
                        // We do not support QoS 2, disconnect the client
                        return None;
                    }
                }
                if !publish.dup {
                    // Send to broker
                    let packet = Packet::Publish(publish.to_owned());
                    self.client_tx.send(packet).await.unwrap();
                }
            }
            Packet::Subscribe(subscribe) => {
                let pid = subscribe.pid;
                let topics = &subscribe.topics;
                let return_codes = topics
                    .iter()
                    .map(|topic| codec::SubscribeReturnCodes::Success(topic.qos))
                    .collect();
                let suback = Packet::Suback(codec::Suback { pid, return_codes });
                self.send_packet(&suback).await;

                // Send to broker
                let packet = Packet::Subscribe(subscribe.to_owned());
                self.client_tx.send(packet).await.unwrap();
            }
            Packet::Unsubscribe(unsubscribe) => {
                let pid = unsubscribe.pid;
                let unsuback = Packet::Unsuback(pid);
                self.send_packet(&unsuback).await;

                // Send to broker
                let packet = Packet::Unsubscribe(unsubscribe.to_owned());
                self.client_tx.send(packet).await.unwrap();
            }
            _ => {
                // We do not support other incoming packets, disconnect the client
                return None;
            }
        }
        Some(())
    }

    async fn send_packet(&mut self, packet: &Packet<'_>) {
        let mut encoded = [0u8; 1024];
        let len = encode_slice(packet, &mut encoded).unwrap();
        self.socket.write_all(&encoded[..len]).await.unwrap();
    }
}
