mod codec;
mod topic;

use bytes::{Buf, BytesMut};
use codec::{decode_slice, encode_slice, Packet};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::StreamExt;
use tokio_stream::{wrappers::BroadcastStream, StreamMap};
pub use topic::TopicMatcher;

pub struct Broker {
    listener: TcpListener,
    broker_tx: broadcast::Sender<Packet<'static>>,
    client_tx: mpsc::Sender<ConnectionRequest>,
    client_rx: mpsc::Receiver<ConnectionRequest>,
    subscribers: TopicMatcher<broadcast::Sender<Packet<'static>>>,
}

impl Broker {
    pub async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:1883").await.unwrap();
        let (broker_tx, _) = broadcast::channel(32);
        let (client_tx, client_rx) = mpsc::channel(32);
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
                    let client_tx = self.client_tx.clone();
                    let broker_rx = self.broker_tx.subscribe();
                    let mut client = Connection::new(socket, client_tx, broker_rx);
                    tokio::spawn(async move {
                        client.run().await;
                    });
                }
                req = self.client_rx.recv() => {
                    match req.unwrap() {
                        ConnectionRequest::Publish(publish) => {
                            let topic = &publish.topic_name;
                            let sender = self.subscribers.get(topic);
                            if let Some(sender) = sender {
                                sender.send(Packet::Publish(publish.to_owned())).unwrap();
                            }
                        }
                        ConnectionRequest::Subscribe(subscribe, res_tx) => {
                            let topics = &subscribe.topics;
                            if topics.len() != 1 {
                                // We do not support multiple topics in a single subscribe packet, disconnect the client
                                continue;
                            }
                            let topic = &topics[0];
                            if let Some(sub_tx) = self.subscribers.get_mut(&topic.topic_path) {
                                let sub_rx = sub_tx.subscribe();
                                res_tx.send(sub_rx.into()).unwrap();
                            } else {
                                let (sub_tx, sub_rx) = broadcast::channel(32);
                                self.subscribers.insert(topic.topic_path.to_owned(), sub_tx);
                                res_tx.send(sub_rx.into()).unwrap();
                            }
                        }
                        _ => (),
                    }
                }
            }
        }
    }
}

struct Connection {
    socket: TcpStream,
    client_tx: mpsc::Sender<ConnectionRequest>,
    broker_rx: broadcast::Receiver<Packet<'static>>,
    subscription_streams: StreamMap<String, BroadcastStream<Packet<'static>>>,
}

#[derive(Debug)]
enum ConnectionRequest {
    Publish(codec::Publish<'static>),
    Subscribe(
        codec::Subscribe,
        oneshot::Sender<BroadcastStream<Packet<'static>>>,
    ),
    Unsubscribe(
        codec::Unsubscribe,
        oneshot::Sender<BroadcastStream<Packet<'static>>>,
    ),
}

impl Connection {
    fn new(
        socket: TcpStream,
        client_tx: mpsc::Sender<ConnectionRequest>,
        broker_rx: broadcast::Receiver<Packet<'static>>,
    ) -> Self {
        let subscription_streams = StreamMap::new();
        Self {
            socket,
            broker_rx,
            client_tx,
            subscription_streams,
        }
    }

    async fn run(&mut self) {
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
                Some(
                    (_, packet)
                ) = self.subscription_streams.next() => {
                    let packet = packet.unwrap();
                    self.send_packet(&packet).await;
                }
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
                    let req = ConnectionRequest::Publish(publish.to_owned());
                    self.client_tx.send(req).await.unwrap();
                }
            }
            Packet::Subscribe(subscribe) => {
                let topics = &subscribe.topics;
                if topics.len() != 1 {
                    // We do not support multiple topics in a single subscribe packet, disconnect the client
                    return None;
                }
                let topic = &topics[0];

                let (req_tx, req_rx) = oneshot::channel::<BroadcastStream<Packet>>();
                let req = ConnectionRequest::Subscribe(subscribe.to_owned(), req_tx);
                self.client_tx.send(req).await.unwrap();

                // Wait for the broker to ack the subscription
                let res = req_rx.await.unwrap();
                let topic_path = topic.topic_path.clone();
                self.subscription_streams.insert(topic_path, res);

                // Send suback
                let pid = subscribe.pid;
                let return_codes = vec![codec::SubscribeReturnCodes::Success(topic.qos)];
                let suback = Packet::Suback(codec::Suback { pid, return_codes });
                self.send_packet(&suback).await;
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