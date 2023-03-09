mod codec;
mod topic_matcher;

use bytes::{Buf, BytesMut};
use codec::{decode_slice, encode_slice, Packet, SubscribeTopic};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let mut broker = Broker::new().await;
    broker.run().await;
}

struct Broker {
    listener: TcpListener,
    broker_tx: tokio::sync::mpsc::Sender<Packet<'static>>,
    broker_rx: tokio::sync::mpsc::Receiver<Packet<'static>>,
    clients_tx: Vec<tokio::sync::mpsc::Sender<Packet<'static>>>,
}

impl Broker {
    pub async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:1883").await.unwrap();
        let (broker_tx, broker_rx) = tokio::sync::mpsc::channel(32);
        let clients_tx = Vec::new();
        Self {
            listener,
            broker_tx,
            broker_rx,
            clients_tx,
        }
    }

    pub async fn run(&mut self) {
        loop {
            let (socket, _) = self.listener.accept().await.unwrap();
            let (client_tx, client_rx) = tokio::sync::mpsc::channel(32);
            self.clients_tx.push(client_tx.clone());
            let mut client = Client::new(socket, self.broker_tx.clone(), client_rx);
            tokio::spawn(async move {
                client.run().await;
            });
        }
    }
}

struct Client {
    socket: TcpStream,
    broker_tx: tokio::sync::mpsc::Sender<Packet<'static>>,
    client_rx: tokio::sync::mpsc::Receiver<Packet<'static>>,
    subscriptions: Vec<SubscribeTopic>,
}

impl Client {
    pub fn new(
        socket: TcpStream,
        broker_tx: tokio::sync::mpsc::Sender<Packet<'static>>,
        client_rx: tokio::sync::mpsc::Receiver<Packet<'static>>,
    ) -> Self {
        let subscriptions = Vec::new();
        Self {
            socket,
            broker_tx,
            client_rx,
            subscriptions,
        }
    }

    pub async fn run(&mut self) {
        const MAX_PACKET_SIZE: usize = std::mem::size_of::<Packet>();
        let mut buffer = BytesMut::with_capacity(MAX_PACKET_SIZE);

        loop {
            tokio::select! {
                Some(packet) = self.client_rx.recv() => {
                    // TODO: we should only support Publish packets and filter according to subscriptions
                    send_packet(&mut self.socket, &packet).await;
                }
                n = self.socket.read_buf(&mut buffer) => {
                    let packet = decode_slice(&buffer);
                    let packet = match packet {
                        Ok(packet) => packet,
                        Err(e) => {
                            println!("{:?}", e);
                            break;
                        }
                    };
                    let packet = match packet {
                        Some(packet) => packet,
                        None => break,
                    };
                    println!("Received packet: {:?}", packet);
                    match self.handle_packet(&packet).await {
                        Some(()) => (),
                        None => break,
                    }
                    buffer.advance(n.unwrap());
                }
            }
        }
    }

    async fn handle_packet(&mut self, packet: &Packet<'_>) -> Option<()> {
        match packet {
            Packet::Connect(connect) => {
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
                send_packet(&mut self.socket, &connack).await;
            }
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
                        send_packet(&mut self.socket, &puback).await;
                    }
                    codec::QoS::ExactlyOnce => {
                        let pid = qospid.pid().unwrap();
                        let pubrec = Packet::Pubrec(pid);
                        send_packet(&mut self.socket, &pubrec).await;
                    }
                }
            }
            Packet::Pubrel(pid) => {
                let pubcomp = Packet::Pubcomp(*pid);
                send_packet(&mut self.socket, &pubcomp).await;
            }
            Packet::Pingreq => {
                let pingresp = Packet::Pingresp;
                send_packet(&mut self.socket, &pingresp).await;
            }
            Packet::Subscribe(subscribe) => {
                let pid = subscribe.pid;
                let topics = &subscribe.topics;
                self.subscriptions.extend_from_slice(topics);
                println!("{:?}", self.subscriptions);
                let return_codes = topics
                    .iter()
                    .map(|topic| codec::SubscribeReturnCodes::Success(topic.qos))
                    .collect();
                let suback = Packet::Suback(codec::Suback { pid, return_codes });
                send_packet(&mut self.socket, &suback).await;
            }
            Packet::Unsubscribe(unsubscribe) => {
                let pid = unsubscribe.pid;
                let topics = &unsubscribe.topics;
                self.subscriptions
                    .retain(|topic| !topics.contains(&topic.topic_path));
                println!("{:?}", self.subscriptions);
                let unsuback = Packet::Unsuback(pid);
                send_packet(&mut self.socket, &unsuback).await;
            }
            _ => {
                panic!("SHOULD NOT HAPPEN");
            }
        }
        Some(())
    }
}

async fn send_packet(socket: &mut TcpStream, packet: &Packet<'_>) {
    let mut encoded = [0u8; 1024];
    let len = encode_slice(packet, &mut encoded).unwrap();
    socket.write_all(&encoded[..len]).await.unwrap();
}
