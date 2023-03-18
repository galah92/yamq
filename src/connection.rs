use super::codec;
use futures::SinkExt;
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
};
use tokio_stream::{wrappers::BroadcastStream, StreamExt, StreamMap};
use tokio_util::codec::Framed;

pub struct Connection {
    framed: Framed<TcpStream, codec::MqttCodec>,
    client_tx: mpsc::Sender<ConnectionRequest>,
    subscription_streams: StreamMap<String, BroadcastStream<codec::Publish>>,
}

#[derive(Debug)]
pub struct SubscribeRequest {
    pub subscribe: codec::Subscribe,
    pub res_tx: oneshot::Sender<Vec<(codec::TopicFilter, BroadcastStream<codec::Publish>)>>,
}

#[derive(Debug)]
pub enum ConnectionRequest {
    Publish(codec::Publish),
    Subscribe(SubscribeRequest),
    Unsubscribe(codec::Unsubscribe),
}

impl Connection {
    pub fn new(socket: TcpStream, client_tx: mpsc::Sender<ConnectionRequest>) -> Self {
        Self {
            framed: Framed::new(socket, codec::MqttCodec),
            client_tx,
            subscription_streams: StreamMap::new(),
        }
    }

    pub async fn run(&mut self) {
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
                    let publish = packet.unwrap();
                    let packet = codec::Packet::Publish(publish);
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
                let return_codes = topics
                    .iter()
                    .map(|_| codec::SubscribeAckReason::GrantedQoSOne)
                    .collect();
                let pid = subscribe.pid;

                if topics.is_empty() {
                    // [MQTT-3.8.3-3] Invalid subscribe packet, disconnect the client
                    return None;
                }

                // Send to broker
                let (req_tx, req_rx) = oneshot::channel();
                let subscribe_req = SubscribeRequest {
                    subscribe,
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
