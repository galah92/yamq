use bytes::Bytes;
use futures::{stream::SplitSink, SinkExt, StreamExt};
use tokio::{net::TcpStream, sync::mpsc};
use tokio_util::codec::Framed;

use crate::codec;

pub struct Client {
    sender: SplitSink<Framed<TcpStream, codec::MqttCodec>, codec::Packet>,
    publish_receiver: mpsc::UnboundedReceiver<codec::Publish>,
    suback_receiver: mpsc::UnboundedReceiver<codec::Suback>,
}

impl Client {
    pub async fn connect(address: &str) -> Self {
        let stream = TcpStream::connect(address).await.unwrap();
        let framed = Framed::new(stream, codec::MqttCodec);
        let (mut sender, mut receiver) = framed.split();
        let (publish_sender, publish_receiver) = mpsc::unbounded_channel();
        let (suback_sender, suback_receiver) = mpsc::unbounded_channel();

        let connect = codec::Connect {
            protocol: codec::Protocol::V311,
            keep_alive: 0,
            client_id: "test".to_string(),
            clean_session: true,
            will: None,
            username: None,
            password: None,
        };
        let connect = codec::Packet::Connect(connect);
        sender.send(connect).await.unwrap();

        let packet = receiver.next().await.unwrap().unwrap();
        match packet {
            codec::Packet::Connack(connack) => {
                assert_eq!(connack.code, codec::ConnectCode::Accepted);
            }
            _ => panic!("unexpected packet"),
        }

        tokio::spawn(async move {
            while let Some(packet) = receiver.next().await {
                let packet = packet.unwrap();
                match packet {
                    codec::Packet::Publish(publish) => {
                        publish_sender.send(publish).unwrap();
                    }
                    codec::Packet::Suback(suback) => {
                        suback_sender.send(suback).unwrap();
                    }
                    _ => {}
                }
            }
        });

        Self {
            sender,
            publish_receiver,
            suback_receiver,
        }
    }

    pub async fn publish(&mut self, topic: &str, payload: Bytes) {
        let publish = codec::Publish {
            dup: false,
            qos: codec::QoS::AtMostOnce,
            retain: false,
            topic: codec::Topic::try_from(topic).unwrap(),
            pid: None,
            payload,
        };
        let publish = codec::Packet::Publish(publish);
        self.sender.send(publish).await.unwrap();
    }

    pub async fn subscribe(&mut self, topic: &str) {
        let subscribe = codec::Subscribe {
            pid: 1,
            subscription_topics: vec![codec::SubscriptionTopic {
                topic_path: codec::Topic::try_from(topic).unwrap(),
                topic_filter: codec::TopicFilter::Concrete,
                qos: codec::QoS::AtMostOnce,
            }],
        };
        let subscribe = codec::Packet::Subscribe(subscribe);
        self.sender.send(subscribe).await.unwrap();

        let suback = self.suback_receiver.recv().await.unwrap();
        assert_eq!(suback.pid, 1);
    }

    pub async fn read(&mut self) -> codec::Publish {
        self.publish_receiver.recv().await.unwrap()
    }
}
