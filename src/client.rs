use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::codec;

pub struct Client {
    framed: Framed<TcpStream, codec::MqttCodec>,
}

impl Client {
    pub async fn new(address: &str) -> Self {
        let stream = TcpStream::connect(address).await.unwrap();
        let framed = Framed::new(stream, codec::MqttCodec);
        Self { framed }
    }

    pub async fn connect(&mut self) -> codec::Connack {
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
        self.framed.send(connect).await.unwrap();

        let packet = self.framed.next().await.unwrap().unwrap();
        match packet {
            codec::Packet::Connack(connack) => connack,
            _ => panic!("unexpected packet"),
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
        self.framed.send(publish).await.unwrap();
    }

    pub async fn subscribe(&mut self, topic: &str) -> codec::Suback {
        let subscribe = codec::Subscribe {
            pid: 1,
            subscription_topics: vec![codec::SubscriptionTopic {
                topic_path: codec::Topic::try_from(topic).unwrap(),
                topic_filter: codec::TopicFilter::Concrete,
                qos: codec::QoS::AtMostOnce,
            }],
        };
        let subscribe = codec::Packet::Subscribe(subscribe);
        self.framed.send(subscribe).await.unwrap();

        let packet = self.framed.next().await.unwrap().unwrap();
        match packet {
            codec::Packet::Suback(suback) => suback,
            _ => panic!("unexpected packet"),
        }
    }

    pub async fn recv(&mut self) -> codec::Packet {
        let packet = self.framed.next().await.unwrap().unwrap();
        packet
    }
}
