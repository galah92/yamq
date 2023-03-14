use bytes::Bytes;
use futures::{stream::SplitSink, SinkExt, StreamExt};
use tokio::{net::TcpStream, sync::mpsc};
use tokio_util::codec::Framed;

use crate::codec;

pub struct Client {
    sender: SplitSink<Framed<TcpStream, codec::MqttCodec>, codec::Packet>,
    publish_receiver: mpsc::UnboundedReceiver<codec::Publish>,
    suback_receiver: mpsc::UnboundedReceiver<codec::Suback>,
    unsuback_receiver: mpsc::UnboundedReceiver<codec::Unsuback>,
}

impl Client {
    pub async fn connect(address: &str) -> Result<Self, ConnectError> {
        let stream = TcpStream::connect(address).await?;
        let framed = Framed::new(stream, codec::MqttCodec);
        let (mut sender, mut receiver) = framed.split();
        let (publish_sender, publish_receiver) = mpsc::unbounded_channel();
        let (suback_sender, suback_receiver) = mpsc::unbounded_channel();
        let (unsuback_sender, unsuback_receiver) = mpsc::unbounded_channel();

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
        sender.send(connect).await?;

        match receiver.next().await {
            Some(Ok(codec::Packet::Connack(connack))) => {
                if connack.code != codec::ConnectCode::Accepted {
                    return Err(ConnectError::Connack(connack.code));
                }
            }
            None => panic!("unexpected end of stream"),
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
                    codec::Packet::Unsuback(unsuback) => {
                        unsuback_sender.send(unsuback).unwrap();
                    }
                    _ => {}
                }
            }
        });

        Ok(Self {
            sender,
            publish_receiver,
            suback_receiver,
            unsuback_receiver,
        })
    }

    pub async fn publish(&mut self, topic: &str, payload: Bytes) -> Result<(), PublishError> {
        let publish = codec::Publish {
            dup: false,
            qos: codec::QoS::AtMostOnce,
            retain: false,
            topic: codec::Topic::try_from(topic)?,
            pid: None,
            payload,
        };
        let publish = codec::Packet::Publish(publish);
        self.sender.send(publish).await?;

        Ok(())
    }

    pub async fn subscribe(&mut self, topic: &str) -> Result<(), SubscribeError> {
        let subscribe = codec::Subscribe {
            pid: 1,
            subscription_topics: vec![codec::SubscriptionTopic {
                topic_filter: codec::TopicFilter::try_from(topic)?,
                qos: codec::QoS::AtMostOnce,
            }],
        };
        let subscribe = codec::Packet::Subscribe(subscribe);
        self.sender.send(subscribe).await?;

        let suback = self.suback_receiver.recv().await;
        suback.ok_or(SubscribeError::SubackNotReceived).map(|_| ())
    }

    pub async fn unsubscribe(&mut self, topic: &str) -> Result<(), UnsubscribeError> {
        let unsubscribe = codec::Unsubscribe {
            pid: 1,
            topics: vec![codec::TopicFilter::try_from(topic)?],
        };
        let unsubscribe = codec::Packet::Unsubscribe(unsubscribe);
        self.sender.send(unsubscribe).await?;

        let unsuback = self.unsuback_receiver.recv().await;
        unsuback
            .ok_or(UnsubscribeError::UnsubackNotReceived)
            .map(|_| ())
    }

    pub async fn read(&mut self) -> Option<(codec::Topic, Bytes)> {
        let publish = self.publish_receiver.recv().await;
        publish.map(|publish| (publish.topic, publish.payload))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error("decode error: {0}")]
    Decode(#[from] codec::DecodeError),
    #[error("encode error: {0}")]
    Encode(#[from] codec::EncodeError),
    #[error("connect error: {0}")]
    Connect(#[from] tokio::io::Error),
    #[error("connack error")]
    Connack(codec::ConnectCode),
}

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("topic parse error: {0}")]
    Decode(#[from] codec::TopicParseError),
    #[error("encode error: {0}")]
    Encode(#[from] codec::EncodeError),
}

#[derive(Debug, thiserror::Error)]
pub enum SubscribeError {
    #[error("topic filter parse error: {0}")]
    Decode(#[from] codec::TopicFilterParseError),
    #[error("decode error: {0}")]
    Encode(#[from] codec::EncodeError),
    #[error("suback not received")]
    SubackNotReceived,
}

#[derive(Debug, thiserror::Error)]
pub enum UnsubscribeError {
    #[error("topic filter parse error: {0}")]
    Decode(#[from] codec::TopicFilterParseError),
    #[error("decode error: {0}")]
    Encode(#[from] codec::EncodeError),
    #[error("unsuback not received")]
    UnsubackNotReceived,
}
