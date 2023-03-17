use super::connection::{Connection, ConnectionRequest, SubscribeRequest};
use super::{codec, TopicMatcher};
use async_trait::async_trait;
use bytes::Bytes;
use futures::Future;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::StreamExt;

pub struct Broker {
    listener: TcpListener,
    client_tx: mpsc::Sender<ConnectionRequest>,
    client_rx: mpsc::Receiver<ConnectionRequest>,
    subscribers: TopicMatcher<broadcast::Sender<codec::Publish>>,
    retained: TopicMatcher<codec::Publish>,
}

#[async_trait]
pub trait SubscriptionHandler {
    async fn on_publish(&mut self, publish: codec::Publish);
}

#[derive(Debug, thiserror::Error)]
pub enum SubscriptionError {
    #[error("topic filter parse error: {0}")]
    InvalidTopicFilter(#[from] codec::TopicFilterParseError),
    #[error("send error")]
    SendError,
}

#[derive(Debug, Clone)]
pub struct Publisher {
    client_tx: mpsc::Sender<ConnectionRequest>,
}

impl Publisher {
    pub async fn publish<T>(&self, topic: T, payload: Bytes) -> Result<(), PublishError>
    where
        T: TryInto<codec::Topic>,
        PublishError: From<<T as TryInto<codec::Topic>>::Error>,
    {
        let publish = codec::Publish {
            dup: false,
            qos: codec::QoS::AtLeastOnce,
            retain: false,
            topic: topic.try_into()?,
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

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("topic parse error: {0}")]
    InvalidTopic(#[from] codec::TopicParseError),
    #[error("send error")]
    SendError,
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

    pub fn subscription<T, A>(
        &self,
        topic_filter: T,
        mut handler: A,
    ) -> Result<(), SubscriptionError>
    where
        A: SubscriptionHandler + Send + Sync + 'static,
        T: TryInto<codec::TopicFilter>,
        SubscriptionError: From<<T as TryInto<codec::TopicFilter>>::Error>,
    {
        let topic_filter = topic_filter.try_into()?;
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
                if let Ok(publish) = packet {
                    handler.on_publish(publish).await;
                }
            }
        });

        Ok(())
    }

    pub fn subscription_handler<T, F, Fut>(
        &self,
        topic_filter: T,
        handler: F,
    ) -> Result<(), SubscriptionError>
    where
        F: FnOnce(codec::Publish) -> Fut + Send + Copy + 'static,
        Fut: Future<Output = ()> + Send,
        T: TryInto<codec::TopicFilter>,
        SubscriptionError: From<<T as TryInto<codec::TopicFilter>>::Error>,
    {
        let topic_filter = topic_filter.try_into()?;
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
                if let Ok(publish) = packet {
                    handler(publish).await;
                }
            }
        });

        Ok(())
    }

    pub fn publisher(&self) -> Publisher {
        let client_tx = self.client_tx.clone();
        Publisher { client_tx }
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
            if let Err(SendError(err)) = tx.send(publish.clone()) {
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
                sub_tx.send(publish.clone()).unwrap();
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
