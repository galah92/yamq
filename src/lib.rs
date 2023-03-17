mod broker;
mod client;
mod codec;
mod connection;
mod topic;

pub use broker::{Broker, Publisher, SubscriptionHandler};
pub use client::{Client, ConnectError};
pub use topic::TopicMatcher;

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;

    async fn setup_broker_and_client() -> Result<Client, ConnectError> {
        let address = "127.0.0.1:0";
        let mut broker = Broker::new(address).await;
        let address = broker.address();
        tokio::spawn(async move { broker.run().await });

        Client::connect(&address).await
    }

    #[tokio::test]
    async fn test_connect() -> Result<(), Box<dyn std::error::Error>> {
        let _client = setup_broker_and_client().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_publish() -> Result<(), Box<dyn std::error::Error>> {
        let mut client = setup_broker_and_client().await?;

        client.publish("testtopic", "testdata".into()).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_subscribe() -> Result<(), Box<dyn std::error::Error>> {
        let mut client = setup_broker_and_client().await?;

        client.subscribe("testtopic").await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_subscribe_publish() -> Result<(), Box<dyn std::error::Error>> {
        let mut client = setup_broker_and_client().await?;
        client.subscribe("testtopic").await?;

        client.publish("testtopic", "testdata".into()).await?;

        if let Some((topic, payload)) = client.read().await {
            assert_eq!(topic, codec::Topic::try_from("testtopic")?);
            assert_eq!(payload, "testdata");
        } else {
            panic!("no message received");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_subscription() -> Result<(), Box<dyn std::error::Error>> {
        let mut broker = Broker::new("127.0.0.1:0").await;
        let publisher = broker.publisher();

        struct TestSubscriptionHandler {
            cancellation_tx: Option<tokio::sync::oneshot::Sender<codec::Publish>>,
        }

        #[async_trait]
        impl SubscriptionHandler for TestSubscriptionHandler {
            async fn on_publish(&mut self, publish: codec::Publish) {
                if let Some(cancellation_tx) = self.cancellation_tx.take() {
                    cancellation_tx.send(publish).unwrap();
                }
            }
        }

        let (cancellation_tx, cancellation_rx) = tokio::sync::oneshot::channel();
        let handler = TestSubscriptionHandler {
            cancellation_tx: Some(cancellation_tx),
        };

        let topic = "testtopic";
        let payload = "testdata";

        broker.subscription(topic, handler)?;

        broker.subscription_handler(topic, hello_subscriber)?;

        tokio::spawn(async move { broker.run().await });

        publisher.publish("testtopic", payload.into()).await?;

        let published = cancellation_rx.await?;
        assert_eq!(published.topic.as_ref(), topic);
        assert_eq!(published.payload, payload);

        Ok(())
    }

    async fn hello_subscriber(publish: codec::Publish) {
        let topic = publish.topic.as_ref();
        let payload = publish.payload;
        println!("echoing message {topic:?} {payload:?}");
        // publisher.publish(topic, payload).await;  // TODO: support this
    }
}
