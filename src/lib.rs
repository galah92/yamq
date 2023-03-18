mod broker;
mod client;
mod codec;
mod connection;
mod topic;

pub use broker::{Broker, Publisher};
pub use client::{Client, ConnectError};
pub use topic::TopicMatcher;

#[cfg(test)]
mod tests {
    use tokio_stream::StreamExt;

    use super::*;

    async fn setup_broker_and_client() -> Result<Client, ConnectError> {
        let address = "127.0.0.1:0";
        let broker = Broker::bind(address).await;
        let address = broker.address();

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
        let broker = Broker::bind("127.0.0.1:0").await;
        let publisher = broker.publisher();

        let topic = "testtopic";
        let payload = "testdata";

        let mut subscriber = broker.subscription(topic).await?;

        publisher.publish("testtopic", payload.into()).await?;

        let published = subscriber.next().await;
        let published = published.unwrap().unwrap();
        assert_eq!(published.topic.as_ref(), topic);
        assert_eq!(published.payload, payload);

        Ok(())
    }
}
