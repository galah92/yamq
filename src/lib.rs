mod broker;
mod client;
mod codec;
mod topic;

pub use broker::Broker;
pub use client::{Client, ConnectError};
pub use topic::TopicMatcher;

#[cfg(test)]
mod tests {
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
}
