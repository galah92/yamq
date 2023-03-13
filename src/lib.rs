mod broker;
mod client;
mod codec;
mod topic;

pub use broker::Broker;
pub use client::Client;
pub use topic::TopicMatcher;

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup_broker_and_client() -> Client {
        let address = "127.0.0.1:0";
        let mut broker = Broker::new(address).await;
        let address = broker.address();
        tokio::spawn(async move { broker.run().await });

        let client = Client::connect(&address).await;
        client
    }

    #[tokio::test]
    async fn test_connect() -> Result<(), Box<dyn std::error::Error>> {
        let _client = setup_broker_and_client().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_publish() -> Result<(), Box<dyn std::error::Error>> {
        let mut client = setup_broker_and_client().await;

        client.publish("testtopic", "testdata".into()).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_subscribe() -> Result<(), Box<dyn std::error::Error>> {
        let mut client = setup_broker_and_client().await;

        client.subscribe("testtopic").await;

        Ok(())
    }

    #[tokio::test]
    async fn test_subscribe_publish() -> Result<(), Box<dyn std::error::Error>> {
        let mut client = setup_broker_and_client().await;
        client.subscribe("testtopic").await;

        client.publish("testtopic", "testdata".into()).await;

        let (topic, payload) = client.read().await;
        assert_eq!(topic, codec::Topic::try_from("testtopic").unwrap());
        assert_eq!(payload, "testdata");

        Ok(())
    }
}
