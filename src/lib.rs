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

    #[tokio::test]
    async fn test_connect() -> Result<(), Box<dyn std::error::Error>> {
        let address = "127.0.0.1:0";
        let mut broker = Broker::new(address).await;
        let address = broker.address();
        tokio::spawn(async move { broker.run().await });

        let mut client = Client::new(&address).await;

        let connack = client.connect().await;
        assert_eq!(
            connack,
            codec::Connack {
                session_present: false,
                code: codec::ConnectCode::Accepted
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_publish() -> Result<(), Box<dyn std::error::Error>> {
        let address = "127.0.0.1:0";
        let mut broker = Broker::new(address).await;
        let address = broker.address();
        tokio::spawn(async move { broker.run().await });

        let mut client = Client::new(&address).await;

        let _connack = client.connect().await;

        client.publish("testtopic", "testdata".into()).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_subscribe() -> Result<(), Box<dyn std::error::Error>> {
        let address = "127.0.0.1:0";
        let mut broker = Broker::new(address).await;
        let address = broker.address();
        tokio::spawn(async move { broker.run().await });

        let mut client = Client::new(&address).await;

        let _connack = client.connect().await;

        let suback = client.subscribe("testtopic").await;
        assert_eq!(
            suback,
            codec::Suback {
                pid: 1,
                return_codes: vec![codec::SubscribeAckReason::GrantedQoSOne]
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_subscribe_publish() -> Result<(), Box<dyn std::error::Error>> {
        let address = "127.0.0.1:0";
        let mut broker = Broker::new(address).await;
        let address = broker.address();
        tokio::spawn(async move { broker.run().await });

        let mut client = Client::new(&address).await;

        let _connack = client.connect().await;
        let _suback = client.subscribe("testtopic").await;

        client.publish("testtopic", "testdata".into()).await;

        let published = client.recv().await;

        assert_eq!(
            published,
            codec::Packet::Publish(codec::Publish {
                dup: false,
                qos: codec::QoS::AtMostOnce,
                retain: false,
                topic: codec::Topic::try_from("testtopic").unwrap(),
                pid: None,
                payload: "testdata".into(),
            })
        );

        Ok(())
    }
}
