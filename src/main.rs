use yamq::Broker;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() {
    let broker = Broker::new("127.0.0.1:1883").await;
    println!("Listening on {}", broker.address());

    let mut subscriber = broker.subscription("#").await.unwrap();
    tokio::spawn(async move {
        loop {
            let message = subscriber.next().await.unwrap();
            println!("Received: {:?}", message);
        }
    });

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}
