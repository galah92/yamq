use tokio_stream::StreamExt;
use yamq::Broker;

#[tokio::main]
async fn main() {
    let broker = Broker::bind("127.0.0.1:1883").await;
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
