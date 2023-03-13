use yamq::Broker;

#[tokio::main]
async fn main() {
    println!("Listening...");
    let address = "127.0.0.1:1883";
    let mut broker = Broker::new(address).await;
    broker.run().await;
}
