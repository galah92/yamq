use yamq::Broker;

#[tokio::main]
async fn main() {
    println!("Listening...");
    let mut broker = Broker::new().await;
    broker.run().await;
}
