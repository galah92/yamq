use yamq::Broker;

#[tokio::main]
async fn main() {
    let broker = Broker::new("127.0.0.1:1883").await;
    println!("Listening on {}", broker.address());
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}
