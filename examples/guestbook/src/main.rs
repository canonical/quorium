use std::net::SocketAddr;
use std::time::Duration;

use guestbook::Guestbook;
use quorium::{Discovery, Quorium};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let name = &args[1];
    let port: u16 = args[2].parse()?;
    let seeds: Vec<SocketAddr> =
        args[3..].iter().map(|p| format!("127.0.0.1:{p}").parse()).collect::<Result<_, _>>()?;
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse()?;

    let node = Quorium::builder()
        .data_dir(format!("data/{name}"))
        .bind(addr)
        .advertise(addr)
        .discovery(Discovery::Static(seeds))
        .replicas(3)
        .insecure_plaintext()
        .start()
        .await?;
    println!("{name} started as node {}", node.status().id);

    let book = node.entity::<Guestbook>("lobby");
    loop {
        match book.sign(name.clone()).await {
            Ok(count) => println!("{name} signed; the book has {count} entries"),
            Err(e) => println!("{name} could not sign: {e}"),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
