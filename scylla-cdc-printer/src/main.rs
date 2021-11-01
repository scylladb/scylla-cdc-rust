use std::error::Error;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Hello");
    tokio::time::sleep(Duration::from_secs(1)).await;

    println!("Printer");
    Ok(())
}
