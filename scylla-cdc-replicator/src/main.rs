pub mod replicator_consumer;

use replicator_consumer::ReplicatorConsumerFactory;
use scylla::SessionBuilder;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Hello");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Create a factory to remove the warning about unused new() function.
    let _factory = ReplicatorConsumerFactory::new(
        Arc::new(
            SessionBuilder::new()
                .known_node("127.0.0.1:9042")
                .build()
                .await?,
        ),
        "ks".to_string(),
        "t".to_string(),
    );

    println!("Replicator");
    Ok(())
}
