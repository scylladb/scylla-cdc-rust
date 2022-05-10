mod replication_tests;
mod replicator;
pub mod replicator_consumer;

use std::time::Duration;

use clap::Parser;
use futures_util::StreamExt;
use tokio::select;

use crate::replicator::Replicator;

#[derive(Parser)]
struct Args {
    /// Keyspace name
    #[clap(short, long)]
    keyspace: String,

    /// Table names provided as a comma delimited string
    #[clap(short, long)]
    table: String,

    /// Address of a node in source cluster
    #[clap(short, long)]
    source: String,

    /// Address of a node in destination cluster
    #[clap(short, long)]
    destination: String,

    /// Window size in seconds
    #[clap(long, default_value_t = 60.)]
    window_size: f64,

    /// Safety interval in seconds
    #[clap(long, default_value_t = 30.)]
    safety_interval: f64,

    /// Sleep interval in seconds
    #[clap(long, default_value_t = 10.)]
    sleep_interval: f64,

    /// Start datetime as RFC 3339 formatted string
    #[clap(long)]
    start_datetime: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let tables: Vec<_> = args.table.split(',').map(|s| s.to_string()).collect();
    assert!(!tables.is_empty(), "no tables were provided");
    let start_timestamp = chrono::Duration::milliseconds(match args.start_datetime {
        Some(s) => chrono::DateTime::parse_from_rfc3339(&s)?.timestamp_millis(),
        None => chrono::Local::now().timestamp_millis(),
    });
    let sleep_interval = Duration::from_secs_f64(args.sleep_interval);
    let mut result = Ok(());

    let (mut replicator, mut handles) = Replicator::new(
        args.source,
        args.keyspace.clone(),
        tables.clone(),
        args.destination,
        args.keyspace,
        tables,
        start_timestamp,
        Duration::from_secs_f64(args.window_size),
        Duration::from_secs_f64(args.safety_interval),
        sleep_interval,
    )
    .await?;

    select! {
        _ = tokio::signal::ctrl_c() => {
            println!("Shutting down...");
        }
        res = handles.next() => {
            // The only way this could have happened is an error in on of the CDCLogReaders so we
            // stop all of them.
            result = res.unwrap();
            println!("{:?}", result);
        }
    }

    replicator.stop();
    while let Some(res) = handles.next().await {
        if res.is_err() {
            result = res;
            println!("{:?}", result);
        }
    }
    result
}
