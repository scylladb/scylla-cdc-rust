pub mod printer;

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use scylla::SessionBuilder;
use scylla_cdc::log_reader::CDCLogReaderBuilder;

use crate::printer::PrinterConsumerFactory;

#[derive(Parser)]
struct Args {
    /// Keyspace name
    #[clap(short, long, action = clap::ArgAction::Set)]
    keyspace: String,

    /// Table name
    #[clap(short, long, action = clap::ArgAction::Set)]
    table: String,

    /// Address of a node in source cluster
    #[clap(short, long, action = clap::ArgAction::Set)]
    hostname: String,

    /// Window size in seconds
    #[clap(long, default_value_t = 60., action = clap::ArgAction::Set)]
    window_size: f64,

    /// Safety interval in seconds
    #[clap(long, default_value_t = 30., action = clap::ArgAction::Set)]
    safety_interval: f64,

    /// Sleep interval in seconds
    #[clap(long, default_value_t = 10., action = clap::ArgAction::Set)]
    sleep_interval: f64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let session = Arc::new(
        SessionBuilder::new()
            .known_node(args.hostname)
            .build()
            .await?,
    );
    let (mut cdc_log_printer, handle) = CDCLogReaderBuilder::new()
        .session(session)
        .keyspace(&args.keyspace)
        .table_name(&args.table)
        .window_size(Duration::from_secs_f64(args.window_size))
        .safety_interval(Duration::from_secs_f64(args.safety_interval))
        .sleep_interval(Duration::from_secs_f64(args.sleep_interval))
        .consumer_factory(Arc::new(PrinterConsumerFactory))
        .build()
        .await?;

    tokio::signal::ctrl_c().await.unwrap();

    println!("Shutting down...");

    cdc_log_printer.stop();

    handle.await
}
