//! Async library for consuming [Scylla Change Data Capture](https://docs.scylladb.com/using-scylla/cdc/) log,
//! built on top of the [Scylla Rust Driver](https://docs.rs/scylla/latest/scylla/).
//!
//! # Why use a library?
//! The CDC log format is too complicated to be conveniently used in its raw form.
//! The library enables the user to ignore the intricacies of the log's internal structure and concentrate on the business logic.
//!
//! # Other documentation
//! * [Documentation of Scylla Rust Driver](https://docs.rs/scylla/latest/scylla/)
//! * [Scylla documentation](https://docs.scylladb.com)
//! * [Cassandra® documentation](https://cassandra.apache.org/doc/latest/)
//! * [CDC documentation](https://docs.scylladb.com/using-scylla/cdc/)
//!
//! # Getting started
//! The following code will start reading the CDC log from now until forever and will print type of every operation read.
//! To learn in more detail about how to use the library, please refer to the [tutorial](https://github.com/scylladb/scylla-cdc-rust/blob/main/tutorial.md).
//! ```rust,no_run
//! use async_trait::async_trait;
//! use scylla::SessionBuilder;
//! use scylla_cdc::consumer::*;
//! use scylla_cdc::log_reader::CDCLogReaderBuilder;
//! use std::sync::Arc;
//!
//! struct TypePrinterConsumer;
//!
//! #[async_trait]
//! impl Consumer for TypePrinterConsumer {
//!     async fn consume_cdc(&mut self, data: Vec<CDCRow<'_>>) -> anyhow::Result<()> {
//!         for data in data {
//!             println!("{}", data.operation);
//!         }
//!         Ok(())
//!     }
//! }
//!
//! struct TypePrinterConsumerFactory;
//!
//! #[async_trait]
//! impl ConsumerFactory for TypePrinterConsumerFactory {
//!     async fn new_consumer(&self) -> Box<dyn Consumer> {
//!         Box::new(TypePrinterConsumer)
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let session = Arc::new(
//!         SessionBuilder::new()
//!             .known_node("172.17.0.2:9042")
//!             .build()
//!             .await?,
//!     );
//!
//!     let factory = Arc::new(TypePrinterConsumerFactory);
//!
//!     let (_, handle) = CDCLogReaderBuilder::new()
//!         .session(session)
//!         .keyspace("ks")
//!         .table_name("t")
//!         .consumer_factory(factory)
//!         .build()
//!         .await
//!         .expect("Creating the log reader failed!");
//!
//!     handle.await
//! }
//! ```

pub mod cdc_types;
pub mod checkpoints;
pub mod consumer;
mod e2e_tests;
pub mod log_reader;
mod stream_generations;
mod stream_reader;
