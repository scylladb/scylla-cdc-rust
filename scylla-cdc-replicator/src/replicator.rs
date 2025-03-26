use std::sync::Arc;
use std::time;

use futures_util::future::RemoteHandle;
use futures_util::stream::FuturesUnordered;
use scylla::client::session_builder::SessionBuilder;
use scylla_cdc::log_reader::{CDCLogReader, CDCLogReaderBuilder};

use crate::replicator_consumer::ReplicatorConsumerFactory;

pub struct Replicator {
    log_readers: Vec<CDCLogReader>,
}

impl Replicator {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        source_uri: String,
        source_keyspace: String,
        source_tables: Vec<String>,
        dest_uri: String,
        dest_keyspace: String,
        dest_tables: Vec<String>,
        start_timestamp: chrono::Duration,
        window_size: time::Duration,
        safety_interval: time::Duration,
        sleep_interval: time::Duration,
    ) -> anyhow::Result<(Self, FuturesUnordered<RemoteHandle<anyhow::Result<()>>>)> {
        let source_session = Arc::new(SessionBuilder::new().known_node(source_uri).build().await?);
        let dest_session = Arc::new(SessionBuilder::new().known_node(dest_uri).build().await?);
        let mut log_readers = vec![];
        let handles = FuturesUnordered::new();

        for (source_table, dest_table) in source_tables.into_iter().zip(dest_tables) {
            let factory = ReplicatorConsumerFactory::new(
                dest_session.clone(),
                dest_keyspace.clone(),
                dest_table,
            )?;
            let (log_reader, handle) = CDCLogReaderBuilder::new()
                .session(source_session.clone())
                .keyspace(&source_keyspace)
                .table_name(&source_table)
                .start_timestamp(start_timestamp)
                .window_size(window_size)
                .safety_interval(safety_interval)
                .sleep_interval(sleep_interval)
                .consumer_factory(Arc::new(factory))
                .build()
                .await?;
            log_readers.push(log_reader);
            handles.push(handle);
        }
        Ok((Replicator { log_readers }, handles))
    }

    pub fn stop(&mut self) {
        for log_reader in &mut self.log_readers {
            log_reader.stop();
        }
    }
}
