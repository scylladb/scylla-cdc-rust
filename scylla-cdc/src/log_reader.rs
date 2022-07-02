//! A module containing the logic behind reading and consuming the data.
//!
//! Only one stream generation is being read at a time,
//! while data from each stream is being read concurrently by different StreamReaders.
//!
//! Every StreamReader reads all CDC rows added in a time window
//! (not longer than `window_size`), then waiting for amount of time equal to `sleep_interval`
//! before reading the next window.
//! Before a newly added row is being read by the reader,
//! a set amount of time, represented by `safety_interval`, must pass.
//!
//! For more information about the stream generations,
//! please refer to the [Scylla CDC documentation](https://docs.scylladb.com/using-scylla/cdc/cdc-stream-generations/).

use std::cmp::max;
use std::sync::Arc;
use std::time;
use std::time::SystemTime;

use anyhow;
use futures::future::RemoteHandle;
use futures::stream::{FusedStream, FuturesUnordered, StreamExt};
use futures::FutureExt;
use scylla::Session;
use tracing::warn;

use crate::cdc_types::GenerationTimestamp;
use crate::checkpoints::CDCCheckpointSaver;
use crate::consumer::ConsumerFactory;
use crate::stream_generations::GenerationFetcher;
use crate::stream_reader::{CDCReaderConfig, StreamReader};

const SECOND_IN_MILLIS: i64 = 1_000;
const DEFAULT_SLEEP_INTERVAL: i64 = SECOND_IN_MILLIS * 10;
const DEFAULT_WINDOW_SIZE: i64 = SECOND_IN_MILLIS * 60;
const DEFAULT_SAFETY_INTERVAL: i64 = SECOND_IN_MILLIS * 30;
const DEFAULT_PAUSE: u64 = 10;

/// To create a new CDCLogReader instance please see documentation for [`CDCLogReaderBuilder`]
pub struct CDCLogReader {
    // Tells the worker to stop
    // Usage of the "watch" channel will make it possible to change the the timestamp later,
    // for example if somebody loses patience and wants to stop now not later
    end_timestamp: tokio::sync::watch::Sender<chrono::Duration>,
}

impl CDCLogReader {
    fn new(end_timestamp: tokio::sync::watch::Sender<chrono::Duration>) -> Self {
        CDCLogReader { end_timestamp }
    }

    // Tell the worker to set the end timestamp and then stop
    pub fn stop_at(&mut self, when: chrono::Duration) {
        let _ = self.end_timestamp.send(when);
    }

    // Tell the worker to stop immediately
    pub fn stop(&mut self) {
        self.stop_at(chrono::Duration::min_value());
    }
}

struct CDCReaderWorker {
    session: Arc<Session>,
    keyspace: String,
    table_name: String,
    end_timestamp: chrono::Duration,
    readers: Vec<Arc<StreamReader>>,
    end_timestamp_receiver: tokio::sync::watch::Receiver<chrono::Duration>,
    consumer_factory: Arc<dyn ConsumerFactory>,
    config: CDCReaderConfig,
}

impl CDCReaderWorker {
    pub async fn run(&mut self) -> anyhow::Result<()> {
        let fetcher = Arc::new(GenerationFetcher::new(&self.session));
        let (mut generation_receiver, _future_handle) = fetcher
            .clone()
            .fetch_generations_continuously(self.config.lower_timestamp, self.config.sleep_interval)
            .await?;

        let mut stream_reader_tasks = FuturesUnordered::new();

        let mut next_generation: Option<GenerationTimestamp> = None;
        let mut current_generation: Option<GenerationTimestamp> = None;
        let mut err: Option<anyhow::Error> = None;

        let mut reader_config = self.config.clone();

        loop {
            tokio::select! {
                Some(evt) = stream_reader_tasks.next(), if !stream_reader_tasks.is_terminated() => {
                    match evt {
                        Err(error) => {
                            err = Some(anyhow::Error::new(error));
                            self.stop_now().await;
                        }
                        Ok(Err(error)) => {
                            err = Some(error);
                            self.stop_now().await;
                        },
                        _ => {}
                    }
                }
                Some(generation) = generation_receiver.recv(), if next_generation.is_none() && err.is_none() => {
                    next_generation = Some(generation.clone());
                    self.set_upper_timestamp(generation.timestamp).await;
                }
                Ok(_) = self.end_timestamp_receiver.changed(), if err.is_none() => {
                    let timestamp = *self.end_timestamp_receiver.borrow_and_update();
                    self.end_timestamp = timestamp;
                    self.set_upper_timestamp(timestamp).await;
                }
            }

            if stream_reader_tasks.is_empty() {
                if let Some(err) = err {
                    return Err(err);
                }

                if let Some(generation) = next_generation.take() {
                    current_generation = Some(generation.clone());
                    if generation.timestamp > self.end_timestamp {
                        return Ok(());
                    }

                    if self.config.should_save_progress {
                        self.config
                            .checkpoint_saver
                            .as_ref()
                            .unwrap()
                            .save_new_generation(&generation)
                            .await?;
                    }

                    reader_config.lower_timestamp =
                        max(self.config.lower_timestamp, generation.timestamp);

                    self.readers = fetcher
                        .fetch_stream_ids(&generation)
                        .await?
                        .into_iter()
                        .map(|stream_ids| {
                            Arc::new(StreamReader::new(
                                &self.session,
                                stream_ids,
                                reader_config.clone(),
                            ))
                        })
                        .collect();

                    self.set_upper_timestamp(self.end_timestamp).await;

                    stream_reader_tasks = self
                        .readers
                        .iter()
                        .map(|reader| {
                            let reader = Arc::clone(reader);
                            let keyspace = self.keyspace.clone();
                            let table_name = self.table_name.clone();
                            let factory = Arc::clone(&self.consumer_factory);
                            tokio::spawn(async move {
                                let consumer = factory.new_consumer().await;
                                reader.fetch_cdc(keyspace, table_name, consumer).await
                            })
                        })
                        .collect();
                } else if let Some(current) = current_generation.take() {
                    if let Ok(Some(generation)) = fetcher.fetch_next_generation(&current).await {
                        if generation.timestamp <= self.end_timestamp {
                            // Fetched next generation with lower or equal timestamp than end_timestamp
                            next_generation = Some(generation.clone());
                            self.set_upper_timestamp(generation.timestamp).await;
                            continue;
                        } else {
                            // Fetched next generation with bigger timestamp than end_timestamp
                            return Ok(());
                        }
                    } else {
                        // Current generation's next generation is not available
                        warn!("Next generation is not available, some rows in the CDC log table may be unread.");
                        return Ok(());
                    }
                } else {
                    // First generation is not fetched
                    return Ok(());
                }
            }
        }
    }

    async fn set_upper_timestamp(&self, new_upper_timestamp: chrono::Duration) {
        for reader in self.readers.iter() {
            reader.set_upper_timestamp(new_upper_timestamp).await;
        }
    }

    async fn stop_now(&self) {
        self.set_upper_timestamp(chrono::Duration::min_value())
            .await;
    }
}

/// CDCLogReaderBuilder is used to create new [`CDCLogReader`] instances.
/// # Example
///
/// ```
/// # use scylla_cdc::log_reader::{CDCLogReader, CDCLogReaderBuilder};
/// # use scylla_cdc::consumer::{ConsumerFactory, Consumer, CDCRow};
/// # use futures::future::RemoteHandle;
/// # use scylla::{Session, SessionBuilder};
/// # use std::sync::Arc;
/// # use async_trait::async_trait;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// struct DummyConsumer;
///
/// #[async_trait]
/// impl Consumer for DummyConsumer {
///     async fn consume_cdc(&mut self, data: CDCRow<'_>) -> anyhow::Result<()> {
///         // ... consume received data ...
///         Ok(())
///     }
/// }
///
/// struct DummyConsumerFactory;
///
/// #[async_trait]
/// impl ConsumerFactory for DummyConsumerFactory {
///     async fn new_consumer(&self) -> Box<dyn Consumer> {
///         Box::new(DummyConsumer)
///     }
/// }
///
/// let session = SessionBuilder::new().known_node("127.0.0.1:9042").build().await.unwrap();
/// let (cdc_log_printer, handle): (CDCLogReader, RemoteHandle<anyhow::Result<()>>) = CDCLogReaderBuilder::new()
///     .session(Arc::new(session))
///     .keyspace("log_reader_ks")
///     .table_name("log_reader_table")
///     .consumer_factory(Arc::new(DummyConsumerFactory))
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct CDCLogReaderBuilder {
    session: Option<Arc<Session>>,
    keyspace: Option<String>,
    table_name: Option<String>,
    start_timestamp: chrono::Duration,
    end_timestamp: chrono::Duration,
    window_size: time::Duration,
    safety_interval: time::Duration,
    sleep_interval: time::Duration,
    consumer_factory: Option<Arc<dyn ConsumerFactory>>,
    should_load_progress: bool,
    should_save_progress: bool,
    checkpoint_saver: Option<Arc<dyn CDCCheckpointSaver>>,
    pause_between_saves: time::Duration,
}

impl CDCLogReaderBuilder {
    /// Creates new CDCLogReaderBuilder with default configuration.
    ///
    /// # Default configuration
    /// * start_timestamp: current timestamp
    /// * window_size: 60 seconds
    /// * safety_interval: 30 seconds
    /// * sleep_interval: 10 seconds
    /// * end_timestamp: the maximum possible duration - i64::MAX milliseconds
    /// * should_load_progress: false
    /// * should_save_progress: false,
    /// * pause_between_saves: 10 seconds
    pub fn new() -> CDCLogReaderBuilder {
        let end_timestamp = chrono::Duration::max_value();
        let session = None;
        let keyspace = None;
        let table_name = None;
        let start_timestamp = chrono::Duration::from_std(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap(),
        )
        .unwrap();
        let window_size = time::Duration::from_millis(DEFAULT_WINDOW_SIZE as u64);
        let safety_interval = time::Duration::from_millis(DEFAULT_SAFETY_INTERVAL as u64);
        let sleep_interval = time::Duration::from_millis(DEFAULT_SLEEP_INTERVAL as u64);
        let consumer_factory = None;
        let should_load_progress = false;
        let should_save_progress = false;
        let checkpoint_saver = None;
        let pause_between_saves = time::Duration::from_secs(DEFAULT_PAUSE);
        CDCLogReaderBuilder {
            session,
            keyspace,
            table_name,
            start_timestamp,
            end_timestamp,
            window_size,
            safety_interval,
            sleep_interval,
            consumer_factory,
            should_load_progress,
            should_save_progress,
            checkpoint_saver,
            pause_between_saves,
        }
    }

    /// Set session to enable [`CDCLogReader`] perform queries.
    /// This is a required field for [`CDCLogReaderBuilder::build()`]
    /// In case it is not set [`CDCLogReaderBuilder::build()`] will return error message:
    /// `failed to create the cdc reader: missing session`
    pub fn session(mut self, session: Arc<Session>) -> Self {
        self.session = Some(session);
        self
    }

    /// Set keyspace of the CDC log table that [`CDCLogReader`] instance will read data from.
    /// This is a required field for [`CDCLogReaderBuilder::build()`]
    /// In case it is not set [`CDCLogReaderBuilder::build()`] will return error message:
    /// `failed to create the cdc reader: missing keyspace`
    pub fn keyspace(mut self, keyspace: &str) -> Self {
        self.keyspace = Some(keyspace.to_string());
        self
    }

    /// Set table name of the CDC log table that [`CDCLogReader`] instance will read data from.
    /// This is a required field for [`CDCLogReaderBuilder::build()`]
    /// In case it is not set [`CDCLogReaderBuilder::build()`] will return error message:
    /// `failed to create the cdc reader: missing table name`
    pub fn table_name(mut self, table_name: &str) -> Self {
        self.table_name = Some(table_name.to_string());
        self
    }

    /// Set start timestamp from which [`CDCLogReader`] instance will start reading
    /// from the user specified CDC log table.
    pub fn start_timestamp(mut self, start_timestamp: chrono::Duration) -> Self {
        self.start_timestamp = start_timestamp;
        self
    }

    /// Set end timestamp to stop [`CDCLogReader`] instance reading data
    /// from the user specified CDC log table.
    pub fn end_timestamp(mut self, end_timestamp: chrono::Duration) -> Self {
        self.end_timestamp = end_timestamp;
        self
    }

    /// Set window size for the [`CDCLogReader`] instance
    /// to read data from the user specified CDC log table window by window.
    /// Default window size is 60 seconds.
    pub fn window_size(mut self, window_size: time::Duration) -> Self {
        self.window_size = window_size;
        self
    }

    /// Set safety interval for the [`CDCLogReader`] instance
    /// to not read data from the CDC log table that has timestamp later than now.
    /// Default safety interval is 30 seconds.
    pub fn safety_interval(mut self, safety_interval: time::Duration) -> Self {
        self.safety_interval = safety_interval;
        self
    }

    /// Set sleep interval for the [`CDCLogReader`] instance
    /// to sleep for the specified amount of time after reading and consuming data from all
    /// streams from a certain time window.
    /// Default sleep interval is 10 seconds.
    pub fn sleep_interval(mut self, sleep_interval: time::Duration) -> Self {
        self.sleep_interval = sleep_interval;
        self
    }

    /// Set consumer factory which will be used in the [`CDCLogReader`] instance to create
    /// consumers and feed them with data fetched from the CDC log.
    pub fn consumer_factory(mut self, consumer_factory: Arc<dyn ConsumerFactory>) -> Self {
        self.consumer_factory = Some(consumer_factory);
        self
    }

    /// Set flag indicating whether the [`CDCLogReader`] instance should resume
    /// progress from set previously checkpoints.
    /// Default value is `false`.
    pub fn should_load_progress(mut self, value: bool) -> Self {
        self.should_load_progress = value;
        self
    }

    /// Set flag indicating whether the [`CDCLogReader`] instance should
    /// periodically save progress.
    /// Default value is `false`.
    pub fn should_save_progress(mut self, value: bool) -> Self {
        self.should_save_progress = value;
        self
    }

    /// Set duration of the pause between creating two consecutive checkpoints.
    /// Default value is 10 seconds.
    pub fn pause_between_saves(mut self, pause: time::Duration) -> Self {
        self.pause_between_saves = pause;
        self
    }

    /// Set instance of [`CDCCheckpointSaver`] trait object to load/save checkpoints.
    pub fn checkpoint_saver(mut self, cp_saver: Arc<dyn CDCCheckpointSaver>) -> Self {
        self.checkpoint_saver = Some(cp_saver);
        self
    }

    /// Build the CDCLogReader after setting all the options
    /// It will fail with an error message if all the required fields are not set.
    /// Currently required fields are the following:
    /// `session`, `keyspace`, `table_name`, `consumer_factory`
    pub async fn build(self) -> anyhow::Result<(CDCLogReader, RemoteHandle<anyhow::Result<()>>)> {
        let should_use_checkpoint_saver = self.should_save_progress || self.should_load_progress;

        let table_name = self.table_name.ok_or_else(|| {
            anyhow::anyhow!("failed to create the cdc reader: missing table name")
        })?;
        let keyspace = self.keyspace.ok_or_else(|| {
            anyhow::anyhow!("failed to create the cdc reader: missing keyspace name")
        })?;
        let session = self
            .session
            .ok_or_else(|| anyhow::anyhow!("failed to create the cdc reader: missing session"))?;

        if should_use_checkpoint_saver && self.checkpoint_saver.is_none() {
            return Err(anyhow::anyhow!(
                "to save or load progress you have to provide checkpoint_saver"
            ));
        }

        let consumer_factory = self.consumer_factory.ok_or_else(|| {
            anyhow::anyhow!("failed to create the cdc reader: missing consumer factory")
        })?;

        let end_timestamp = chrono::Duration::max_value();
        let (end_timestamp_sender, end_timestamp_receiver) =
            tokio::sync::watch::channel(end_timestamp);
        let readers = vec![];

        let mut start_timestamp = self.start_timestamp;
        if self.should_load_progress {
            if let Some(generation) = self
                .checkpoint_saver
                .as_ref()
                .ok_or_else(|| {
                    anyhow::anyhow!("checkpoint_saver is not set, while should_load is true")
                })?
                .load_last_generation()
                .await?
            {
                start_timestamp = std::cmp::max(generation.timestamp, start_timestamp);
            }
        }

        let config = CDCReaderConfig {
            lower_timestamp: start_timestamp,
            window_size: self.window_size,
            safety_interval: self.safety_interval,
            sleep_interval: self.sleep_interval,
            should_load_progress: self.should_load_progress,
            should_save_progress: self.should_save_progress,
            checkpoint_saver: self.checkpoint_saver.clone(),
            pause_between_saves: self.pause_between_saves,
        };

        let mut cdc_reader_worker = CDCReaderWorker {
            session,
            keyspace,
            table_name,
            end_timestamp: self.end_timestamp,
            readers,
            end_timestamp_receiver,
            consumer_factory,
            config,
        };

        let (fut, handle) = async move { cdc_reader_worker.run().await }.remote_handle();
        tokio::task::spawn(fut);
        let printer = CDCLogReader::new(end_timestamp_sender);

        Ok((printer, handle))
    }
}

/// Create a [`CDCLogReaderBuilder`] with default configuration, same as [`CDCLogReaderBuilder::new()`]
impl Default for CDCLogReaderBuilder {
    fn default() -> Self {
        Self::new()
    }
}
