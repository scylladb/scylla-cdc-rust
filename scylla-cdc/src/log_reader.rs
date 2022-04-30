use std::cmp::max;
use std::sync::Arc;
use std::time;
use std::time::SystemTime;

use anyhow;
use futures::future::RemoteHandle;
use futures::stream::{FusedStream, FuturesUnordered, StreamExt};
use futures::FutureExt;
use scylla::Session;

use crate::cdc_types::GenerationTimestamp;
use crate::consumer::ConsumerFactory;
use crate::stream_generations::GenerationFetcher;
use crate::stream_reader::StreamReader;

const SECOND_IN_MILLIS: i64 = 1_000;
const DEFAULT_SLEEP_INTERVAL: i64 = SECOND_IN_MILLIS * 10;
const DEFAULT_WINDOW_SIZE: i64 = SECOND_IN_MILLIS * 60;
const DEFAULT_SAFETY_INTERVAL: i64 = SECOND_IN_MILLIS * 3;

pub struct CDCLogReader {
    // Tells the worker to stop
    // Usage of the "watch" channel will make it possible to change the the timestamp later,
    // for example if somebody loses patience and wants to stop now not later
    end_timestamp: tokio::sync::watch::Sender<chrono::Duration>,
}

impl CDCLogReader {
    // Creates a reader and runs it immediately
    pub fn new(end_timestamp: tokio::sync::watch::Sender<chrono::Duration>) -> Self {
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
    start_timestamp: chrono::Duration,
    end_timestamp: chrono::Duration,
    sleep_interval: time::Duration,
    window_size: time::Duration,
    safety_interval: time::Duration,
    readers: Vec<Arc<StreamReader>>,
    end_timestamp_receiver: tokio::sync::watch::Receiver<chrono::Duration>,
    consumer_factory: Arc<dyn ConsumerFactory>,
}

impl CDCReaderWorker {
    pub async fn run(&mut self) -> anyhow::Result<()> {
        let fetcher = Arc::new(GenerationFetcher::new(&self.session));
        let (mut generation_receiver, _future_handle) = fetcher
            .clone()
            .fetch_generations_continuously(self.start_timestamp, self.sleep_interval)
            .await?;

        let mut stream_reader_tasks = FuturesUnordered::new();

        let mut next_generation: Option<GenerationTimestamp> = None;
        let mut had_first_generation: bool = false;
        let mut err: Option<anyhow::Error> = None;

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
                    had_first_generation = true;
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
                    if generation.timestamp > self.end_timestamp {
                        return Ok(());
                    }

                    self.readers = fetcher
                        .fetch_stream_ids(&generation)
                        .await?
                        .iter()
                        .flatten()
                        .map(|stream_id| {
                            Arc::new(StreamReader::new(
                                &self.session,
                                vec![stream_id.clone()],
                                max(self.start_timestamp, generation.timestamp),
                                self.window_size,
                                self.safety_interval,
                                self.sleep_interval,
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
                } else if had_first_generation {
                    // FIXME: There may be another generation coming in the future with timestamp < end_timestamp
                    // that could have been missed because of earlier fetching failures.
                    // More on this here https://github.com/piodul/scylla-cdc-rust/pull/10#discussion_r826865162
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
}

impl CDCLogReaderBuilder {
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
        }
    }

    pub fn session(mut self, session: Arc<Session>) -> Self {
        self.session = Some(session);
        self
    }

    pub fn keyspace(mut self, keyspace: &str) -> Self {
        self.keyspace = Some(keyspace.to_string());
        self
    }

    pub fn table_name(mut self, table_name: &str) -> Self {
        self.table_name = Some(table_name.to_string());
        self
    }

    pub fn start_timestamp(mut self, start_timestamp: chrono::Duration) -> Self {
        self.start_timestamp = start_timestamp;
        self
    }

    pub fn end_timestamp(mut self, end_timestamp: chrono::Duration) -> Self {
        self.end_timestamp = end_timestamp;
        self
    }

    pub fn window_size(mut self, window_size: time::Duration) -> Self {
        self.window_size = window_size;
        self
    }

    pub fn safety_interval(mut self, safety_interval: time::Duration) -> Self {
        self.safety_interval = safety_interval;
        self
    }

    pub fn sleep_interval(mut self, sleep_interval: time::Duration) -> Self {
        self.sleep_interval = sleep_interval;
        self
    }

    pub fn consumer_factory(mut self, consumer_factory: Arc<dyn ConsumerFactory>) -> Self {
        self.consumer_factory = Some(consumer_factory);
        self
    }

    pub async fn build(self) -> anyhow::Result<(CDCLogReader, RemoteHandle<anyhow::Result<()>>)> {
        let table_name = self.table_name.ok_or_else(|| {
            anyhow::anyhow!("failed to create the cdc reader: missing table name")
        })?;
        let keyspace = self.keyspace.ok_or_else(|| {
            anyhow::anyhow!("failed to create the cdc reader: missing keyspace name")
        })?;
        let session = self
            .session
            .ok_or_else(|| anyhow::anyhow!("failed to create the cdc reader: missing session"))?;
        let consumer_factory = self.consumer_factory.ok_or_else(|| {
            anyhow::anyhow!("failed to create the cdc reader: missing consumer factory")
        })?;

        let end_timestamp = chrono::Duration::max_value();
        let (end_timestamp_sender, end_timestamp_receiver) =
            tokio::sync::watch::channel(end_timestamp);
        let readers = vec![];

        let mut cdc_reader_worker = CDCReaderWorker {
            session,
            keyspace,
            table_name,
            start_timestamp: self.start_timestamp,
            end_timestamp: self.end_timestamp,
            window_size: self.window_size,
            safety_interval: self.safety_interval,
            sleep_interval: self.sleep_interval,
            readers,
            end_timestamp_receiver,
            consumer_factory,
        };

        let (fut, handle) = async move { cdc_reader_worker.run().await }.remote_handle();
        tokio::task::spawn(fut);
        let printer = CDCLogReader::new(end_timestamp_sender);

        Ok((printer, handle))
    }
}

impl Default for CDCLogReaderBuilder {
    fn default() -> Self {
        Self::new()
    }
}
