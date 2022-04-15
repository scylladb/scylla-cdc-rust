use std::cmp::max;
use std::sync::Arc;
use std::time;

use anyhow;
use futures::future::RemoteHandle;
use futures::stream::{FusedStream, FuturesUnordered, StreamExt};
use futures::FutureExt;
use scylla::Session;

use crate::cdc_types::GenerationTimestamp;
use crate::consumer::ConsumerFactory;
use crate::stream_generations::GenerationFetcher;
use crate::stream_reader::StreamReader;

pub struct CDCLogReader {
    // Tells the worker to stop
    // Usage of the "watch" channel will make it possible to change the the timestamp later,
    // for example if somebody loses patience and wants to stop now not later
    end_timestamp: tokio::sync::watch::Sender<chrono::Duration>,
}

impl CDCLogReader {
    // Creates a reader and runs it immediately
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session: Arc<Session>,
        keyspace: String,
        table_name: String,
        start_timestamp: chrono::Duration,
        end_timestamp: chrono::Duration,
        window_size: time::Duration,
        safety_interval: time::Duration,
        sleep_interval: time::Duration,
        consumer_factory: Arc<dyn ConsumerFactory>,
    ) -> (Self, RemoteHandle<anyhow::Result<()>>) {
        let (end_timestamp_sender, end_timestamp_receiver) =
            tokio::sync::watch::channel(end_timestamp);
        let mut worker = CDCReaderWorker::new(
            session,
            keyspace,
            table_name,
            start_timestamp,
            end_timestamp,
            window_size,
            safety_interval,
            sleep_interval,
            end_timestamp_receiver,
            consumer_factory,
        );
        let (fut, handle) = async move { worker.run().await }.remote_handle();
        tokio::task::spawn(fut);
        let printer = CDCLogReader {
            end_timestamp: end_timestamp_sender,
        };
        (printer, handle)
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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session: Arc<Session>,
        keyspace: String,
        table_name: String,
        start_timestamp: chrono::Duration,
        end_timestamp: chrono::Duration,
        window_size: time::Duration,
        safety_interval: time::Duration,
        sleep_interval: time::Duration,
        end_timestamp_receiver: tokio::sync::watch::Receiver<chrono::Duration>,
        consumer_factory: Arc<dyn ConsumerFactory>,
    ) -> CDCReaderWorker {
        CDCReaderWorker {
            session,
            keyspace,
            table_name,
            start_timestamp,
            end_timestamp,
            window_size,
            safety_interval,
            sleep_interval,
            readers: vec![],
            end_timestamp_receiver,
            consumer_factory,
        }
    }

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
                Some(evt) = stream_reader_tasks.next(), if !stream_reader_tasks.is_terminated()  => {
                    match evt {
                        Err(error) => {
                            err = Some(anyhow::Error::new(error));
                            self.set_upper_timestamp(chrono::Duration::min_value()).await;
                        }
                        Ok(Err(error)) => {
                            err = Some(error);
                            self.set_upper_timestamp(chrono::Duration::min_value()).await;
                        },
                        _ => {}
                    }
                }
                Some(generation) = generation_receiver.recv(), if next_generation.is_none() => {
                    next_generation = Some(generation.clone());
                    had_first_generation = true;
                    self.set_upper_timestamp(generation.timestamp).await;
                }
                Ok(_) = self.end_timestamp_receiver.changed() => {
                    let timestamp = *self.end_timestamp_receiver.borrow_and_update();
                    self.end_timestamp = timestamp;
                    self.set_upper_timestamp(timestamp).await;
                }
            }

            if stream_reader_tasks.is_empty() {
                if let Some(err) = err {
                    return Err(err);
                }

                if let Some(generation) = next_generation {
                    next_generation = None;

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
}
