use std::cmp::max;
use std::sync::Arc;

use anyhow;
use chrono::Duration;
use core::time;
use futures::future::RemoteHandle;
use futures::stream::{FusedStream, FuturesUnordered, StreamExt};
use futures::FutureExt;
use scylla::frame::response::result::Row;
use scylla::Session;

use scylla_cdc::cdc_types::GenerationTimestamp;
use scylla_cdc::reader::StreamReader;
use scylla_cdc::stream_generations::GenerationFetcher;

pub struct CDCLogPrinter {
    // Tells the worker to stop
    // Usage of the "watch" channel will make it possible to change the the timestamp later,
    // for example if somebody loses patience and wants to stop now not later
    end_timestamp: tokio::sync::watch::Sender<Duration>,
}

impl CDCLogPrinter {
    // Creates a printer and runs it immediately
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session: &Arc<Session>,
        keyspace: String,
        table_name: String,
        start_timestamp: Duration,
        end_timestamp: Duration,
        window_size: Duration,
        safety_interval: Duration,
        sleep_interval: time::Duration,
    ) -> (Self, RemoteHandle<anyhow::Result<()>>) {
        let (end_timestamp_sender, end_timestamp_receiver) =
            tokio::sync::watch::channel(end_timestamp);
        let mut worker = CDCLogPrinterWorker::new(
            session,
            keyspace,
            table_name,
            start_timestamp,
            end_timestamp,
            window_size,
            safety_interval,
            sleep_interval,
            end_timestamp_receiver,
        );
        let (fut, handle) = async move { worker.run().await }.remote_handle();
        tokio::task::spawn(fut);
        let printer = CDCLogPrinter {
            end_timestamp: end_timestamp_sender,
        };
        (printer, handle)
    }

    // Tell the worker to set the end timestamp and then stop
    pub fn stop_at(&mut self, when: Duration) {
        let _ = self.end_timestamp.send(when);
    }

    // Tell the worker to stop immediately
    pub fn stop(&mut self) {
        self.stop_at(Duration::min_value());
    }
}

struct CDCLogPrinterWorker {
    session: Arc<Session>,
    keyspace: String,
    table_name: String,
    start_timestamp: Duration,
    end_timestamp: Duration,
    sleep_interval: time::Duration,
    window_size: Duration,
    safety_interval: Duration,
    readers: Vec<Arc<StreamReader>>,
    end_timestamp_receiver: tokio::sync::watch::Receiver<Duration>,
}

impl CDCLogPrinterWorker {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session: &Arc<Session>,
        keyspace: String,
        table_name: String,
        start_timestamp: Duration,
        end_timestamp: Duration,
        window_size: Duration,
        safety_interval: Duration,
        sleep_interval: time::Duration,
        end_timestamp_receiver: tokio::sync::watch::Receiver<Duration>,
    ) -> CDCLogPrinterWorker {
        CDCLogPrinterWorker {
            session: Arc::clone(session),
            keyspace,
            table_name,
            start_timestamp,
            end_timestamp,
            window_size,
            safety_interval,
            sleep_interval,
            readers: vec![],
            end_timestamp_receiver,
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        let fetcher = Arc::new(GenerationFetcher::new(&self.session));
        let (mut generation_receiver, _future_handle) = fetcher
            .clone()
            .fetch_generations_continuously(self.start_timestamp, self.sleep_interval)
            .await?;

        fn print_row(row: Row) {
            println!("{:?}", row.columns);
        }

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
                            tokio::spawn(async move {
                                reader.fetch_cdc(keyspace, table_name, print_row).await
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

    async fn set_upper_timestamp(&self, new_upper_timestamp: Duration) {
        for reader in self.readers.iter() {
            reader.set_upper_timestamp(new_upper_timestamp).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use scylla::batch::Consistency;
    use scylla::query::Query;
    use scylla::SessionBuilder;
    use tokio::time::sleep;

    use super::*;

    const SECOND_IN_MILLIS: i64 = 1_000;
    const TEST_KEYSPACE: &str = "test";
    const TEST_TABLE: &str = "t";
    const SLEEP_INTERVAL: i64 = SECOND_IN_MILLIS / 10;
    const WINDOW_SIZE: i64 = SECOND_IN_MILLIS / 10 * 3;
    const SAFETY_INTERVAL: i64 = SECOND_IN_MILLIS / 10;

    fn get_create_table_query() -> String {
        format!("CREATE TABLE IF NOT EXISTS {}.{} (pk int, t int, v text, s text, PRIMARY KEY (pk, t)) WITH cdc = {{'enabled':true}};",
                TEST_KEYSPACE,
                TEST_TABLE
        )
    }

    async fn create_test_db(session: &Arc<Session>) -> anyhow::Result<()> {
        let mut create_keyspace_query = Query::new(format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}};",
            TEST_KEYSPACE
        ));
        create_keyspace_query.set_consistency(Consistency::All);

        session.query(create_keyspace_query, &[]).await?;
        session.await_schema_agreement().await?;

        // Create test table
        let create_table_query = get_create_table_query();
        session.query(create_table_query, &[]).await?;
        session.await_schema_agreement().await?;

        let table_name_under_keyspace = format!("{}.{}", TEST_KEYSPACE, TEST_TABLE);
        session
            .query(format!("TRUNCATE {};", table_name_under_keyspace), &[])
            .await?;
        session
            .query(
                format!("TRUNCATE {}_scylla_cdc_log;", table_name_under_keyspace),
                &[],
            )
            .await?;
        Ok(())
    }

    async fn populate_db_with_pk(session: &Arc<Session>, pk: u32) -> anyhow::Result<()> {
        let table_name_under_keyspace = format!("{}.{}", TEST_KEYSPACE, TEST_TABLE);
        for i in 0..3 {
            session
                .query(
                    format!(
                        "INSERT INTO {} (pk, t, v, s) VALUES ({}, {}, 'val{}', 'static{}');",
                        table_name_under_keyspace, pk, i, i, i
                    ),
                    &[],
                )
                .await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_cdc_log_printer() {
        let start = Duration::from_std(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap(),
        )
        .unwrap();
        let end = start + Duration::seconds(2);

        let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        let session = SessionBuilder::new().known_node(uri).build().await.unwrap();
        let shared_session = Arc::new(session);

        let partition_key_1 = 0;
        let partition_key_2 = 1;
        create_test_db(&shared_session).await.unwrap();
        populate_db_with_pk(&shared_session, partition_key_1)
            .await
            .unwrap();
        populate_db_with_pk(&shared_session, partition_key_2)
            .await
            .unwrap();

        let (mut cdc_log_printer, _handle) = CDCLogPrinter::new(
            &shared_session,
            TEST_KEYSPACE.to_string(),
            TEST_TABLE.to_string(),
            start,
            end,
            Duration::milliseconds(WINDOW_SIZE),
            Duration::milliseconds(SAFETY_INTERVAL),
            time::Duration::from_millis(SLEEP_INTERVAL as u64),
        );

        sleep(time::Duration::from_secs(2)).await;

        cdc_log_printer.stop();
    }
}
