//! A module containing the logic responsible for reading data from one stream.

use std::cmp::{max, min};
use std::sync::Arc;
use std::time;

use async_trait::async_trait;
use itertools::Itertools;
use scylla::client::session::Session;
use scylla::errors::{DbError, ExecutionError, PrepareError, RequestAttemptError};
use scylla::response::query_result::QueryResult;
use scylla::response::{PagingState, PagingStateResponse};
use scylla::statement::prepared::PreparedStatement;
use scylla::value;
use scylla::value::Row;
use tokio::sync::watch;
use tokio::time::sleep;
use tracing::{enabled, warn};

use crate::cdc_types::{GenerationTimestamp, StreamID};
use crate::checkpoints::{start_saving_checkpoints, CDCCheckpointSaver, Checkpoint};
use crate::consumer::{CDCRow, CDCRowSchema, Consumer};

const BASIC_TIMEOUT_SLEEP_MS: u128 = 10;
const TIMEOUT_FACTOR: u128 = 2;

#[derive(Clone)]
pub struct CDCReaderConfig {
    pub lower_timestamp: chrono::Duration,
    pub window_size: time::Duration,
    pub safety_interval: time::Duration,
    pub sleep_interval: time::Duration,
    pub should_load_progress: bool,
    pub should_save_progress: bool,
    pub checkpoint_saver: Option<Arc<dyn CDCCheckpointSaver>>,
    pub pause_between_saves: time::Duration,
}

/// A wrapper for `Session` objects used to make mocking the Session possible.
#[async_trait]
trait StreamSession: Sync + Send {
    async fn prepare_statement(&self, query: String) -> Result<PreparedStatement, PrepareError>;
    async fn execute_paged_statement(
        &self,
        statement: &PreparedStatement,
        ids: &[StreamID],
        window_begin: &value::CqlTimestamp,
        window_end: &value::CqlTimestamp,
        paging_state: PagingState,
    ) -> Result<(QueryResult, PagingStateResponse), ExecutionError>;
}

#[async_trait]
impl StreamSession for Session {
    async fn prepare_statement(&self, query: String) -> Result<PreparedStatement, PrepareError> {
        self.prepare(query).await
    }

    async fn execute_paged_statement(
        &self,
        statement: &PreparedStatement,
        ids: &[StreamID],
        window_begin: &value::CqlTimestamp,
        window_end: &value::CqlTimestamp,
        paging_state: PagingState,
    ) -> Result<(QueryResult, PagingStateResponse), ExecutionError> {
        let (query_result, paging_state_response) = self
            .execute_single_page(statement, (ids, window_begin, window_end), paging_state)
            .await?;

        Ok((query_result, paging_state_response))
    }
}

/// A component responsible for reading data from one stream.
/// For the description of the reading algorithm,
/// please see the documentation of the [`log_reader`](crate::log_reader) module.
pub struct StreamReader {
    session: Arc<dyn StreamSession>,
    stream_id_vec: Vec<StreamID>,
    upper_timestamp: tokio::sync::Mutex<Option<chrono::Duration>>,
    config: CDCReaderConfig,
}

impl StreamReader {
    pub fn new(
        session: &Arc<Session>,
        stream_ids: Vec<StreamID>,
        config: CDCReaderConfig,
    ) -> StreamReader {
        StreamReader {
            session: session.clone(),
            stream_id_vec: stream_ids,
            upper_timestamp: Default::default(),
            config,
        }
    }

    pub async fn set_upper_timestamp(&self, new_upper_timestamp: chrono::Duration) {
        let mut guard = self.upper_timestamp.lock().await;
        *guard = Some(new_upper_timestamp);
    }

    pub async fn fetch_cdc(
        &self,
        keyspace: String,
        table_name: String,
        mut consumer: Box<dyn Consumer>,
    ) -> anyhow::Result<()> {
        let query = format!(
            "SELECT * FROM {keyspace}.{table_name}_scylla_cdc_log \
            WHERE \"cdc$stream_id\" in ? \
            AND \"cdc$time\" >= minTimeuuid(?) \
            AND \"cdc$time\" < minTimeuuid(?)  BYPASS CACHE"
        );
        let query_base = self.session.prepare_statement(query).await?;
        let mut window_begin = self.config.lower_timestamp;
        let window_size = chrono::Duration::from_std(self.config.window_size)?;
        let safety_interval = chrono::Duration::from_std(self.config.safety_interval)?;
        let mut checkpoint = Checkpoint {
            timestamp: window_begin.to_std()?,
            stream_id: self.stream_id_vec[0].clone(),
            generation: GenerationTimestamp {
                timestamp: window_begin,
            },
        };
        let (sender, receiver) = watch::channel(checkpoint.clone());

        if self.config.should_load_progress {
            let mut loaded_timestamp = chrono::Duration::MAX;
            for stream in &self.stream_id_vec {
                if let Some(timestamp) = self
                    .config
                    .checkpoint_saver
                    .as_ref()
                    .unwrap()
                    .load_last_checkpoint(stream)
                    .await?
                {
                    loaded_timestamp = min(timestamp, loaded_timestamp);
                }
            }
            if loaded_timestamp != chrono::Duration::MAX {
                window_begin = max(window_begin, loaded_timestamp);
            }
        }

        let mut _handle;
        if self.config.should_save_progress {
            _handle = start_saving_checkpoints(
                self.stream_id_vec.clone(),
                self.config.checkpoint_saver.as_ref().unwrap().clone(),
                receiver,
                self.config.pause_between_saves,
            );
        }

        loop {
            let now_timestamp =
                chrono::Duration::milliseconds(chrono::Local::now().timestamp_millis());
            let window_end = max(
                window_begin,
                min(window_begin + window_size, now_timestamp - safety_interval),
            );

            self.fetch_and_consume_rows(
                &query_base,
                &mut consumer,
                value::CqlTimestamp(window_begin.num_milliseconds()),
                value::CqlTimestamp(window_end.num_milliseconds()),
            )
            .await?;

            if let Some(timestamp_to_stop) = self.upper_timestamp.lock().await.as_ref() {
                if window_end >= *timestamp_to_stop {
                    break;
                }
            }

            window_begin = window_end;
            checkpoint.timestamp = window_begin.to_std()?;
            sender.send(checkpoint.clone())?;
            sleep(self.config.sleep_interval).await;
        }

        if self.config.should_save_progress {
            self.config
                .checkpoint_saver
                .as_ref()
                .unwrap()
                .save_checkpoint(&checkpoint)
                .await?;
        }

        Ok(())
    }

    async fn fetch_and_consume_rows(
        &self,
        query_base: &PreparedStatement,
        consumer: &mut Box<dyn Consumer>,
        window_begin: value::CqlTimestamp,
        window_end: value::CqlTimestamp,
    ) -> anyhow::Result<()> {
        let mut sleep_after_timeout = BASIC_TIMEOUT_SLEEP_MS;

        let mut next_state = PagingState::start();
        let mut page_no = 0;
        loop {
            let state_clone = next_state.clone();
            let query_res = self
                .session
                .execute_paged_statement(
                    query_base,
                    &self.stream_id_vec,
                    &window_begin,
                    &window_end,
                    next_state,
                )
                .await;
            match query_res {
                Ok((query_result, paging_state_response)) => {
                    sleep_after_timeout = BASIC_TIMEOUT_SLEEP_MS;
                    page_no += 1;
                    let query_rows_result = query_result.into_rows_result()?;
                    let schema = CDCRowSchema::new(query_rows_result.column_specs());
                    let rows = query_rows_result.rows::<Row>()?;
                    for row in rows {
                        consumer
                            .consume_cdc(CDCRow::from_row(row?, &schema))
                            .await?;
                    }
                    match paging_state_response {
                        PagingStateResponse::HasMorePages { state } => next_state = state,
                        PagingStateResponse::NoMorePages => break,
                    }
                }
                Err(
                    ExecutionError::RequestTimeout(_)
                    | ExecutionError::LastAttemptError(RequestAttemptError::DbError(
                        DbError::ReadTimeout { .. },
                        _,
                    )),
                ) => {
                    self.print_timeout_warning(
                        &window_begin,
                        &window_end,
                        sleep_after_timeout,
                        page_no,
                    )
                    .await;
                    sleep(time::Duration::from_millis(sleep_after_timeout as u64)).await;
                    sleep_after_timeout *= TIMEOUT_FACTOR;
                    if sleep_after_timeout >= self.config.sleep_interval.as_millis() {
                        sleep_after_timeout = self.config.sleep_interval.as_millis();
                    }

                    next_state = state_clone;
                }
                Err(e) => {
                    return Err(anyhow::Error::new(e)
                        .context("Session returned an error while fetching CDC rows."));
                }
            }
        }
        Ok(())
    }

    async fn print_timeout_warning(
        &self,
        window_begin: &value::CqlTimestamp,
        window_end: &value::CqlTimestamp,
        backoff: u128,
        page_no: u64,
    ) {
        if enabled!(tracing::Level::WARN) {
            let ids_str = self
                .stream_id_vec
                .iter()
                .map(|x| format!("0x{}", hex::encode(&x.id)))
                .join(", ");

            warn!(
                stream_ids = ids_str,
                window_begin_ms = window_begin.0,
                window_end_ms = window_end.0,
                page_no = page_no,
                current_backoff = backoff,
                "Timeout while fetching CDC rows."
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use futures::stream::StreamExt;
    use rstest::rstest;
    use scylla::errors::{ExecutionError, PrepareError, RequestAttemptError};
    use scylla::statement::unprepared::Statement;
    use scylla_cdc_test_utils::{
        now, populate_simple_db_with_pk, prepare_simple_db, skip_if_not_supported, TEST_TABLE,
    };
    use std::sync::atomic::AtomicIsize;
    use std::sync::atomic::Ordering::Relaxed;
    use tokio::sync::Mutex;

    use super::*;

    const SECOND_IN_MILLIS: u64 = 1_000;
    const SLEEP_INTERVAL: u64 = SECOND_IN_MILLIS / 10;
    const WINDOW_SIZE: u64 = SECOND_IN_MILLIS / 10 * 3;
    const SAFETY_INTERVAL: u64 = SECOND_IN_MILLIS / 10;
    const START_TIME_DELAY_IN_SECONDS: i64 = 2;

    impl StreamReader {
        fn test_new(
            session: &Arc<Session>,
            stream_ids: Vec<StreamID>,
            start_timestamp: chrono::Duration,
            window_size: time::Duration,
            safety_interval: time::Duration,
            sleep_interval: time::Duration,
        ) -> StreamReader {
            let config = CDCReaderConfig {
                lower_timestamp: start_timestamp,
                window_size,
                safety_interval,
                sleep_interval,
                should_load_progress: false,
                should_save_progress: false,
                checkpoint_saver: None,
                pause_between_saves: Default::default(),
            };

            StreamReader {
                session: session.clone(),
                stream_id_vec: stream_ids,
                upper_timestamp: Default::default(),
                config,
            }
        }
    }

    async fn get_test_stream_reader(session: &Arc<Session>) -> anyhow::Result<StreamReader> {
        let stream_id_vec = get_cdc_stream_id(session).await?;

        let start_timestamp = now() - chrono::Duration::seconds(START_TIME_DELAY_IN_SECONDS);
        let sleep_interval = time::Duration::from_millis(SLEEP_INTERVAL);
        let window_size = time::Duration::from_millis(WINDOW_SIZE);
        let safety_interval = time::Duration::from_millis(SAFETY_INTERVAL);

        let reader = StreamReader::test_new(
            session,
            stream_id_vec,
            start_timestamp,
            window_size,
            safety_interval,
            sleep_interval,
        );

        Ok(reader)
    }

    async fn get_cdc_stream_id(session: &Arc<Session>) -> anyhow::Result<Vec<StreamID>> {
        let query_stream_id =
            format!("SELECT DISTINCT \"cdc$stream_id\" FROM {TEST_TABLE}_scylla_cdc_log;");

        let mut rows = session
            .query_iter(query_stream_id, ())
            .await?
            .rows_stream::<(StreamID,)>()?;

        let mut stream_ids_vec = Vec::new();
        while let Some(row) = rows.next().await {
            let casted_row = row?.0;
            stream_ids_vec.push(casted_row);
        }

        Ok(stream_ids_vec)
    }

    type TestResult = (i32, String, i32, String);

    struct FetchTestConsumer {
        fetched_rows: Arc<Mutex<Vec<TestResult>>>,
    }

    #[async_trait]
    impl Consumer for FetchTestConsumer {
        async fn consume_cdc(&mut self, mut data: CDCRow<'_>) -> anyhow::Result<()> {
            let new_val = (
                data.take_value("pk").unwrap().as_int().unwrap(),
                data.take_value("s").unwrap().as_text().unwrap().to_string(),
                data.take_value("t").unwrap().as_int().unwrap(),
                data.take_value("v").unwrap().as_text().unwrap().to_string(),
            );
            self.fetched_rows.lock().await.push(new_val);
            Ok(())
        }
    }

    struct TimeoutSession {
        session: Arc<Session>,
        counter: Arc<AtomicIsize>,
    }

    #[async_trait]
    impl StreamSession for TimeoutSession {
        async fn prepare_statement(
            &self,
            query: String,
        ) -> Result<PreparedStatement, PrepareError> {
            self.session.prepare(query).await
        }

        async fn execute_paged_statement(
            &self,
            statement: &PreparedStatement,
            ids: &[StreamID],
            window_begin: &value::CqlTimestamp,
            window_end: &value::CqlTimestamp,
            paging_state: PagingState,
        ) -> Result<(QueryResult, PagingStateResponse), ExecutionError> {
            if self.counter.fetch_sub(1, Relaxed) >= 0 {
                let read_timeout = DbError::ReadTimeout {
                    consistency: Default::default(),
                    received: 0,
                    required: 0,
                    data_present: false,
                };
                Err(ExecutionError::LastAttemptError(
                    RequestAttemptError::DbError(read_timeout, String::new()),
                ))
            } else {
                let (query_result, paging_state_response) = self
                    .session
                    .execute_single_page(statement, (ids, window_begin, window_end), paging_state)
                    .await?;
                Ok((query_result, paging_state_response))
            }
        }
    }

    #[rstest]
    #[case::vnodes(false)]
    #[case::tablets(true)]
    #[tokio::test]
    async fn check_fetch_cdc_with_multiple_stream_id(#[case] tablets_enabled: bool) {
        let (shared_session, ks) = skip_if_not_supported!(prepare_simple_db(tablets_enabled));

        let partition_key_1 = 0;
        let partition_key_2 = 1;
        populate_simple_db_with_pk(&shared_session, partition_key_1)
            .await
            .unwrap();
        populate_simple_db_with_pk(&shared_session, partition_key_2)
            .await
            .unwrap();

        let cdc_reader = get_test_stream_reader(&shared_session).await.unwrap();
        cdc_reader
            .set_upper_timestamp(now() + chrono::Duration::seconds(1))
            .await;
        let fetched_rows = Arc::new(Mutex::new(vec![]));
        let consumer = Box::new(FetchTestConsumer {
            fetched_rows: Arc::clone(&fetched_rows),
        });

        cdc_reader
            .fetch_cdc(ks, TEST_TABLE.to_string(), consumer)
            .await
            .unwrap();

        let mut row_count_with_pk1 = 0;
        let mut row_count_with_pk2 = 0;
        let mut count1 = 0;
        let mut count2 = 0;

        for row in fetched_rows.lock().await.iter() {
            let (pk, s, t, v) = row.clone();

            if pk == partition_key_1 as i32 {
                assert_eq!(pk, partition_key_1 as i32);
                assert_eq!(t, count1);
                assert_eq!(v.to_string(), format!("val{count1}"));
                assert_eq!(s.to_string(), format!("static{count1}"));
                count1 += 1;
                row_count_with_pk1 += 1;
            } else {
                assert_eq!(pk, partition_key_2 as i32);
                assert_eq!(t, count2);
                assert_eq!(v.to_string(), format!("val{count2}"));
                assert_eq!(s.to_string(), format!("static{count2}"));
                count2 += 1;
                row_count_with_pk2 += 1;
            }
        }

        assert_eq!(row_count_with_pk2, 3);
        assert_eq!(row_count_with_pk1, 3);
    }

    #[rstest]
    #[case::vnodes(false)]
    #[case::tablets(true)]
    #[tokio::test]
    async fn check_fetch_cdc_with_one_stream_id(#[case] tablets_enabled: bool) {
        let (shared_session, ks) = skip_if_not_supported!(prepare_simple_db(tablets_enabled));

        let partition_key = 0;
        populate_simple_db_with_pk(&shared_session, partition_key)
            .await
            .unwrap();

        let cdc_reader = get_test_stream_reader(&shared_session).await.unwrap();
        cdc_reader
            .set_upper_timestamp(now() + chrono::Duration::seconds(1))
            .await;
        let fetched_rows = Arc::new(Mutex::new(vec![]));
        let consumer = Box::new(FetchTestConsumer {
            fetched_rows: Arc::clone(&fetched_rows),
        });

        cdc_reader
            .fetch_cdc(ks, TEST_TABLE.to_string(), consumer)
            .await
            .unwrap();

        for (count, row) in fetched_rows.lock().await.iter().enumerate() {
            let (pk, s, t, v) = row.clone();
            assert_eq!(pk, partition_key as i32);
            assert_eq!(t, count as i32);
            assert_eq!(v.to_string(), format!("val{count}"));
            assert_eq!(s.to_string(), format!("static{count}"));
        }
    }

    #[rstest]
    #[case::vnodes(false)]
    #[case::tablets(true)]
    #[tokio::test]
    async fn check_set_upper_timestamp_in_fetch_cdc(#[case] tablets_enabled: bool) {
        let (shared_session, ks) = skip_if_not_supported!(prepare_simple_db(tablets_enabled));

        let mut insert_before_upper_timestamp_query = Statement::new(format!(
            "INSERT INTO {} (pk, t, v, s) VALUES ({}, {}, '{}', '{}');",
            TEST_TABLE, 0, 0, "val0", "static0"
        ));
        let second_ago = chrono::Local::now() - chrono::Duration::seconds(1);
        let second_ago_timestamp = chrono::Duration::milliseconds(second_ago.timestamp_millis());
        insert_before_upper_timestamp_query.set_timestamp(second_ago_timestamp.num_microseconds());
        shared_session
            .query_unpaged(insert_before_upper_timestamp_query, ())
            .await
            .unwrap();

        let cdc_reader = get_test_stream_reader(&shared_session).await.unwrap();
        cdc_reader.set_upper_timestamp(now()).await;

        let mut insert_after_upper_timestamp_query = Statement::new(format!(
            "INSERT INTO {} (pk, t, v, s) VALUES ({}, {}, '{}', '{}');",
            TEST_TABLE, 0, 1, "val1", "static1"
        ));
        let second_later = chrono::Local::now() + chrono::Duration::seconds(1);
        let second_later_timestamp =
            chrono::Duration::milliseconds(second_later.timestamp_millis());
        insert_after_upper_timestamp_query.set_timestamp(second_later_timestamp.num_microseconds());
        shared_session
            .query_unpaged(insert_after_upper_timestamp_query, ())
            .await
            .unwrap();
        let fetched_rows = Arc::new(Mutex::new(vec![]));
        let consumer = Box::new(FetchTestConsumer {
            fetched_rows: Arc::clone(&fetched_rows),
        });

        cdc_reader
            .fetch_cdc(ks, TEST_TABLE.to_string(), consumer)
            .await
            .unwrap();

        for row in fetched_rows.lock().await.iter() {
            let (pk, s, t, v) = row.clone();
            assert_eq!(pk, 0);
            assert_eq!(t, 0);
            assert_eq!(v.to_string(), "val0".to_string());
            assert_eq!(s.to_string(), "static0".to_string());
        }
    }

    #[rstest]
    #[case::vnodes(false)]
    #[case::tablets(true)]
    #[tokio::test]
    async fn timeout_retry_test(#[case] tablets_enabled: bool) {
        let (shared_session, ks) = skip_if_not_supported!(prepare_simple_db(tablets_enabled));

        let partition_key = 0;
        populate_simple_db_with_pk(&shared_session, partition_key)
            .await
            .unwrap();

        let mut cdc_reader = get_test_stream_reader(&shared_session).await.unwrap();
        let mocked_session = TimeoutSession {
            session: shared_session,
            counter: Arc::new(AtomicIsize::new(8)),
        };
        cdc_reader.session = Arc::new(mocked_session);
        // Modify default sleep interval so that the test terminates faster
        // (maximal wait time in backoff is equal to sleep_interval).
        cdc_reader.config.sleep_interval = time::Duration::from_millis(1500);
        cdc_reader
            .set_upper_timestamp(now() + chrono::Duration::seconds(1))
            .await;
        let fetched_rows = Arc::new(Mutex::new(vec![]));
        let consumer = Box::new(FetchTestConsumer {
            fetched_rows: Arc::clone(&fetched_rows),
        });

        cdc_reader
            .fetch_cdc(ks, TEST_TABLE.to_string(), consumer)
            .await
            .unwrap();

        for (count, row) in fetched_rows.lock().await.iter().enumerate() {
            let (pk, s, t, v) = row.clone();
            assert_eq!(pk, partition_key as i32);
            assert_eq!(t, count as i32);
            assert_eq!(v.to_string(), format!("val{count}"));
            assert_eq!(s.to_string(), format!("static{count}"));
        }
    }
}
