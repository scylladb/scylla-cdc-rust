//! A module containing the logic responsible for reading data from one stream.

use std::cmp;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use itertools::Itertools;
use scylla::client::session::Session;
use scylla::errors::ConnectionPoolError;
use scylla::errors::DbError;
use scylla::errors::ExecutionError;
use scylla::errors::PrepareError;
use scylla::errors::RequestAttemptError;
use scylla::response::PagingState;
use scylla::response::PagingStateResponse;
use scylla::response::query_result::QueryResult;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::Row;
use tokio::sync::watch;
use tokio::time::sleep;
use tracing::debug;
use tracing::enabled;
use tracing::error;
use tracing::warn;

use crate::CqlIdentifier;
use crate::cdc_types::GenerationTimestamp;
use crate::cdc_types::StreamID;
use crate::cdc_types::Timestamp;
use crate::checkpoints::CDCCheckpointSaver;
use crate::checkpoints::Checkpoint;
use crate::checkpoints::start_saving_checkpoints;
use crate::consumer::CDCRow;
use crate::consumer::CDCRowSchema;
use crate::consumer::Consumer;

const BASIC_TIMEOUT_SLEEP: tokio::time::Duration = tokio::time::Duration::from_millis(100);
const TIMEOUT_FACTOR: u32 = 2;

#[derive(Clone)]
pub struct CDCReaderConfig {
    pub lower_timestamp: Duration,
    pub window_size: Duration,
    pub safety_interval: Duration,
    pub sleep_interval: Duration,
    pub should_load_progress: bool,
    pub should_save_progress: bool,
    pub checkpoint_saver: Option<Arc<dyn CDCCheckpointSaver>>,
    pub pause_between_saves: Duration,
}

/// A wrapper for `Session` objects used to make mocking the Session possible.
#[async_trait]
trait StreamSession: Sync + Send {
    async fn prepare_statement(&self, query: String) -> Result<PreparedStatement, PrepareError>;
    async fn execute_paged_statement(
        &self,
        statement: &PreparedStatement,
        ids: &[StreamID],
        window_begin: &Timestamp,
        window_end: &Timestamp,
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
        window_begin: &Timestamp,
        window_end: &Timestamp,
        paging_state: PagingState,
    ) -> Result<(QueryResult, PagingStateResponse), ExecutionError> {
        let (query_result, paging_state_response) = self
            .execute_single_page(statement, (ids, window_begin, window_end), paging_state)
            .await?;

        Ok((query_result, paging_state_response))
    }
}

/// The attempt here is to determine whether the error is transient,
/// by looking at the error types. Errors that we consider transient are:
///   - any kind of error to the network connection to the database
///     (BrokenConnectionError / ConnectionPoolError / RequestTimeout)
///   - any database error that suggest that while request is correct, database cannot handle it now
///     (Overloaded / RateLimitReached / Unavailable / ...)
///   - driver side problems that will likely be solved in the near future
///     (UnableToAllocStreamId)
fn is_transient_error(error: &ExecutionError) -> bool {
    #[deny(clippy::wildcard_enum_match_arm)]
    match error {
        ExecutionError::RequestTimeout(_) => true,
        #[deny(clippy::wildcard_enum_match_arm)]
        ExecutionError::LastAttemptError(error) => match error {
            RequestAttemptError::BrokenConnectionError(_)
            | RequestAttemptError::UnableToAllocStreamId => true,
            #[deny(clippy::wildcard_enum_match_arm)]
            RequestAttemptError::DbError(db_error, _) => match db_error {
                DbError::Unavailable { .. }
                | DbError::ReadTimeout { .. }
                | DbError::Overloaded
                | DbError::IsBootstrapping
                | DbError::RateLimitReached { .. } => true,
                DbError::SyntaxError
                | DbError::Invalid
                | DbError::AlreadyExists { .. }
                | DbError::FunctionFailure { .. }
                | DbError::AuthenticationError
                | DbError::Unauthorized
                | DbError::ConfigError
                | DbError::TruncateError
                | DbError::WriteTimeout { .. }
                | DbError::ReadFailure { .. }
                | DbError::WriteFailure { .. }
                | DbError::Unprepared { .. }
                | DbError::ServerError
                | DbError::ProtocolError
                | DbError::Other(_) => false,
                _ => unreachable!(),
            },
            RequestAttemptError::SerializationError(_)
            | RequestAttemptError::CqlRequestSerialization(_)
            | RequestAttemptError::BodyExtensionsParseError(_)
            | RequestAttemptError::CqlResultParseError(_)
            | RequestAttemptError::CqlErrorParseError(_)
            | RequestAttemptError::UnexpectedResponse(_)
            | RequestAttemptError::RepreparedIdChanged { .. }
            | RequestAttemptError::RepreparedIdMissingInBatch
            | RequestAttemptError::NonfinishedPagingState => false,
            _ => unreachable!(),
        },
        #[deny(clippy::wildcard_enum_match_arm)]
        ExecutionError::ConnectionPoolError(error) => match error {
            ConnectionPoolError::NodeDisabledByHostFilter => false,
            ConnectionPoolError::Broken { .. } | ConnectionPoolError::Initializing => true,
            _ => unreachable!(),
        },
        ExecutionError::BadQuery(_)
        | ExecutionError::EmptyPlan
        | ExecutionError::PrepareError(_)
        | ExecutionError::UseKeyspaceError(_)
        | ExecutionError::SchemaAgreementError(_)
        | ExecutionError::MetadataError(_) => false,
        _ => unreachable!(),
    }
}

/// A component responsible for reading data from one stream.
/// For the description of the reading algorithm,
/// please see the documentation of the [`log_reader`](crate::log_reader) module.
pub struct StreamReader {
    session: Arc<dyn StreamSession>,
    stream_id_vec: Vec<StreamID>,
    upper_timestamp: tokio::sync::Mutex<Option<Timestamp>>,
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

    /// Sets the upper timestamp for the reader.
    /// The [`fetch_cdc`](Self::fetch_cdc) method will stop when this timestamp is reached.
    /// You can update this timestamp even while the [`fetch_cdc`](Self::fetch_cdc) method is running.
    pub(crate) async fn set_upper_timestamp(&self, ts: Timestamp) {
        let mut guard = self.upper_timestamp.lock().await;
        *guard = Some(ts);
    }

    /// Continuously fetches CDC rows from the specified keyspace.table and passes them to the provided consumer.
    /// This function returns when the upper timestamp is reached or an error other than the timeout occurs.
    /// By default the upper timestamp is not set, so the function continues indefinitely.
    /// You can set it using the [`set_upper_timestamp`](Self::set_upper_timestamp) method.
    ///
    /// When the request to the database fails due to timeout, it continues retrying with exponential backoff.
    pub async fn fetch_cdc(
        &self,
        keyspace: String,
        table_name: String,
        mut consumer: Box<dyn Consumer>,
    ) -> anyhow::Result<()> {
        let keyspace = CqlIdentifier::new(keyspace);
        let table_name = CqlIdentifier::new(format!("{table_name}_scylla_cdc_log"));
        let query = format!(
            "SELECT * FROM {keyspace}.{table_name} \
            WHERE \"cdc$stream_id\" in ? \
            AND \"cdc$time\" >= minTimeuuid(?) \
            AND \"cdc$time\" < minTimeuuid(?)  BYPASS CACHE"
        );
        let query_base = {
            let mut query_base = self.session.prepare_statement(query).await?;
            query_base.set_is_idempotent(true);
            query_base
        };

        let mut window_begin = Timestamp::from_duration_since_epoch(self.config.lower_timestamp);
        let window_size = self.config.window_size;
        let safety_interval = self.config.safety_interval;
        let mut checkpoint = Checkpoint {
            timestamp: window_begin.to_duration_since_epoch(),
            stream_id: self.stream_id_vec[0].clone(),
            generation: GenerationTimestamp {
                timestamp: window_begin,
            },
        };
        let (sender, receiver) = watch::channel(checkpoint.clone());

        if self.config.should_load_progress {
            let mut loaded_timestamp = Timestamp::MAX;
            for stream in &self.stream_id_vec {
                if let Some(ts) = self
                    .config
                    .checkpoint_saver
                    .as_ref()
                    .unwrap()
                    .load_last_checkpoint(stream)
                    .await?
                    .map(Timestamp::from_duration_since_epoch)
                {
                    loaded_timestamp = loaded_timestamp.min(ts);
                }
            }
            if loaded_timestamp != Timestamp::MAX {
                window_begin = window_begin.max(loaded_timestamp);
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

        let mut now_timestamp = Timestamp::now();
        // Calculate the timestamp until which it is safe to read.
        // We should not read data newer than (now - `safety_interval`),
        // because clock drift and various kinds of latency may influence too recent results.
        let mut safe_to_read_until = now_timestamp - safety_interval;

        // The first `window_begin` is set by the user (or loaded from checkpoint).
        if window_begin > safe_to_read_until {
            // If it is too close to the current time, we wait until we can start reading.
            // This is done to prevent errors such as reading out-of-order changes, or skipping some changes.
            if window_begin > now_timestamp {
                // If it it's in the future, we issue an error message to make it clear that wrong timestamp was set up as `window_begin`.
                // Then we wait before starting to read, to satisfy safety interval.
                error!(
                    requested_begin_ms_timestamp = window_begin.as_millis(),
                    current_ms_timestamp = now_timestamp.as_millis(),
                    "Provided `CDCReaderConfig::lower_timestamp` that was in the future!\
                    Ensure that the start timestamp is not set in the future or you have a valid checkpoint state.\
                    The CDC readers will wait up to `CDCReaderConfig::safety_interval` before starting to read data."
                );
            } else {
                // If it does not respect safety interval BUT is still in the past, we inform about it.
                // This may happen if we start from "now" without any checkpoint.
                // Then we wait before starting to read, to satisfy safety interval.
                // This is done also not to require user to think about safety interval when setting start timestamp.
                //
                // TODO: consider making this log print rate-limited, because there were reports of this being too spammy.
                debug!(
                    requested_begin_ms_timestamp = window_begin.as_millis(),
                    current_ms_timestamp = now_timestamp.as_millis(),
                    safety_interval_ms = safety_interval.as_millis(),
                    "Provided `CDCReaderConfig::lower_timestamp` that was in a too recent past; it did not include safety interval.\
                    This is expected and minor issue if you provided NOW as the `CDCReaderConfig::lower_timestamp` timestamp.\
                    The CDC readers will wait up to `CDCReaderConfig::safety_interval` before starting to read data."
                );
            }
            sleep(
                window_begin
                    .checked_duration_since(safe_to_read_until)
                    .unwrap_or(Duration::ZERO)
                    // Adding a small buffer to ensure we are past the safety interval, not exactly at its edge.
                    // This is to avoid issues with clock precision.
                    + Duration::from_millis(100),
            )
            .await;
        }

        loop {
            now_timestamp = Timestamp::now();
            safe_to_read_until = now_timestamp - safety_interval;

            // The only possible way for this to happen is when the current `now_timestamp` is less than the previous `now_timestamp`.
            // This is because we possibly waited before the loop to ensure `window_begin <= safe_to_read_until`.
            // This means TIME TRAVEL! But still we have to handle it gracefully.
            // In this case, we again wait until the time is safe to read.
            if window_begin > safe_to_read_until {
                error!(
                    last_request_end_ms = window_begin.as_millis(),
                    current_timestamp_ms = now_timestamp.as_millis(),
                    expected_window_end_ms = safe_to_read_until.as_millis(),
                    "The current time broke the monotonicity when creating a CDC request. Ensure the system clock is stable, and is within the safety interval of the database clock."
                );
                sleep(
                    window_begin
                        .checked_duration_since(safe_to_read_until)
                        .unwrap_or(Duration::ZERO)
                        // Adding a small buffer to ensure we are past the safety interval, not exactly at its edge.
                        // This is to avoid issues with clock precision.
                        + Duration::from_millis(100),
                )
                .await;
                continue;
            }

            // Ask for windows no larger that `window_size`, but also ensure we respect the safety interval.
            let window_end = cmp::min(window_begin + window_size, safe_to_read_until);

            self.fetch_and_consume_rows(&query_base, &mut consumer, window_begin, window_end)
                .await?;

            if let Some(timestamp_to_stop) = self.upper_timestamp.lock().await.as_ref()
                && window_end >= *timestamp_to_stop
            {
                break;
            }

            window_begin = window_end;
            checkpoint.timestamp = window_begin.to_duration_since_epoch();
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

    /// Attempts to execute statement fetching all pages of CDC rows for the given time window.
    /// In case of timeouts, it retries with exponential backoff.
    async fn fetch_and_consume_rows(
        &self,
        query_base: &PreparedStatement,
        consumer: &mut Box<dyn Consumer>,
        window_begin: Timestamp,
        window_end: Timestamp,
    ) -> anyhow::Result<()> {
        let mut sleep_after_timeout = BASIC_TIMEOUT_SLEEP;

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
                    sleep_after_timeout = BASIC_TIMEOUT_SLEEP;
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
                Err(err) => {
                    // The assumption here is we want to have the CDC running when we encounter some transient errors.
                    // The rest of the logic will assume that we will still collect all data, even if we lag behind.
                    // Why not use the retry policy here? The rust driver does not support async retry policies,
                    // meaning we cannot delay the next retry when using the policy.
                    // On the other hand, we would prefer to have (exponential) backoff here, to avoid overloading the database,
                    // especially this CDC is the part of the problem (remember that we may have a few hundred streamIDs - and as a result
                    // create a few hundred requests to the database at a single moment).
                    if is_transient_error(&err) {
                        self.print_request_failure_warning(
                            &window_begin,
                            &window_end,
                            sleep_after_timeout,
                            page_no,
                            anyhow::Error::new(err),
                        )
                        .await;
                        // Waiting here is a bit suboptimal, as if this happens after generation change,
                        // we will still sleep, slowing down the process of opening streams for new generation.
                        // Those streams will be opened only after all instances of fetch_cdc return.
                        sleep(sleep_after_timeout).await;
                        sleep_after_timeout *= TIMEOUT_FACTOR;
                        if sleep_after_timeout >= self.config.sleep_interval {
                            sleep_after_timeout = self.config.sleep_interval;
                        }

                        next_state = state_clone;
                    } else {
                        return Err(anyhow::Error::new(err)
                            .context("Session returned an error while fetching CDC rows."));
                    }
                }
            }
        }
        Ok(())
    }

    async fn print_request_failure_warning(
        &self,
        window_begin: &Timestamp,
        window_end: &Timestamp,
        backoff: Duration,
        page_no: u64,
        driver_error: anyhow::Error,
    ) {
        if enabled!(tracing::Level::WARN) {
            let ids_str = self
                .stream_id_vec
                .iter()
                .map(|x| format!("0x{}", hex::encode(&x.id)))
                .join(", ");

            warn!(
                stream_ids = ids_str,
                window_begin_ms = window_begin.as_millis(),
                window_end_ms = window_end.as_millis(),
                page_no = page_no,
                current_backoff_ms = backoff.as_millis(),
                driver_error = format_args!("{:#}", driver_error),
                "Encountered a transient error while fetching CDC rows."
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use futures::stream::StreamExt;
    use rstest::rstest;
    use scylla::errors::ExecutionError;
    use scylla::errors::PrepareError;
    use scylla::errors::RequestAttemptError;
    use scylla::statement::unprepared::Statement;
    use scylla_cdc_test_utils::TEST_TABLE;
    use scylla_cdc_test_utils::now;
    use scylla_cdc_test_utils::populate_simple_db_with_pk;
    use scylla_cdc_test_utils::prepare_simple_db;
    use scylla_cdc_test_utils::skip_if_not_supported;
    use std::sync::atomic::AtomicIsize;
    use std::sync::atomic::Ordering::Relaxed;
    use std::time::SystemTime;
    use tokio::sync::Mutex;

    use super::*;

    const SECOND_IN_MILLIS: u64 = 1_000;
    const SLEEP_INTERVAL: u64 = SECOND_IN_MILLIS / 10;
    const WINDOW_SIZE: u64 = SECOND_IN_MILLIS / 10 * 3;
    const SAFETY_INTERVAL: u64 = SECOND_IN_MILLIS / 10;
    const START_TIME_DELAY_IN_SECONDS: i64 = 2;

    impl StreamReader {
        async fn set_upper_ts(&self, d: Duration) {
            self.set_upper_timestamp(Timestamp::from_duration_since_epoch(d))
                .await;
        }

        fn test_new(
            session: &Arc<Session>,
            stream_ids: Vec<StreamID>,
            start_timestamp: Duration,
            window_size: Duration,
            safety_interval: Duration,
            sleep_interval: Duration,
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

        let start_timestamp =
            now().saturating_sub(Duration::from_secs(START_TIME_DELAY_IN_SECONDS as u64));
        let sleep_interval = Duration::from_millis(SLEEP_INTERVAL);
        let window_size = Duration::from_millis(WINDOW_SIZE);
        let safety_interval = Duration::from_millis(SAFETY_INTERVAL);

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
            window_begin: &Timestamp,
            window_end: &Timestamp,
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
            .set_upper_ts(now().saturating_add(Duration::from_secs(1)))
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
            .set_upper_ts(now().saturating_add(Duration::from_secs(1)))
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
        let second_ago_timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("system time is before Unix epoch")
            .saturating_sub(Duration::from_secs(1));
        insert_before_upper_timestamp_query.set_timestamp(Some(
            i64::try_from(second_ago_timestamp.as_micros()).unwrap_or(i64::MAX),
        ));
        shared_session
            .query_unpaged(insert_before_upper_timestamp_query, ())
            .await
            .unwrap();

        let cdc_reader = get_test_stream_reader(&shared_session).await.unwrap();
        cdc_reader.set_upper_ts(now()).await;

        let mut insert_after_upper_timestamp_query = Statement::new(format!(
            "INSERT INTO {} (pk, t, v, s) VALUES ({}, {}, '{}', '{}');",
            TEST_TABLE, 0, 1, "val1", "static1"
        ));
        let second_later_timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("system time is before Unix epoch")
            .saturating_add(Duration::from_secs(1));
        insert_after_upper_timestamp_query.set_timestamp(Some(
            i64::try_from(second_later_timestamp.as_micros()).unwrap_or(i64::MAX),
        ));
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
        cdc_reader.config.sleep_interval = Duration::from_millis(1500);
        cdc_reader
            .set_upper_ts(now().saturating_add(Duration::from_secs(1)))
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
