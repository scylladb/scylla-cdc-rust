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
use crate::cdc_types::CqlTimestampExt;
use crate::cdc_types::GenerationTimestamp;
use crate::cdc_types::StreamID;
use crate::checkpoints::CDCCheckpointSaver;
use crate::checkpoints::Checkpoint;
use crate::checkpoints::start_saving_checkpoints;
use crate::consumer::CDCRow;
use crate::consumer::CDCRowSchema;
use crate::consumer::Consumer;
use scylla::value::CqlTimestamp;

const BASIC_TIMEOUT_SLEEP: tokio::time::Duration = tokio::time::Duration::from_millis(100);
const TIMEOUT_FACTOR: u32 = 2;

fn capped_window_end(
    window_begin: CqlTimestamp,
    window_size: Duration,
    cap: CqlTimestamp,
) -> CqlTimestamp {
    let remaining = cap
        .checked_duration_since(window_begin)
        .unwrap_or(Duration::ZERO);
    if window_size >= remaining {
        cap
    } else {
        // `window_size` is smaller than the representable distance to `cap`, so this cannot
        // overflow even when `window_begin` is close to `CqlTimestamp::MAX`.
        window_begin + window_size
    }
}

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
        window_begin: &CqlTimestamp,
        window_end: &CqlTimestamp,
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
        window_begin: &CqlTimestamp,
        window_end: &CqlTimestamp,
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
    // Authoritative public end/stop bound shared by every reader.
    end_timestamp_receiver: watch::Receiver<CqlTimestamp>,
    // Per-reader generation boundary or sibling-error stop bound.
    upper_timestamp: tokio::sync::Mutex<Option<CqlTimestamp>>,
    config: CDCReaderConfig,
}

impl StreamReader {
    pub fn new(
        session: &Arc<Session>,
        stream_ids: Vec<StreamID>,
        config: CDCReaderConfig,
        end_timestamp_receiver: watch::Receiver<CqlTimestamp>,
    ) -> StreamReader {
        StreamReader {
            session: session.clone(),
            stream_id_vec: stream_ids,
            end_timestamp_receiver,
            upper_timestamp: Default::default(),
            config,
        }
    }

    /// Sets or monotonically lowers the upper timestamp for the reader.
    ///
    /// This is a stop request, not immediate cancellation. If a logical paged request has already
    /// started, all of its continuation pages retain the request's original
    /// `[window_begin, window_end)` bounds and may deliver rows beyond a newly lowered timestamp.
    /// The new timestamp is observed before another logical request starts.
    pub(crate) async fn set_upper_timestamp(&self, ts: CqlTimestamp) {
        let mut guard = self.upper_timestamp.lock().await;
        *guard = Some(match *guard {
            Some(current) => cmp::min(current, ts),
            None => ts,
        });
    }

    /// Runs `decision` against the earliest stop bound while both bound guards remain held.
    /// Keeping the guards through the decision prevents either bound from being lowered between
    /// reading it and acting on it.
    async fn with_effective_upper_timestamp<T>(
        &self,
        decision: impl FnOnce(CqlTimestamp) -> T,
    ) -> T {
        let local_upper_timestamp = self.upper_timestamp.lock().await;
        let end_timestamp = self.end_timestamp_receiver.borrow();
        let effective_upper_timestamp = local_upper_timestamp.map_or(*end_timestamp, |timestamp| {
            cmp::min(timestamp, *end_timestamp)
        });

        decision(effective_upper_timestamp)
    }

    /// Checks whether `timestamp` has reached either the public end timestamp or the local
    /// generation/error cap.
    async fn upper_timestamp_reached(&self, timestamp: CqlTimestamp) -> bool {
        self.with_effective_upper_timestamp(|effective_upper_timestamp| {
            timestamp >= effective_upper_timestamp
        })
        .await
    }

    /// Atomically admits the next logical window against both stop bounds. Once this returns a
    /// window, later bound updates apply after all pages and retries for that window finish.
    async fn admit_window(
        &self,
        window_begin: CqlTimestamp,
        window_size: Duration,
        safe_to_read_until: CqlTimestamp,
    ) -> Option<CqlTimestamp> {
        self.with_effective_upper_timestamp(|effective_upper_timestamp| {
            if window_begin >= effective_upper_timestamp {
                return None;
            }

            let window_cap = cmp::min(safe_to_read_until, effective_upper_timestamp);
            Some(capped_window_end(window_begin, window_size, window_cap))
        })
        .await
    }

    /// Continuously fetches CDC rows from the specified keyspace.table and passes them to the provided consumer.
    /// This function returns when the upper timestamp is reached or an error other than the timeout occurs.
    /// By default the upper timestamp is not set, so the function continues indefinitely.
    /// You can set it using the [`set_upper_timestamp`](Self::set_upper_timestamp) method.
    ///
    /// An upper timestamp is a stop request, not immediate cancellation. Each time window is one
    /// logical request that includes its initial page and all paging continuations. Once such a
    /// request starts, it completes with its original `[window_begin, window_end)` bounds before a
    /// newly lowered upper timestamp is observed, so its remaining rows are still passed to the
    /// consumer.
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

        let mut window_begin = CqlTimestamp::from_duration_since_epoch(self.config.lower_timestamp);
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
            let mut loaded_timestamp = CqlTimestamp::MAX;
            for stream in &self.stream_id_vec {
                if let Some(ts) = self
                    .config
                    .checkpoint_saver
                    .as_ref()
                    .unwrap()
                    .load_last_checkpoint(stream)
                    .await?
                    .map(CqlTimestamp::from_duration_since_epoch)
                {
                    loaded_timestamp = loaded_timestamp.min(ts);
                }
            }
            if loaded_timestamp != CqlTimestamp::MAX {
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

        let mut now_timestamp = CqlTimestamp::now();
        // Calculate the timestamp until which it is safe to read.
        // We should not read data newer than (now - `safety_interval`),
        // because clock drift and various kinds of latency may influence too recent results.
        let mut safe_to_read_until = now_timestamp - safety_interval;

        // Initial gate: avoid entering the initial safety wait when a preset bound already stops
        // the reader. The first `window_begin` is set by the user (or loaded from checkpoint).
        if !self.upper_timestamp_reached(window_begin).await && window_begin > safe_to_read_until {
            // If it is too close to the current time, we wait until we can start reading.
            // This is done to prevent errors such as reading out-of-order changes, or skipping some changes.
            if window_begin > now_timestamp {
                // If it it's in the future, we issue an error message to make it clear that wrong timestamp was set up as `window_begin`.
                // Then we wait before starting to read, to satisfy safety interval.
                error!(
                    requested_begin_ms_timestamp = window_begin.0,
                    current_ms_timestamp = now_timestamp.0,
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
                    requested_begin_ms_timestamp = window_begin.0,
                    current_ms_timestamp = now_timestamp.0,
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
            // Post-wait gate: observe bounds changed during the initial safety wait, an
            // inter-window sleep, or a clock-regression sleep before doing more work.
            if self.upper_timestamp_reached(window_begin).await {
                break;
            }

            now_timestamp = CqlTimestamp::now();
            safe_to_read_until = now_timestamp - safety_interval;

            // The only possible way for this to happen is when the current `now_timestamp` is less than the previous `now_timestamp`.
            // This is because we possibly waited before the loop to ensure `window_begin <= safe_to_read_until`.
            // This means TIME TRAVEL! But still we have to handle it gracefully.
            // In this case, we again wait until the time is safe to read.
            if window_begin > safe_to_read_until {
                error!(
                    last_request_end_ms = window_begin.0,
                    current_timestamp_ms = now_timestamp.0,
                    expected_window_end_ms = safe_to_read_until.0,
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

            // Pre-request gate: synchronously admit the next logical paged request against the
            // public end timestamp, the local generation/error cap, and the safety frontier. A
            // bound lowered after admission applies once all of this window's pages finish.
            let Some(window_end) = self
                .admit_window(window_begin, window_size, safe_to_read_until)
                .await
            else {
                break;
            };

            self.fetch_and_consume_rows(&query_base, &mut consumer, window_begin, window_end)
                .await?;

            // Post-request gate: the completed logical request includes every continuation page;
            // only now can a bound lowered while it was in flight stop the reader.
            if self.upper_timestamp_reached(window_end).await {
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

    /// Executes one logical CDC request for the fixed `[window_begin, window_end)` time window.
    ///
    /// The request includes the initial page and every paging continuation. All page round trips
    /// retain the original window parameters. In case of timeouts, the current page is retried
    /// with exponential backoff.
    async fn fetch_and_consume_rows(
        &self,
        query_base: &PreparedStatement,
        consumer: &mut Box<dyn Consumer>,
        window_begin: CqlTimestamp,
        window_end: CqlTimestamp,
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
        window_begin: &CqlTimestamp,
        window_end: &CqlTimestamp,
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
                window_begin_ms = window_begin.0,
                window_end_ms = window_end.0,
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
    use std::sync::atomic::AtomicUsize;
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
            self.set_upper_timestamp(CqlTimestamp::from_duration_since_epoch(d))
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
            Self::test_new_with_session(
                session.clone(),
                stream_ids,
                start_timestamp,
                window_size,
                safety_interval,
                sleep_interval,
            )
        }

        fn test_new_with_session(
            session: Arc<dyn StreamSession>,
            stream_ids: Vec<StreamID>,
            start_timestamp: Duration,
            window_size: Duration,
            safety_interval: Duration,
            sleep_interval: Duration,
        ) -> StreamReader {
            let (_, end_timestamp_receiver) = watch::channel(CqlTimestamp::MAX);
            Self::test_new_with_session_and_end_timestamp(
                session,
                stream_ids,
                start_timestamp,
                window_size,
                safety_interval,
                sleep_interval,
                end_timestamp_receiver,
            )
        }

        fn test_new_with_session_and_end_timestamp(
            session: Arc<dyn StreamSession>,
            stream_ids: Vec<StreamID>,
            start_timestamp: Duration,
            window_size: Duration,
            safety_interval: Duration,
            sleep_interval: Duration,
            end_timestamp_receiver: watch::Receiver<CqlTimestamp>,
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
                session,
                stream_id_vec: stream_ids,
                end_timestamp_receiver,
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
            self.fetched_rows.lock().await.push((
                data.take_value("pk").unwrap().as_int().unwrap(),
                data.take_value("s").unwrap().as_text().unwrap().to_string(),
                data.take_value("t").unwrap().as_int().unwrap(),
                data.take_value("v").unwrap().as_text().unwrap().to_string(),
            ));
            Ok(())
        }
    }

    struct LoweringEndTimestampConsumer {
        rows_consumed: Arc<AtomicUsize>,
        end_timestamp_sender: Option<watch::Sender<CqlTimestamp>>,
    }

    #[async_trait]
    impl Consumer for LoweringEndTimestampConsumer {
        async fn consume_cdc(&mut self, _data: CDCRow<'_>) -> anyhow::Result<()> {
            self.rows_consumed.fetch_add(1, Relaxed);
            if let Some(sender) = self.end_timestamp_sender.take() {
                sender.send(CqlTimestamp::MIN).unwrap();
            }
            Ok(())
        }
    }

    struct TimeoutSession {
        session: Arc<Session>,
        counter: Arc<AtomicIsize>,
    }

    struct UnusedSession;

    #[async_trait]
    impl StreamSession for UnusedSession {
        async fn prepare_statement(
            &self,
            _query: String,
        ) -> Result<PreparedStatement, PrepareError> {
            unreachable!()
        }

        async fn execute_paged_statement(
            &self,
            _statement: &PreparedStatement,
            _ids: &[StreamID],
            _window_begin: &CqlTimestamp,
            _window_end: &CqlTimestamp,
            _paging_state: PagingState,
        ) -> Result<(QueryResult, PagingStateResponse), ExecutionError> {
            unreachable!()
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum RecordedPagingState {
        Initial,
        Continuation,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct RecordedPageRequest {
        window_begin: CqlTimestamp,
        window_end: CqlTimestamp,
        paging_state: RecordedPagingState,
    }

    struct RecordingSession {
        session: Arc<Session>,
        page_size: Option<i32>,
        page_requests: Arc<Mutex<Vec<RecordedPageRequest>>>,
    }

    #[async_trait]
    impl StreamSession for RecordingSession {
        async fn prepare_statement(
            &self,
            query: String,
        ) -> Result<PreparedStatement, PrepareError> {
            let mut statement = self.session.prepare(query).await?;
            if let Some(page_size) = self.page_size {
                statement.set_page_size(page_size);
            }
            Ok(statement)
        }

        async fn execute_paged_statement(
            &self,
            statement: &PreparedStatement,
            ids: &[StreamID],
            window_begin: &CqlTimestamp,
            window_end: &CqlTimestamp,
            paging_state: PagingState,
        ) -> Result<(QueryResult, PagingStateResponse), ExecutionError> {
            let recorded_paging_state = if paging_state.as_bytes_slice().is_none() {
                RecordedPagingState::Initial
            } else {
                RecordedPagingState::Continuation
            };
            let result = self
                .session
                .execute_single_page(statement, (ids, window_begin, window_end), paging_state)
                .await?;
            self.page_requests.lock().await.push(RecordedPageRequest {
                window_begin: *window_begin,
                window_end: *window_end,
                paging_state: recorded_paging_state,
            });
            Ok(result)
        }
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
            window_begin: &CqlTimestamp,
            window_end: &CqlTimestamp,
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

    #[tokio::test]
    async fn upper_bounds_are_monotonic_and_gate_window_admission() {
        let (end_timestamp_sender, end_timestamp_receiver) = watch::channel(CqlTimestamp(300));
        let reader = StreamReader::test_new_with_session_and_end_timestamp(
            Arc::new(UnusedSession),
            Vec::new(),
            Duration::ZERO,
            Duration::from_millis(100),
            Duration::ZERO,
            Duration::ZERO,
            end_timestamp_receiver,
        );
        reader.set_upper_timestamp(CqlTimestamp(200)).await;
        reader.set_upper_timestamp(CqlTimestamp(300)).await;

        assert_eq!(
            reader
                .admit_window(
                    CqlTimestamp(150),
                    Duration::from_millis(100),
                    CqlTimestamp(500),
                )
                .await,
            Some(CqlTimestamp(200)),
            "a later local cap must not replace an earlier one"
        );

        reader.set_upper_timestamp(CqlTimestamp(100)).await;
        assert_eq!(
            reader
                .admit_window(
                    CqlTimestamp(50),
                    Duration::from_millis(100),
                    CqlTimestamp(500),
                )
                .await,
            Some(CqlTimestamp(100)),
            "an earlier local cap must lower the admitted window"
        );

        end_timestamp_sender.send(CqlTimestamp(75)).unwrap();

        assert_eq!(
            *reader.upper_timestamp.lock().await,
            Some(CqlTimestamp(100)),
            "the public bound remains separate from the local generation cap"
        );
        assert_eq!(
            reader
                .admit_window(
                    CqlTimestamp(50),
                    Duration::from_millis(100),
                    CqlTimestamp(500),
                )
                .await,
            Some(CqlTimestamp(75)),
            "the public bound caps an admitted window directly"
        );
        assert_eq!(
            reader
                .admit_window(
                    CqlTimestamp(75),
                    Duration::from_millis(100),
                    CqlTimestamp(500),
                )
                .await,
            None,
            "no logical window starts at the public bound"
        );
    }

    #[test]
    fn window_end_is_capped_without_overflow() {
        assert_eq!(
            capped_window_end(
                CqlTimestamp(i64::MAX - 10),
                Duration::from_millis(20),
                CqlTimestamp::MAX,
            ),
            CqlTimestamp::MAX
        );
    }

    #[tokio::test]
    async fn query_windows_stop_at_upper_timestamp() {
        let (shared_session, ks) = prepare_simple_db(false).await.unwrap();
        populate_simple_db_with_pk(&shared_session, 0)
            .await
            .unwrap();
        let stream_ids = get_cdc_stream_id(&shared_session).await.unwrap();

        let page_requests = Arc::new(Mutex::new(Vec::new()));
        let recording_session: Arc<dyn StreamSession> = Arc::new(RecordingSession {
            session: shared_session,
            page_size: None,
            page_requests: Arc::clone(&page_requests),
        });
        let start_timestamp = now().saturating_sub(Duration::from_secs(2));
        let upper_timestamp = start_timestamp + Duration::from_millis(50);
        let start = CqlTimestamp::from_duration_since_epoch(start_timestamp);
        let upper = CqlTimestamp::from_duration_since_epoch(upper_timestamp);

        for (reader_start, expected_requests) in [
            (
                start_timestamp,
                vec![RecordedPageRequest {
                    window_begin: start,
                    window_end: upper,
                    paging_state: RecordedPagingState::Initial,
                }],
            ),
            (upper_timestamp, vec![]),
        ] {
            page_requests.lock().await.clear();
            let reader = StreamReader::test_new_with_session(
                Arc::clone(&recording_session),
                stream_ids.clone(),
                reader_start,
                Duration::from_secs(1),
                Duration::ZERO,
                Duration::ZERO,
            );
            reader.set_upper_ts(upper_timestamp).await;
            reader
                .fetch_cdc(
                    ks.clone(),
                    TEST_TABLE.to_string(),
                    Box::new(FetchTestConsumer {
                        fetched_rows: Arc::new(Mutex::new(Vec::new())),
                    }),
                )
                .await
                .unwrap();

            assert_eq!(*page_requests.lock().await, expected_requests);
        }
    }

    #[tokio::test]
    async fn lowered_public_end_timestamp_finishes_in_flight_paged_window() {
        let (shared_session, ks) = prepare_simple_db(false).await.unwrap();
        let start_timestamp = now().saturating_sub(Duration::from_secs(2));
        populate_simple_db_with_pk(&shared_session, 0)
            .await
            .unwrap();
        // The query's upper bound has millisecond precision and is exclusive. Let the database
        // clock move past the final insert so every fixture row falls inside the first window.
        sleep(Duration::from_millis(100)).await;
        let stream_ids = get_cdc_stream_id(&shared_session).await.unwrap();

        let expected_window_begin = CqlTimestamp::from_duration_since_epoch(start_timestamp);
        let page_requests = Arc::new(Mutex::new(Vec::new()));
        let recording_session: Arc<dyn StreamSession> = Arc::new(RecordingSession {
            session: shared_session,
            page_size: Some(1),
            page_requests: Arc::clone(&page_requests),
        });
        let (end_timestamp_sender, end_timestamp_receiver) = watch::channel(CqlTimestamp::MAX);
        let reader = StreamReader::test_new_with_session_and_end_timestamp(
            recording_session,
            stream_ids,
            start_timestamp,
            Duration::from_secs(60),
            Duration::ZERO,
            Duration::ZERO,
            end_timestamp_receiver,
        );

        let rows_consumed = Arc::new(AtomicUsize::new(0));
        let consumer = Box::new(LoweringEndTimestampConsumer {
            rows_consumed: Arc::clone(&rows_consumed),
            end_timestamp_sender: Some(end_timestamp_sender),
        });

        tokio::time::timeout(
            Duration::from_secs(30),
            reader.fetch_cdc(ks, TEST_TABLE.to_string(), consumer),
        )
        .await
        .expect("reader did not finish its in-flight paged window within 30 seconds")
        .unwrap();

        let requests = page_requests.lock().await;
        assert!(
            requests.len() >= 3,
            "page size one should require multiple physical pages for three rows"
        );
        assert_eq!(requests[0].window_begin, expected_window_begin);
        let original_bounds = (requests[0].window_begin, requests[0].window_end);
        assert!(
            requests
                .iter()
                .all(|request| { (request.window_begin, request.window_end) == original_bounds })
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.paging_state == RecordedPagingState::Initial)
                .count(),
            1,
            "lowering the bound must not start another logical window"
        );
        assert_eq!(requests[0].paging_state, RecordedPagingState::Initial);
        assert!(
            requests[1..]
                .iter()
                .all(|request| { request.paging_state == RecordedPagingState::Continuation })
        );
        drop(requests);

        assert_eq!(rows_consumed.load(Relaxed), 3);
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
