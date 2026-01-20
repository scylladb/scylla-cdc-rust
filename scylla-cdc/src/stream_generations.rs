use async_trait::async_trait;
use futures::FutureExt;
use futures::stream::StreamExt;
use futures::{TryStreamExt, future::RemoteHandle};
use scylla::client::session::Session;
use scylla::statement::Consistency;
use scylla::statement::unprepared::Statement;
use scylla::value;
use std::sync::Arc;
use std::time::{self, Duration};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::warn;

use crate::cdc_types::{GenerationTimestamp, StreamID};

/// Component responsible for managing stream generations.
#[async_trait]
pub(crate) trait GenerationFetcher: Send + Sync + 'static {
    /// In case of a success returns a vector containing all the generations in the database.
    /// Propagates all errors.
    async fn fetch_all_generations(&self) -> anyhow::Result<Vec<GenerationTimestamp>>;

    /// Given a timestamp of an operation fetch generation that was operating when this operation was performed.
    /// If no such generation exists, returns `None`.
    /// Propagates errors.
    async fn fetch_generation_by_timestamp(
        &self,
        time: &Duration,
    ) -> anyhow::Result<Option<GenerationTimestamp>>;

    /// Given a generation returns the next generation.
    /// If given generation is currently operating, returns `None`.
    /// Propagates errors.
    async fn fetch_next_generation(
        &self,
        generation: &GenerationTimestamp,
    ) -> anyhow::Result<Option<GenerationTimestamp>>;

    /// Given a generation return grouped identifiers of all streams of this generation.
    async fn fetch_stream_ids(
        &self,
        generation: &GenerationTimestamp,
    ) -> anyhow::Result<Vec<Vec<StreamID>>>;

    /// Fetches all generations until at least one is found, then returns the latest one.
    async fn get_latest_generation(&self, sleep_interval: time::Duration) -> GenerationTimestamp {
        loop {
            match self.fetch_all_generations().await {
                Ok(vectors) => match vectors.into_iter().next_back() {
                    None => sleep(sleep_interval).await,
                    Some(generation) => break generation,
                },
                Err(err) => {
                    warn!("Failed to fetch all generations: {err}");
                    sleep(sleep_interval).await
                }
            }
        }
    }

    /// Continuously monitors and fetches new generations as they become available.
    /// Returns a receiver for new generations and a handle to the background task.
    async fn fetch_generations_continuously(
        self: Arc<Self>,
        start_timestamp: Duration,
        sleep_interval: time::Duration,
    ) -> anyhow::Result<(mpsc::Receiver<GenerationTimestamp>, RemoteHandle<()>)> {
        let (generation_sender, generation_receiver) = mpsc::channel(1);

        let (future, future_handle) = async move {
            let mut generation = loop {
                match self.fetch_generation_by_timestamp(&start_timestamp).await {
                    Ok(Some(generation)) => break generation,
                    Ok(None) => {
                        break self.get_latest_generation(sleep_interval).await;
                    }
                    Err(err) => {
                        warn!("Failed to fetch generation by timestamp: {err}");
                        sleep(sleep_interval).await
                    }
                }
            };
            if generation_sender.send(generation.clone()).await.is_err() {
                return;
            }

            loop {
                generation = loop {
                    match self.fetch_next_generation(&generation).await {
                        Ok(Some(generation)) => break generation,
                        Ok(None) => sleep(sleep_interval).await,
                        Err(err) => {
                            warn!("Failed to fetch next generation: {err}");
                            sleep(sleep_interval).await
                        }
                    }
                };
                if generation_sender.send(generation.clone()).await.is_err() {
                    break;
                }
            }
        }
        .remote_handle();
        tokio::spawn(future);
        Ok((generation_receiver, future_handle))
    }
}

/// implementation of GenerationFetcher for vnodes-based keyspaces.
pub struct VnodeGenerationFetcher {
    generations_table_name: String,
    streams_table_name: String,
    session: Arc<Session>,
}

// Number taken from: https://www.scylladb.com/2017/11/17/7-rules-planning-queries-maximum-performance/.
const DEFAULT_PAGE_SIZE: i32 = 5000;

const VNODE_GENERATIONS_TABLE: &str = "system_distributed.cdc_generation_timestamps";
const VNODE_STREAMS_TABLE: &str = "system_distributed.cdc_streams_descriptions_v2";

impl VnodeGenerationFetcher {
    pub fn new(session: &Arc<Session>) -> VnodeGenerationFetcher {
        VnodeGenerationFetcher {
            generations_table_name: VNODE_GENERATIONS_TABLE.to_string(),
            streams_table_name: VNODE_STREAMS_TABLE.to_string(),
            session: Arc::clone(session),
        }
    }

    // Function instead of constant for testing purposes.
    fn get_all_stream_generations_query(&self) -> String {
        format!(
            "
        SELECT time
        FROM {}
        WHERE key = 'timestamps';",
            self.generations_table_name
        )
    }

    fn get_generation_by_timestamp_query(&self) -> String {
        format!(
            "
        SELECT time
        FROM {}
        WHERE key = 'timestamps'
        AND time <= ?
        ORDER BY time DESC
        LIMIT 1;",
            self.generations_table_name
        )
    }

    fn get_next_generation_query(&self) -> String {
        format!(
            "
        SELECT time
        FROM {}
        WHERE key = 'timestamps'
        AND time > ?
        ORDER BY time ASC
        LIMIT 1;",
            self.generations_table_name
        )
    }

    fn get_stream_ids_by_time_query(&self) -> String {
        format!(
            "
        SELECT streams
        FROM {}
        WHERE time = ?;",
            self.streams_table_name
        )
    }
}

#[async_trait]
impl GenerationFetcher for VnodeGenerationFetcher {
    async fn fetch_all_generations(&self) -> anyhow::Result<Vec<GenerationTimestamp>> {
        let mut generations = Vec::new();
        let mut query =
            new_distributed_system_query(self.get_all_stream_generations_query(), &self.session)
                .await?;
        query.set_page_size(DEFAULT_PAGE_SIZE);

        let mut rows = self
            .session
            .query_iter(query, &[])
            .await?
            .rows_stream::<(GenerationTimestamp,)>()?;

        while let Some(generation) = rows.next().await {
            generations.push(generation?.0)
        }

        Ok(generations)
    }

    async fn fetch_generation_by_timestamp(
        &self,
        time: &Duration,
    ) -> anyhow::Result<Option<GenerationTimestamp>> {
        let query =
            new_distributed_system_query(self.get_generation_by_timestamp_query(), &self.session)
                .await?;

        let result = self
            .session
            .query_unpaged(query, (value::CqlTimestamp(time.as_millis() as i64),))
            .await?
            .into_rows_result()?
            .maybe_first_row::<(GenerationTimestamp,)>()?
            .map(|(ts,)| ts);

        Ok(result)
    }

    async fn fetch_next_generation(
        &self,
        generation: &GenerationTimestamp,
    ) -> anyhow::Result<Option<GenerationTimestamp>> {
        let query =
            new_distributed_system_query(self.get_next_generation_query(), &self.session).await?;

        let result = self
            .session
            .query_unpaged(query, (generation,))
            .await?
            .into_rows_result()?
            .maybe_first_row::<(GenerationTimestamp,)>()?
            .map(|(ts,)| ts);

        Ok(result)
    }

    // Streams are grouped by vnodes
    async fn fetch_stream_ids(
        &self,
        generation: &GenerationTimestamp,
    ) -> anyhow::Result<Vec<Vec<StreamID>>> {
        let mut result_vec = Vec::new();

        let mut query =
            new_distributed_system_query(self.get_stream_ids_by_time_query(), &self.session)
                .await?;
        query.set_page_size(DEFAULT_PAGE_SIZE);

        let mut rows = self
            .session
            .query_iter(query, (generation,))
            .await?
            .rows_stream::<(Vec<StreamID>,)>()?;

        while let Some(next_row) = rows.next().await {
            let (ids,) = next_row?;
            result_vec.push(ids);
        }

        Ok(result_vec)
    }
}

/// implementation of GenerationFetcher for tablets-based keyspaces.
pub struct TabletsGenerationFetcher {
    timestamps_table_name: String,
    streams_table_name: String,
    keyspace_name: String,
    table_name: String,
    session: Arc<Session>,
}

// follows stream_state in system.cdc_streams
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i8)]
enum StreamState {
    Current = 0,
    #[allow(dead_code)]
    Closed = 1,
    #[allow(dead_code)]
    Opened = 2,
}

const TABLETS_TIMESTAMPS_TABLE: &str = "system.cdc_timestamps";
const TABLETS_STREAMS_TABLE: &str = "system.cdc_streams";

impl TabletsGenerationFetcher {
    pub fn new(
        session: &Arc<Session>,
        keyspace_name: String,
        table_name: String,
    ) -> TabletsGenerationFetcher {
        TabletsGenerationFetcher {
            timestamps_table_name: TABLETS_TIMESTAMPS_TABLE.to_string(),
            streams_table_name: TABLETS_STREAMS_TABLE.to_string(),
            keyspace_name,
            table_name,
            session: Arc::clone(session),
        }
    }

    fn get_all_stream_generations_query(&self) -> String {
        format!(
            r#"
        SELECT timestamp
        FROM {}
        WHERE keyspace_name = ? AND table_name = ?;
        "#,
            self.timestamps_table_name
        )
    }

    fn get_generation_by_timestamp_query(&self) -> String {
        format!(
            r#"
        SELECT timestamp
        FROM {}
        WHERE keyspace_name = ? AND table_name = ? AND timestamp <= ?
        ORDER BY timestamp DESC
        LIMIT 1;
        "#,
            self.timestamps_table_name
        )
    }

    fn get_next_generation_query(&self) -> String {
        format!(
            r#"
        SELECT timestamp
        FROM {}
        WHERE keyspace_name = ? AND table_name = ? AND timestamp > ?
        ORDER BY timestamp ASC
        LIMIT 1;
        "#,
            self.timestamps_table_name
        )
    }

    fn get_stream_ids_by_time_query(&self) -> String {
        format!(
            r#"
        SELECT stream_id
        FROM {}
        WHERE keyspace_name = ? AND table_name = ? AND timestamp = ? AND stream_state = ?;
        "#,
            self.streams_table_name
        )
    }
}

#[async_trait]
impl GenerationFetcher for TabletsGenerationFetcher {
    async fn fetch_all_generations(&self) -> anyhow::Result<Vec<GenerationTimestamp>> {
        let mut query = Statement::new(self.get_all_stream_generations_query());
        query.set_page_size(DEFAULT_PAGE_SIZE);

        let generations = self
            .session
            .query_iter(query, (&self.keyspace_name, &self.table_name))
            .await?
            .rows_stream::<(GenerationTimestamp,)>()?
            .map(|r| r.map(|(ts,)| ts))
            .try_collect::<Vec<_>>()
            .await?;

        Ok(generations)
    }

    async fn fetch_generation_by_timestamp(
        &self,
        time: &Duration,
    ) -> anyhow::Result<Option<GenerationTimestamp>> {
        let query = Statement::new(self.get_generation_by_timestamp_query());

        let result = self
            .session
            .query_unpaged(
                query,
                (
                    &self.keyspace_name,
                    &self.table_name,
                    value::CqlTimestamp(time.as_millis() as i64),
                ),
            )
            .await?
            .into_rows_result()?
            .maybe_first_row::<(GenerationTimestamp,)>()?
            .map(|(ts,)| ts);

        Ok(result)
    }

    async fn fetch_next_generation(
        &self,
        generation: &GenerationTimestamp,
    ) -> anyhow::Result<Option<GenerationTimestamp>> {
        let query = Statement::new(self.get_next_generation_query());

        let result = self
            .session
            .query_unpaged(query, (&self.keyspace_name, &self.table_name, generation))
            .await?
            .into_rows_result()?
            .maybe_first_row::<(GenerationTimestamp,)>()?
            .map(|(ts,)| ts);

        Ok(result)
    }

    async fn fetch_stream_ids(
        &self,
        generation: &GenerationTimestamp,
    ) -> anyhow::Result<Vec<Vec<StreamID>>> {
        let mut query = Statement::new(self.get_stream_ids_by_time_query());
        query.set_page_size(DEFAULT_PAGE_SIZE);

        let result = self
            .session
            .query_iter(
                query,
                (
                    &self.keyspace_name,
                    &self.table_name,
                    generation,
                    StreamState::Current as i8,
                ),
            )
            .await?
            .rows_stream::<(StreamID,)>()?
            .map(|r| r.map(|(id,)| vec![id]))
            .try_collect::<Vec<_>>()
            .await?;

        Ok(result)
    }
}

// Returns current cluster size in case of a success.
async fn get_cluster_size(session: &Session) -> anyhow::Result<usize> {
    // We are using default consistency here since the system keyspace is special and
    // the coordinator which handles the query will only read local data
    // and will not contact other nodes, so the query will work with any cluster size larger than 0.
    let (peers_num,) = session
        .query_unpaged("SELECT COUNT(*) FROM system.peers", &[])
        .await?
        .into_rows_result()?
        .first_row::<(i64,)>()?;

    // Query returns a number of peers in a cluster, so we need to add 1 to count current node.
    Ok(peers_num as usize + 1)
}

// Choose appropriate consistency level depending on the cluster size.
async fn select_consistency(session: &Session, query: &mut Statement) -> anyhow::Result<()> {
    query.set_consistency(match get_cluster_size(session).await? {
        1 => Consistency::One,
        _ => Consistency::Quorum,
    });
    Ok(())
}

async fn new_distributed_system_query(
    stmt: String,
    session: &Session,
) -> anyhow::Result<Statement> {
    let mut query = Statement::new(stmt);
    select_consistency(session, &mut query).await?;

    Ok(query)
}

pub fn get_generation_fetcher(
    session: &Arc<scylla::client::session::Session>,
    keyspace: &str,
    table_name: &str,
    uses_tablets: bool,
) -> Arc<dyn GenerationFetcher> {
    if uses_tablets {
        Arc::new(TabletsGenerationFetcher::new(
            session,
            keyspace.to_string(),
            table_name.to_string(),
        ))
    } else {
        Arc::new(VnodeGenerationFetcher::new(session))
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use rstest::rstest;
    use scylla_cdc_test_utils::prepare_db;

    use super::*;

    const GENERATION_NEW_MILLISECONDS: i64 = 1635882326384;
    const GENERATION_OLD_MILLISECONDS: i64 = 1635882224341;
    const TEST_STREAM_1: &str = "0x7fb9f781956cea08c651295720000001";
    const TEST_STREAM_2: &str = "0x7fc0000000000000c298b9f168000001";

    mod vnode_tests {
        use super::*;

        const TEST_STREAM_TABLE: &str = "cdc_streams_descriptions_v2";
        const TEST_GENERATION_TABLE: &str = "cdc_generation_timestamps";

        impl VnodeGenerationFetcher {
            // Constructor intended for testing purposes.
            fn test_new(session: &Arc<Session>) -> VnodeGenerationFetcher {
                VnodeGenerationFetcher {
                    streams_table_name: TEST_STREAM_TABLE.to_string(),
                    generations_table_name: TEST_GENERATION_TABLE.to_string(),
                    session: Arc::clone(session),
                }
            }
        }

        // Constructs mock table with the same schema as the original one's.
        fn construct_generation_table_query() -> String {
            format!(
                "
    CREATE TABLE IF NOT EXISTS {TEST_GENERATION_TABLE}(
    key text,
    time timestamp,
    expired timestamp,
    PRIMARY KEY (key, time)
) WITH CLUSTERING ORDER BY (time DESC);"
            )
        }

        // Constructs mock table with the same schema as the original one's.
        fn construct_stream_table_query() -> String {
            format!(
                "
    CREATE TABLE IF NOT EXISTS {TEST_STREAM_TABLE} (
    time timestamp,
    range_end bigint,
    streams frozen<set<blob>>,
    PRIMARY KEY (time, range_end)
) WITH CLUSTERING ORDER BY (range_end ASC);",
            )
        }

        pub(super) async fn insert_generation_timestamp(session: &Session, generation: i64) {
            let query = new_distributed_system_query(
            format!(
                "INSERT INTO {TEST_GENERATION_TABLE} (key, time, expired) VALUES ('timestamps', ?, NULL);",
            ),
            session,
        )
        .await
        .unwrap();

            session
                .query_unpaged(query, (value::CqlTimestamp(generation),))
                .await
                .unwrap();
        }

        // Populate test tables with given data.
        async fn populate_test_db(session: &Session) {
            let stream_generation = value::CqlTimestamp(GENERATION_NEW_MILLISECONDS);

            for generation in &[GENERATION_NEW_MILLISECONDS, GENERATION_OLD_MILLISECONDS] {
                insert_generation_timestamp(session, *generation).await;
            }

            let query = new_distributed_system_query(
            format!(
                "INSERT INTO {TEST_STREAM_TABLE}(time, range_end, streams) VALUES (?, -1, {{{TEST_STREAM_1}, {TEST_STREAM_2}}});"
            ),
            session,
            )
            .await
            .unwrap();

            session
                .query_unpaged(query, (stream_generation,))
                .await
                .unwrap();
        }

        // Create setup for tests.
        pub(super) async fn setup() -> anyhow::Result<VnodeGenerationFetcher> {
            let session = prepare_db(
                &[
                    construct_generation_table_query(),
                    construct_stream_table_query(),
                ],
                1,
                false,
            )
            .await?
            .0;
            populate_test_db(&session).await;

            let generation_fetcher = VnodeGenerationFetcher::test_new(&session);

            Ok(generation_fetcher)
        }
    }

    mod tablets_tests {
        use super::*;

        const TEST_KEYSPACE: &str = "test_tablets_ks";
        const TEST_TABLE: &str = "test_tablets_table";

        impl TabletsGenerationFetcher {
            // Constructor intended for testing purposes.
            fn test_new(
                session: &Arc<Session>,
                keyspace: &str,
                table: &str,
            ) -> TabletsGenerationFetcher {
                TabletsGenerationFetcher {
                    timestamps_table_name: "cdc_timestamps".to_string(),
                    streams_table_name: "cdc_streams".to_string(),
                    keyspace_name: keyspace.to_string(),
                    table_name: table.to_string(),
                    session: Arc::clone(session),
                }
            }
        }

        pub(super) async fn insert_generation_timestamp(session: &Session, timestamp: i64) {
            let query = new_distributed_system_query(
                "INSERT INTO cdc_timestamps (keyspace_name, table_name, timestamp) VALUES (?, ?, ?);".to_string(), session)
                .await
                .unwrap();

            session
                .query_unpaged(
                    query,
                    (TEST_KEYSPACE, TEST_TABLE, value::CqlTimestamp(timestamp)),
                )
                .await
                .unwrap();
        }

        // TabletsGenerationFetcher tests
        pub(super) async fn setup() -> anyhow::Result<TabletsGenerationFetcher> {
            let session = Arc::new(prepare_db(&[], 1, false).await?.0);
            // Create CDC tables for tablets in the current keyspace
            session
                .query_unpaged(
                    "CREATE TABLE IF NOT EXISTS cdc_timestamps (
                keyspace_name text,
                table_name text,
                timestamp timestamp,
                PRIMARY KEY ((keyspace_name, table_name), timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC);",
                    &[],
                )
                .await?;
            session
                .query_unpaged(
                    "CREATE TABLE IF NOT EXISTS cdc_streams (
                keyspace_name text,
                table_name text,
                timestamp timestamp,
                stream_state tinyint,
                stream_id blob,
                PRIMARY KEY ((keyspace_name, table_name), timestamp, stream_state, stream_id)
            ) WITH CLUSTERING ORDER BY (timestamp ASC, stream_state ASC, stream_id ASC);",
                    &[],
                )
                .await?;

            // Insert generations
            for ts in &[GENERATION_NEW_MILLISECONDS, GENERATION_OLD_MILLISECONDS] {
                insert_generation_timestamp(&session, *ts).await;
            }

            // Insert streams
            for sid in &[TEST_STREAM_1, TEST_STREAM_2] {
                let stream_id = hex::decode(sid.strip_prefix("0x").unwrap()).unwrap();
                let query = new_distributed_system_query(
                    "INSERT INTO cdc_streams (keyspace_name, table_name, timestamp, stream_state, stream_id) VALUES (?, ?, ?, ?, ?);".to_string(),
                    &session,
                )
                .await
                .unwrap();

                for st in &[StreamState::Current, StreamState::Opened] {
                    session
                        .query_unpaged(
                            query.clone(),
                            (
                                TEST_KEYSPACE,
                                TEST_TABLE,
                                value::CqlTimestamp(GENERATION_NEW_MILLISECONDS),
                                *st as i8,
                                stream_id.clone(),
                            ),
                        )
                        .await
                        .unwrap();
                }
            }

            Ok(TabletsGenerationFetcher::test_new(
                &session,
                TEST_KEYSPACE,
                TEST_TABLE,
            ))
        }
    }

    // helper function to setup and get appropriate GenerationFetcher and Session for tests
    async fn setup(
        tablets_enabled: bool,
    ) -> anyhow::Result<(Arc<dyn GenerationFetcher>, Arc<Session>)> {
        if tablets_enabled {
            let fetcher = tablets_tests::setup().await?;
            let session = Arc::clone(&fetcher.session);
            let fetcher_arc: Arc<dyn GenerationFetcher> = Arc::new(fetcher);
            Ok((fetcher_arc, session))
        } else {
            let fetcher = vnode_tests::setup().await?;
            let session = Arc::clone(&fetcher.session);
            let fetcher_arc: Arc<dyn GenerationFetcher> = Arc::new(fetcher);
            Ok((fetcher_arc, session))
        }
    }

    // helper function to insert a new generation
    async fn insert_generation_timestamp(
        session: &Session,
        tablets_enabled: bool,
        generation: i64,
    ) {
        if tablets_enabled {
            tablets_tests::insert_generation_timestamp(session, generation).await;
        } else {
            vnode_tests::insert_generation_timestamp(session, generation).await;
        }
    }

    #[rstest]
    #[case::vnodes(false)]
    #[case::tablets(true)]
    #[tokio::test]
    async fn test_fetch_all_generations(#[case] tablets_enabled: bool) {
        let fetcher = setup(tablets_enabled).await.unwrap().0;

        let correct_generations = vec![
            GenerationTimestamp {
                timestamp: Duration::from_millis(GENERATION_NEW_MILLISECONDS as u64),
            },
            GenerationTimestamp {
                timestamp: Duration::from_millis(GENERATION_OLD_MILLISECONDS as u64),
            },
        ];

        let generations = fetcher.fetch_all_generations().await.unwrap();

        assert_eq!(generations, correct_generations);
    }

    #[rstest]
    #[case::vnodes(false)]
    #[case::tablets(true)]
    #[tokio::test]
    async fn test_get_generation_by_timestamp(#[case] tablets_enabled: bool) {
        let fetcher = setup(tablets_enabled).await.unwrap().0;

        // Input.
        let timestamps_ms = [
            GENERATION_OLD_MILLISECONDS - 1,
            GENERATION_OLD_MILLISECONDS,
            (GENERATION_NEW_MILLISECONDS + GENERATION_OLD_MILLISECONDS) / 2,
            GENERATION_NEW_MILLISECONDS,
            GENERATION_NEW_MILLISECONDS + 1,
        ];
        // Expected output.
        let correct_generations = [
            None,
            Some(GENERATION_OLD_MILLISECONDS),
            Some(GENERATION_OLD_MILLISECONDS),
            Some(GENERATION_NEW_MILLISECONDS),
            Some(GENERATION_NEW_MILLISECONDS),
        ];

        assert_eq!(
            timestamps_ms.len(),
            correct_generations.len(),
            "These two vectors should have the same length."
        );

        for i in 0..timestamps_ms.len() {
            let timestamp = Duration::from_millis(timestamps_ms[i] as u64);

            let generations = fetcher
                .fetch_generation_by_timestamp(&timestamp)
                .await
                .unwrap();

            assert_eq!(
                generations,
                correct_generations[i].map(|gen_ms| GenerationTimestamp {
                    timestamp: Duration::from_millis(gen_ms as u64)
                }),
            );
        }
    }

    #[rstest]
    #[case::vnodes(false)]
    #[case::tablets(true)]
    #[tokio::test]
    async fn test_get_next_generation(#[case] tablets_enabled: bool) {
        let fetcher = setup(tablets_enabled).await.unwrap().0;

        let generations = fetcher.fetch_all_generations().await.unwrap();

        let gen_new_next = fetcher
            .fetch_next_generation(&generations[0])
            .await
            .unwrap();
        assert!(gen_new_next.is_none());

        let gen_old_next = fetcher
            .fetch_next_generation(&generations[1])
            .await
            .unwrap();
        assert_eq!(
            gen_old_next.unwrap(),
            GenerationTimestamp {
                timestamp: Duration::from_millis(GENERATION_NEW_MILLISECONDS as u64)
            }
        );
    }

    #[rstest]
    #[case::vnodes(false)]
    #[case::tablets(true)]
    #[tokio::test]
    async fn test_get_next_generation_correct_order(#[case] tablets_enabled: bool) {
        let fetcher = setup(tablets_enabled).await.unwrap().0;

        let gen_before_all_others = GenerationTimestamp {
            timestamp: Duration::from_millis((GENERATION_OLD_MILLISECONDS - 1) as u64),
        };
        let first_gen = GenerationTimestamp {
            timestamp: Duration::from_millis(GENERATION_OLD_MILLISECONDS as u64),
        };
        let gen_before_others_next = fetcher
            .fetch_next_generation(&gen_before_all_others)
            .await
            .unwrap();
        assert_eq!(gen_before_others_next.unwrap(), first_gen);
    }

    #[rstest]
    #[case::vnodes(false)]
    #[case::tablets(true)]
    #[tokio::test]
    async fn test_do_get_stream_ids(#[case] tablets_enabled: bool) {
        let fetcher = setup(tablets_enabled).await.unwrap().0;

        let generation = GenerationTimestamp {
            timestamp: Duration::from_millis(GENERATION_NEW_MILLISECONDS as u64),
        };

        let stream_ids = fetcher.fetch_stream_ids(&generation).await.unwrap();

        let stream1 =
            StreamID::new(hex::decode(TEST_STREAM_1.strip_prefix("0x").unwrap()).unwrap());
        let stream2 =
            StreamID::new(hex::decode(TEST_STREAM_2.strip_prefix("0x").unwrap()).unwrap());

        if tablets_enabled {
            assert_eq!(stream_ids, vec![[stream1], [stream2]]);
        } else {
            assert_eq!(stream_ids, vec![vec![stream1, stream2]]);
        }
    }

    #[rstest]
    #[case::vnodes(false)]
    #[case::tablets(true)]
    #[tokio::test]
    async fn test_get_generations_continuously(#[case] tablets_enabled: bool) {
        let (fetcher, session) = setup(tablets_enabled).await.unwrap();

        let (mut generation_receiver, _future) = fetcher
            .fetch_generations_continuously(
                Duration::from_millis((GENERATION_OLD_MILLISECONDS - 1) as u64),
                time::Duration::from_millis(100),
            )
            .await
            .unwrap();

        let first_gen = GenerationTimestamp {
            timestamp: Duration::from_millis(GENERATION_OLD_MILLISECONDS as u64),
        };

        let next_gen = GenerationTimestamp {
            timestamp: Duration::from_millis(GENERATION_NEW_MILLISECONDS as u64),
        };

        let generation = generation_receiver.recv().await.unwrap();
        assert_eq!(generation, first_gen);

        let generation = generation_receiver.recv().await.unwrap();
        assert_eq!(generation, next_gen);

        let new_gen = GenerationTimestamp {
            timestamp: Duration::from_millis((GENERATION_NEW_MILLISECONDS + 100) as u64),
        };

        insert_generation_timestamp(&session, tablets_enabled, GENERATION_NEW_MILLISECONDS + 100)
            .await;

        let generation = generation_receiver.recv().await.unwrap();
        assert_eq!(generation, new_gen);
    }
}
