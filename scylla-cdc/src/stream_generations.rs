use futures::stream::StreamExt;
use scylla::batch::Consistency;
use scylla::frame::response::result::Row;
use scylla::frame::value::Timestamp;
use scylla::query::Query;
use scylla::{IntoTypedRows, Session};
use std::error::Error;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct Generation(chrono::Duration);

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct StreamID(Vec<u8>);

/// Component responsible for managing stream generations.
pub struct GenerationFetcher {
    generations_table_name: String,
    streams_table_name: String,
    session: Arc<Session>,
}

// Number taken from: https://www.scylladb.com/2017/11/17/7-rules-planning-queries-maximum-performance/.
const DEFAULT_PAGE_SIZE: i32 = 5000;

impl GenerationFetcher {
    /// Creates new instance of GenerationFetcher.
    pub fn new(session: &Arc<Session>) -> GenerationFetcher {
        GenerationFetcher {
            generations_table_name: "system_distributed.cdc_generation_timestamps".to_string(),
            streams_table_name: "system_distributed.cdc_streams_descriptions_v2".to_string(),
            session: Arc::clone(session),
        }
    }

    // Function instead of constant for testing purposes.
    fn get_all_stream_generations_query(&self) -> String {
        format!(
            r#"
    SELECT time
    FROM {}
    WHERE key = 'timestamps';
    "#,
            self.generations_table_name
        )
    }

    /// In case of a success returns a vector containing all the generations in the database.
    /// Propagates all errors.
    pub async fn fetch_all_generations(&self) -> Result<Vec<Generation>, Box<dyn Error>> {
        let mut query = Query::new(self.get_all_stream_generations_query());
        select_consistency(&self.session, &mut query).await?;
        query.set_page_size(DEFAULT_PAGE_SIZE);

        let mut rows = self
            .session
            .query_iter(query, &[])
            .await?
            .into_typed::<(chrono::Duration,)>();

        let mut generations = vec![];

        while let Some(next_row) = rows.next().await {
            let (generation,) = next_row?;
            generations.push(Generation(generation));
        }

        Ok(generations)
    }

    // Function instead of constant for testing purposes.
    fn get_generation_by_timestamp_query(&self) -> String {
        format!(
            r#"
    SELECT time
    FROM {}
    WHERE key = 'timestamps'
    AND time <= ?
    ORDER BY time DESC
    LIMIT 1;
    "#,
            self.generations_table_name
        )
    }

    /// Given a timestamp of an operation fetch generation that was operating when this operation was performed.
    /// If no such generation exists, returns None.
    /// Propagates errors.
    pub async fn fetch_generation_by_timestamp(
        &self,
        time: &chrono::Duration,
    ) -> Result<Option<Generation>, Box<dyn Error>> {
        let mut query = Query::new(self.get_generation_by_timestamp_query());
        select_consistency(&self.session, &mut query).await?;

        let result = self.session.query(query, (Timestamp(*time),)).await?.rows;

        GenerationFetcher::return_single_row(result)
    }

    /// Given a timeuuid of a ti of an operation fetch generation that was operating when this operation was performed.
    /// If no such generation exists, returns None.
    /// Propagates errors.
    pub async fn fetch_generation_by_timeuuid(
        &self,
        time: &Uuid,
    ) -> Result<Option<Generation>, Box<dyn Error>> {
        let (secs, nanos) = time
            .to_timestamp()
            .ok_or("Uuid is not a timeuuid")?
            .to_unix();
        let timestamp =
            chrono::Duration::seconds(secs as i64) + chrono::Duration::nanoseconds(nanos as i64);

        self.fetch_generation_by_timestamp(&timestamp).await
    }

    // Function instead of constant for testing purposes.
    fn get_next_generation_query(&self) -> String {
        format!(
            r#"
    SELECT time
    FROM {}
    WHERE key = 'timestamps'
    AND time > ?
    ORDER BY time ASC
    LIMIT 1;
    "#,
            self.generations_table_name
        )
    }

    /// Given a generation returns the next generation.
    /// If given generation is currently operating, returns None.
    /// Propagates errors.
    pub async fn fetch_next_generation(
        &self,
        generation: &Generation,
    ) -> Result<Option<Generation>, Box<dyn Error>> {
        let mut query = Query::new(self.get_next_generation_query());
        select_consistency(&self.session, &mut query).await?;

        let result = self
            .session
            .query(query, (Timestamp(generation.0),))
            .await?
            .rows;

        GenerationFetcher::return_single_row(result)
    }

    // Function instead of constant for testing purposes.
    fn get_stream_ids_by_time_query(&self) -> String {
        format!(
            r#"
    SELECT streams
    FROM {}
    WHERE time = ?;
    "#,
            self.streams_table_name
        )
    }

    /// Given a generation return identifiers of all streams of this generation.
    pub async fn fetch_stream_ids(
        &self,
        generation: &Generation,
    ) -> Result<Vec<StreamID>, Box<dyn Error>> {
        let mut result_vec = vec![];

        let mut query = Query::new(self.get_stream_ids_by_time_query());
        select_consistency(&self.session, &mut query).await?;
        query.set_page_size(DEFAULT_PAGE_SIZE);

        let mut rows = self
            .session
            .query_iter(query, (Timestamp(generation.0),))
            .await?
            .into_typed::<(Vec<Vec<u8>>,)>();

        while let Some(next_row) = rows.next().await {
            let (ids,) = next_row?;
            for id in ids {
                result_vec.push(StreamID(id));
            }
        }
        Ok(result_vec)
    }

    // Return single row containing generation.
    fn return_single_row(row: Option<Vec<Row>>) -> Result<Option<Generation>, Box<dyn Error>> {
        if let Some(row) = row {
            if let Some(row) = row.into_typed::<(chrono::Duration,)>().next() {
                let time = row?.0;
                return Ok(Some(Generation(time)));
            }
        }

        Ok(None)
    }
}

// Returns current cluster size in case of a success.
async fn get_cluster_size(session: &Session) -> Result<usize, Box<dyn Error>> {
    // We are using default consistency here since the system keyspace is special and
    // the coordinator which handles the query will only read local data
    // and will not contact other nodes, so the query will work with any cluster size larger than 0.
    let mut rows = session
        .query("SELECT COUNT(*) FROM system.peers", &[])
        .await?
        .rows
        .unwrap()
        .into_typed::<(i64,)>();

    // Query returns a number of peers in a cluster, so we need to add 1 to count current node.
    Ok(rows.next().unwrap().unwrap().0 as usize + 1)
}

// Choose appropriate consistency level depending on the cluster size.
async fn select_consistency(session: &Session, query: &mut Query) -> Result<(), Box<dyn Error>> {
    query.set_consistency(match get_cluster_size(session).await? {
        1 => Consistency::One,
        _ => Consistency::Quorum,
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use scylla::statement::Consistency;
    use scylla::SessionBuilder;
    use uuid::v1::{Context, Timestamp};
    use uuid::Uuid;

    const TEST_STREAM_TABLE: &str = "Test.cdc_streams_descriptions_v2";
    const TEST_GENERATION_TABLE: &str = "Test.cdc_generation_timestamps";
    const TEST_KEYSPACE: &str = "Test";
    const GENERATION_NEW_MILLISECONDS: i64 = 1635882326384;
    const GENERATION_OLD_MILLISECONDS: i64 = 1635882224341;
    const TEST_STREAM_1: &str = "0x7fb9f781956cea08c651295720000001";
    const TEST_STREAM_2: &str = "0x7fc0000000000000c298b9f168000001";

    impl GenerationFetcher {
        // Constructor intended for testing purposes.
        fn test_new(session: &Arc<Session>) -> GenerationFetcher {
            GenerationFetcher {
                streams_table_name: TEST_STREAM_TABLE.to_string(),
                generations_table_name: TEST_GENERATION_TABLE.to_string(),
                session: Arc::clone(session),
            }
        }
    }

    // Constructs mock table with the same schema as the original one's.
    fn construct_generation_table_query() -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {}(
    key text,
    time timestamp,
    expired timestamp,
    PRIMARY KEY (key, time)
) WITH CLUSTERING ORDER BY (time DESC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {{'keys': 'ALL', 'rows_per_partition': 'ALL'}}
    AND comment = ''
    AND compaction = {{'class': 'SizeTieredCompactionStrategy'}}
    AND compression = {{'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';",
            TEST_GENERATION_TABLE
        )
    }

    // Constructs mock table with the same schema as the original one's.
    fn construct_stream_table_query() -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
    time timestamp,
    range_end bigint,
    streams frozen<set<blob>>,
    PRIMARY KEY (time, range_end)
) WITH CLUSTERING ORDER BY (range_end ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {{'keys': 'ALL', 'rows_per_partition': 'ALL'}}
    AND comment = ''
    AND compaction = {{'class': 'SizeTieredCompactionStrategy'}}
    AND compression = {{'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';",
            TEST_STREAM_TABLE
        )
    }

    // Creates test keyspace and tables if they don't exist.
    // Test data was sampled from a local copy of database.
    async fn create_test_db(session: &Session) {
        let mut query = Query::new(format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH replication
                = {{'class':'SimpleStrategy', 'replication_factor': 3}};",
            TEST_KEYSPACE
        ));
        query.set_consistency(Consistency::All);

        session.query(query, &[]).await.unwrap();

        // Create test tables containing information about generations and streams.
        for query in vec![
            construct_generation_table_query(),
            construct_stream_table_query(),
        ] {
            session.query(query, &[]).await.unwrap();
        }

        // Delete all leftovers from previous tests.
        for table in vec![
            TEST_STREAM_TABLE.to_string(),
            TEST_GENERATION_TABLE.to_string(),
        ] {
            session
                .query(format!("TRUNCATE {};", table), &[])
                .await
                .unwrap();
        }
    }

    // Populate test tables with given data.
    async fn populate_test_db(session: &Session) {
        let stream_generation =
            Timestamp(chrono::Duration::milliseconds(GENERATION_NEW_MILLISECONDS));

        for generation in &[GENERATION_NEW_MILLISECONDS, GENERATION_OLD_MILLISECONDS] {
            let mut query = Query::new(format!(
                "INSERT INTO {} (key, time, expired) VALUES ('timestamps', ?, NULL);",
                TEST_GENERATION_TABLE
            ));
            select_consistency(session, &mut query).await.unwrap();

            session
                .query(
                    query,
                    (Timestamp(chrono::Duration::milliseconds(*generation)),),
                )
                .await
                .unwrap();
        }

        let mut query = Query::new(format!(
            "INSERT INTO {}(time, range_end, streams) VALUES (?, -1, {{{}, {}}});",
            TEST_STREAM_TABLE, TEST_STREAM_1, TEST_STREAM_2
        ));
        select_consistency(session, &mut query).await.unwrap();
        session.query(query, (stream_generation,)).await.unwrap();
    }

    // Create setup for tests.
    async fn setup() -> Result<GenerationFetcher, Box<dyn Error>> {
        let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

        let session = SessionBuilder::new().known_node(uri).build().await?;

        create_test_db(&session).await;
        populate_test_db(&session).await;

        let generation_fetcher = GenerationFetcher::test_new(&Arc::new(session));

        Ok(generation_fetcher)
    }

    #[tokio::test]
    async fn test_fetch_all_generations() {
        let fetcher = setup().await.unwrap();

        let correct_gen = vec![
            Generation(chrono::Duration::milliseconds(GENERATION_NEW_MILLISECONDS)),
            Generation(chrono::Duration::milliseconds(GENERATION_OLD_MILLISECONDS)),
        ];

        let gen = fetcher.fetch_all_generations().await.unwrap();

        assert_eq!(gen, correct_gen);
    }

    fn construct_timeuuid(milliseconds: i64) -> Uuid {
        const CONTEXT_COUNT: u16 = 2137; // Just a random number.
        let context = Context::new(CONTEXT_COUNT);

        let time = std::time::Duration::from_millis(milliseconds as u64);
        let timestamp = Timestamp::from_unix(context, time.as_secs(), time.subsec_nanos());

        let u = Uuid::new_v1(timestamp, &[1, 2, 3, 4, 5, 6])
            .expect("Couldn't create a valid uuid value.");
        println!("{}", u);
        u
    }

    #[tokio::test]
    async fn test_get_generation_by_timeuuid() {
        let fetcher = setup().await.unwrap();

        // Input.
        let timestamp_ms_vec = vec![
            GENERATION_OLD_MILLISECONDS - 1,
            GENERATION_OLD_MILLISECONDS,
            (GENERATION_NEW_MILLISECONDS + GENERATION_OLD_MILLISECONDS) / 2,
            GENERATION_NEW_MILLISECONDS,
            GENERATION_NEW_MILLISECONDS + 1,
        ];
        // Expected output.
        let correct_generation_vec = vec![
            None,
            Some(GENERATION_OLD_MILLISECONDS),
            Some(GENERATION_OLD_MILLISECONDS),
            Some(GENERATION_NEW_MILLISECONDS),
            Some(GENERATION_NEW_MILLISECONDS),
        ];

        assert_eq!(timestamp_ms_vec.len(), correct_generation_vec.len());

        for i in 0..timestamp_ms_vec.len() {
            let gen = fetcher
                .fetch_generation_by_timeuuid(&construct_timeuuid(timestamp_ms_vec[i]))
                .await
                .unwrap();

            match correct_generation_vec[i] {
                Some(correct_gen) => assert_eq!(
                    gen.unwrap(),
                    Generation(chrono::Duration::milliseconds(correct_gen))
                ),
                None => assert_eq!(gen, None),
            }
        }
    }

    #[tokio::test]
    async fn test_get_generation_by_timestamp() {
        let fetcher = setup().await.unwrap();

        // Input.
        let timestamp_ms_vec = vec![
            GENERATION_OLD_MILLISECONDS - 1,
            GENERATION_OLD_MILLISECONDS,
            (GENERATION_NEW_MILLISECONDS + GENERATION_OLD_MILLISECONDS) / 2,
            GENERATION_NEW_MILLISECONDS,
            GENERATION_NEW_MILLISECONDS + 1,
        ];
        // Expected output.
        let correct_generation_vec = vec![
            None,
            Some(GENERATION_OLD_MILLISECONDS),
            Some(GENERATION_OLD_MILLISECONDS),
            Some(GENERATION_NEW_MILLISECONDS),
            Some(GENERATION_NEW_MILLISECONDS),
        ];

        assert_eq!(
            timestamp_ms_vec.len(),
            correct_generation_vec.len(),
            "These two vectors should have the same length."
        );

        for i in 0..timestamp_ms_vec.len() {
            let timestamp = chrono::Duration::milliseconds(timestamp_ms_vec[i]);

            let gen = fetcher
                .fetch_generation_by_timestamp(&timestamp)
                .await
                .unwrap();

            assert_eq!(
                gen,
                correct_generation_vec[i]
                    .map(|gen_ms| Generation(chrono::Duration::milliseconds(gen_ms))),
            );
        }
    }

    #[tokio::test]
    async fn test_get_next_generation() {
        let fetcher = setup().await.unwrap();

        let gen = fetcher.fetch_all_generations().await.unwrap();

        let gen_new_next = fetcher.fetch_next_generation(&gen[0]).await.unwrap();
        assert!(gen_new_next.is_none());

        let gen_old_next = fetcher.fetch_next_generation(&gen[1]).await.unwrap();
        assert_eq!(
            gen_old_next.unwrap(),
            Generation(chrono::Duration::milliseconds(GENERATION_NEW_MILLISECONDS))
        );
    }

    #[tokio::test]
    async fn test_get_next_generation_correct_order() {
        let fetcher = setup().await.unwrap();

        let gen_before_all_others = Generation(chrono::Duration::milliseconds(
            GENERATION_OLD_MILLISECONDS - 1,
        ));
        let first_gen = Generation(chrono::Duration::milliseconds(GENERATION_OLD_MILLISECONDS));
        let gen_before_others_next = fetcher
            .fetch_next_generation(&gen_before_all_others)
            .await
            .unwrap();
        assert_eq!(gen_before_others_next.unwrap(), first_gen);
    }

    #[tokio::test]
    async fn test_do_get_stream_ids() {
        let fetcher = setup().await.unwrap();

        let gen = Generation(chrono::Duration::milliseconds(GENERATION_NEW_MILLISECONDS));

        let stream_ids = fetcher.fetch_stream_ids(&gen).await.unwrap();

        let correct_stream_ids: Vec<StreamID> = vec![TEST_STREAM_1, TEST_STREAM_2]
            .iter()
            .map(|stream| StreamID(hex::decode(stream.strip_prefix("0x").unwrap()).unwrap()))
            .collect();

        assert_eq!(stream_ids, correct_stream_ids);
    }
}
