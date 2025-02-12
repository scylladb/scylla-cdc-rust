use futures::future::RemoteHandle;
use futures::stream::StreamExt;
use futures::FutureExt;
use scylla::frame::value;
use scylla::query::Query;
use scylla::statement::Consistency;
use scylla::Session;
use std::sync::Arc;
use std::time;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::warn;

use crate::cdc_types::{GenerationTimestamp, StreamID};

/// Component responsible for managing stream generations.
pub struct GenerationFetcher {
    generations_table_name: String,
    streams_table_name: String,
    session: Arc<Session>,
}

// Number taken from: https://www.scylladb.com/2017/11/17/7-rules-planning-queries-maximum-performance/.
const DEFAULT_PAGE_SIZE: i32 = 5000;

impl GenerationFetcher {
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
            "
    SELECT time
    FROM {}
    WHERE key = 'timestamps';
    ",
            self.generations_table_name
        )
    }

    /// In case of a success returns a vector containing all the generations in the database.
    /// Propagates all errors.
    pub async fn fetch_all_generations(&self) -> anyhow::Result<Vec<GenerationTimestamp>> {
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

    // Function instead of constant for testing purposes.
    fn get_generation_by_timestamp_query(&self) -> String {
        format!(
            "
    SELECT time
    FROM {}
    WHERE key = 'timestamps'
    AND time <= ?
    ORDER BY time DESC
    LIMIT 1;
    ",
            self.generations_table_name
        )
    }

    /// Given a timestamp of an operation fetch generation that was operating when this operation was performed.
    /// If no such generation exists, returns `None`.
    /// Propagates errors.
    pub async fn fetch_generation_by_timestamp(
        &self,
        time: &chrono::Duration,
    ) -> anyhow::Result<Option<GenerationTimestamp>> {
        let query =
            new_distributed_system_query(self.get_generation_by_timestamp_query(), &self.session)
                .await?;

        let result = self
            .session
            .query_unpaged(query, (value::CqlTimestamp(time.num_milliseconds()),))
            .await?
            .into_rows_result()?
            .maybe_first_row::<(GenerationTimestamp,)>()?
            .map(|(ts,)| ts);

        Ok(result)
    }

    // Function instead of constant for testing purposes.
    fn get_next_generation_query(&self) -> String {
        format!(
            "
    SELECT time
    FROM {}
    WHERE key = 'timestamps'
    AND time > ?
    ORDER BY time ASC
    LIMIT 1;
    ",
            self.generations_table_name
        )
    }

    /// Given a generation returns the next generation.
    /// If given generation is currently operating, returns `None`.
    /// Propagates errors.
    pub async fn fetch_next_generation(
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

    // Function instead of constant for testing purposes.
    fn get_stream_ids_by_time_query(&self) -> String {
        format!(
            "
    SELECT streams
    FROM {}
    WHERE time = ?;
    ",
            self.streams_table_name
        )
    }

    /// Given a generation return identifiers of all streams of this generation.
    /// Streams are grouped by vnodes.
    pub async fn fetch_stream_ids(
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

    pub async fn fetch_generations_continuously(
        self: Arc<Self>,
        start_timestamp: chrono::Duration,
        sleep_interval: time::Duration,
    ) -> anyhow::Result<(mpsc::Receiver<GenerationTimestamp>, RemoteHandle<()>)> {
        let (generation_sender, generation_receiver) = mpsc::channel(1);

        let (future, future_handle) = async move {
            let mut generation = loop {
                match self.fetch_generation_by_timestamp(&start_timestamp).await {
                    Ok(Some(generation)) => break generation,
                    Ok(None) => {
                        break {
                            loop {
                                match self.fetch_all_generations().await {
                                    Ok(vectors) => match vectors.last() {
                                        None => sleep(sleep_interval).await,
                                        Some(generation) => break generation.clone(),
                                    },
                                    _ => warn!("Failed to fetch all generations"),
                                }
                            }
                        }
                    }
                    _ => warn!("Failed to fetch generation by timestamp"),
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
                        _ => warn!("Failed to fetch next generation"),
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

// Returns current cluster size in case of a success.
async fn get_cluster_size(session: &Session) -> anyhow::Result<usize> {
    // We are using default consistency here since the system keyspace is special and
    // the coordinator which handles the query will only read local data
    // and will not contact other nodes, so the query will work with any cluster size larger than 0.
    let (peers_num,) = session
        .query_iter("SELECT COUNT(*) FROM system.peers", &[])
        .await?
        .rows_stream::<(i64,)>()?
        .next()
        .await
        .unwrap_or(Ok((0,)))?;

    // Query returns a number of peers in a cluster, so we need to add 1 to count current node.
    Ok(peers_num as usize + 1)
}

// Choose appropriate consistency level depending on the cluster size.
async fn select_consistency(session: &Session, query: &mut Query) -> anyhow::Result<()> {
    query.set_consistency(match get_cluster_size(session).await? {
        1 => Consistency::One,
        _ => Consistency::Quorum,
    });
    Ok(())
}

async fn new_distributed_system_query(stmt: String, session: &Session) -> anyhow::Result<Query> {
    let mut query = Query::new(stmt);
    select_consistency(session, &mut query).await?;

    Ok(query)
}

#[cfg(test)]
mod tests {
    use scylla_cdc_test_utils::prepare_db;

    use super::*;

    const TEST_STREAM_TABLE: &str = "cdc_streams_descriptions_v2";
    const TEST_GENERATION_TABLE: &str = "cdc_generation_timestamps";
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
            "
    CREATE TABLE IF NOT EXISTS {}(
    key text,
    time timestamp,
    expired timestamp,
    PRIMARY KEY (key, time)
) WITH CLUSTERING ORDER BY (time DESC);",
            TEST_GENERATION_TABLE
        )
    }

    // Constructs mock table with the same schema as the original one's.
    fn construct_stream_table_query() -> String {
        format!(
            "
    CREATE TABLE IF NOT EXISTS {} (
    time timestamp,
    range_end bigint,
    streams frozen<set<blob>>,
    PRIMARY KEY (time, range_end)
) WITH CLUSTERING ORDER BY (range_end ASC);",
            TEST_STREAM_TABLE
        )
    }

    async fn insert_generation_timestamp(session: &Session, generation: i64) {
        let query = new_distributed_system_query(
            format!(
                "INSERT INTO {} (key, time, expired) VALUES ('timestamps', ?, NULL);",
                TEST_GENERATION_TABLE
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
                "INSERT INTO {}(time, range_end, streams) VALUES (?, -1, {{{}, {}}});",
                TEST_STREAM_TABLE, TEST_STREAM_1, TEST_STREAM_2
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
    async fn setup() -> anyhow::Result<GenerationFetcher> {
        let session = prepare_db(
            &[
                construct_generation_table_query(),
                construct_stream_table_query(),
            ],
            3,
        )
        .await?
        .0;
        populate_test_db(&session).await;

        let generation_fetcher = GenerationFetcher::test_new(&session);

        Ok(generation_fetcher)
    }

    #[tokio::test]
    async fn test_fetch_all_generations() {
        let fetcher = setup().await.unwrap();

        let correct_gen = vec![
            GenerationTimestamp {
                timestamp: chrono::Duration::milliseconds(GENERATION_NEW_MILLISECONDS),
            },
            GenerationTimestamp {
                timestamp: chrono::Duration::milliseconds(GENERATION_OLD_MILLISECONDS),
            },
        ];

        let gen = fetcher.fetch_all_generations().await.unwrap();

        assert_eq!(gen, correct_gen);
    }

    #[tokio::test]
    async fn test_get_generation_by_timestamp() {
        let fetcher = setup().await.unwrap();

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
            let timestamp = chrono::Duration::milliseconds(timestamps_ms[i]);

            let gen = fetcher
                .fetch_generation_by_timestamp(&timestamp)
                .await
                .unwrap();

            assert_eq!(
                gen,
                correct_generations[i].map(|gen_ms| GenerationTimestamp {
                    timestamp: chrono::Duration::milliseconds(gen_ms)
                }),
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
            GenerationTimestamp {
                timestamp: chrono::Duration::milliseconds(GENERATION_NEW_MILLISECONDS)
            }
        );
    }

    #[tokio::test]
    async fn test_get_next_generation_correct_order() {
        let fetcher = setup().await.unwrap();

        let gen_before_all_others = GenerationTimestamp {
            timestamp: chrono::Duration::milliseconds(GENERATION_OLD_MILLISECONDS - 1),
        };
        let first_gen = GenerationTimestamp {
            timestamp: chrono::Duration::milliseconds(GENERATION_OLD_MILLISECONDS),
        };
        let gen_before_others_next = fetcher
            .fetch_next_generation(&gen_before_all_others)
            .await
            .unwrap();
        assert_eq!(gen_before_others_next.unwrap(), first_gen);
    }

    #[tokio::test]
    async fn test_do_get_stream_ids() {
        let fetcher = setup().await.unwrap();

        let gen = GenerationTimestamp {
            timestamp: chrono::Duration::milliseconds(GENERATION_NEW_MILLISECONDS),
        };

        let stream_ids = fetcher.fetch_stream_ids(&gen).await.unwrap();

        let correct_stream_ids: Vec<Vec<StreamID>> = [[TEST_STREAM_1, TEST_STREAM_2]]
            .iter()
            .map(|stream_vec| {
                stream_vec
                    .iter()
                    .map(|stream| StreamID {
                        id: hex::decode(stream.strip_prefix("0x").unwrap()).unwrap(),
                    })
                    .collect()
            })
            .collect();

        assert_eq!(stream_ids, correct_stream_ids);
    }

    #[tokio::test]
    async fn test_get_generations_continuously() {
        let fetcher = setup().await.unwrap();
        let session = fetcher.session.clone();

        let (mut generation_receiver, _future) = Arc::new(fetcher)
            .fetch_generations_continuously(
                chrono::Duration::milliseconds(GENERATION_OLD_MILLISECONDS - 1),
                time::Duration::from_millis(100),
            )
            .await
            .unwrap();

        let first_gen = GenerationTimestamp {
            timestamp: chrono::Duration::milliseconds(GENERATION_OLD_MILLISECONDS),
        };

        let next_gen = GenerationTimestamp {
            timestamp: chrono::Duration::milliseconds(GENERATION_NEW_MILLISECONDS),
        };

        let generation = generation_receiver.recv().await.unwrap();
        assert_eq!(generation, first_gen);

        let generation = generation_receiver.recv().await.unwrap();
        assert_eq!(generation, next_gen);

        let new_gen = GenerationTimestamp {
            timestamp: chrono::Duration::milliseconds(GENERATION_NEW_MILLISECONDS + 100),
        };

        insert_generation_timestamp(&session, GENERATION_NEW_MILLISECONDS + 100).await;
        let generation = generation_receiver.recv().await.unwrap();
        assert_eq!(generation, new_gen);
    }
}
