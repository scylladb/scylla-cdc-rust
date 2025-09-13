//! A module representing the logic behind saving progress.
use crate::cdc_types::{GenerationTimestamp, StreamID};
use anyhow;
use async_trait::async_trait;
use futures::future::RemoteHandle;
use futures::FutureExt;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use scylla::value;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Checkpoint {
    pub timestamp: Duration,
    pub stream_id: StreamID,
    pub generation: GenerationTimestamp,
}

/// Spawns a new task that periodically saves checkpoints.
/// Takes vector of tracked by a [`StreamReader`](crate::stream_reader::StreamReader) stream IDs.
/// Receives value of the last checkpoint via channel.
/// We are using [`tokio::sync::watch`] channel, because we only need the latest value.
/// Returns handle to check on this newly created task.
pub(crate) fn start_saving_checkpoints(
    tracked_streams: Vec<StreamID>,
    checkpoint_saver: Arc<dyn CDCCheckpointSaver>,
    receiver: tokio::sync::watch::Receiver<Checkpoint>,
    saving_period: Duration,
) -> RemoteHandle<()> {
    let (fut, handle) = async move {
        loop {
            for stream in tracked_streams.iter() {
                let mut checkpoint = receiver.borrow().clone();
                checkpoint.stream_id = stream.clone();

                if checkpoint_saver.save_checkpoint(&checkpoint).await.is_err() {
                    warn!(
                        "Saving checkpoint for stream 0x{} failed.",
                        hex::encode(&checkpoint.stream_id.id)
                    );
                }
            }

            sleep(saving_period).await;
        }
    }
    .remote_handle();

    tokio::task::spawn(fut);

    handle
}

/// Customizable trait responsible for saving checkpoints.
#[async_trait]
pub trait CDCCheckpointSaver: Send + Sync {
    /// Saves given checkpoint.
    async fn save_checkpoint(&self, checkpoint: &Checkpoint) -> anyhow::Result<()>;
    /// When a new generation comes into effect, it saves it into the checkpoint table.
    async fn save_new_generation(&self, generation: &GenerationTimestamp) -> anyhow::Result<()>;
    /// Loads last seen generation from the checkpoint table.
    async fn load_last_generation(&self) -> anyhow::Result<Option<GenerationTimestamp>>;
    /// Loads last seen timestamp for given stream id from the checkpoint table.
    async fn load_last_checkpoint(
        &self,
        stream_id: &StreamID,
    ) -> anyhow::Result<Option<chrono::Duration>>;
}

/// Default implementation for [`CDCCheckpointSaver`] trait.
/// Saves checkpoints in a special table using ScyllaDB.
/// Along with checkpoints for each StreamReader, the table contains
/// a special row used for storing the latest generation.
pub struct TableBackedCheckpointSaver {
    session: Arc<Session>,
    checkpoint_table: String,
    make_checkpoint_stmt: PreparedStatement,
}

impl TableBackedCheckpointSaver {
    /// Creates new [`TableBackedCheckpointSaver`].
    /// Will create a table if `keyspace.table_name` doesn't exists.
    /// Created checkpoints will have Time To Live equal to 7 days.
    pub async fn new_with_default_ttl(
        session: Arc<Session>,
        keyspace: &str,
        table_name: &str,
    ) -> anyhow::Result<Self> {
        const DEFAULT_TTL: i64 = 604800; // 7 days
        TableBackedCheckpointSaver::new(session, keyspace, table_name, DEFAULT_TTL).await
    }

    /// Creates new [`TableBackedCheckpointSaver`].
    /// Will create a table if `keyspace.table_name` doesn't exists.
    pub async fn new(
        session: Arc<Session>,
        keyspace: &str,
        table_name: &str,
        ttl: i64,
    ) -> anyhow::Result<Self> {
        let checkpoint_table = format!("{keyspace}.{table_name}");

        TableBackedCheckpointSaver::create_checkpoints_table(&session, &checkpoint_table).await?;

        let make_checkpoint_stmt = session
            .prepare(format!(
                "UPDATE {checkpoint_table} USING TTL {ttl}
                SET generation = ?, time = ?
                WHERE stream_id = ?"
            ))
            .await?;

        let cp_saver = TableBackedCheckpointSaver {
            session,
            checkpoint_table,
            make_checkpoint_stmt,
        };

        Ok(cp_saver)
    }

    // Creates new table with specified schema in the given keyspace with provided name.
    // If a table with such name already exists, doesn't do anything.
    async fn create_checkpoints_table(
        session: &Arc<Session>,
        checkpoint_table: &str,
    ) -> anyhow::Result<()> {
        let schema = get_checkpoint_table_schema(checkpoint_table);

        session.query_unpaged(schema, ()).await?;
        session.await_schema_agreement().await?;

        Ok(())
    }
}

fn get_default_generation_pk() -> StreamID {
    StreamID { id: vec![0] }
}

fn get_checkpoint_table_schema(table_name: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {table_name} (
            stream_id blob PRIMARY KEY,
            generation timestamp,
            time timestamp)"
    )
}

#[async_trait]
impl CDCCheckpointSaver for TableBackedCheckpointSaver {
    /// Writes new record containing given timestamp to the checkpoint table.
    async fn save_checkpoint(&self, checkpoint: &Checkpoint) -> anyhow::Result<()> {
        let timestamp = value::CqlTimestamp(checkpoint.timestamp.as_millis() as i64);

        self.session
            .execute_unpaged(
                &self.make_checkpoint_stmt,
                (&checkpoint.generation, timestamp, &checkpoint.stream_id),
            )
            .await?;

        Ok(())
    }

    async fn save_new_generation(&self, generation: &GenerationTimestamp) -> anyhow::Result<()> {
        let marked_stream_id = get_default_generation_pk();
        let dummy_timestamp = value::CqlTimestamp(i64::MAX);

        self.session
            .execute_unpaged(
                &self.make_checkpoint_stmt,
                (generation, dummy_timestamp, marked_stream_id),
            )
            .await?;

        Ok(())
    }

    /// Loads last seen generation timestamp or `None`.
    async fn load_last_generation(&self) -> anyhow::Result<Option<GenerationTimestamp>> {
        let generation = self
            .session
            .query_unpaged(
                format!(
                    "SELECT generation FROM {}
                    WHERE stream_id = ?",
                    self.checkpoint_table
                ),
                (get_default_generation_pk(),),
            )
            .await?
            .into_rows_result()?
            .maybe_first_row::<(GenerationTimestamp,)>()?
            .map(|row| row.0);

        Ok(generation)
    }

    /// Loads last seen timestamp or `None`.
    async fn load_last_checkpoint(
        &self,
        stream_id: &StreamID,
    ) -> anyhow::Result<Option<chrono::Duration>> {
        Ok(self
            .session
            .query_unpaged(
                format!(
                    "SELECT time FROM {}
                    WHERE stream_id = ?
                    LIMIT 1",
                    self.checkpoint_table
                ),
                (stream_id,),
            )
            .await?
            .into_rows_result()?
            .maybe_first_row::<(value::CqlTimestamp,)>()?
            .map(|t| chrono::Duration::milliseconds(t.0 .0)))
    }
}

#[cfg(test)]
mod tests {
    use crate::cdc_types::{GenerationTimestamp, StreamID};
    use crate::checkpoints::{CDCCheckpointSaver, Checkpoint, TableBackedCheckpointSaver};
    use futures::{StreamExt, TryStreamExt};
    use rand::prelude::*;
    use scylla::client::session::Session;
    use scylla::value;
    use scylla_cdc_test_utils::{prepare_db, unique_name};
    use std::ops::Add;
    use std::sync::Arc;
    use std::time::Duration;

    async fn setup() -> (Arc<Session>, String, Arc<TableBackedCheckpointSaver>) {
        const DEFAULT_TTL: i64 = 300;
        let (session, ks) = prepare_db(&[], 1, false).await.unwrap();
        let table_name = unique_name();

        let cp_saver = Arc::new(
            TableBackedCheckpointSaver::new(session.clone(), &ks, &table_name, DEFAULT_TTL)
                .await
                .unwrap(),
        );

        (session, table_name, cp_saver)
    }

    async fn get_checkpoints(session: &Arc<Session>, table: &str) -> Vec<Checkpoint> {
        session
            .query_iter(format!("SELECT * FROM {table}"), ())
            .await
            .unwrap()
            .rows_stream::<(StreamID, GenerationTimestamp, value::CqlTimestamp)>()
            .unwrap()
            .map(|res| {
                res.map(|(id, gen, time)| Checkpoint {
                    stream_id: id,
                    generation: gen,
                    timestamp: Duration::from_millis(time.0 as u64),
                })
            })
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_save_checkpoint_multiple_times() {
        const N: usize = 20;
        let (session, table_name, cp_saver) = setup().await;

        let mut checkpoint = Checkpoint {
            timestamp: Duration::from_secs(128),
            stream_id: StreamID {
                id: vec![1, 1, 1, 1, 1, 1, 1, 1],
            },
            generation: GenerationTimestamp {
                timestamp: chrono::Duration::MIN,
            },
        };

        let delta = Duration::from_secs(32);

        for _ in 0..N {
            checkpoint.timestamp += delta;
            cp_saver.save_checkpoint(&checkpoint).await.unwrap();
        }

        let checkpoints = get_checkpoints(&session, &table_name).await;

        assert_eq!(checkpoints.len(), 1); // Constant stream id for every query.

        let saved_checkpoint = &checkpoints[0];
        assert_eq!(*saved_checkpoint, checkpoint);

        assert_eq!(
            checkpoint.timestamp,
            cp_saver
                .load_last_checkpoint(&checkpoint.stream_id)
                .await
                .unwrap()
                .unwrap()
                .to_std()
                .unwrap()
        )
    }

    #[tokio::test]
    async fn test_save_generation_multiple_times() {
        const N: usize = 20;
        let (session, table_name, cp_saver) = setup().await;

        let mut generation = GenerationTimestamp {
            timestamp: chrono::Duration::zero(),
        };

        let delta = chrono::Duration::seconds(10);

        for _ in 0..N {
            generation.timestamp = generation.timestamp.add(delta);
            cp_saver.save_new_generation(&generation).await.unwrap();
        }

        let checkpoints = get_checkpoints(&session, &table_name).await;

        assert_eq!(checkpoints.len(), 1); // Constant stream id for every query.

        let saved_checkpoint = &checkpoints[0];
        assert_eq!(saved_checkpoint.generation, generation);

        assert_eq!(
            cp_saver.load_last_generation().await.unwrap().unwrap(),
            generation
        );
    }

    #[tokio::test]
    async fn test_save_checkpoint_multiple_streams() {
        const N_OF_IDS: u8 = 200;

        let (_, _, cp_saver) = setup().await;

        let mut checkpoints = Vec::<Checkpoint>::with_capacity(N_OF_IDS as usize);

        for i in 0..N_OF_IDS {
            let checkpoint = Checkpoint {
                timestamp: Duration::from_secs(random::<u64>() % (100u64 * N_OF_IDS as u64)),
                stream_id: StreamID { id: vec![0, i] },
                generation: GenerationTimestamp {
                    timestamp: chrono::Duration::MAX,
                },
            };

            checkpoints.push(checkpoint);
        }

        for cp in &checkpoints {
            cp_saver.save_checkpoint(cp).await.unwrap();
        }

        for cp in &checkpoints {
            let saved_checkpoint = cp_saver
                .load_last_checkpoint(&cp.stream_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(saved_checkpoint.to_std().unwrap(), cp.timestamp);
        }
    }
}
