use std::cmp::{max, min};
use std::sync::Arc;
use std::time;

use chrono;
use futures::StreamExt;
use itertools::{repeat_n, Itertools};
use scylla::frame::response::result::CqlValue;
use scylla::Session;
use tokio::sync::watch;
use tokio::time::sleep;

use crate::cdc_types::{GenerationTimestamp, StreamID, ToTimestamp};
use crate::checkpoints::{start_saving_checkpoints, CDCCheckpointSaver, Checkpoint};
use crate::consumer::{CDCRow, CDCRowSchema, Consumer};

pub struct StreamReader {
    session: Arc<Session>,
    stream_id_vec: Vec<StreamID>,
    lower_timestamp: chrono::Duration,
    window_size: time::Duration,
    safety_interval: time::Duration,
    upper_timestamp: tokio::sync::Mutex<Option<chrono::Duration>>,
    sleep_interval: time::Duration,
    should_load_progress: bool,
    should_save_progress: bool,
    checkpoint_saver: Option<Arc<dyn CDCCheckpointSaver>>,
    pause_between_saves: time::Duration,
}

impl StreamReader {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session: &Arc<Session>,
        stream_ids: Vec<StreamID>,
        start_timestamp: chrono::Duration,
        window_size: time::Duration,
        safety_interval: time::Duration,
        sleep_interval: time::Duration,
        should_load_progress: bool,
        should_save_progress: bool,
        checkpoint_saver: Option<Arc<dyn CDCCheckpointSaver>>,
        pause_between_saves: time::Duration,
    ) -> StreamReader {
        StreamReader {
            session: session.clone(),
            stream_id_vec: stream_ids,
            lower_timestamp: start_timestamp,
            window_size,
            safety_interval,
            upper_timestamp: Default::default(),
            sleep_interval,
            should_load_progress,
            should_save_progress,
            checkpoint_saver,
            pause_between_saves,
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
        let bindings = repeat_n('?', self.stream_id_vec.len()).join(",");
        let query = format!(
            "SELECT * FROM {}.{}_scylla_cdc_log \
            WHERE \"cdc$stream_id\" in ({}) \
            AND \"cdc$time\" >= minTimeuuid(?) \
            AND \"cdc$time\" < minTimeuuid(?)",
            keyspace, table_name, bindings
        );
        let query_base = self.session.prepare(query).await?;
        let mut window_begin = self.lower_timestamp;
        let window_size = chrono::Duration::from_std(self.window_size)?;
        let safety_interval = chrono::Duration::from_std(self.safety_interval)?;
        let mut checkpoint = Checkpoint {
            timestamp: window_begin.to_std()?,
            stream_id: self.stream_id_vec[0].clone(),
            generation: GenerationTimestamp {
                timestamp: window_begin,
            },
        };
        let (sender, receiver) = watch::channel(checkpoint.clone());
        if self.should_load_progress {
            let mut loaded_timestamp = chrono::Duration::max_value();
            for stream in &self.stream_id_vec {
                if let Some(timestamp) = self
                    .checkpoint_saver
                    .as_ref()
                    .unwrap()
                    .load_last_checkpoint(stream)
                    .await?
                {
                    loaded_timestamp = min(timestamp, loaded_timestamp);
                }
            }
            if loaded_timestamp != chrono::Duration::max_value() {
                window_begin = max(window_begin, loaded_timestamp);
            }
        }

        let mut values: Vec<CqlValue> = self
            .stream_id_vec
            .iter()
            .map(|x| CqlValue::Blob(x.clone().id))
            .collect();
        let begin_index = values.len();
        let end_index = values.len() + 1;
        values.extend(vec![
            CqlValue::Timestamp(window_begin),
            CqlValue::Timestamp(window_begin),
        ]); // Add dummy values at the end to resize the vector.

        let mut _handle;
        if self.should_save_progress {
            _handle = start_saving_checkpoints(
                self.stream_id_vec.clone(),
                self.checkpoint_saver.as_ref().unwrap().clone(),
                receiver,
                self.pause_between_saves,
            );
        }

        loop {
            let window_end = max(
                window_begin,
                min(
                    window_begin + window_size,
                    chrono::Local::now().to_timestamp() - safety_interval,
                ),
            );
            values[begin_index] = CqlValue::Timestamp(window_begin);
            values[end_index] = CqlValue::Timestamp(window_end);

            let mut rows_stream = self
                .session
                .execute_iter(query_base.clone(), &values)
                .await?;

            let schema = CDCRowSchema::new(rows_stream.get_column_specs());

            while let Some(row) = rows_stream.next().await {
                consumer
                    .consume_cdc(CDCRow::from_row(row?, &schema))
                    .await?;
            }

            if let Some(timestamp_to_stop) = self.upper_timestamp.lock().await.as_ref() {
                if window_end >= *timestamp_to_stop {
                    break;
                }
            }

            window_begin = window_end;
            checkpoint.timestamp = window_begin.to_std()?;
            sender.send(checkpoint.clone())?;
            sleep(self.sleep_interval).await;
        }

        if self.should_save_progress {
            self.checkpoint_saver
                .as_ref()
                .unwrap()
                .save_checkpoint(&checkpoint)
                .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use futures::stream::StreamExt;
    use scylla::query::Query;
    use tokio::sync::Mutex;

    use super::*;
    use crate::test_utilities::{populate_simple_db_with_pk, prepare_simple_db, TEST_TABLE};

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
            StreamReader {
                session: session.clone(),
                stream_id_vec: stream_ids,
                lower_timestamp: start_timestamp,
                window_size,
                safety_interval,
                upper_timestamp: Default::default(),
                sleep_interval,
                should_load_progress: false,
                should_save_progress: false,
                checkpoint_saver: None,
                pause_between_saves: Default::default(),
            }
        }
    }

    async fn get_test_stream_reader(session: &Arc<Session>) -> anyhow::Result<StreamReader> {
        let stream_id_vec = get_cdc_stream_id(session).await?;

        let start_timestamp = chrono::Local::now().to_timestamp()
            - chrono::Duration::seconds(START_TIME_DELAY_IN_SECONDS);
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
        let query_stream_id = format!(
            "SELECT DISTINCT \"cdc$stream_id\" FROM {}_scylla_cdc_log;",
            TEST_TABLE
        );

        let mut rows = session
            .query_iter(query_stream_id, ())
            .await?
            .into_typed::<StreamID>();

        let mut stream_ids_vec = Vec::new();
        while let Some(row) = rows.next().await {
            let casted_row = row?;
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

    #[tokio::test]
    async fn check_fetch_cdc_with_multiple_stream_id() {
        let (shared_session, ks) = prepare_simple_db().await.unwrap();

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
            .set_upper_timestamp(chrono::Local::now().to_timestamp() + chrono::Duration::seconds(1))
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
                assert_eq!(t, count1 as i32);
                assert_eq!(v.to_string(), format!("val{}", count1));
                assert_eq!(s.to_string(), format!("static{}", count1));
                count1 += 1;
                row_count_with_pk1 += 1;
            } else {
                assert_eq!(pk, partition_key_2 as i32);
                assert_eq!(t, count2 as i32);
                assert_eq!(v.to_string(), format!("val{}", count2));
                assert_eq!(s.to_string(), format!("static{}", count2));
                count2 += 1;
                row_count_with_pk2 += 1;
            }
        }

        assert_eq!(row_count_with_pk2, 3);
        assert_eq!(row_count_with_pk1, 3);
    }

    #[tokio::test]
    async fn check_fetch_cdc_with_one_stream_id() {
        let (shared_session, ks) = prepare_simple_db().await.unwrap();

        let partition_key = 0;
        populate_simple_db_with_pk(&shared_session, partition_key)
            .await
            .unwrap();

        let cdc_reader = get_test_stream_reader(&shared_session).await.unwrap();
        cdc_reader
            .set_upper_timestamp(chrono::Local::now().to_timestamp() + chrono::Duration::seconds(1))
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
            assert_eq!(v.to_string(), format!("val{}", count));
            assert_eq!(s.to_string(), format!("static{}", count));
        }
    }

    #[tokio::test]
    async fn check_set_upper_timestamp_in_fetch_cdc() {
        let (shared_session, ks) = prepare_simple_db().await.unwrap();

        let mut insert_before_upper_timestamp_query = Query::new(format!(
            "INSERT INTO {} (pk, t, v, s) VALUES ({}, {}, '{}', '{}');",
            TEST_TABLE, 0, 0, "val0", "static0"
        ));
        let second_ago = chrono::Local::now() - chrono::Duration::seconds(1);
        insert_before_upper_timestamp_query
            .set_timestamp(second_ago.to_timestamp().num_microseconds());
        shared_session
            .query(insert_before_upper_timestamp_query, ())
            .await
            .unwrap();

        let cdc_reader = get_test_stream_reader(&shared_session).await.unwrap();
        cdc_reader
            .set_upper_timestamp(chrono::Local::now().to_timestamp())
            .await;

        let mut insert_after_upper_timestamp_query = Query::new(format!(
            "INSERT INTO {} (pk, t, v, s) VALUES ({}, {}, '{}', '{}');",
            TEST_TABLE, 0, 1, "val1", "static1"
        ));
        let second_later = chrono::Local::now() + chrono::Duration::seconds(1);
        insert_after_upper_timestamp_query
            .set_timestamp(second_later.to_timestamp().num_microseconds());
        shared_session
            .query(insert_after_upper_timestamp_query, ())
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
}
