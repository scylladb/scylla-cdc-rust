use anyhow;
use async_trait::async_trait;

use scylla_cdc::consumer::{CDCRow, Consumer, ConsumerFactory};

struct PrinterConsumer;

#[async_trait]
impl Consumer for PrinterConsumer {
    async fn consume_cdc(&mut self, data: CDCRow<'_>) -> anyhow::Result<()> {
        // [TODO]: Add prettier printing
        println!(
            "time: {}, batch_seq_no: {}, end_of_batch: {}, operation: {:?}, ttl: {:?}",
            data.time, data.batch_seq_no, data.end_of_batch, data.operation, data.ttl
        );
        Ok(())
    }
}

struct PrinterConsumerFactory;

#[async_trait]
impl ConsumerFactory for PrinterConsumerFactory {
    async fn new_consumer(&self) -> Box<dyn Consumer> {
        Box::new(PrinterConsumer)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time;

    use super::*;
    use scylla_cdc::log_reader::CDCLogReader;
    use scylla_cdc::test_utilities::{populate_simple_db_with_pk, prepare_simple_db, TEST_TABLE};

    const SECOND_IN_MILLIS: u64 = 1_000;
    const SLEEP_INTERVAL: u64 = SECOND_IN_MILLIS / 10;
    const WINDOW_SIZE: u64 = SECOND_IN_MILLIS / 10 * 3;
    const SAFETY_INTERVAL: u64 = SECOND_IN_MILLIS / 10;

    trait ToTimestamp {
        fn to_timestamp(&self) -> chrono::Duration;
    }

    impl<Tz: chrono::TimeZone> ToTimestamp for chrono::DateTime<Tz> {
        fn to_timestamp(&self) -> chrono::Duration {
            chrono::Duration::milliseconds(self.timestamp_millis())
        }
    }

    #[tokio::test]
    async fn test_cdc_log_printer() {
        let start = chrono::Local::now().to_timestamp();
        let end = start + chrono::Duration::seconds(2);

        let (shared_session, ks) = prepare_simple_db().await.unwrap();

        let partition_key_1 = 0;
        let partition_key_2 = 1;
        populate_simple_db_with_pk(&shared_session, partition_key_1)
            .await
            .unwrap();
        populate_simple_db_with_pk(&shared_session, partition_key_2)
            .await
            .unwrap();

        let (mut _cdc_log_printer, handle) = CDCLogReader::new(
            shared_session,
            ks,
            TEST_TABLE.to_string(),
            start,
            end,
            time::Duration::from_millis(WINDOW_SIZE),
            time::Duration::from_millis(SAFETY_INTERVAL),
            time::Duration::from_millis(SLEEP_INTERVAL),
            Arc::new(PrinterConsumerFactory),
        );

        handle.await.unwrap();
    }
}
