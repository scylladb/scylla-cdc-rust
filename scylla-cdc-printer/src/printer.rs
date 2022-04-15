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

    use scylla::batch::Consistency;
    use scylla::query::Query;
    use scylla::Session;
    use scylla::SessionBuilder;
    use tokio::time::sleep;

    use super::*;
    use scylla_cdc::log_reader::CDCLogReader;
    use scylla_cdc::test_utilities::unique_name;

    const SECOND_IN_MILLIS: u64 = 1_000;
    const TEST_TABLE: &str = "t";
    const SLEEP_INTERVAL: u64 = SECOND_IN_MILLIS / 10;
    const WINDOW_SIZE: u64 = SECOND_IN_MILLIS / 10 * 3;
    const SAFETY_INTERVAL: u64 = SECOND_IN_MILLIS / 10;

    fn get_create_table_query() -> String {
        format!("CREATE TABLE IF NOT EXISTS {} (pk int, t int, v text, s text, PRIMARY KEY (pk, t)) WITH cdc = {{'enabled':true}};",
                TEST_TABLE
        )
    }

    async fn create_test_db(session: &Arc<Session>) -> anyhow::Result<String> {
        let ks = unique_name();
        let mut create_keyspace_query = Query::new(format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}};",
            ks
        ));
        create_keyspace_query.set_consistency(Consistency::All);

        session.query(create_keyspace_query, &[]).await?;
        session.await_schema_agreement().await?;
        session.use_keyspace(&ks, false).await?;

        // Create test table
        let create_table_query = get_create_table_query();
        session.query(create_table_query, &[]).await?;
        session.await_schema_agreement().await?;
        Ok(ks)
    }

    async fn populate_db_with_pk(session: &Arc<Session>, pk: u32) -> anyhow::Result<()> {
        for i in 0..3 {
            session
                .query(
                    format!(
                        "INSERT INTO {} (pk, t, v, s) VALUES ({}, {}, 'val{}', 'static{}');",
                        TEST_TABLE, pk, i, i, i
                    ),
                    &[],
                )
                .await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_cdc_log_printer() {
        let start = chrono::Duration::milliseconds(chrono::Local::now().timestamp_millis());
        let end = start + chrono::Duration::seconds(2);

        let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        let session = SessionBuilder::new().known_node(uri).build().await.unwrap();
        let shared_session = Arc::new(session);

        let partition_key_1 = 0;
        let partition_key_2 = 1;
        let ks = create_test_db(&shared_session).await.unwrap();
        populate_db_with_pk(&shared_session, partition_key_1)
            .await
            .unwrap();
        populate_db_with_pk(&shared_session, partition_key_2)
            .await
            .unwrap();

        let (mut cdc_log_printer, _handle) = CDCLogReader::new(
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

        sleep(time::Duration::from_secs(2)).await;

        cdc_log_printer.stop();
    }
}
