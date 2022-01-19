#[cfg(test)]
mod tests {
    use crate::replicator_consumer::ReplicatorConsumer;
    use anyhow::anyhow;
    use itertools::Itertools;
    use scylla::frame::response::result::{CqlValue, Row};
    use scylla::{Session, SessionBuilder};
    use scylla_cdc::consumer::{CDCRow, CDCRowSchema, Consumer};
    use std::sync::Arc;

    /// Tuple representing a column in the table that will be replicated.
    /// The first string is the name of the column.
    /// The second string is the name of the type of the column.
    pub type TestColumn<'a> = (&'a str, &'a str);

    pub struct TestTableSchema<'a> {
        name: String,
        partition_key: Vec<TestColumn<'a>>,
        clustering_key: Vec<TestColumn<'a>>,
        other_columns: Vec<TestColumn<'a>>,
    }

    /// Tuple representing an operation to be performed on the table before replicating.
    /// The string is the CQL query with the operation. Keyspace does not have to be specified.
    /// The vector of values are the values that will be bound to the query.
    pub type TestOperation<'a> = (&'a str, Vec<CqlValue>);

    enum FailureReason {
        WrongRowsCount(usize, usize),
        RowNotMatching(usize),
    }

    async fn setup_tables(session: &Session, schema: &TestTableSchema<'_>) -> anyhow::Result<()> {
        session.query("CREATE KEYSPACE IF NOT EXISTS test_src WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1}", ()).await?;
        session.query("CREATE KEYSPACE IF NOT EXISTS test_dst WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1}", ()).await?;
        session
            .query(format!("DROP TABLE IF EXISTS test_src.{}", schema.name), ())
            .await?;
        session
            .query(format!("DROP TABLE IF EXISTS test_dst.{}", schema.name), ())
            .await?;

        let partition_key_name = match schema.partition_key.as_slice() {
            [pk] => pk.0.to_string(),
            _ => format!(
                "({})",
                schema.partition_key.iter().map(|(name, _)| name).join(",")
            ),
        };
        let create_table_query = format!(
            "({}, PRIMARY KEY ({}, {}))",
            schema
                .partition_key
                .iter()
                .chain(schema.clustering_key.iter())
                .chain(schema.other_columns.iter())
                .map(|(name, col_type)| format!("{} {}", name, col_type))
                .join(","),
            partition_key_name,
            schema.clustering_key.iter().map(|(name, _)| name).join(",")
        );

        session
            .query(
                format!(
                    "CREATE TABLE test_src.{} {} WITH cdc = {{'enabled' : true}}",
                    schema.name, create_table_query
                ),
                (),
            )
            .await?;
        session
            .query(
                format!(
                    "CREATE TABLE test_dst.{} {}",
                    schema.name, create_table_query
                ),
                (),
            )
            .await?;

        session.refresh_metadata().await?;

        Ok(())
    }

    async fn execute_queries(
        session: &Session,
        operations: Vec<TestOperation<'_>>,
    ) -> anyhow::Result<()> {
        session.use_keyspace("test_src", false).await?;
        for operation in operations {
            session.query(operation.0, operation.1).await?;
        }

        Ok(())
    }

    async fn replicate(session: &Arc<Session>, name: &str) -> anyhow::Result<()> {
        let result = session
            .query(
                format!("SELECT * FROM test_src.{}_scylla_cdc_log", name),
                (),
            )
            .await?;

        let table_schema = session
            .get_cluster_data()
            .get_keyspace_info()
            .get("test_dst")
            .ok_or(anyhow!("Keyspace not found"))?
            .tables
            .get(&name.to_ascii_lowercase())
            .ok_or(anyhow!("Table not found"))?
            .clone();

        let mut consumer = ReplicatorConsumer::new(
            session.clone(),
            "test_dst".to_string(),
            name.to_string(),
            table_schema,
        )
        .await;

        let schema = CDCRowSchema::new(&result.col_specs);

        for log in result.rows.unwrap_or(vec![]) {
            consumer.consume_cdc(CDCRow::from_row(log, &schema)).await;
        }

        Ok(())
    }

    fn fail_test(
        table_name: &str,
        original_rows: &[Row],
        replicated_rows: &[Row],
        failure_reason: FailureReason,
    ) {
        eprintln!("Replication test for table {} failed.", table_name);
        eprint!("Failure reason: ");
        match failure_reason {
            FailureReason::WrongRowsCount(o, r) => eprintln!(
                "Number of rows not matching. Original table: {} rows, replicated table: {} rows.",
                o, r
            ),
            FailureReason::RowNotMatching(n) => eprintln!("Row {} is not equal in both tables.", n),
        }

        eprintln!("ORIGINAL TABLE:");
        for (i, row) in original_rows.iter().enumerate() {
            eprintln!("Row {}: {:?}", i + 1, row.columns);
        }

        eprintln!("REPLICATED TABLE:");
        for (i, row) in replicated_rows.iter().enumerate() {
            eprintln!("Row {}: {:?}", i + 1, row.columns);
        }

        panic!()
    }

    async fn compare_changes(session: &Session, name: &str) -> anyhow::Result<()> {
        let original_rows = session
            .query(format!("SELECT * FROM test_src.{}", name), ())
            .await?
            .rows
            .unwrap_or(vec![]);
        let replicated_rows = session
            .query(format!("SELECT * FROM test_dst.{}", name), ())
            .await?
            .rows
            .unwrap_or(vec![]);

        if original_rows.len() == replicated_rows.len() {
            for (i, (original, replicated)) in
                original_rows.iter().zip(replicated_rows.iter()).enumerate()
            {
                if original != replicated {
                    fail_test(
                        name,
                        &original_rows,
                        &replicated_rows,
                        FailureReason::RowNotMatching(i + 1),
                    );
                }
            }
        } else {
            fail_test(
                name,
                &original_rows,
                &replicated_rows,
                FailureReason::WrongRowsCount(original_rows.len(), replicated_rows.len()),
            );
        }

        Ok(())
    }

    /// Function that tests replication process.
    /// Different tests in the same cluster must have different table names.
    async fn test_replication(
        node_uri: &str,
        schema: TestTableSchema<'_>,
        operations: Vec<TestOperation<'_>>,
    ) -> anyhow::Result<()> {
        let session = Arc::new(SessionBuilder::new().known_node(node_uri).build().await?);
        setup_tables(&session, &schema).await?;
        execute_queries(&session, operations).await?;
        replicate(&session, &schema.name).await?;
        compare_changes(&session, &schema.name).await?;
        Ok(())
    }

    fn get_uri() -> String {
        std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string())
    }

    #[tokio::test]
    async fn simple_insert_test() {
        let schema = TestTableSchema {
            name: "SIMPLE_INSERT".to_string(),
            partition_key: vec![("pk", "int")],
            clustering_key: vec![("ck", "int")],
            other_columns: vec![("v1", "int"), ("v2", "boolean")],
        };

        let operations = vec![
            (
                "INSERT INTO SIMPLE_INSERT (pk, ck, v1, v2) VALUES (1, 2, 3, true)",
                vec![],
            ),
            (
                "INSERT INTO SIMPLE_INSERT (pk, ck, v1, v2) VALUES (3, 2, 1, false)",
                vec![],
            ),
        ];

        test_replication(&get_uri(), schema, operations)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn simple_update_test() {
        let schema = TestTableSchema {
            name: "SIMPLE_UPDATE".to_string(),
            partition_key: vec![("pk", "int")],
            clustering_key: vec![("ck", "int")],
            other_columns: vec![("v1", "int"), ("v2", "boolean")],
        };

        let operations = vec![
            (
                "INSERT INTO SIMPLE_UPDATE (pk, ck, v1, v2) VALUES (1, 2, 3, true)",
                vec![],
            ),
            (
                "UPDATE SIMPLE_UPDATE SET v2 = false WHERE pk = 1 AND ck = 2",
                vec![],
            ),
            (
                "DELETE v1 FROM SIMPLE_UPDATE WHERE pk = 1 AND ck = 2",
                vec![],
            ),
        ];

        test_replication(&get_uri(), schema, operations)
            .await
            .unwrap();
    }
}
