#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::convert::identity;
    use std::hash::Hash;
    use std::sync::Arc;
    use std::time;

    use anyhow::{bail, Result};
    use async_trait::async_trait;
    use itertools::{repeat_n, Itertools};
    use scylla::frame::response::result::CqlValue;
    use scylla::frame::value::{Value, ValueTooBig};
    use scylla::prepared_statement::PreparedStatement;
    use scylla::Session;
    use tokio::sync::Mutex;

    use crate::cdc_types::ToTimestamp;
    use crate::consumer::*;
    use crate::log_reader::CDCLogReader;
    use crate::test_utilities::prepare_db;

    const SECOND_IN_MILLIS: u64 = 1_000;
    const SLEEP_INTERVAL: u64 = SECOND_IN_MILLIS / 10;
    const WINDOW_SIZE: u64 = SECOND_IN_MILLIS / 10 * 3;
    const SAFETY_INTERVAL: u64 = SECOND_IN_MILLIS / 10;

    type OperationsMap = Arc<Mutex<HashMap<Vec<PrimaryKeyValue>, VecDeque<Operation>>>>;

    // The driver's CqlValue cannot be used as HashMap key,
    // because it doesn't have the Eq trait.
    #[derive(Debug, Eq, PartialEq, Hash)]
    enum PrimaryKeyValue {
        // Name consistency with CqlValue from the driver is recommended.
        Int(i32),
        Text(String),
        List(Vec<PrimaryKeyValue>),
    }

    impl Value for PrimaryKeyValue {
        fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
            self.to_cql().serialize(buf)?;

            Ok(())
        }
    }

    impl PrimaryKeyValue {
        pub fn from_cql(cql_val: &CqlValue) -> Option<PrimaryKeyValue> {
            match cql_val {
                CqlValue::Int(x) => Some(PrimaryKeyValue::Int(*x)),
                CqlValue::Text(s) => Some(PrimaryKeyValue::Text(s.clone())),
                CqlValue::List(v) => v
                    .iter()
                    .map(PrimaryKeyValue::from_cql)
                    .collect::<Option<Vec<PrimaryKeyValue>>>()
                    .map(PrimaryKeyValue::List),
                _ => None,
            }
        }

        pub fn to_cql(&self) -> CqlValue {
            match self {
                PrimaryKeyValue::Int(x) => CqlValue::Int(*x),
                PrimaryKeyValue::Text(s) => CqlValue::Text(s.clone()),
                PrimaryKeyValue::List(v) => {
                    CqlValue::List(v.iter().map(PrimaryKeyValue::to_cql).collect())
                }
            }
        }
    }

    #[derive(Debug, Eq, PartialEq)]
    struct Operation {
        operation_type: OperationType,
        clustering_key: Option<i32>,
        value: Option<i32>,
    }

    impl Operation {
        fn new(
            operation_type: OperationType,
            clustering_key: Option<i32>,
            value: Option<i32>,
        ) -> Operation {
            Operation {
                operation_type,
                clustering_key,
                value,
            }
        }
    }

    struct TestConsumer {
        read_operations: OperationsMap,
    }

    #[async_trait]
    impl Consumer for TestConsumer {
        async fn consume_cdc(&mut self, mut data: CDCRow<'_>) -> Result<()> {
            let pk_val = {
                // Primary key columns have names pk1, pk2...
                let mut values = Vec::new();
                let mut i = 1;
                while data.column_exists(&format!("pk{}", i)) {
                    let val = data.get_value(&format!("pk{}", i)).as_ref().unwrap();
                    values.push(PrimaryKeyValue::from_cql(val).unwrap());
                    i += 1;
                }

                values
            };
            let op_type = data.operation.clone();
            let ck = match data.take_value("ck") {
                Some(CqlValue::Int(x)) => Some(x),
                None => None,
                Some(cql) => bail!("Unexpected ck type: {:?}", cql),
            };
            let val = data.take_value("v").map(|cql| cql.as_int().unwrap());

            self.read_operations
                .lock()
                .await
                .entry(pk_val)
                .or_insert_with(VecDeque::new)
                .push_back(Operation::new(op_type, ck, val));

            Ok(())
        }
    }

    struct TestConsumerFactory {
        read_operations: OperationsMap,
    }

    impl TestConsumerFactory {
        fn new(operations: OperationsMap) -> TestConsumerFactory {
            TestConsumerFactory {
                read_operations: operations,
            }
        }
    }

    #[async_trait]
    impl ConsumerFactory for TestConsumerFactory {
        async fn new_consumer(&self) -> Box<dyn Consumer> {
            Box::new(TestConsumer {
                read_operations: Arc::clone(&self.read_operations),
            })
        }
    }

    // Get queries to create, insert to and update a table.
    fn get_queries(table_name: &str, pk_type_names: Vec<&str>) -> (String, String, String) {
        let pk_definitions = pk_type_names
            .iter()
            .enumerate()
            .map(|(i, type_name)| format!("pk{} {}", i + 1, type_name))
            .join(", ");
        let primary_key_tuple = (1..pk_type_names.len() + 1)
            .map(|i| format!("pk{}", i))
            .join(", ");
        let binds = repeat_n('?', pk_type_names.len()).join(", ");
        let pk_conditions = (1..pk_type_names.len() + 1)
            .map(|i| format!("pk{} = ?", i))
            .join(" AND ");

        (
            format!("CREATE TABLE {} ({}, ck int, v int, primary key (({}), ck)) WITH cdc = {{'enabled' : true}}",
                    table_name, pk_definitions, primary_key_tuple),
            format!("INSERT INTO {} (v, {}, ck) VALUES ({}, ?, ?)", table_name, primary_key_tuple, binds),
            format!("UPDATE {} SET v = ? WHERE {} AND ck = ?", table_name, pk_conditions)
        )
    }

    fn now() -> chrono::Duration {
        chrono::Local::now().to_timestamp()
    }

    struct Test {
        session: Arc<Session>,
        keyspace: String,
        performed_operations: HashMap<Vec<PrimaryKeyValue>, VecDeque<Operation>>,
        table_name: String,
        insert_query: PreparedStatement,
        update_query: PreparedStatement,
    }

    impl Test {
        async fn new(table_name: &str, pk_type_names: Vec<&str>) -> Result<Test> {
            let (create_query, insert_query, update_query) = get_queries(table_name, pk_type_names);

            let (session, keyspace) = prepare_db(&[create_query], 1).await?;
            let insert_query = session.prepare(insert_query).await?;
            let update_query = session.prepare(update_query).await?;

            Ok(Test {
                session,
                keyspace,
                performed_operations: HashMap::new(),
                table_name: table_name.to_string(),
                insert_query,
                update_query,
            })
        }

        fn push_back(&mut self, pk: Vec<PrimaryKeyValue>, operation: Operation) {
            self.performed_operations
                .entry(pk)
                .or_insert_with(VecDeque::new)
                .push_back(operation);
        }

        fn get_value_list(
            pk_vec: &[PrimaryKeyValue],
            ck: i32,
            v: Option<i32>,
        ) -> Vec<Option<CqlValue>> {
            let mut list: Vec<Option<CqlValue>> = vec![v.map(CqlValue::Int)];
            list.extend(pk_vec.iter().map(|x| Some(x.to_cql())));
            list.push(Some(CqlValue::Int(ck)));

            list
        }

        async fn insert(
            &mut self,
            pk_vec: Vec<PrimaryKeyValue>,
            ck: i32,
            v: Option<i32>,
        ) -> Result<()> {
            self.session
                .execute(&self.insert_query, Test::get_value_list(&pk_vec, ck, v))
                .await?;
            let operation = Operation::new(OperationType::RowInsert, Some(ck), v);
            self.push_back(pk_vec, operation);

            Ok(())
        }

        async fn update(
            &mut self,
            pk_vec: Vec<PrimaryKeyValue>,
            ck: i32,
            v: Option<i32>,
        ) -> Result<()> {
            self.session
                .execute(&self.update_query, Test::get_value_list(&pk_vec, ck, v))
                .await?;
            let operation = Operation::new(OperationType::RowUpdate, Some(ck), v);
            self.push_back(pk_vec, operation);

            Ok(())
        }

        async fn compare(&mut self, result: OperationsMap) -> bool {
            let mut results = result.lock().await.iter_mut().map(|(pk, actual_operations)| {
                let mut expected_operations = match self.performed_operations.remove(pk) {
                    Some(ops) => ops,
                    None => {
                        eprintln!("Unexpected primary key {:?}", pk);
                        return false;
                    }
                };
                let mut i = 0;

                loop {
                    match (expected_operations.pop_front(), actual_operations.pop_front()) {
                        (Some(next_expected), Some(next_actual)) => {
                            i += 1;
                            if next_expected == next_actual {
                                continue;
                            }
                            eprintln!("Operation no. {} not matching for primary key {:?}.", i, pk);
                            eprintln!("\tExpected: {:?}, actual: {:?}", next_expected, next_actual);
                        },
                        (None, None) => return true,
                        (None, _) => eprintln!("Too many read operations for primary key {:?}. Operations left: {}", pk, actual_operations.len() + 1),
                        (_, None) => eprintln!("Too little read operations for primary key {:?}. Missing operations count: {}", pk, expected_operations.len() + 1),
                    }
                    return false;
                }
            }).collect::<Vec<_>>();

            for pk in self.performed_operations.keys() {
                eprintln!("Expected primary key {:?} not found", pk);
                results.push(false);
            }
            results.into_iter().all(identity)
        }

        async fn test_cdc(mut self, start: chrono::Duration) -> Result<()> {
            let results = Arc::new(Mutex::new(HashMap::new()));
            let factory = Arc::new(TestConsumerFactory::new(Arc::clone(&results)));
            let end = now();

            let (_tester, handle) = CDCLogReader::new(
                Arc::clone(&self.session),
                self.keyspace.clone(),
                self.table_name.clone(),
                start - chrono::Duration::seconds(2),
                end + chrono::Duration::seconds(2),
                time::Duration::from_millis(WINDOW_SIZE),
                time::Duration::from_millis(SAFETY_INTERVAL),
                time::Duration::from_millis(SLEEP_INTERVAL),
                factory,
            );

            handle.await.unwrap();

            if !self.compare(results).await {
                panic!(
                    "{}",
                    format!("Test not passed for table {}.", self.table_name)
                );
            }

            Ok(())
        }
    }

    #[tokio::test]
    async fn e2e_test_small() {
        let mut test = Test::new("int_small_test", vec!["int"]).await.unwrap();
        let start = now();

        for i in 0..10 {
            for j in (3..6).rev() {
                test.insert(vec![PrimaryKeyValue::Int(i)], j, Some(i * j))
                    .await
                    .unwrap();
            }
        }

        for i in (0..10).rev() {
            for j in 3..6 {
                test.update(vec![PrimaryKeyValue::Int(i)], j, Some((i - j) * (i + j)))
                    .await
                    .unwrap();
            }
        }

        test.test_cdc(start).await.unwrap();
    }

    #[tokio::test]
    async fn e2e_test_int_pk() {
        let mut test = Test::new("int_test", vec!["int"]).await.unwrap();
        let start = now();

        for i in 0..100 {
            for j in (300..400).rev() {
                test.insert(vec![PrimaryKeyValue::Int(i)], j, Some(i * j))
                    .await
                    .unwrap();
            }
        }

        for i in (0..100).rev() {
            for j in 300..400 {
                test.update(vec![PrimaryKeyValue::Int(i)], j, Some((i - j) * (i + j)))
                    .await
                    .unwrap();
            }
        }

        test.test_cdc(start).await.unwrap();
    }

    #[tokio::test]
    async fn e2e_test_int_string_pk() {
        let mut test = Test::new("int_string_test", vec!["int", "text"])
            .await
            .unwrap();
        let strings = vec!["blep".to_string(), "nghu".to_string(), "pkeee".to_string()];
        let start = now();

        for i in 0..100 {
            for j in (300..400).rev() {
                test.insert(
                    vec![
                        PrimaryKeyValue::Int(i),
                        PrimaryKeyValue::Text(strings[(i % 3) as usize].clone()),
                    ],
                    j,
                    Some(i * j),
                )
                .await
                .unwrap();
            }
        }

        for i in (0..100).rev() {
            for j in 300..400 {
                test.update(
                    vec![
                        PrimaryKeyValue::Int(i),
                        PrimaryKeyValue::Text(strings[(i % 3) as usize].clone()),
                    ],
                    j,
                    Some((i - j) * (i + j)),
                )
                .await
                .unwrap();
            }
        }

        test.test_cdc(start).await.unwrap();
    }
}
