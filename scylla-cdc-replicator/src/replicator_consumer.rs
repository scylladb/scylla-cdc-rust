use anyhow::anyhow;
use async_trait::async_trait;
use itertools::Itertools;
use scylla::frame::response::result::CqlValue;
use scylla::prepared_statement::PreparedStatement;
use scylla::query::Query;
use scylla::transport::topology::{ColumnKind, CqlType, Table};
use scylla::Session;
use scylla_cdc::consumer::*;
use std::sync::Arc;

pub(crate) struct ReplicatorConsumer {
    dest_session: Arc<Session>,
    dest_keyspace_name: String,
    dest_table_name: String,
    table_schema: Table,
    non_key_columns: Vec<String>,

    // Prepared queries.
    insert_query: PreparedStatement,

    // Strings for queries created dynamically:
    keys_cond: String,
}

impl ReplicatorConsumer {
    pub(crate) async fn new(
        dest_session: Arc<Session>,
        dest_keyspace_name: String,
        dest_table_name: String,
        table_schema: Table,
    ) -> ReplicatorConsumer {
        // Iterator for both: partition keys and clustering keys.
        let keys_iter = ReplicatorConsumer::get_keys_iter(&table_schema);
        // Collect names of columns that are not clustering or partition key.
        let non_key_columns = table_schema
            .columns
            .iter()
            .filter(|column| {
                column.1.kind != ColumnKind::Clustering && column.1.kind != ColumnKind::PartitionKey
            })
            .map(|column| column.0.clone())
            .collect::<Vec<String>>();

        // Clone, because the iterator is consumed.
        let names = keys_iter.clone().join(",");
        let markers = keys_iter.clone().map(|_| "?").join(",");

        let insert_query = dest_session
            .prepare(format!(
                "INSERT INTO {}.{} ({}) VALUES ({}) USING TTL ?",
                dest_keyspace_name, dest_table_name, names, markers
            ))
            .await
            .expect("Preparing insert query failed.");

        let keys_cond = keys_iter.map(|name| format!("{} = ?", name)).join(" AND ");

        ReplicatorConsumer {
            dest_session,
            dest_keyspace_name,
            dest_table_name,
            table_schema,
            non_key_columns,
            insert_query,
            keys_cond,
        }
    }

    fn get_keys_iter(table_schema: &Table) -> impl std::iter::Iterator<Item = &String> + Clone {
        table_schema
            .partition_key
            .iter()
            .chain(table_schema.clustering_key.iter())
    }

    fn get_timestamp(data: &CDCRow<'_>) -> i64 {
        const NANOS_IN_MILLIS: u64 = 1000;
        (data.time.to_timestamp().unwrap().to_unix_nanos() / NANOS_IN_MILLIS) as i64
    }

    async fn update(&self, data: CDCRow<'_>) {
        self.update_or_insert(data, false).await;
    }

    async fn insert(&self, data: CDCRow<'_>) {
        self.update_or_insert(data, true).await;
    }

    async fn update_atomic_or_frozen<'a>(
        &self,
        column_name: &str,
        data: &'a CDCRow<'_>,
        values: &mut Vec<&'a CqlValue>,
        timestamp: i64,
    ) {
        if let Some(value) = data.get_value(column_name) {
            // Order of values: ttl, inserted value, pk condition values.
            values[1] = value;
            self.run_statement(
                Query::new(format!(
                    "UPDATE {}.{} USING TTL ? SET {} = ? WHERE {}",
                    self.dest_keyspace_name, self.dest_table_name, column_name, self.keys_cond
                )),
                values,
                timestamp,
            )
            .await;
        } else if data.is_value_deleted(column_name) {
            self.run_statement(
                Query::new(format!(
                    "DELETE {} FROM {}.{} WHERE {}",
                    column_name, self.dest_keyspace_name, self.dest_table_name, self.keys_cond
                )),
                &values[2..],
                timestamp,
            )
            .await;
        }
    }

    async fn update_or_insert(&self, data: CDCRow<'_>, is_insert: bool) {
        let keys_iter = ReplicatorConsumer::get_keys_iter(&self.table_schema);
        let ttl = CqlValue::Int(data.ttl.unwrap_or(0) as i32); // If data is inserted without TTL, setting it to 0 deletes existing TTL.
        let timestamp = ReplicatorConsumer::get_timestamp(&data);
        let primary_key_values = keys_iter
            .map(|col_name| data.get_value(col_name).as_ref().unwrap())
            .collect::<Vec<&CqlValue>>();

        if is_insert {
            // Insert row with nulls, the rest will be done through an update.
            let mut insert_values = Vec::with_capacity(primary_key_values.len() + 1);
            insert_values.extend(primary_key_values.iter());
            insert_values.push(&ttl);

            self.run_prepared_statement(self.insert_query.clone(), &insert_values, timestamp)
                .await;
        }

        let mut update_values = Vec::with_capacity(2 + primary_key_values.len());
        update_values.extend([&ttl, &CqlValue::Int(0)]);
        update_values.extend(primary_key_values.iter());

        for column_name in &self.non_key_columns {
            match self.table_schema.columns.get(column_name).unwrap().type_ {
                CqlType::Native(_)
                | CqlType::Tuple(_)
                | CqlType::Collection { frozen: true, .. }
                | CqlType::UserDefinedType { frozen: true, .. } => {
                    self.update_atomic_or_frozen(column_name, &data, &mut update_values, timestamp)
                        .await
                }
                _ => todo!("This type of data can't be replicated yet!"),
            }
        }
    }

    async fn run_prepared_statement(
        &self,
        mut query: PreparedStatement,
        values: &[&CqlValue],
        timestamp: i64,
    ) {
        query.set_timestamp(Some(timestamp));
        self.dest_session
            .execute(&query, values)
            .await
            // If a query fails, the replication will not be done correctly, panic.
            .expect("Querying the database failed!");
    }

    async fn run_statement(&self, mut query: Query, values: &[&CqlValue], timestamp: i64) {
        query.set_timestamp(Some(timestamp));
        self.dest_session
            .query(query, values)
            .await
            // If a query fails, the replication will not be done correctly, panic.
            .expect("Querying the database failed!");
    }
}

#[async_trait]
impl Consumer for ReplicatorConsumer {
    async fn consume_cdc(&mut self, data: CDCRow<'_>) {
        match data.operation {
            OperationType::RowUpdate => self.update(data).await,
            OperationType::RowInsert => self.insert(data).await,
            _ => todo!("This type of operation is not supported yet."),
        }
    }
}

pub struct ReplicatorConsumerFactory {
    dest_session: Arc<Session>,
    dest_keyspace_name: String,
    dest_table_name: String,
    table_schema: Table,
}

impl ReplicatorConsumerFactory {
    /// Creates a new instance of ReplicatorConsumerFactory.
    /// Fetching schema metadata must be enabled in the session.
    pub fn new(
        dest_session: Arc<Session>,
        dest_keyspace_name: String,
        dest_table_name: String,
    ) -> anyhow::Result<ReplicatorConsumerFactory> {
        let table_schema = dest_session
            .get_cluster_data()
            .get_keyspace_info()
            .get(&dest_keyspace_name)
            .ok_or(anyhow!("Keyspace not found"))?
            .tables
            .get(&dest_table_name.to_ascii_lowercase())
            .ok_or(anyhow!("Table not found"))?
            .clone();

        Ok(ReplicatorConsumerFactory {
            dest_session,
            dest_keyspace_name,
            dest_table_name,
            table_schema,
        })
    }
}

#[async_trait]
impl ConsumerFactory for ReplicatorConsumerFactory {
    async fn new_consumer(&self) -> Box<dyn Consumer> {
        Box::new(
            ReplicatorConsumer::new(
                self.dest_session.clone(),
                self.dest_keyspace_name.clone(),
                self.dest_table_name.clone(),
                self.table_schema.clone(),
            )
            .await,
        )
    }
}
