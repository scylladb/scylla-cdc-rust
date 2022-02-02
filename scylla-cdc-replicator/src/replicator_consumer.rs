use anyhow::anyhow;
use async_trait::async_trait;
use itertools::Itertools;
use scylla::frame::response::result::CqlValue;
use scylla::frame::response::result::CqlValue::{Map, Set};
use scylla::prepared_statement::PreparedStatement;
use scylla::query::Query;
use scylla::transport::topology::{CollectionType, ColumnKind, CqlType, Table};
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

    async fn update(&self, data: CDCRow<'_>) -> anyhow::Result<()> {
        self.update_or_insert(data, false).await?;

        Ok(())
    }

    async fn insert(&self, data: CDCRow<'_>) -> anyhow::Result<()> {
        self.update_or_insert(data, true).await?;

        Ok(())
    }

    // Function replicates adding and deleting elements
    // using v = v + {} and v = v - {} cql syntax to non-frozen map.
    // Chained operations are supported.
    async fn update_map_elements<'a>(
        &self,
        column_name: &str,
        data: &'a CDCRow<'_>,
        values_for_update: &mut Vec<&'a CqlValue>,
        timestamp: i64,
    ) -> anyhow::Result<()> {
        let empty_map = Map(vec![]); // We have to declare it, because we can't take a reference to tmp value in unwrap.
        let value = data.get_value(column_name).as_ref().unwrap_or(&empty_map);
        // Order of values: ttl, added elements, pk condition values.

        let deleted_set = Set(Vec::from(data.get_deleted_elements(column_name)));
        let mut values_for_update: Vec<&CqlValue> = values_for_update.clone();

        values_for_update[1] = value;
        values_for_update.insert(2, &deleted_set);
        // New order of values: ttl, added elements, deleted elements, pk condition values.

        self.run_statement(
            Query::new(format!(
                "UPDATE {ks}.{tbl} USING TTL ? SET {cname} = {cname} + ?, {cname} = {cname} - ? WHERE {cond}",
                ks = self.dest_keyspace_name,
                tbl = self.dest_table_name,
                cond = self.keys_cond,
                cname = column_name,
            )),
            &values_for_update,
            timestamp,
        )
        .await?;

        Ok(())
    }

    // Recreates INSERT/DELETE/UPDATE statement on non-frozen map.
    async fn update_map<'a>(
        &self,
        column_name: &str,
        data: &'a CDCRow<'_>,
        values_for_update: &mut Vec<&'a CqlValue>,
        values_for_delete: &[&CqlValue],
        timestamp: i64,
    ) -> anyhow::Result<()> {
        if data.is_value_deleted(column_name) {
            // INSERT/DELETE/OVERWRITE
            self.overwrite_column(
                column_name,
                data,
                values_for_update,
                values_for_delete,
                timestamp,
            )
            .await?;
        } else {
            // adding/removing elements
            self.update_map_elements(column_name, data, values_for_update, timestamp)
                .await?;
        }

        Ok(())
    }

    // Returns tuple consisting of TTL, timestamp and vector of consecutive values from primary key.
    fn get_common_cdc_row_data<'a>(&self, data: &'a CDCRow) -> (CqlValue, i64, Vec<&'a CqlValue>) {
        let keys_iter = ReplicatorConsumer::get_keys_iter(&self.table_schema);
        let ttl = CqlValue::Int(data.ttl.unwrap_or(0) as i32); // If data is inserted without TTL, setting it to 0 deletes existing TTL.
        let timestamp = ReplicatorConsumer::get_timestamp(data);
        let values = keys_iter
            .clone()
            .map(|col_name| data.get_value(col_name).as_ref().unwrap())
            .collect::<Vec<&CqlValue>>();

        (ttl, timestamp, values)
    }

    // Recreates INSERT/UPDATE/DELETE statement for single column in a row.
    async fn overwrite_column<'a>(
        &self,
        column_name: &str,
        data: &'a CDCRow<'_>,
        values_for_update: &mut Vec<&'a CqlValue>,
        values_for_delete: &[&CqlValue],
        timestamp: i64,
    ) -> anyhow::Result<()> {
        if let Some(value) = data.get_value(column_name) {
            // Order of values: ttl, inserted value, pk condition values.
            values_for_update[1] = value;
            self.run_statement(
                Query::new(format!(
                    "UPDATE {}.{} USING TTL ? SET {} = ? WHERE {}",
                    self.dest_keyspace_name, self.dest_table_name, column_name, self.keys_cond
                )),
                values_for_update,
                timestamp,
            )
            .await?;
        } else if data.is_value_deleted(column_name) {
            // We have to use UPDATE with … = null syntax instead of a DELETE statement as
            // DELETE will use incorrect timestamp for non-frozen collections. More info in the documentation:
            // https://docs.scylladb.com/using-scylla/cdc/cdc-advanced-types/#collection-wide-tombstones-and-timestamps
            self.run_statement(
                Query::new(format!(
                    "UPDATE {}.{} SET {} = NULL WHERE {}",
                    self.dest_keyspace_name, self.dest_table_name, column_name, self.keys_cond
                )),
                values_for_delete,
                timestamp,
            )
            .await?;
        }

        Ok(())
    }

    async fn update_or_insert(&self, data: CDCRow<'_>, is_insert: bool) -> anyhow::Result<()> {
        let (ttl, timestamp, values) = self.get_common_cdc_row_data(&data);

        if is_insert {
            // Insert row with nulls, the rest will be done through an update.
            let mut insert_values = Vec::with_capacity(values.len() + 1);
            insert_values.extend(values.iter());
            insert_values.push(&ttl);

            self.run_prepared_statement(self.insert_query.clone(), &insert_values, timestamp)
                .await?;
        }

        let mut values_for_update = Vec::with_capacity(2 + values.len());
        values_for_update.extend([&ttl, &CqlValue::Int(0)]);
        values_for_update.extend(values.iter());

        for column_name in &self.non_key_columns {
            match &self.table_schema.columns.get(column_name).unwrap().type_ {
                CqlType::Native(_)
                | CqlType::Tuple(_)
                | CqlType::Collection { frozen: true, .. }
                | CqlType::UserDefinedType { frozen: true, .. } => {
                    self.overwrite_column(
                        column_name,
                        &data,
                        &mut values_for_update,
                        &values,
                        timestamp,
                    )
                    .await?
                }
                CqlType::Collection {
                    frozen: false,
                    type_: t,
                } => match t {
                    CollectionType::List(_) => {
                        todo!("This type of data can't be replicated yet!")
                    }
                    CollectionType::Map(_, _) => {
                        self.update_map(
                            column_name,
                            &data,
                            &mut values_for_update,
                            &values,
                            timestamp,
                        )
                        .await?
                    }
                    CollectionType::Set(_) => {
                        todo!("This type of data can't be replicated yet!")
                    }
                },
                _ => todo!("This type of data can't be replicated yet!"),
            }
        }

        Ok(())
    }

    async fn run_prepared_statement(
        &self,
        mut query: PreparedStatement,
        values: &[&CqlValue],
        timestamp: i64,
    ) -> anyhow::Result<()> {
        query.set_timestamp(Some(timestamp));
        self.dest_session.execute(&query, values).await?;

        Ok(())
    }

    async fn run_statement(
        &self,
        mut query: Query,
        values: &[&CqlValue],
        timestamp: i64,
    ) -> anyhow::Result<()> {
        query.set_timestamp(Some(timestamp));
        self.dest_session.query(query, values).await?;

        Ok(())
    }
}

#[async_trait]
impl Consumer for ReplicatorConsumer {
    async fn consume_cdc(&mut self, data: CDCRow<'_>) -> anyhow::Result<()> {
        match data.operation {
            OperationType::RowUpdate => self.update(data).await?,
            OperationType::RowInsert => self.insert(data).await?,
            _ => todo!("This type of operation is not supported yet."),
        }

        Ok(())
    }
}

pub struct ReplicatorConsumerFactory {
    dest_session: Arc<Session>,
    dest_keyspace_name: String,
    dest_table_name: String,
    table_schema: Table,
}

impl ReplicatorConsumerFactory {
    /// Creates a new instance of `ReplicatorConsumerFactory`.
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
