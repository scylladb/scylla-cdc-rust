use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use itertools::Itertools;
use scylla::frame::response::result::CqlValue;
use scylla::frame::response::result::CqlValue::{Map, Set};
use scylla::frame::value::Value;
use scylla::prepared_statement::PreparedStatement;
use scylla::query::Query;
use scylla::transport::topology::{CollectionType, ColumnKind, CqlType, Table};
use scylla::Session;

use scylla_cdc::consumer::*;

struct DestinationTableParams {
    dest_session: Arc<Session>,
    dest_keyspace_name: String,
    dest_table_name: String,

    // Strings for queries created dynamically:
    keys_cond: String,
}

struct PrecomputedQueries {
    destination_table_params: DestinationTableParams,
    insert_query: PreparedStatement,
    partition_delete_query: PreparedStatement,
    delete_query: PreparedStatement,
}

impl PrecomputedQueries {
    async fn new(
        dest_session: Arc<Session>,
        dest_keyspace_name: String,
        dest_table_name: String,
        table_schema: &Table,
    ) -> anyhow::Result<PrecomputedQueries> {
        // Iterator for both: partition keys and clustering keys.
        let keys_iter = get_keys_iter(table_schema);

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
        let partition_keys_cond = &table_schema
            .partition_key
            .iter()
            .map(|name| format!("{} = ?", name))
            .join(" AND ");

        let partition_delete_query = dest_session
            .prepare(format!(
                "DELETE FROM {}.{} WHERE {}",
                dest_keyspace_name, dest_table_name, partition_keys_cond
            ))
            .await
            .expect("Preparing partition delete query failed.");

        let delete_query = dest_session
            .prepare(format!(
                "DELETE FROM {}.{} WHERE {}",
                dest_keyspace_name, dest_table_name, keys_cond
            ))
            .await
            .expect("Preparing delete query failed.");

        let destination_table_params = DestinationTableParams {
            dest_session,
            dest_keyspace_name,
            dest_table_name,
            keys_cond,
        };

        Ok(PrecomputedQueries {
            destination_table_params,
            insert_query,
            partition_delete_query,
            delete_query,
        })
    }

    async fn delete_partition(
        &mut self,
        values: &[impl Value],
        timestamp: i64,
    ) -> anyhow::Result<()> {
        run_prepared_statement(
            &self.destination_table_params.dest_session,
            self.partition_delete_query.clone(),
            values,
            timestamp,
        )
        .await?;

        Ok(())
    }

    async fn insert_value(&mut self, values: &[impl Value], timestamp: i64) -> anyhow::Result<()> {
        run_prepared_statement(
            &self.destination_table_params.dest_session,
            self.insert_query.clone(),
            values,
            timestamp,
        )
        .await?;

        Ok(())
    }

    async fn delete_row(&mut self, values: &[impl Value], timestamp: i64) -> anyhow::Result<()> {
        run_prepared_statement(
            &self.destination_table_params.dest_session,
            self.delete_query.clone(),
            values,
            timestamp,
        )
        .await?;

        Ok(())
    }

    async fn overwrite_value(
        &mut self,
        values: &[impl Value],
        timestamp: i64,
        column_name: &str,
    ) -> anyhow::Result<()> {
        run_statement(
            &self.destination_table_params.dest_session,
            Query::new(format!(
                "UPDATE {}.{} USING TTL ? SET {} = ? WHERE {}",
                self.destination_table_params.dest_keyspace_name,
                self.destination_table_params.dest_table_name,
                column_name,
                self.destination_table_params.keys_cond
            )),
            values,
            timestamp,
        )
        .await?;

        Ok(())
    }

    async fn delete_value(
        &mut self,
        values: &[impl Value],
        timestamp: i64,
        column_name: &str,
    ) -> anyhow::Result<()> {
        // We have to use UPDATE with … = null syntax instead of a DELETE statement as
        // DELETE will use incorrect timestamp for non-frozen collections. More info in the documentation:
        // https://docs.scylladb.com/using-scylla/cdc/cdc-advanced-types/#collection-wide-tombstones-and-timestamps
        run_statement(
            &self.destination_table_params.dest_session,
            Query::new(format!(
                "UPDATE {}.{} SET {} = NULL WHERE {}",
                self.destination_table_params.dest_keyspace_name,
                self.destination_table_params.dest_table_name,
                column_name,
                self.destination_table_params.keys_cond
            )),
            values,
            timestamp,
        )
        .await?;

        Ok(())
    }

    async fn update_map_or_set_elements(
        &mut self,
        values: &[impl Value],
        timestamp: i64,
        column_name: &str,
    ) -> anyhow::Result<()> {
        run_statement(
            &self.destination_table_params.dest_session,
            Query::new(format!(
                "UPDATE {ks}.{tbl} USING TTL ? SET {cname} = {cname} + ?, {cname} = {cname} - ? WHERE {cond}",
                ks = self.destination_table_params.dest_keyspace_name,
                tbl = self.destination_table_params.dest_table_name,
                cond = self.destination_table_params.keys_cond,
                cname = column_name,
            )),
            values,
            timestamp,
        ).await?;

        Ok(())
    }

    async fn delete_list_value(
        &mut self,
        values: &[impl Value],
        timestamp: i64,
        column_name: &str,
    ) -> anyhow::Result<()> {
        // If the list was replaced with syntax "(...) SET l = [(...)]",
        // we have to manually delete the list
        // and add every element with timeuuid from the original table,
        // because we want to preserve the timestamps of the list elements.
        // More information:
        // https://docs.scylladb.com/using-scylla/cdc/cdc-advanced-types/#collection-wide-tombstones-and-timestamps
        run_statement(
            &self.destination_table_params.dest_session,
            Query::new(format!(
                "UPDATE {ks}.{tbl} SET {col} = null WHERE {cond}",
                ks = self.destination_table_params.dest_keyspace_name,
                tbl = self.destination_table_params.dest_table_name,
                col = column_name,
                cond = self.destination_table_params.keys_cond,
            )),
            values,
            timestamp,
        )
        .await?;

        Ok(())
    }

    async fn update_list_elements(
        &mut self,
        values: &[impl Value],
        timestamp: i64,
        column_name: &str,
    ) -> anyhow::Result<()> {
        // We use the same query for updating and deleting values.
        // In case of deleting, we simply set the value to null.
        let update_query = Query::new(format!(
            "UPDATE {ks}.{tbl} USING TTL ? SET {list}[SCYLLA_TIMEUUID_LIST_INDEX(?)] = ? WHERE {cond}",
            ks = self.destination_table_params.dest_keyspace_name,
            tbl = self.destination_table_params.dest_table_name,
            list = column_name,
            cond = self.destination_table_params.keys_cond
        ));

        run_statement(
            &self.destination_table_params.dest_session,
            update_query.clone(),
            values,
            timestamp,
        )
        .await?;

        Ok(())
    }

    async fn update_udt_elements(
        &mut self,
        values: &[impl Value],
        timestamp: i64,
        column_name: &str,
    ) -> anyhow::Result<()> {
        let update_query = format!(
            "UPDATE {ks}.{tbl} USING TTL ? SET {cname} = ? WHERE {cond}",
            ks = self.destination_table_params.dest_keyspace_name,
            tbl = self.destination_table_params.dest_table_name,
            cname = column_name,
            cond = self.destination_table_params.keys_cond,
        );

        run_statement(
            &self.destination_table_params.dest_session,
            Query::new(update_query),
            values,
            timestamp,
        )
        .await?;

        Ok(())
    }

    async fn delete_udt_elements(
        &mut self,
        values: &[impl Value],
        timestamp: i64,
        removed_fields: &str,
    ) -> anyhow::Result<()> {
        let remove_query = format!(
            "UPDATE {ks}.{tbl} USING TTL ? SET {removed_fields} WHERE {cond}",
            ks = self.destination_table_params.dest_keyspace_name,
            tbl = self.destination_table_params.dest_table_name,
            removed_fields = removed_fields,
            cond = self.destination_table_params.keys_cond,
        );

        run_statement(
            &self.destination_table_params.dest_session,
            Query::new(remove_query),
            values,
            timestamp,
        )
        .await?;

        Ok(())
    }
}

async fn run_prepared_statement(
    dest_session: &Arc<Session>,
    mut query: PreparedStatement,
    values: &[impl Value],
    timestamp: i64,
) -> anyhow::Result<()> {
    query.set_timestamp(Some(timestamp));
    dest_session.execute(&query, values).await?;

    Ok(())
}

async fn run_statement(
    dest_session: &Arc<Session>,
    mut query: Query,
    values: &[impl Value],
    timestamp: i64,
) -> anyhow::Result<()> {
    query.set_timestamp(Some(timestamp));
    dest_session.query(query, values).await?;

    Ok(())
}

struct SourceTableData {
    table_schema: Table,
    non_key_columns: Vec<String>,
}

pub(crate) struct ReplicatorConsumer {
    source_table_data: SourceTableData,
    precomputed_queries: PrecomputedQueries,
}

impl ReplicatorConsumer {
    pub(crate) async fn new(
        dest_session: Arc<Session>,
        dest_keyspace_name: String,
        dest_table_name: String,
        table_schema: Table,
    ) -> ReplicatorConsumer {
        // Collect names of columns that are not clustering or partition key.
        let non_key_columns = table_schema
            .columns
            .iter()
            .filter(|column| {
                column.1.kind != ColumnKind::Clustering && column.1.kind != ColumnKind::PartitionKey
            })
            .map(|column| column.0.clone())
            .collect::<Vec<String>>();

        let precomputed_queries = PrecomputedQueries::new(
            dest_session,
            dest_keyspace_name,
            dest_table_name,
            &table_schema,
        )
        .await
        .expect("Preparing precomputed queries failed.");

        let source_table_data = SourceTableData {
            table_schema,
            non_key_columns,
        };

        ReplicatorConsumer {
            source_table_data,
            precomputed_queries,
        }
    }

    fn get_timestamp(data: &CDCRow<'_>) -> i64 {
        const NANOS_IN_MILLIS: u64 = 1000;
        (data.time.to_timestamp().unwrap().to_unix_nanos() / NANOS_IN_MILLIS) as i64
    }

    async fn delete_partition(&mut self, data: CDCRow<'_>) -> anyhow::Result<()> {
        let timestamp = ReplicatorConsumer::get_timestamp(&data);
        let partition_keys_iter = self.source_table_data.table_schema.partition_key.iter();
        let values = partition_keys_iter
            .map(|col_name| data.get_value(col_name).as_ref().unwrap())
            .collect::<Vec<&CqlValue>>();

        self.precomputed_queries
            .delete_partition(&values, timestamp)
            .await?;

        Ok(())
    }

    async fn update(&mut self, data: CDCRow<'_>) -> anyhow::Result<()> {
        self.update_or_insert(data, false).await?;

        Ok(())
    }

    async fn insert(&mut self, data: CDCRow<'_>) -> anyhow::Result<()> {
        self.update_or_insert(data, true).await?;

        Ok(())
    }

    // Function replicates adding and deleting elements
    // using v = v + {} and v = v - {} cql syntax to non-frozen map or set.
    // Chained operations are supported.
    async fn update_map_or_set_elements<'a>(
        &mut self,
        column_name: &str,
        data: &'a CDCRow<'_>,
        values_for_update: &mut [&'a CqlValue],
        timestamp: i64,
        sentinel: CqlValue,
    ) -> anyhow::Result<()> {
        let value = data.get_value(column_name).as_ref().unwrap_or(&sentinel);
        // Order of values: ttl, added elements, pk condition values.

        let deleted_set = Set(Vec::from(data.get_deleted_elements(column_name)));
        let mut values_for_update = values_for_update.to_vec();

        values_for_update[1] = value;
        values_for_update.insert(2, &deleted_set);
        // New order of values: ttl, added elements, deleted elements, pk condition values.

        self.precomputed_queries
            .update_map_or_set_elements(&values_for_update, timestamp, column_name)
            .await?;

        Ok(())
    }

    // Recreates INSERT/DELETE/UPDATE statement on non-frozen map or set.
    async fn update_map_or_set<'a>(
        &mut self,
        column_name: &str,
        data: &'a CDCRow<'_>,
        values_for_update: &mut [&'a CqlValue],
        values_for_delete: &[&CqlValue],
        timestamp: i64,
        sentinel: CqlValue,
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
            self.update_map_or_set_elements(
                column_name,
                data,
                values_for_update,
                timestamp,
                sentinel,
            )
            .await?;
        }

        Ok(())
    }

    // Recreates INSERT/DELETE/UPDATE statement on non-frozen list.
    async fn update_list<'a>(
        &mut self,
        column_name: &str,
        data: &'a CDCRow<'_>,
        values_for_update: &mut [Option<&'a CqlValue>],
        pk_values: &[&CqlValue],
        timestamp: i64,
    ) -> anyhow::Result<()> {
        if data.is_value_deleted(column_name) {
            self.precomputed_queries
                .delete_list_value(pk_values, timestamp, column_name)
                .await?;
        }

        if let Some(added_elements) = data.get_value(column_name) {
            let added_elements = added_elements.as_map().unwrap();
            for (key, val) in added_elements {
                values_for_update[1] = Some(key);
                values_for_update[2] = Some(val);

                self.precomputed_queries
                    .update_list_elements(values_for_update, timestamp, column_name)
                    .await?;
            }
        }

        for removed in data.get_deleted_elements(column_name) {
            values_for_update[1] = Some(removed);
            values_for_update[2] = None;

            self.precomputed_queries
                .update_list_elements(values_for_update, timestamp, column_name)
                .await?;
        }

        Ok(())
    }

    // Function replicates adding and deleting elements in udt
    async fn update_udt_elements<'a>(
        &mut self,
        column_name: &str,
        data: &'a CDCRow<'_>,
        values_for_update: &mut [&'a CqlValue],
        timestamp: i64,
    ) -> anyhow::Result<()> {
        let empty_udt = CqlValue::UserDefinedType {
            keyspace: "".to_string(),
            type_name: "".to_string(),
            fields: vec![],
        };
        let value = data.get_value(column_name).as_ref().unwrap_or(&empty_udt);
        // Order of values: ttl, added elements, pk condition values.

        let values_for_update = &mut values_for_update.to_vec();
        values_for_update[1] = value;

        self.precomputed_queries
            .update_udt_elements(values_for_update, timestamp, column_name)
            .await?;

        self.delete_udt_elements(column_name, data, timestamp, value, values_for_update)
            .await?;

        Ok(())
    }

    async fn delete_udt_elements<'a>(
        &mut self,
        column_name: &str,
        data: &'a CDCRow<'_>,
        timestamp: i64,
        value: &CqlValue,
        values_for_update: &mut [&'a CqlValue],
    ) -> anyhow::Result<()> {
        let deleted_set = Vec::from(data.get_deleted_elements(column_name));

        if !deleted_set.is_empty() {
            let udt_fields = value.as_udt().unwrap();
            let removed_fields = deleted_set
                .iter()
                .map(|deleted_idx| {
                    let idx = deleted_idx.as_smallint().unwrap();
                    let (udt_fields_by_id, _) = udt_fields.get(idx as usize).unwrap();
                    format!("{}.{} = null", column_name, udt_fields_by_id)
                })
                .join(",");

            let values_for_update = &mut values_for_update.to_vec();
            values_for_update.remove(1);

            self.precomputed_queries
                .delete_udt_elements(values_for_update, timestamp, removed_fields.as_str())
                .await?;
        }

        Ok(())
    }

    async fn update_udt<'a>(
        &mut self,
        column_name: &str,
        data: &'a CDCRow<'_>,
        values_for_update: &mut [&'a CqlValue],
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
            // add/remove elements in udt
            self.update_udt_elements(column_name, data, values_for_update, timestamp)
                .await?;
        }

        Ok(())
    }

    // Returns tuple consisting of TTL, timestamp and vector of consecutive values from primary key.
    fn get_common_cdc_row_data<'a>(&self, data: &'a CDCRow) -> (CqlValue, i64, Vec<&'a CqlValue>) {
        let keys_iter = get_keys_iter(&self.source_table_data.table_schema);
        let ttl = CqlValue::Int(data.ttl.unwrap_or(0) as i32); // If data is inserted without TTL, setting it to 0 deletes existing TTL.
        let timestamp = ReplicatorConsumer::get_timestamp(data);
        let values = keys_iter
            .clone()
            .map(|col_name| data.get_value(col_name).as_ref().unwrap())
            .collect::<Vec<&CqlValue>>();

        (ttl, timestamp, values)
    }

    // Recreates row deletion.
    async fn delete_row(&mut self, data: CDCRow<'_>) -> anyhow::Result<()> {
        let (_, timestamp, values) = self.get_common_cdc_row_data(&data);
        self.precomputed_queries
            .delete_row(&values, timestamp)
            .await?;

        Ok(())
    }

    // Recreates INSERT/UPDATE/DELETE statement for single column in a row.
    async fn overwrite_column<'a>(
        &mut self,
        column_name: &str,
        data: &'a CDCRow<'_>,
        values_for_update: &mut [&'a CqlValue],
        values_for_delete: &[&CqlValue],
        timestamp: i64,
    ) -> anyhow::Result<()> {
        if let Some(value) = data.get_value(column_name) {
            // Order of values: ttl, inserted value, pk condition values.
            values_for_update[1] = value;
            self.precomputed_queries
                .overwrite_value(values_for_update, timestamp, column_name)
                .await?;
        } else if data.is_value_deleted(column_name) {
            self.precomputed_queries
                .delete_value(values_for_delete, timestamp, column_name)
                .await?;
        }

        Ok(())
    }

    async fn update_or_insert(&mut self, data: CDCRow<'_>, is_insert: bool) -> anyhow::Result<()> {
        let (ttl, timestamp, values) = self.get_common_cdc_row_data(&data);

        if is_insert {
            // Insert row with nulls, the rest will be done through an update.
            let mut insert_values = Vec::with_capacity(values.len() + 1);
            insert_values.extend(values.iter());
            insert_values.push(&ttl);

            self.precomputed_queries
                .insert_value(&insert_values, timestamp)
                .await?;
        }

        let mut values_for_update = Vec::with_capacity(2 + values.len());
        values_for_update.extend([&ttl, &CqlValue::Int(0)]);
        values_for_update.extend(values.iter());

        let mut values_for_list_update = Vec::with_capacity(3 + values.len());
        values_for_list_update.extend([Some(&ttl), None, None]);
        values_for_list_update.extend(values.iter().map(|x| Some(*x)));

        for column_name in &self.source_table_data.non_key_columns.clone() {
            match &self
                .source_table_data
                .table_schema
                .columns
                .get(column_name)
                .unwrap()
                .type_
            {
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
                        self.update_list(
                            column_name,
                            &data,
                            &mut values_for_list_update,
                            &values,
                            timestamp,
                        )
                        .await?
                    }
                    CollectionType::Map(_, _) => {
                        self.update_map_or_set(
                            column_name,
                            &data,
                            &mut values_for_update,
                            &values,
                            timestamp,
                            Map(vec![]),
                        )
                        .await?
                    }
                    CollectionType::Set(_) => {
                        self.update_map_or_set(
                            column_name,
                            &data,
                            &mut values_for_update,
                            &values,
                            timestamp,
                            Set(vec![]),
                        )
                        .await?
                    }
                },
                CqlType::UserDefinedType { frozen: false, .. } => {
                    self.update_udt(
                        column_name,
                        &data,
                        &mut values_for_update,
                        &values,
                        timestamp,
                    )
                    .await?
                }
            }
        }

        Ok(())
    }
}

fn get_keys_iter(table_schema: &Table) -> impl std::iter::Iterator<Item = &String> + Clone {
    table_schema
        .partition_key
        .iter()
        .chain(table_schema.clustering_key.iter())
}

#[async_trait]
impl Consumer for ReplicatorConsumer {
    async fn consume_cdc(&mut self, data: CDCRow<'_>) -> anyhow::Result<()> {
        match data.operation {
            OperationType::RowUpdate => self.update(data).await?,
            OperationType::RowInsert => self.insert(data).await?,
            OperationType::RowDelete => self.delete_row(data).await?,
            OperationType::PartitionDelete => self.delete_partition(data).await?,
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
            .ok_or_else(|| anyhow!("Keyspace not found"))?
            .tables
            .get(&dest_table_name.to_ascii_lowercase())
            .ok_or_else(|| anyhow!("Table not found"))?
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
