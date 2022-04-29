use std::collections::HashMap;
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
    session: Arc<Session>,
    keyspace_table_name: String,

    // Strings for queries created dynamically:
    keys_cond: String,
}

struct PrecomputedQueries {
    destination_table_params: DestinationTableParams,
    insert_query: PreparedStatement,
    partition_delete_query: PreparedStatement,
    delete_query: PreparedStatement,
    delete_value_queries: HashMap<String, PreparedStatement>,
    overwrite_queries: HashMap<String, PreparedStatement>,
    update_list_elements_queries: HashMap<String, PreparedStatement>,
    update_map_or_set_elements_queries: HashMap<String, PreparedStatement>,
}

impl PrecomputedQueries {
    async fn new(
        session: Arc<Session>,
        dest_keyspace_name: String,
        dest_table_name: String,
        table_schema: &Table,
    ) -> anyhow::Result<PrecomputedQueries> {
        // Iterator for both: partition keys and clustering keys.
        let keys_iter = get_keys_iter(table_schema);

        let keyspace_table_name = format!("{}.{}", dest_keyspace_name, dest_table_name);
        // Clone, because the iterator is consumed.
        let names = keys_iter.clone().join(",");
        let markers = keys_iter.clone().map(|_| "?").join(",");

        let insert_query = session
            .prepare(format!(
                "INSERT INTO {} ({}) VALUES ({}) USING TTL ?",
                keyspace_table_name, names, markers
            ))
            .await
            .expect("Preparing insert query failed.");

        let keys_cond = keys_iter.map(|name| format!("{} = ?", name)).join(" AND ");
        let partition_keys_cond = &table_schema
            .partition_key
            .iter()
            .map(|name| format!("{} = ?", name))
            .join(" AND ");

        let partition_delete_query = session
            .prepare(format!(
                "DELETE FROM {} WHERE {}",
                keyspace_table_name, partition_keys_cond
            ))
            .await
            .expect("Preparing partition delete query failed.");

        let delete_query = session
            .prepare(format!(
                "DELETE FROM {} WHERE {}",
                keyspace_table_name, keys_cond
            ))
            .await
            .expect("Preparing delete query failed.");

        let destination_table_params = DestinationTableParams {
            session,
            keyspace_table_name,
            keys_cond,
        };

        let delete_value_queries = HashMap::new();
        let overwrite_queries = HashMap::new();
        let update_list_elements_queries = HashMap::new();
        let update_map_or_set_elements_queries = HashMap::new();

        Ok(PrecomputedQueries {
            destination_table_params,
            insert_query,
            partition_delete_query,
            delete_query,
            delete_value_queries,
            overwrite_queries,
            update_list_elements_queries,
            update_map_or_set_elements_queries,
        })
    }

    async fn delete_partition(
        &mut self,
        values: &[impl Value],
        timestamp: i64,
    ) -> anyhow::Result<()> {
        run_prepared_statement(
            &self.destination_table_params.session,
            self.partition_delete_query.clone(),
            values,
            timestamp,
        )
        .await
    }

    async fn insert_value(&mut self, values: &[impl Value], timestamp: i64) -> anyhow::Result<()> {
        run_prepared_statement(
            &self.destination_table_params.session,
            self.insert_query.clone(),
            values,
            timestamp,
        )
        .await
    }

    async fn delete_row(&mut self, values: &[impl Value], timestamp: i64) -> anyhow::Result<()> {
        run_prepared_statement(
            &self.destination_table_params.session,
            self.delete_query.clone(),
            values,
            timestamp,
        )
        .await
    }

    async fn overwrite_value(
        &mut self,
        values: &[impl Value],
        timestamp: i64,
        column_name: &str,
    ) -> anyhow::Result<()> {
        let overwrite_query = match self.overwrite_queries.get_mut(column_name) {
            Some(query) => query,
            None => self
                .overwrite_queries
                .entry(column_name.to_string())
                .or_insert(
                    self.destination_table_params
                        .session
                        .prepare(format!(
                            "UPDATE {} USING TTL ? SET {} = ? WHERE {}",
                            self.destination_table_params.keyspace_table_name,
                            column_name,
                            self.destination_table_params.keys_cond
                        ))
                        .await?,
                ),
        };

        run_prepared_statement(
            &self.destination_table_params.session,
            overwrite_query.clone(),
            values,
            timestamp,
        )
        .await
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
        let delete_query = match self.delete_value_queries.get_mut(column_name) {
            Some(query) => query,
            None => self
                .delete_value_queries
                .entry(column_name.to_string())
                .or_insert(
                    self.destination_table_params
                        .session
                        .prepare(format!(
                            "UPDATE {} SET {} = NULL WHERE {}",
                            self.destination_table_params.keyspace_table_name,
                            column_name,
                            self.destination_table_params.keys_cond
                        ))
                        .await?,
                ),
        };

        run_prepared_statement(
            &self.destination_table_params.session,
            delete_query.clone(),
            values,
            timestamp,
        )
        .await
    }

    async fn update_map_or_set_elements(
        &mut self,
        values: &[impl Value],
        timestamp: i64,
        column_name: &str,
    ) -> anyhow::Result<()> {
        let update_query = match self.update_map_or_set_elements_queries.get_mut(column_name) {
            Some(query) => query,
            None => {
                self.update_map_or_set_elements_queries
                    .entry(column_name.to_string())
                    .or_insert(
                        self.destination_table_params.session.prepare(
                            format!(
                                "UPDATE {tbl} USING TTL ? SET {cname} = {cname} + ?, {cname} = {cname} - ? WHERE {cond}",
                                tbl = self.destination_table_params.keyspace_table_name,
                                cond = self.destination_table_params.keys_cond,
                                cname = column_name,
                            )
                        ).await?
                    )
            }
        };

        run_prepared_statement(
            &self.destination_table_params.session,
            update_query.clone(),
            values,
            timestamp,
        )
        .await
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
        let delete_query = match self.delete_value_queries.get_mut(column_name) {
            Some(query) => query,
            None => self
                .delete_value_queries
                .entry(column_name.to_string())
                .or_insert(
                    self.destination_table_params
                        .session
                        .prepare(format!(
                            "UPDATE {tbl} SET {col} = null WHERE {cond}",
                            tbl = self.destination_table_params.keyspace_table_name,
                            col = column_name,
                            cond = self.destination_table_params.keys_cond,
                        ))
                        .await?,
                ),
        };

        run_prepared_statement(
            &self.destination_table_params.session,
            delete_query.clone(),
            values,
            timestamp,
        )
        .await
    }

    async fn update_list_elements(
        &mut self,
        values: &[impl Value],
        timestamp: i64,
        column_name: &str,
    ) -> anyhow::Result<()> {
        // We use the same query for updating and deleting values.
        // In case of deleting, we simply set the value to null.
        let update_query = match self.update_list_elements_queries.get_mut(column_name) {
            Some(query) => query,
            None => self
                .update_list_elements_queries
                .entry(column_name.to_string())
                .or_insert(
                    self.destination_table_params
                        .session
                        .prepare(
                            format!(
                                "UPDATE {tbl} USING TTL ? SET {list}[SCYLLA_TIMEUUID_LIST_INDEX(?)] = ? WHERE {cond}",
                                tbl = self.destination_table_params.keyspace_table_name,
                                list = column_name,
                                cond = self.destination_table_params.keys_cond
                            )
                        )
                        .await?,
                ),
        };

        run_prepared_statement(
            &self.destination_table_params.session,
            update_query.clone(),
            values,
            timestamp,
        )
        .await
    }

    async fn update_udt_elements(
        &mut self,
        values: &[impl Value],
        timestamp: i64,
        column_name: &str,
    ) -> anyhow::Result<()> {
        let update_query = match self.overwrite_queries.get_mut(column_name) {
            Some(query) => query,
            None => self
                .overwrite_queries
                .entry(column_name.to_string())
                .or_insert(
                    self.destination_table_params
                        .session
                        .prepare(format!(
                            "UPDATE {tbl} USING TTL ? SET {cname} = ? WHERE {cond}",
                            tbl = self.destination_table_params.keyspace_table_name,
                            cname = column_name,
                            cond = self.destination_table_params.keys_cond,
                        ))
                        .await?,
                ),
        };

        run_prepared_statement(
            &self.destination_table_params.session,
            update_query.clone(),
            values,
            timestamp,
        )
        .await
    }

    async fn delete_udt_elements(
        &mut self,
        values: &[impl Value],
        timestamp: i64,
        removed_fields: &str,
    ) -> anyhow::Result<()> {
        let remove_query = format!(
            "UPDATE {tbl} USING TTL ? SET {removed_fields} WHERE {cond}",
            tbl = self.destination_table_params.keyspace_table_name,
            removed_fields = removed_fields,
            cond = self.destination_table_params.keys_cond,
        );

        run_statement(
            &self.destination_table_params.session,
            Query::new(remove_query),
            values,
            timestamp,
        )
        .await
    }
}

async fn run_prepared_statement(
    session: &Arc<Session>,
    mut query: PreparedStatement,
    values: &[impl Value],
    timestamp: i64,
) -> anyhow::Result<()> {
    query.set_timestamp(Some(timestamp));
    session.execute(&query, values).await?;

    Ok(())
}

async fn run_statement(
    session: &Arc<Session>,
    mut query: Query,
    values: &[impl Value],
    timestamp: i64,
) -> anyhow::Result<()> {
    query.set_timestamp(Some(timestamp));
    session.query(query, values).await?;

    Ok(())
}

struct SourceTableData {
    table_schema: Table,
    non_key_columns: Vec<String>,
}

pub(crate) struct ReplicatorConsumer {
    source_table_data: SourceTableData,
    precomputed_queries: PrecomputedQueries,

    // Stores data for left side range delete while waiting for its right counterpart.
    left_range_included: bool,
    left_range_values: Vec<Option<CqlValue>>,
}

impl ReplicatorConsumer {
    pub(crate) async fn new(
        session: Arc<Session>,
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

        let precomputed_queries =
            PrecomputedQueries::new(session, dest_keyspace_name, dest_table_name, &table_schema)
                .await
                .expect("Preparing precomputed queries failed.");

        let source_table_data = SourceTableData {
            table_schema,
            non_key_columns,
        };

        ReplicatorConsumer {
            source_table_data,
            precomputed_queries,
            left_range_included: false,
            left_range_values: vec![],
        }
    }

    fn get_timestamp(data: &CDCRow<'_>) -> i64 {
        const NANOS_IN_MILLIS: u64 = 1000;
        (data.time.get_timestamp().unwrap().to_unix_nanos() / NANOS_IN_MILLIS) as i64
    }

    async fn delete_partition(&mut self, data: CDCRow<'_>) -> anyhow::Result<()> {
        let timestamp = ReplicatorConsumer::get_timestamp(&data);
        let partition_keys_iter = self.source_table_data.table_schema.partition_key.iter();
        let values = partition_keys_iter
            .map(|col_name| data.get_value(col_name).as_ref().unwrap())
            .collect::<Vec<&CqlValue>>();

        self.precomputed_queries
            .delete_partition(&values, timestamp)
            .await
    }

    async fn update(&mut self, data: CDCRow<'_>) -> anyhow::Result<()> {
        self.update_or_insert(data, false).await
    }

    async fn insert(&mut self, data: CDCRow<'_>) -> anyhow::Result<()> {
        self.update_or_insert(data, true).await
    }

    // Function replicates adding and deleting elements
    // using v = v + {} and v = v - {} cql syntax to non-frozen map or set.
    // Chained operations are supported.
    async fn update_map_or_set_elements<'a>(
        &mut self,
        column_name: &str,
        data: &'a CDCRow<'_>,
        values_for_update: &mut [Option<&'a CqlValue>],
        timestamp: i64,
        sentinel: CqlValue,
    ) -> anyhow::Result<()> {
        let value = Some(data.get_value(column_name).as_ref().unwrap_or(&sentinel));
        // Order of values: ttl, added elements, pk condition values.

        let deleted_set = Set(Vec::from(data.get_deleted_elements(column_name)));
        let mut values_for_update = values_for_update.to_vec();

        values_for_update[1] = value;
        values_for_update.insert(2, Some(&deleted_set));
        // New order of values: ttl, added elements, deleted elements, pk condition values.

        self.precomputed_queries
            .update_map_or_set_elements(&values_for_update, timestamp, column_name)
            .await
    }

    // Recreates INSERT/DELETE/UPDATE statement on non-frozen map or set.
    async fn update_map_or_set<'a>(
        &mut self,
        column_name: &str,
        data: &'a CDCRow<'_>,
        values_for_update: &mut [Option<&'a CqlValue>],
        values_for_delete: &[Option<&CqlValue>],
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
            .await
        } else {
            // adding/removing elements
            self.update_map_or_set_elements(
                column_name,
                data,
                values_for_update,
                timestamp,
                sentinel,
            )
            .await
        }
    }

    // Recreates INSERT/DELETE/UPDATE statement on non-frozen list.
    async fn update_list<'a>(
        &mut self,
        column_name: &str,
        data: &'a CDCRow<'_>,
        values_for_update: &mut [Option<&'a CqlValue>],
        pk_values: &[Option<&CqlValue>],
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
        values_for_update: &mut [Option<&'a CqlValue>],
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
        values_for_update[1] = Some(value);

        self.precomputed_queries
            .update_udt_elements(values_for_update, timestamp, column_name)
            .await?;

        self.delete_udt_elements(column_name, data, timestamp, value, values_for_update)
            .await
    }

    async fn delete_udt_elements<'a>(
        &mut self,
        column_name: &str,
        data: &'a CDCRow<'_>,
        timestamp: i64,
        value: &CqlValue,
        values_for_update: &mut [Option<&'a CqlValue>],
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
        values_for_update: &mut [Option<&'a CqlValue>],
        values_for_delete: &[Option<&CqlValue>],
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
            .await
        } else {
            // add/remove elements in udt
            self.update_udt_elements(column_name, data, values_for_update, timestamp)
                .await
        }
    }

    fn delete_row_range_left(&mut self, mut data: CDCRow<'_>, included: bool) {
        self.left_range_included = included;
        self.left_range_values =
            take_clustering_keys_values(&self.source_table_data.table_schema, &mut data);
    }

    async fn delete_row_range_right(
        &mut self,
        data: CDCRow<'_>,
        right_included: bool,
    ) -> anyhow::Result<()> {
        let table_schema = &self.source_table_data.table_schema;
        if table_schema.clustering_key.is_empty() {
            return Ok(());
        }

        let values_left = std::mem::take(&mut self.left_range_values);
        let values_left = values_left.iter().map(|x| x.as_ref()).collect::<Vec<_>>();

        let (_, timestamp, values) = self.get_common_cdc_row_data(&data);

        let values_right = &values[table_schema.partition_key.len()..];

        let first_unequal_position = values_left
            .iter()
            .zip(values_right.iter())
            .position(|pair| pair.0 != pair.1)
            .unwrap_or(table_schema.clustering_key.len());

        let keys_equality_cond = table_schema
            .partition_key
            .iter()
            .map(|name| format!("{} = ?", name))
            .join(" AND ");

        let mut conditions = vec![keys_equality_cond];

        let mut query_values = values[..table_schema.partition_key.len()].to_vec();

        let less_than = if right_included { "<=" } else { "<" };
        let greater_than = if self.left_range_included { ">=" } else { ">" };

        self.add_range_condition(
            &mut conditions,
            &mut query_values,
            &values_left,
            first_unequal_position,
            greater_than,
        );
        self.add_range_condition(
            &mut conditions,
            &mut query_values,
            values_right,
            first_unequal_position,
            less_than,
        );

        let query = Query::new(format!(
            "DELETE FROM {} WHERE {}",
            self.precomputed_queries
                .destination_table_params
                .keyspace_table_name,
            conditions.join(" AND ")
        ));

        run_statement(
            &self.precomputed_queries.destination_table_params.session,
            query,
            &query_values,
            timestamp,
        )
        .await
    }

    fn add_range_condition<'a>(
        &self,
        conditions: &mut Vec<String>,
        query_values: &mut Vec<Option<&'a CqlValue>>,
        values: &[Option<&'a CqlValue>],
        first_unequal_position: usize,
        relation: &str,
    ) {
        let (condition, new_query_values) =
            self.generate_range_condition(values, first_unequal_position, relation);
        if !new_query_values.is_empty() {
            conditions.push(condition);
            query_values.extend(new_query_values.iter());
        }
    }

    fn generate_range_condition<'a>(
        &self,
        values: &[Option<&'a CqlValue>],
        starting_position: usize,
        relation: &str,
    ) -> (String, Vec<Option<&'a CqlValue>>) {
        let table_schema = &self.source_table_data.table_schema;
        let first_null_index = values[starting_position..]
            .iter()
            .position(|x| x.is_none())
            .map_or(table_schema.clustering_key.len(), |x| x + starting_position);

        let condition = format!(
            "({}) {} ({})",
            table_schema.clustering_key[..first_null_index]
                .iter()
                .join(","),
            relation,
            std::iter::repeat("?").take(first_null_index).join(",")
        );

        let query_values = values[..first_null_index].to_vec();

        (condition, query_values)
    }

    // Returns tuple consisting of TTL, timestamp and vector of consecutive values from primary key.
    fn get_common_cdc_row_data<'a>(
        &self,
        data: &'a CDCRow,
    ) -> (CqlValue, i64, Vec<Option<&'a CqlValue>>) {
        let keys_iter = get_keys_iter(&self.source_table_data.table_schema);
        let ttl = CqlValue::Int(data.ttl.unwrap_or(0) as i32); // If data is inserted without TTL, setting it to 0 deletes existing TTL.
        let timestamp = ReplicatorConsumer::get_timestamp(data);
        let values = keys_iter
            .map(|col_name| data.get_value(col_name).as_ref())
            .collect();

        (ttl, timestamp, values)
    }

    // Recreates row deletion.
    async fn delete_row(&mut self, data: CDCRow<'_>) -> anyhow::Result<()> {
        let (_, timestamp, values) = self.get_common_cdc_row_data(&data);
        self.precomputed_queries
            .delete_row(&values, timestamp)
            .await
    }

    // Recreates INSERT/UPDATE/DELETE statement for single column in a row.
    async fn overwrite_column<'a>(
        &mut self,
        column_name: &str,
        data: &'a CDCRow<'_>,
        values_for_update: &mut [Option<&'a CqlValue>],
        values_for_delete: &[Option<&CqlValue>],
        timestamp: i64,
    ) -> anyhow::Result<()> {
        if let value @ Some(_) = data.get_value(column_name) {
            // Order of values: ttl, inserted value, pk condition values.
            values_for_update[1] = value.as_ref();
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
            insert_values.push(Some(&ttl));

            self.precomputed_queries
                .insert_value(&insert_values, timestamp)
                .await?;
        }

        let mut values_for_update = Vec::with_capacity(2 + values.len());
        values_for_update.extend([Some(&ttl), Some(&CqlValue::Int(0))]);
        values_for_update.extend(values.iter());

        let mut values_for_list_update = Vec::with_capacity(3 + values.len());
        values_for_list_update.extend([Some(&ttl), None, None]);
        values_for_list_update.extend(values.iter());

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

fn take_clustering_keys_values(table_schema: &Table, data: &mut CDCRow) -> Vec<Option<CqlValue>> {
    table_schema
        .clustering_key
        .iter()
        .map(|ck| data.take_value(ck))
        .collect()
}

#[async_trait]
impl Consumer for ReplicatorConsumer {
    async fn consume_cdc(&mut self, data: CDCRow<'_>) -> anyhow::Result<()> {
        match data.operation {
            OperationType::RowUpdate => self.update(data).await?,
            OperationType::RowInsert => self.insert(data).await?,
            OperationType::RowDelete => self.delete_row(data).await?,
            OperationType::PartitionDelete => self.delete_partition(data).await?,
            OperationType::RowRangeDelExclLeft => self.delete_row_range_left(data, false),
            OperationType::RowRangeDelInclLeft => self.delete_row_range_left(data, true),
            OperationType::RowRangeDelExclRight => self.delete_row_range_right(data, false).await?,
            OperationType::RowRangeDelInclRight => self.delete_row_range_right(data, true).await?,
            _ => todo!("This type of operation is not supported yet."),
        }

        Ok(())
    }
}

pub struct ReplicatorConsumerFactory {
    session: Arc<Session>,
    dest_keyspace_name: String,
    dest_table_name: String,
    table_schema: Table,
}

impl ReplicatorConsumerFactory {
    /// Creates a new instance of `ReplicatorConsumerFactory`.
    /// Fetching schema metadata must be enabled in the session.
    pub fn new(
        session: Arc<Session>,
        dest_keyspace_name: String,
        dest_table_name: String,
    ) -> anyhow::Result<ReplicatorConsumerFactory> {
        let table_schema = session
            .get_cluster_data()
            .get_keyspace_info()
            .get(&dest_keyspace_name)
            .ok_or_else(|| anyhow!("Keyspace not found"))?
            .tables
            .get(&dest_table_name.to_ascii_lowercase())
            .ok_or_else(|| anyhow!("Table not found"))?
            .clone();

        Ok(ReplicatorConsumerFactory {
            session,
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
                self.session.clone(),
                self.dest_keyspace_name.clone(),
                self.dest_table_name.clone(),
                self.table_schema.clone(),
            )
            .await,
        )
    }
}
