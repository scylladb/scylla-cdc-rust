//! A module representing the logic behind consuming the data.
use crate::cdc_types::StreamID;
use async_trait::async_trait;
use num_enum::TryFromPrimitive;
use scylla::frame::response::result::CqlValue::Set;
use scylla::frame::response::result::{CqlValue, Row};
use scylla::transport::query_result::ColumnSpecs;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;

/// Trait used to represent a user-defined callback
/// used for processing read CDC rows.
/// During reading the CDC log, the stream ids are grouped by VNodes.
/// One Consumer is created for each such group
/// when reading of a new generation starts
/// and is destroyed after every row from that generation has been read.
///
/// For more information about the reading algorithms,
/// please refer to the documentation of [`crate::log_reader`] module.
#[async_trait]
pub trait Consumer: Send {
    async fn consume_cdc(&mut self, data: Vec<CDCRow<'_>>) -> anyhow::Result<()>;
}

/// Trait used to represent a factory of [`Consumer`] instances.
/// For rules about creating new consumers,
/// please refer to the documentation of ['Consumer'].
#[async_trait]
pub trait ConsumerFactory: Sync + Send {
    async fn new_consumer(&self) -> Box<dyn Consumer>;
}

/// Represents different types of CDC operations.
/// For more information, see [the CDC documentation](<https://docs.scylladb.com/using-scylla/cdc/cdc-log-table/#operation-column>).
#[derive(Clone, Debug, Eq, PartialEq, TryFromPrimitive)]
#[repr(i8)]
pub enum OperationType {
    PreImage,
    RowUpdate,
    RowInsert,
    RowDelete,
    PartitionDelete,
    RowRangeDelInclLeft,
    RowRangeDelExclLeft,
    RowRangeDelInclRight,
    RowRangeDelExclRight,
    PostImage,
}

impl fmt::Display for OperationType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            OperationType::PreImage => write!(f, "PreImage"),
            OperationType::RowUpdate => write!(f, "RowUpdate"),
            OperationType::RowInsert => write!(f, "RowInsert"),
            OperationType::RowDelete => write!(f, "RowDelete"),
            OperationType::PartitionDelete => write!(f, "PartitionDelete"),
            OperationType::RowRangeDelInclLeft => write!(f, "RowRangeDelInclLeft"),
            OperationType::RowRangeDelExclLeft => write!(f, "RowRangeDelExclLeft"),
            OperationType::RowRangeDelInclRight => write!(f, "RowRangeDelInclRight"),
            OperationType::RowRangeDelExclRight => write!(f, "RowRangeDelExclRight"),
            OperationType::PostImage => write!(f, "PostImage"),
        }
    }
}

const STREAM_ID_NAME: &str = "cdc$stream_id";
const TIME_NAME: &str = "cdc$time";
const BATCH_SEQ_NO_NAME: &str = "cdc$batch_seq_no";
const END_OF_BATCH_NAME: &str = "cdc$end_of_batch";
const OPERATION_NAME: &str = "cdc$operation";
const TTL_NAME: &str = "cdc$ttl";
const IS_DELETED_PREFIX: &str = "cdc$deleted_";
const ARE_ELEMENTS_DELETED_PREFIX: &str = "cdc$deleted_elements_";

/// A structure used to map names of columns in the CDC
/// to their indices in an internal vector.
pub struct CDCRowSchema {
    // The usize values are indices of given values in the Row.columns vector.
    pub(crate) stream_id: usize,
    pub(crate) time: usize,
    pub(crate) batch_seq_no: usize,
    pub(crate) end_of_batch: usize,
    pub(crate) operation: usize,
    pub(crate) ttl: usize,

    // These HashMaps map names of columns in the observed table to matching columns in the CDC table.
    // The usize value is an index of the column in an internal vector inside of the CDCRow struct.

    // Maps name of a column to a matching column in the CDC table.
    pub(crate) mapping: HashMap<String, usize>,
    // Maps name of a column to a column that tells if value in this column was deleted.
    pub(crate) deleted_mapping: HashMap<String, usize>,
    // Maps name of a collection column to a column that tells
    // if elements from this collection were deleted.
    pub(crate) deleted_el_mapping: HashMap<String, usize>,
}

impl CDCRowSchema {
    pub fn new(specs: ColumnSpecs) -> CDCRowSchema {
        let mut stream_id = 0;
        let mut time = 0;
        let mut batch_seq_no = 0;
        let mut end_of_batch = 0;
        let mut operation = 0;
        let mut ttl = 0;
        let mut mapping: HashMap<String, usize> = HashMap::new();
        let mut deleted_mapping: HashMap<String, usize> = HashMap::new();
        let mut deleted_el_mapping: HashMap<String, usize> = HashMap::new();

        let mut j = 0;

        // Hashmaps will have indices of data in a new vector without the hardcoded values.
        for (i, spec) in specs.iter().enumerate() {
            match spec.name() {
                STREAM_ID_NAME => stream_id = i,
                TIME_NAME => time = i,
                BATCH_SEQ_NO_NAME => batch_seq_no = i,
                END_OF_BATCH_NAME => end_of_batch = i,
                OPERATION_NAME => operation = i,
                TTL_NAME => ttl = i,
                x => {
                    if let Some(stripped) = x.strip_prefix(ARE_ELEMENTS_DELETED_PREFIX) {
                        deleted_el_mapping.insert(stripped.to_string(), j);
                    } else if let Some(stripped) = x.strip_prefix(IS_DELETED_PREFIX) {
                        deleted_mapping.insert(stripped.to_string(), j);
                    } else {
                        mapping.insert(x.to_string(), j);
                    }

                    j += 1;
                }
            }
        }

        CDCRowSchema {
            stream_id,
            time,
            batch_seq_no,
            end_of_batch,
            operation,
            ttl,
            mapping,
            deleted_mapping,
            deleted_el_mapping,
        }
    }
}

/// Represents data from a single row in the CDC log.
/// The metadata can be accessed directly like any other member variable,
/// other columns can be accessed by using the struct methods, e.g. [`get_value`].
/// To get more information about the metadata, see [the CDC documentation](<https://docs.scylladb.com/using-scylla/cdc/cdc-log-table/>).
///
/// [`get_value`]: #method.get_value
pub struct CDCRow<'schema> {
    pub stream_id: StreamID,
    pub time: uuid::Uuid,
    pub batch_seq_no: i32,
    pub end_of_batch: bool,
    pub operation: OperationType,
    // Can be NULL in the database.
    pub ttl: Option<i64>,
    data: Vec<Option<CqlValue>>,
    // Maps element name to its index in the data vector.
    schema: &'schema CDCRowSchema,
}

impl CDCRow<'_> {
    pub fn from_row(row: Row, schema: &CDCRowSchema) -> CDCRow {
        // If cdc read was successful, these default values will not be used.
        let mut stream_id_vec = vec![];
        let mut time = uuid::Uuid::default();
        let mut batch_seq_no = i32::MAX;
        let mut end_of_batch = false;
        let mut operation = OperationType::PreImage;
        let mut ttl = None;

        let data_count =
            schema.mapping.len() + schema.deleted_mapping.len() + schema.deleted_el_mapping.len();
        let mut data: Vec<Option<CqlValue>> = Vec::with_capacity(data_count);

        for (i, column) in row.columns.into_iter().enumerate() {
            if i == schema.stream_id {
                stream_id_vec = column.unwrap().into_blob().unwrap();
            } else if i == schema.time {
                time = column.unwrap().as_timeuuid().unwrap().into();
            } else if i == schema.batch_seq_no {
                batch_seq_no = column.unwrap().as_int().unwrap();
            } else if i == schema.end_of_batch {
                end_of_batch = column.is_some() && column.unwrap().as_boolean().unwrap()
            } else if i == schema.operation {
                operation = OperationType::try_from(column.unwrap().as_tinyint().unwrap()).unwrap();
            } else if i == schema.ttl {
                ttl = column.map(|ttl| ttl.as_bigint().unwrap());
            } else {
                data.push(column);
            }
        }

        CDCRow {
            stream_id: StreamID { id: stream_id_vec },
            time,
            batch_seq_no,
            end_of_batch,
            operation,
            ttl,
            data,
            schema,
        }
    }

    /// Allows to get a value from the column that corresponds to the logged table.
    /// Returns `None` if the value is `null`.
    /// Panics if the column does not exist in this table.
    /// To check if such column exists, use [`column_exists`] method.
    ///
    /// [`column_exists`]: #method.column_exists
    pub fn get_value(&self, name: &str) -> &Option<CqlValue> {
        self.schema
            .mapping
            .get(name)
            .map(|id| &self.data[*id])
            .unwrap()
    }

    /// Allows to take a value from the column that corresponds to the logged table.
    /// Leaves `None` in the corresponding column data.
    /// Returns `None` if the value is `null` or such column doesn't exist.
    pub fn take_value(&mut self, name: &str) -> Option<CqlValue> {
        self.schema
            .mapping
            .get(name)
            .and_then(|id| self.data[*id].take())
    }

    /// Allows to get info if a value was deleted in this operation.
    /// Panics if the column does not exist in this table
    /// or the column is a part of primary key (because these values can't be deleted).
    /// To check if such column exists, use [`column_deletable`] method.
    ///
    /// [`column_deletable`]: #method.column_deletable
    pub fn is_value_deleted(&self, name: &str) -> bool {
        self.schema
            .deleted_mapping
            .get(name)
            .map(|id| self.data[*id].is_some())
            .unwrap()
    }

    /// Allows to get deleted elements from a collection.
    /// Returns empty slice if the value is `null`.
    /// Panics if the column does not exist in this table or is not a collection.
    /// To check if such column exists, use [`collection_exists`] method.
    ///
    /// [`collection_exists`]: #method.collection_exists
    pub fn get_deleted_elements(&self, name: &str) -> &[CqlValue] {
        let val = self
            .schema
            .deleted_el_mapping
            .get(name)
            .map(|id| self.data[*id].as_ref().map(|val| val.as_set().unwrap()))
            .unwrap();
        match val {
            Some(vec) => vec,
            None => &[],
        }
    }

    /// Allows to take deleted elements from a collection.
    /// Returns new empty vector if the value is `null` or such column doesn't exist.
    /// The returned value is always owned.
    /// Leaves `None` in place of taken data.
    pub fn take_deleted_elements(&mut self, name: &str) -> Vec<CqlValue> {
        self.schema
            .deleted_el_mapping
            .get(name)
            .and_then(|id| self.data[*id].take())
            .and_then(|x| match x {
                Set(x) => Some(x),
                _ => None,
            })
            .unwrap_or_default()
    }

    pub fn column_exists(&self, name: &str) -> bool {
        self.schema.mapping.contains_key(name)
    }

    pub fn column_deletable(&self, name: &str) -> bool {
        self.schema.deleted_mapping.contains_key(name)
    }

    pub fn collection_exists(&self, name: &str) -> bool {
        self.schema.deleted_el_mapping.contains_key(name)
    }

    pub fn get_non_cdc_column_names(&self) -> impl Iterator<Item = &str> {
        self.schema.mapping.keys().map(|column| column.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use scylla::Session;
    use scylla_cdc_test_utils::prepare_db;
    use std::sync::Arc;
    // These tests should be indifferent to things like number of Scylla nodes,
    // so if run separately, they can be tested on one Scylla instance.

    const TEST_SINGLE_VALUE_TABLE: &str = "single_value";
    const TEST_SINGLE_COLLECTION_TABLE: &str = "single_collection";
    const CDC_CONFIG: &str = "{'enabled': 'true'}";
    const TEST_SINGLE_VALUE_CDC_TABLE: &str = "single_value_scylla_cdc_log";
    const TEST_SINGLE_COLLECTION_CDC_TABLE: &str = "single_collection_scylla_cdc_log";

    fn construct_single_value_table_query() -> String {
        format!(
            "
    CREATE TABLE IF NOT EXISTS {}(
    pk int,
    ck int,
    v int,
    PRIMARY KEY(pk, ck)) WITH cdc = {};",
            TEST_SINGLE_VALUE_TABLE, CDC_CONFIG
        )
    }

    async fn populate_single_value_table(session: &Session) {
        // We want to use ttl, because we check its value in some tests.
        session
            .query_unpaged(
                format!(
                    "INSERT INTO {} (pk, ck, v) VALUES ({}, {}, {}) USING TTL {};",
                    TEST_SINGLE_VALUE_TABLE, 1, 2, 3, 86400
                ),
                (),
            )
            .await
            .unwrap();
    }

    fn construct_single_collection_table_query() -> String {
        format!(
            "
    CREATE TABLE IF NOT EXISTS {}(
    pk int,
    ck int,
    vs set<int>,
    PRIMARY KEY(pk, ck)) WITH cdc = {};",
            TEST_SINGLE_COLLECTION_TABLE, CDC_CONFIG
        )
    }

    async fn populate_single_collection_table(session: &Session) {
        session
            .query_unpaged(
                format!(
                    "INSERT INTO {} (pk, ck, vs) VALUES (?, ?, ?);",
                    TEST_SINGLE_COLLECTION_TABLE
                ),
                (1, 2, vec![1, 2]),
            )
            .await
            .unwrap();
    }

    async fn setup() -> anyhow::Result<Arc<Session>> {
        let session = prepare_db(
            &[
                construct_single_value_table_query(),
                construct_single_collection_table_query(),
            ],
            1,
        )
        .await?
        .0;
        populate_single_value_table(&session).await;
        populate_single_collection_table(&session).await;

        Ok(session)
    }

    // Tests if field are properly set.
    #[tokio::test]
    async fn test_query() {
        let session = setup().await.unwrap();
        let mut result = session
            .query_iter(
                format!("SELECT * FROM {};", TEST_SINGLE_VALUE_CDC_TABLE),
                (),
            )
            .await
            .unwrap()
            .rows_stream::<Row>()
            .unwrap();

        let row = result.next().await.unwrap().unwrap();
        let schema = CDCRowSchema::new(result.column_specs());
        let cdc_row = CDCRow::from_row(row, &schema);

        // Test against the default values in CDCRow::from_row
        assert!(!cdc_row.stream_id.id.is_empty());
        assert_ne!(cdc_row.time, uuid::Uuid::default());
        assert_eq!(cdc_row.batch_seq_no, 0);
        assert!(cdc_row.end_of_batch);
        assert_eq!(cdc_row.operation, OperationType::RowInsert);
        assert!(cdc_row.ttl.is_some());

        assert_eq!(
            cdc_row.get_value("v").as_ref().unwrap().as_int().unwrap(),
            3
        );
        assert!(!cdc_row.is_value_deleted("v"));
        assert!(!cdc_row.collection_exists("v"));
    }

    #[tokio::test]
    async fn test_get_deleted() {
        let session = setup().await.unwrap();
        session
            .query_unpaged(
                format!(
                    "DELETE v FROM {} WHERE pk = {} AND ck = {};",
                    TEST_SINGLE_VALUE_TABLE, 1, 2
                ),
                (),
            )
            .await
            .unwrap();
        // We must allow filtering in order to search by cdc$operation.
        let result = session
            .query_unpaged(format!("SELECT * FROM {} WHERE \"cdc$operation\" = {} AND pk = {} AND ck = {} ALLOW FILTERING;",
                           TEST_SINGLE_VALUE_CDC_TABLE, OperationType::RowUpdate as i8, 1, 2), ())
            .await
            .unwrap()
            .into_rows_result()
            .unwrap();
        let row = result.rows::<Row>().unwrap().next().unwrap().unwrap();
        let schema = CDCRowSchema::new(result.column_specs());
        let cdc_row = CDCRow::from_row(row, &schema);

        assert!(cdc_row.is_value_deleted("v"))
    }

    #[tokio::test]
    async fn test_get_deleted_elements() {
        let session = setup().await.unwrap();
        session
            .query_unpaged(
                format!(
                    "UPDATE {} SET vs = vs - {{{}}} WHERE pk = {} AND ck = {}",
                    TEST_SINGLE_COLLECTION_TABLE, 2, 1, 2
                ),
                (),
            )
            .await
            .unwrap();
        // We must allow filtering in order to search by cdc$operation.
        let result = session
            .query_unpaged(format!("SELECT * FROM {} WHERE \"cdc$operation\" = {} AND pk = {} AND ck = {} ALLOW FILTERING;",
                           TEST_SINGLE_COLLECTION_CDC_TABLE, OperationType::RowUpdate as i8, 1, 2), ())
            .await
            .unwrap()
            .into_rows_result()
            .unwrap();

        let row = result.rows::<Row>().unwrap().next().unwrap().unwrap();
        let schema = CDCRowSchema::new(result.column_specs());
        let cdc_row = CDCRow::from_row(row, &schema);

        let vec = cdc_row.get_deleted_elements("vs");

        assert_eq!(vec.len(), 1);
        assert_eq!(vec[0].as_int().unwrap(), 2);
    }

    // Unit test for schema.
    #[tokio::test]
    async fn test_create_schema() {
        let session = setup().await.unwrap();
        // Set the columns order to test if schema maps that correctly.
        let result = session
            .query_unpaged(
                format!(
                    "SELECT ck, pk, v, \"cdc$deleted_v\",\
                                  \"cdc$time\", \"cdc$stream_id\", \"cdc$batch_seq_no\", \
                                  \"cdc$ttl\", \"cdc$end_of_batch\", \"cdc$operation\"\
                                  FROM {};",
                    TEST_SINGLE_VALUE_CDC_TABLE
                ),
                (),
            )
            .await
            .unwrap()
            .into_rows_result()
            .unwrap();

        let schema = CDCRowSchema::new(result.column_specs());
        // Check fixed values.
        assert_eq!(schema.stream_id, 5);
        assert_eq!(schema.time, 4);
        assert_eq!(schema.batch_seq_no, 6);
        assert_eq!(schema.end_of_batch, 8);
        assert_eq!(schema.operation, 9);
        assert_eq!(schema.ttl, 7);

        // Check values from observed table.
        assert_eq!(*schema.mapping.get("pk").unwrap(), 1_usize);
        assert_eq!(*schema.mapping.get("ck").unwrap(), 0_usize);
        assert_eq!(*schema.mapping.get("v").unwrap(), 2_usize);

        // Check deleted_*.
        assert_eq!(*schema.deleted_mapping.get("v").unwrap(), 3_usize);

        // Check maps' size.
        assert_eq!(schema.mapping.len(), 3);
        assert_eq!(schema.deleted_mapping.len(), 1);
        assert_eq!(schema.deleted_el_mapping.len(), 0);
    }

    #[tokio::test]
    async fn test_take_value() {
        let session = setup().await.unwrap();
        let result = session
            .query_unpaged(
                format!("SELECT * FROM {};", TEST_SINGLE_VALUE_CDC_TABLE),
                (),
            )
            .await
            .unwrap()
            .into_rows_result()
            .unwrap();

        let row = result.rows::<Row>().unwrap().next().unwrap().unwrap();
        let schema = CDCRowSchema::new(result.column_specs());
        let mut cdc_row = CDCRow::from_row(row, &schema);

        assert_eq!(cdc_row.take_value("v").unwrap().as_int().unwrap(), 3);
        assert!(cdc_row.take_value("no_such_column").is_none());
    }

    #[tokio::test]
    async fn test_take_deleted_elements() {
        let session = setup().await.unwrap();
        session
            .query_unpaged(
                format!(
                    "UPDATE {} SET vs = vs - ? WHERE pk = ? AND ck = ?",
                    TEST_SINGLE_COLLECTION_TABLE
                ),
                (vec![2], 1, 2),
            )
            .await
            .unwrap();
        // We must allow filtering in order to search by cdc$operation.
        let result = session
            .query_unpaged(format!("SELECT * FROM {} WHERE \"cdc$operation\" = ? AND pk = ? AND ck = ? ALLOW FILTERING;",
                           TEST_SINGLE_COLLECTION_CDC_TABLE), (OperationType::RowUpdate as i8, 1, 2))
            .await
            .unwrap()
            .into_rows_result()
            .unwrap();

        let row = result.rows::<Row>().unwrap().next().unwrap().unwrap();
        let schema = CDCRowSchema::new(result.column_specs());
        let mut cdc_row = CDCRow::from_row(row, &schema);

        let vec = cdc_row.take_deleted_elements("vs");

        assert_eq!(vec.len(), 1);
        assert_eq!(vec[0].as_int().unwrap(), 2);

        let empty_vec = cdc_row.take_deleted_elements("non-existent-column");

        assert!(empty_vec.is_empty());
    }
}
