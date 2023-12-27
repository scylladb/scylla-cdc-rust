//! A module containing types related to CDC internal structure.
use chrono::{DateTime, Utc};
use scylla::cql_to_rust::{FromCqlVal, FromCqlValError};
use scylla::frame::response::result::ColumnType;
use scylla::frame::response::result::CqlValue;
use scylla::frame::value::{CqlTimestamp, Value, ValueTooBig};
use scylla::serialize::value::SerializeCql;
use scylla::serialize::writers::WrittenCellProof;
use scylla::FromRow;
use std::fmt;

/// A struct representing a timestamp of a stream generation.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, FromRow)]
pub struct GenerationTimestamp {
    pub timestamp: DateTime<Utc>,
}

impl Value for GenerationTimestamp {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        Value::serialize(&CqlTimestamp::from(self.timestamp), buf)
    }
}

impl SerializeCql for GenerationTimestamp {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: scylla::serialize::CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, scylla::serialize::SerializationError> {
        SerializeCql::serialize(&CqlTimestamp::from(self.timestamp), typ, writer)
    }
}

/// A struct representing a stream ID.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, FromRow)]
pub struct StreamID {
    pub(crate) id: Vec<u8>,
}

impl fmt::Display for StreamID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let encoded_stream_id = hex::encode(self.id.clone());
        write!(f, "{}", encoded_stream_id)
    }
}

impl Value for StreamID {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        Value::serialize(&self.id, buf)
    }
}

impl SerializeCql for StreamID {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: scylla::serialize::CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, scylla::serialize::SerializationError> {
        SerializeCql::serialize(&self.id, typ, writer)
    }
}

impl FromCqlVal<CqlValue> for StreamID {
    fn from_cql(cql_val: CqlValue) -> Result<Self, FromCqlValError> {
        let id = cql_val
            .as_blob()
            .ok_or(FromCqlValError::BadCqlType)?
            .to_owned();
        Ok(StreamID { id })
    }
}

impl StreamID {
    pub fn new(stream_id: Vec<u8>) -> StreamID {
        StreamID { id: stream_id }
    }
}
