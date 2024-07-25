//! A module containing types related to CDC internal structure.
use scylla::cql_to_rust::{FromCqlVal, FromCqlValError};
use scylla::frame::response::result::{ColumnType, CqlValue};
use scylla::frame::value::CqlTimestamp;
use scylla::serialize::value::SerializeCql;
use scylla::serialize::writers::WrittenCellProof;
use scylla::serialize::{CellWriter, SerializationError};
use std::fmt;

/// A struct representing a timestamp of a stream generation.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct GenerationTimestamp {
    pub(crate) timestamp: chrono::Duration,
}

impl fmt::Display for GenerationTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.timestamp.num_milliseconds())
    }
}

impl SerializeCql for GenerationTimestamp {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        CqlTimestamp(self.timestamp.num_milliseconds()).serialize(typ, writer)
    }
}

impl FromCqlVal<CqlValue> for GenerationTimestamp {
    fn from_cql(cql_val: CqlValue) -> Result<Self, FromCqlValError> {
        match cql_val {
            CqlValue::Timestamp(val) => {
                let timestamp = chrono::Duration::milliseconds(val.0);
                Ok(GenerationTimestamp { timestamp })
            }
            _ => Err(FromCqlValError::BadCqlType),
        }
    }
}

/// A struct representing a stream ID.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct StreamID {
    pub(crate) id: Vec<u8>,
}

impl fmt::Display for StreamID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let encoded_stream_id = hex::encode(self.id.clone());
        write!(f, "{}", encoded_stream_id)
    }
}

impl SerializeCql for StreamID {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        self.id.serialize(typ, writer)
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
