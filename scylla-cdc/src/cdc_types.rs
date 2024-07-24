//! A module containing types related to CDC internal structure.
use scylla::cql_to_rust::{FromCqlVal, FromCqlValError, FromRowError};
use scylla::frame::response::result::{ColumnType, CqlValue, Row};
use scylla::frame::value;
use scylla::serialize::value::SerializeCql;
use scylla::serialize::writers::WrittenCellProof;
use scylla::serialize::{CellWriter, SerializationError};
use scylla::FromRow;
use std::fmt;

/// A struct representing a timestamp of a stream generation.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct GenerationTimestamp {
    pub timestamp: chrono::Duration,
}

impl SerializeCql for GenerationTimestamp {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        value::CqlTimestamp(self.timestamp.num_milliseconds()).serialize(typ, writer)
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

impl FromRow for GenerationTimestamp {
    fn from_row(row: Row) -> Result<Self, FromRowError> {
        let columns_len = row.columns.len();
        if columns_len != 1 {
            Err(FromRowError::WrongRowSize {
                expected: 1,
                actual: columns_len,
            })
        } else {
            if let Some(val) = &row.columns[0] {
                match val {
                    CqlValue::Timestamp(t) => Ok(GenerationTimestamp {
                        timestamp: chrono::Duration::milliseconds(t.0),
                    }),
                    _ => Err(FromRowError::BadCqlVal {
                        err: FromCqlValError::BadCqlType,
                        column: 0,
                    }),
                }
            } else {
                Err(FromRowError::BadCqlVal {
                    err: FromCqlValError::ValIsNull,
                    column: 0,
                })
            }
        }
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
