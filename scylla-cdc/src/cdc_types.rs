use scylla::cql_to_rust::{FromCqlVal, FromCqlValError};
use scylla::frame::response::result::CqlValue;
use scylla::frame::value::{Timestamp, Value, ValueTooBig};
use scylla::FromRow;
use std::fmt;

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, FromRow)]
pub struct GenerationTimestamp {
    pub timestamp: chrono::Duration,
}

impl Value for GenerationTimestamp {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        Timestamp(self.timestamp).serialize(buf)
    }
}

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
        self.id.serialize(buf)
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

pub(crate) trait ToTimestamp {
    fn to_timestamp(&self) -> chrono::Duration;
}

impl<Tz: chrono::TimeZone> ToTimestamp for chrono::DateTime<Tz> {
    fn to_timestamp(&self) -> chrono::Duration {
        chrono::Duration::milliseconds(self.timestamp_millis())
    }
}
