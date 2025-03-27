//! A module containing types related to CDC internal structure.
use scylla::deserialize::value::DeserializeValue;
use scylla::frame::response::result::ColumnType;
use scylla::serialize::value::SerializeValue;
use scylla::serialize::writers::{CellWriter, WrittenCellProof};
use scylla::serialize::SerializationError;
use scylla::value::CqlTimestamp;
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

impl SerializeValue for GenerationTimestamp {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        CqlTimestamp(self.timestamp.num_milliseconds()).serialize(typ, writer)
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for GenerationTimestamp {
    fn type_check(typ: &ColumnType) -> Result<(), scylla::deserialize::TypeCheckError> {
        <CqlTimestamp as DeserializeValue<'frame, 'metadata>>::type_check(typ)
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<scylla::deserialize::FrameSlice<'frame>>,
    ) -> Result<Self, scylla::deserialize::DeserializationError> {
        let timestamp = <CqlTimestamp as DeserializeValue<'frame, 'metadata>>::deserialize(typ, v)?;

        Ok(GenerationTimestamp {
            timestamp: chrono::Duration::milliseconds(timestamp.0),
        })
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

impl SerializeValue for StreamID {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        self.id.serialize(typ, writer)
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for StreamID {
    fn type_check(typ: &ColumnType) -> Result<(), scylla::deserialize::TypeCheckError> {
        <Vec<u8> as DeserializeValue<'frame, 'metadata>>::type_check(typ)
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<scylla::deserialize::FrameSlice<'frame>>,
    ) -> Result<Self, scylla::deserialize::DeserializationError> {
        let id = <Vec<u8> as DeserializeValue<'frame, 'metadata>>::deserialize(typ, v)?;
        Ok(StreamID { id })
    }
}

impl StreamID {
    pub fn new(stream_id: Vec<u8>) -> StreamID {
        StreamID { id: stream_id }
    }
}
