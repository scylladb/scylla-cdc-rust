//! A module containing types related to CDC internal structure.
use scylla::deserialize::DeserializationError;
use scylla::deserialize::FrameSlice;
use scylla::deserialize::TypeCheckError;
use scylla::deserialize::value::DeserializeValue;
use scylla::frame::response::result::ColumnType;
use scylla::serialize::SerializationError;
use scylla::serialize::value::SerializeValue;
use scylla::serialize::writers::CellWriter;
use scylla::serialize::writers::WrittenCellProof;
use scylla::statement::Statement;
use scylla::value::CqlTimestamp;
use std::fmt;
use std::time::Duration;

/// Conversions between [`CqlTimestamp`] and a [`Duration`] since the Unix epoch.
///
/// The CDC log is addressed by windows expressed as offsets from the epoch, so
/// the reader converts back and forth constantly. Both directions are lossy at
/// the extremes; see the individual methods.
pub(crate) trait CqlTimestampExt {
    /// Converts a [`Duration`] since the Unix epoch to a [`CqlTimestamp`].
    /// Saturates to [`CqlTimestamp::MAX`] if the duration exceeds `i64::MAX` milliseconds.
    fn from_duration_since_epoch(d: Duration) -> Self;

    /// Returns the timestamp as a [`Duration`] since the Unix epoch.
    /// Negative values (pre-epoch) are clamped to [`Duration::ZERO`].
    fn to_duration_since_epoch(self) -> Duration;
}

impl CqlTimestampExt for CqlTimestamp {
    fn from_duration_since_epoch(d: Duration) -> Self {
        CqlTimestamp(i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
    }

    fn to_duration_since_epoch(self) -> Duration {
        Duration::from_millis(self.0.max(0) as u64)
    }
}

/// A struct representing a timestamp of a stream generation.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct GenerationTimestamp {
    pub(crate) timestamp: CqlTimestamp,
}

impl fmt::Display for GenerationTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.timestamp.0)
    }
}

impl SerializeValue for GenerationTimestamp {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        self.timestamp.serialize(typ, writer)
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for GenerationTimestamp {
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        <CqlTimestamp as DeserializeValue<'frame, 'metadata>>::type_check(typ)
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        Ok(GenerationTimestamp {
            timestamp: <CqlTimestamp as DeserializeValue<'frame, 'metadata>>::deserialize(typ, v)?,
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
        write!(f, "{encoded_stream_id}")
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
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        <Vec<u8> as DeserializeValue<'frame, 'metadata>>::type_check(typ)
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        let id = <Vec<u8> as DeserializeValue<'frame, 'metadata>>::deserialize(typ, v)?;
        Ok(StreamID { id })
    }
}

impl StreamID {
    pub fn new(stream_id: Vec<u8>) -> StreamID {
        StreamID { id: stream_id }
    }
}

pub(crate) fn make_idempotent_statement(query: String) -> Statement {
    let mut statement = Statement::new(query);
    statement.set_is_idempotent(true);
    statement
}
