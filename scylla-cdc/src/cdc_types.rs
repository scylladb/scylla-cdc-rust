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
use std::ops::Add;
use std::ops::AddAssign;
use std::ops::Sub;
use std::ops::SubAssign;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

/// A point in time represented as milliseconds since the Unix epoch.
///
/// Using a distinct type rather than [`Duration`] prevents confusing *instants*
/// (a fixed point in time) with *intervals* (a length of time).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub(crate) struct Timestamp(i64);

impl Timestamp {
    /// The earliest representable `Timestamp` (milliseconds = `i64::MIN`).
    pub(crate) const MIN: Self = Timestamp(i64::MIN);
    /// The latest representable `Timestamp` (milliseconds = `i64::MAX`).
    pub(crate) const MAX: Self = Timestamp(i64::MAX);

    /// Returns the current wall-clock time as a `Timestamp`.
    ///
    /// The returned value holds the number of milliseconds since the Unix epoch
    /// (1970-01-01 00:00:00 UTC).
    ///
    /// This is implemented in terms of [`SystemTime::now`] and is therefore
    /// **not monotonic**. Because of OS/NTP clock adjustments, a later call to
    /// `now()` can return a smaller value than an earlier call. Do not use
    /// `now()` to measure elapsed time; use [`std::time::Instant`] for that
    /// purpose instead.
    ///
    /// # Panics
    ///
    /// Panics if the system clock is so far removed from the Unix epoch
    /// (in either direction) that it cannot be represented as `i64` milliseconds.
    pub(crate) fn now() -> Self {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => Self(
                i64::try_from(d.as_millis())
                    .expect("system clock is too far past the Unix epoch to fit in a Timestamp"),
            ),
            Err(e) => {
                let ms = i64::try_from(e.duration().as_millis())
                    .expect("system clock is too far before the Unix epoch to fit in a Timestamp");
                // `ms` is the result of converting a `u128` (always non-negative) into `i64`,
                // so it is in the range [0, i64::MAX]. Negating it therefore cannot overflow.
                Self(-ms)
            }
        }
    }

    /// Converts a [`Duration`] since the Unix epoch to a `Timestamp`.
    /// Saturates to [`Timestamp::MAX`] if the duration exceeds `i64::MAX` milliseconds.
    pub(crate) fn from_duration_since_epoch(d: Duration) -> Self {
        Timestamp(i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
    }

    /// Returns the timestamp as a [`Duration`] since the Unix epoch.
    /// Negative values (pre-epoch) are clamped to [`Duration::ZERO`].
    pub(crate) fn to_duration_since_epoch(self) -> Duration {
        Duration::from_millis(self.0.max(0) as u64)
    }

    /// Constructs a `Timestamp` directly from a raw millisecond value.
    #[cfg(test)]
    pub(crate) fn from_millis(ms: i64) -> Self {
        Timestamp(ms)
    }

    /// Raw millisecond value - identical to the `CqlTimestamp` wire value.
    pub(crate) fn as_millis(self) -> i64 {
        self.0
    }

    /// Returns the amount of time elapsed from `earlier` to `self`.
    ///
    /// Mirrors [`std::time::Instant::checked_duration_since`]. Returns `None`
    /// if `earlier` is actually later than `self`, instead of silently
    /// saturating at zero, since it is easy to swap the operands by mistake.
    pub(crate) fn checked_duration_since(self, earlier: Timestamp) -> Option<Duration> {
        if self.0 < earlier.0 {
            return None;
        }
        // The ordering check above guarantees `self.0 >= earlier.0`,
        // so `abs_diff` gives the correct non-negative `u64` difference even
        // when it wouldn't fit in `i64` (e.g. MAX - MIN).
        Some(Duration::from_millis(self.0.abs_diff(earlier.0)))
    }
}

impl Add<Duration> for Timestamp {
    type Output = Timestamp;

    /// # Panics
    ///
    /// Panics if the resulting timestamp would overflow `i64` milliseconds.
    fn add(self, rhs: Duration) -> Timestamp {
        let rhs_ms = i128::try_from(rhs.as_millis()).unwrap_or(i128::MAX);
        let result = i128::from(self.0) + rhs_ms;
        Timestamp(i64::try_from(result).expect("overflow when adding Duration to Timestamp"))
    }
}

impl AddAssign<Duration> for Timestamp {
    /// # Panics
    ///
    /// Panics if the resulting timestamp would overflow `i64` milliseconds.
    fn add_assign(&mut self, rhs: Duration) {
        *self = *self + rhs;
    }
}

impl Sub<Duration> for Timestamp {
    type Output = Timestamp;

    /// # Panics
    ///
    /// Panics if the resulting timestamp would overflow `i64` milliseconds.
    fn sub(self, rhs: Duration) -> Timestamp {
        let rhs_ms = i128::try_from(rhs.as_millis()).unwrap_or(i128::MAX);
        let result = i128::from(self.0) - rhs_ms;
        Timestamp(i64::try_from(result).expect("overflow when subtracting Duration from Timestamp"))
    }
}

impl SubAssign<Duration> for Timestamp {
    /// # Panics
    ///
    /// Panics if the resulting timestamp would overflow `i64` milliseconds.
    fn sub_assign(&mut self, rhs: Duration) {
        *self = *self - rhs;
    }
}

impl SerializeValue for Timestamp {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        CqlTimestamp(self.0).serialize(typ, writer)
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for Timestamp {
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        <CqlTimestamp as DeserializeValue<'frame, 'metadata>>::type_check(typ)
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        let ts = <CqlTimestamp as DeserializeValue<'frame, 'metadata>>::deserialize(typ, v)?;
        Ok(Timestamp(ts.0))
    }
}

/// A struct representing a timestamp of a stream generation.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct GenerationTimestamp {
    pub(crate) timestamp: Timestamp,
}

impl fmt::Display for GenerationTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.timestamp.as_millis())
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
        Timestamp::type_check(typ)
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        Ok(GenerationTimestamp {
            timestamp: Timestamp::deserialize(typ, v)?,
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
