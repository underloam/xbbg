use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use smallvec::SmallVec;
use xbbg_core::{DataType as BlpDataType, Value};

pub type FieldIndex = u16;
pub type TopicId = u32;

#[derive(Clone, Debug)]
pub struct FieldLayout {
    pub version: u32,
    pub fields: Arc<[FieldMeta]>,
}

#[derive(Clone, Debug)]
pub struct FieldMeta {
    pub name: Arc<str>,
    pub index: FieldIndex,
    pub kind: FieldKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldKind {
    Unknown,
    Bool,
    I32,
    I64,
    F64,
    Str,
    Date32,
    Time64Micros,
    TimestampMicros,
}

#[derive(Clone, Debug)]
pub struct SubscriptionUpdate {
    pub timestamp_us: i64,
    pub topic_id: TopicId,
    pub topic: Arc<str>,
    pub layout: Arc<FieldLayout>,
    /// Sparse delta entries: no entry means unchanged; Null means an explicit clear.
    pub values: SmallVec<[UpdateField; 8]>,
}

#[derive(Clone, Debug)]
pub struct UpdateField {
    pub index: FieldIndex,
    pub value: UpdateValue,
}

#[derive(Clone, Debug)]
pub enum UpdateValue {
    /// An explicitly present Bloomberg null, never an absent or unreadable field.
    Null,
    Bool(bool),
    I32(i32),
    I64(i64),
    F64(f64),
    Str(Arc<str>),
    Date32(i32),
    Time64Micros(i64),
    TimestampMicros(i64),
}
pub type StringValueCache = Option<(u64, Arc<str>)>;

impl FieldLayout {
    pub fn new(version: u32, fields: Vec<FieldMeta>) -> Self {
        Self {
            version,
            fields: Arc::from(fields.into_boxed_slice()),
        }
    }
}

impl FieldMeta {
    pub fn new(name: impl Into<Arc<str>>, index: FieldIndex, kind: FieldKind) -> Self {
        Self {
            name: name.into(),
            index,
            kind,
        }
    }
}

impl FieldKind {
    pub fn from_value(value: &UpdateValue) -> Self {
        match value {
            UpdateValue::Null => Self::Unknown,
            UpdateValue::Bool(_) => Self::Bool,
            UpdateValue::I32(_) => Self::I32,
            UpdateValue::I64(_) => Self::I64,
            UpdateValue::F64(_) => Self::F64,
            UpdateValue::Str(_) => Self::Str,
            UpdateValue::Date32(_) => Self::Date32,
            UpdateValue::Time64Micros(_) => Self::Time64Micros,
            UpdateValue::TimestampMicros(_) => Self::TimestampMicros,
        }
    }

    pub fn from_blp_datatype(datatype: BlpDataType) -> Self {
        match datatype {
            BlpDataType::Bool => Self::Bool,
            BlpDataType::Int32 => Self::I32,
            BlpDataType::Int64 => Self::I64,
            BlpDataType::Float32 | BlpDataType::Float64 | BlpDataType::Decimal => Self::F64,
            BlpDataType::String | BlpDataType::Enumeration => Self::Str,
            BlpDataType::Date => Self::Date32,
            BlpDataType::Time => Self::Time64Micros,
            // Char/Byte may decode as bool or integer; Datetime may be time-only.
            // A null value cannot establish either of those concrete kinds.
            BlpDataType::Char
            | BlpDataType::Byte
            | BlpDataType::Datetime
            | BlpDataType::Sequence
            | BlpDataType::Choice
            | BlpDataType::ByteArray
            | BlpDataType::CorrelationId => Self::Unknown,
        }
    }

    pub fn merge_observed(self, observed: FieldKind) -> FieldKind {
        match (self, observed) {
            (FieldKind::Unknown, kind) => kind,
            (kind, FieldKind::Unknown) => kind,
            (kind, observed) if kind == observed => kind,
            // Preserve existing compatibility behavior: if Bloomberg changes type
            // mid-stream, compatibility adapters expose the field as string.
            _ => FieldKind::Str,
        }
    }
}

impl UpdateValue {
    pub fn from_blp(value: Value<'_>) -> Self {
        Self::from_blp_with_str_cache(value, None)
    }

    pub fn from_blp_with_str_cache(
        value: Value<'_>,
        str_cache: Option<&mut StringValueCache>,
    ) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(v) => Self::Bool(v),
            Value::Int32(v) => Self::I32(v),
            Value::Int64(v) => Self::I64(v),
            Value::Float64(v) => Self::F64(v),
            Value::String(v) | Value::Enum(v) => Self::Str(Self::cached_str(v, str_cache)),
            Value::Date32(v) => Self::Date32(v),
            Value::TimestampMicros(v) => Self::TimestampMicros(v),
            Value::Datetime(v) => Self::TimestampMicros(v.to_micros()),
            Value::Time64Micros(v) => Self::Time64Micros(v),
            Value::Byte(v) => Self::I32(v as i32),
        }
    }

    fn cached_str(value: &str, str_cache: Option<&mut StringValueCache>) -> Arc<str> {
        let Some(str_cache) = str_cache else {
            return Arc::from(value);
        };
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        let hash = hasher.finish();
        if let Some((cached_hash, cached)) = str_cache.as_ref() {
            if *cached_hash == hash && cached.as_ref() == value {
                return Arc::clone(cached);
            }
        }
        let interned = Arc::<str>::from(value);
        *str_cache = Some((hash, Arc::clone(&interned)));
        interned
    }

    pub fn as_string_lossy(&self) -> Option<String> {
        match self {
            Self::Null => None,
            Self::Bool(v) => Some(if *v { "true" } else { "false" }.to_string()),
            Self::I32(v) => Some(v.to_string()),
            Self::I64(v) => Some(v.to_string()),
            Self::F64(v) => Some(v.to_string()),
            Self::Str(v) => Some(v.to_string()),
            Self::Date32(v) => Some(v.to_string()),
            Self::Time64Micros(v) => Some(v.to_string()),
            Self::TimestampMicros(v) => Some(v.to_string()),
        }
    }
}
