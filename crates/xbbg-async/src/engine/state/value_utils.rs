use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use super::refdata::LongMode;
use super::typed_builder::{ArrowType, ColumnSet, TypedBuilder};
use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, StringBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema, SchemaRef};
use xbbg_core::{BlpError, DataType as BlpDataType, Element, Message, Name, Value};

/// Schema-metadata key carrying per-security entitlement IDs
/// (`securityData[].eidData`, JSON: `{"<ticker>": [eid, ...]}`).
pub const METADATA_KEY_EID_DATA: &str = "xbbg.eid_data";
/// Schema-metadata key carrying per-security `securityError` details
/// (JSON: `{"<ticker>": {"category", "code", "subcategory", "message"}}`).
pub const METADATA_KEY_SECURITY_ERRORS: &str = "xbbg.security_errors";
/// Schema-metadata key carrying per-security `fieldExceptions`
/// (JSON: `{"<ticker>": [{"field", "category", "code", "subcategory", "message"}, ...]}`).
pub const METADATA_KEY_FIELD_EXCEPTIONS: &str = "xbbg.field_exceptions";

/// `securityError` details captured for batch metadata.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SecurityErrorMeta {
    pub category: String,
    pub code: i32,
    pub subcategory: String,
    pub message: String,
}

/// One `fieldExceptions[]` entry captured for batch metadata.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FieldExceptionMeta {
    pub field: String,
    pub category: String,
    pub code: i32,
    pub subcategory: String,
    pub message: String,
}

/// Response-level diagnostics that must survive into the result batch: raw
/// responses carry `eidData` / `securityError` / `fieldExceptions` next to the
/// field data, and dropping them silently is exactly the failure mode a
/// market-data consumer cannot detect. Collected during message processing
/// and attached to the final [`RecordBatch`] as Arrow schema metadata (JSON
/// values under the `xbbg.*` keys above) so every output format — long, wide,
/// typed — and every binding surface sees them without shape changes.
#[derive(Debug, Default)]
pub(crate) struct ResponseMetadata {
    eid_data: BTreeMap<String, Vec<i64>>,
    security_errors: BTreeMap<String, SecurityErrorMeta>,
    field_exceptions: BTreeMap<String, Vec<FieldExceptionMeta>>,
}

impl ResponseMetadata {
    /// Record `securityData[].eidData` (an int array element) for `ticker`.
    pub(crate) fn record_eid_data(&mut self, ticker: &str, eids: &Element<'_>) {
        let entry = self.eid_data.entry(ticker.to_string()).or_default();
        for i in 0..eids.len() {
            if let Some(eid) = eids.get_i32(i) {
                entry.push(i64::from(eid));
            }
        }
    }

    pub(crate) fn record_security_error(&mut self, ticker: &str, error: SecurityErrorMeta) {
        self.security_errors.insert(ticker.to_string(), error);
    }

    pub(crate) fn record_field_exception(&mut self, ticker: &str, exception: FieldExceptionMeta) {
        self.field_exceptions
            .entry(ticker.to_string())
            .or_default()
            .push(exception);
    }

    fn is_empty(&self) -> bool {
        self.eid_data.is_empty()
            && self.security_errors.is_empty()
            && self.field_exceptions.is_empty()
    }

    /// Attach the collected diagnostics to `batch` as schema metadata.
    /// Infallible by design: a metadata failure must never turn a good data
    /// batch into an error, so serialization problems only log.
    pub(crate) fn attach(self, batch: RecordBatch) -> RecordBatch {
        if self.is_empty() {
            return batch;
        }
        let mut metadata = batch.schema_ref().metadata().clone();
        Self::insert_json(&mut metadata, METADATA_KEY_EID_DATA, &self.eid_data);
        Self::insert_json(
            &mut metadata,
            METADATA_KEY_SECURITY_ERRORS,
            &self.security_errors,
        );
        Self::insert_json(
            &mut metadata,
            METADATA_KEY_FIELD_EXCEPTIONS,
            &self.field_exceptions,
        );
        let schema = Arc::new(batch.schema_ref().as_ref().clone().with_metadata(metadata));
        match batch.clone().with_schema(schema) {
            Ok(with_meta) => with_meta,
            Err(err) => {
                xbbg_log::warn!(error = %err, "failed to attach response metadata to batch");
                batch
            }
        }
    }

    fn insert_json<T: serde::Serialize>(
        metadata: &mut HashMap<String, String>,
        key: &str,
        value: &T,
    ) {
        let is_empty = match serde_json::to_value(value) {
            Ok(serde_json::Value::Object(map)) => map.is_empty(),
            _ => false,
        };
        if is_empty {
            return;
        }
        match serde_json::to_string(value) {
            Ok(json) => {
                metadata.insert(key.to_string(), json);
            }
            Err(err) => {
                xbbg_log::warn!(key = key, error = %err, "failed to serialize response metadata");
            }
        }
    }

    /// Union response metadata across sharded result batches so shard
    /// concatenation (which keeps only the first batch's schema) does not
    /// silently drop diagnostics from later shards. Shards partition
    /// securities, so per-ticker entries never conflict.
    pub(crate) fn union_of(batches: &[RecordBatch]) -> Self {
        let mut merged = Self::default();
        for batch in batches {
            let metadata = batch.schema_ref().metadata();
            if let Some(map) =
                Self::parse_json::<BTreeMap<String, Vec<i64>>>(metadata.get(METADATA_KEY_EID_DATA))
            {
                merged.eid_data.extend(map);
            }
            if let Some(map) = Self::parse_json::<BTreeMap<String, SecurityErrorMeta>>(
                metadata.get(METADATA_KEY_SECURITY_ERRORS),
            ) {
                merged.security_errors.extend(map);
            }
            if let Some(map) = Self::parse_json::<BTreeMap<String, Vec<FieldExceptionMeta>>>(
                metadata.get(METADATA_KEY_FIELD_EXCEPTIONS),
            ) {
                for (ticker, exceptions) in map {
                    merged
                        .field_exceptions
                        .entry(ticker)
                        .or_default()
                        .extend(exceptions);
                }
            }
        }
        merged
    }

    fn parse_json<T: serde::de::DeserializeOwned>(value: Option<&String>) -> Option<T> {
        let value = value?;
        match serde_json::from_str(value) {
            Ok(parsed) => Some(parsed),
            Err(err) => {
                xbbg_log::warn!(error = %err, "failed to parse response metadata JSON");
                None
            }
        }
    }
}

/// Extract a top-level Bloomberg `responseError` from a response message.
///
/// Bloomberg can reject the whole request (daily capacity, entitlement,
/// malformed request, service-side throttling) while still delivering a
/// syntactically valid `RESPONSE` event. Without this guard the state machines
/// simply find no `securityData`/payload and return an empty batch, hiding the
/// actual vendor error.
pub(crate) fn top_level_response_error(
    msg: &Message<'_>,
    service: &'static str,
    operation: &'static str,
) -> Option<BlpError> {
    let response_error = msg.elements().get_by_str("responseError")?;

    let source = response_error
        .get_by_str("source")
        .and_then(|e| e.get_str(0));
    let code = response_error.get_by_str("code").and_then(|e| e.get_i32(0));
    let category = response_error
        .get_by_str("category")
        .and_then(|e| e.get_str(0));
    let subcategory = response_error
        .get_by_str("subcategory")
        .and_then(|e| e.get_str(0));
    let message = response_error
        .get_by_str("message")
        .and_then(|e| e.get_str(0));

    let mut parts = Vec::with_capacity(5);
    if let Some(source) = source {
        parts.push(format!("source={source}"));
    }
    if let Some(category) = category {
        parts.push(format!("category={category}"));
    }
    if let Some(code) = code {
        parts.push(format!("code={code}"));
    }
    if let Some(subcategory) = subcategory {
        parts.push(format!("subcategory={subcategory}"));
    }
    if let Some(message) = message {
        parts.push(format!("message={}", message.trim()));
    }

    let label = if parts.is_empty() {
        Some("Bloomberg responseError".to_string())
    } else {
        Some(format!("Bloomberg responseError: {}", parts.join("; ")))
    };

    Some(BlpError::RequestFailure {
        service: service.to_string(),
        operation: Some(operation.to_string()),
        cid: None,
        label,
        request_id: None,
        source: None,
    })
}

pub(crate) fn should_emit_scalar_field(element: &Element<'_>) -> bool {
    !element.is_array()
        && !matches!(
            element.datatype(),
            BlpDataType::Sequence
                | BlpDataType::Choice
                | BlpDataType::ByteArray
                | BlpDataType::CorrelationId
        )
}

pub(crate) fn arrow_type_for_element(element: &Element<'_>) -> ArrowType {
    match element.datatype() {
        BlpDataType::Bool => ArrowType::Bool,
        BlpDataType::Char | BlpDataType::Byte | BlpDataType::Int32 => ArrowType::Int32,
        BlpDataType::Int64 => ArrowType::Int64,
        BlpDataType::Float32 | BlpDataType::Float64 | BlpDataType::Decimal => ArrowType::Float64,
        BlpDataType::String | BlpDataType::Enumeration => ArrowType::String,
        BlpDataType::Date => ArrowType::Date32,
        BlpDataType::Time => ArrowType::Time64Micros,
        BlpDataType::Datetime => ArrowType::TimestampMicros,
        BlpDataType::Sequence
        | BlpDataType::Choice
        | BlpDataType::ByteArray
        | BlpDataType::CorrelationId => ArrowType::String,
    }
}

#[inline(always)]
pub(crate) fn get_value_cached_datatype<'a>(
    element: &Element<'a>,
    cached_datatype: &mut Option<BlpDataType>,
) -> Option<Value<'a>> {
    if let Some(cached) = *cached_datatype {
        if let Some(value) = get_value_for_datatype(element, cached, 0) {
            return Some(value);
        }

        let datatype = element.datatype();
        if datatype != cached {
            xbbg_log::debug!(
                cached = ?cached,
                actual = ?datatype,
                "Bloomberg element datatype changed; refreshing extractor cache"
            );
        }
        *cached_datatype = Some(datatype);
        return get_value_for_datatype(element, datatype, 0);
    }

    let datatype = element.datatype();
    *cached_datatype = Some(datatype);
    get_value_for_datatype(element, datatype, 0)
}

#[inline(always)]
fn get_value_for_datatype<'a>(
    element: &Element<'a>,
    datatype: BlpDataType,
    index: usize,
) -> Option<Value<'a>> {
    match datatype {
        BlpDataType::Bool => element.get_bool(index).map(Value::Bool),
        BlpDataType::Char | BlpDataType::Byte => {
            if let Some(value) = element.get_bool(index) {
                return Some(Value::Bool(value));
            }
            element.get_i32(index).map(|value| Value::Byte(value as u8))
        }
        BlpDataType::Int32 => element.get_i32(index).map(Value::Int32),
        BlpDataType::Int64 => element.get_i64(index).map(Value::Int64),
        BlpDataType::Float32 | BlpDataType::Float64 | BlpDataType::Decimal => {
            element.get_f64(index).map(Value::Float64)
        }
        BlpDataType::String => element.get_str(index).map(Value::String),
        BlpDataType::Date => element.get_datetime(index).map(|dt| {
            let micros = dt.to_micros();
            Value::Date32((micros / 86_400_000_000) as i32)
        }),
        BlpDataType::Time => element
            .get_datetime(index)
            .map(|dt| Value::Time64Micros(dt.to_time_micros())),
        BlpDataType::Datetime => element.get_datetime(index).map(|dt| {
            if dt.has_date_parts() {
                Value::TimestampMicros(dt.to_micros())
            } else {
                Value::Time64Micros(dt.to_time_micros())
            }
        }),
        BlpDataType::Enumeration => element.get_str(index).map(Value::Enum),
        BlpDataType::Sequence
        | BlpDataType::Choice
        | BlpDataType::ByteArray
        | BlpDataType::CorrelationId => Some(Value::Null),
    }
}
/// Compute the common Arrow type for the "value" column from requested fields
/// and field type hints.
///
/// If every requested field has a numeric hint, returns Float64 (promoting mixed
/// ints/floats). If any requested field is missing a hint, any hint is
/// non-numeric, or no hints are provided, falls back to String.
pub(crate) fn common_value_type(
    field_names: &[String],
    field_types: &HashMap<String, ArrowType>,
) -> ArrowType {
    if field_names.is_empty() || field_types.is_empty() {
        return ArrowType::String;
    }

    let mut has_float = false;
    let mut has_int = false;

    for field_name in field_names {
        let Some(arrow_type) = field_types.get(field_name) else {
            return ArrowType::String;
        };
        match arrow_type {
            ArrowType::Float64 => has_float = true,
            ArrowType::Int64 | ArrowType::Int32 => has_int = true,
            // Any non-numeric type → fall back to string
            _ => return ArrowType::String,
        }
    }

    if has_float || has_int {
        ArrowType::Float64
    } else {
        ArrowType::String
    }
}

pub(crate) struct LongStringColumns {
    ticker: StringBuilder,
    date: Option<Date32Builder>,
    field: StringBuilder,
    value: TypedBuilder,
    row_count: usize,
}

impl LongStringColumns {
    pub(crate) fn refdata(value_type: ArrowType) -> Self {
        Self::new(value_type, false)
    }

    pub(crate) fn histdata(value_type: ArrowType) -> Self {
        Self::new(value_type, true)
    }

    fn new(value_type: ArrowType, include_date: bool) -> Self {
        Self {
            ticker: StringBuilder::new(),
            date: include_date.then(Date32Builder::new),
            field: StringBuilder::new(),
            value: TypedBuilder::new(value_type),
            row_count: 0,
        }
    }

    pub(crate) fn row_count(&self) -> usize {
        self.row_count
    }

    pub(crate) fn append_refdata_row(
        &mut self,
        ticker: &str,
        field_name: &str,
        value: Option<Value<'_>>,
    ) {
        self.ticker.append_value(ticker);
        self.field.append_value(field_name);
        self.append_value(value);
        self.row_count += 1;
    }

    pub(crate) fn append_histdata_row(
        &mut self,
        ticker: &str,
        date_value: Option<Value<'_>>,
        field_name: &str,
        value: Option<Value<'_>>,
    ) {
        self.ticker.append_value(ticker);
        if let Some(date) = self.date.as_mut() {
            append_date32_value(date, date_value);
        }
        self.field.append_value(field_name);
        self.append_value(value);
        self.row_count += 1;
    }

    fn append_value(&mut self, value: Option<Value<'_>>) {
        match value {
            Some(value) => self.value.append_value(Some(value)),
            None => self.value.append_null(),
        }
    }

    pub(crate) fn finish_refdata(mut self) -> Result<RecordBatch, BlpError> {
        let fields = vec![
            Field::new("ticker", ArrowType::String.to_arrow_datatype(), true),
            Field::new("field", ArrowType::String.to_arrow_datatype(), true),
            Field::new("value", self.value.data_type(), true),
        ];
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.ticker.finish()),
            Arc::new(self.field.finish()),
            self.value.finish(),
        ];
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|e| {
            BlpError::Internal {
                detail: format!("build long ReferenceData RecordBatch: {e}"),
            }
        })
    }

    pub(crate) fn finish_histdata(mut self) -> Result<RecordBatch, BlpError> {
        let Some(mut date) = self.date.take() else {
            return Err(BlpError::Internal {
                detail: "histdata long columns missing date builder".to_string(),
            });
        };
        let fields = vec![
            Field::new("ticker", ArrowType::String.to_arrow_datatype(), true),
            Field::new("date", ArrowType::Date32.to_arrow_datatype(), true),
            Field::new("field", ArrowType::String.to_arrow_datatype(), true),
            Field::new("value", self.value.data_type(), true),
        ];
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.ticker.finish()),
            Arc::new(date.finish()),
            Arc::new(self.field.finish()),
            self.value.finish(),
        ];
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|e| {
            BlpError::Internal {
                detail: format!("build long HistoricalData RecordBatch: {e}"),
            }
        })
    }
}

fn append_date32_value(builder: &mut Date32Builder, value: Option<Value<'_>>) {
    match value {
        Some(Value::Date32(days)) => builder.append_value(days),
        Some(Value::TimestampMicros(micros)) => {
            builder.append_value((micros / 86_400_000_000) as i32)
        }
        _ => builder.append_null(),
    }
}

pub(crate) struct TypedLongColumns {
    ticker: StringBuilder,
    date: Option<Date32Builder>,
    field: StringBuilder,
    value_f64: Float64Builder,
    value_i64: Int64Builder,
    value_str: StringBuilder,
    value_bool: BooleanBuilder,
    value_date: Date32Builder,
    value_ts: TimestampMicrosecondBuilder,
    value_time: Time64MicrosecondBuilder,
    row_count: usize,
    schema: SchemaRef,
}

impl TypedLongColumns {
    pub(crate) fn refdata() -> Self {
        Self::new(false, 0)
    }

    pub(crate) fn histdata() -> Self {
        Self::new(true, 0)
    }

    pub(crate) fn reserve_if_empty(&mut self, row_capacity: usize) {
        if self.row_count == 0 {
            *self = Self::new(self.date.is_some(), row_capacity);
        }
    }

    fn new(include_date: bool, row_capacity: usize) -> Self {
        let string_bytes = row_capacity.saturating_mul(24).max(1);
        Self {
            ticker: StringBuilder::with_capacity(row_capacity, string_bytes),
            date: include_date.then(|| Date32Builder::with_capacity(row_capacity)),
            field: StringBuilder::with_capacity(row_capacity, string_bytes),
            value_f64: Float64Builder::with_capacity(row_capacity),
            value_i64: Int64Builder::with_capacity(row_capacity),
            value_str: StringBuilder::with_capacity(row_capacity, string_bytes),
            value_bool: BooleanBuilder::with_capacity(row_capacity),
            value_date: Date32Builder::with_capacity(row_capacity),
            value_ts: TimestampMicrosecondBuilder::with_capacity(row_capacity),
            value_time: Time64MicrosecondBuilder::with_capacity(row_capacity),
            row_count: 0,
            schema: Self::schema(include_date),
        }
    }

    fn schema(include_date: bool) -> SchemaRef {
        let mut fields = Vec::with_capacity(if include_date { 10 } else { 9 });
        fields.push(Field::new(
            "ticker",
            ArrowType::String.to_arrow_datatype(),
            true,
        ));
        if include_date {
            fields.push(Field::new(
                "date",
                ArrowType::Date32.to_arrow_datatype(),
                true,
            ));
        }
        fields.push(Field::new(
            "field",
            ArrowType::String.to_arrow_datatype(),
            true,
        ));
        fields.push(Field::new(
            "value_f64",
            ArrowType::Float64.to_arrow_datatype(),
            true,
        ));
        fields.push(Field::new(
            "value_i64",
            ArrowType::Int64.to_arrow_datatype(),
            true,
        ));
        fields.push(Field::new(
            "value_str",
            ArrowType::String.to_arrow_datatype(),
            true,
        ));
        fields.push(Field::new(
            "value_bool",
            ArrowType::Bool.to_arrow_datatype(),
            true,
        ));
        fields.push(Field::new(
            "value_date",
            ArrowType::Date32.to_arrow_datatype(),
            true,
        ));
        fields.push(Field::new(
            "value_ts",
            ArrowType::TimestampMicros.to_arrow_datatype(),
            true,
        ));
        fields.push(Field::new(
            "value_time",
            ArrowType::Time64Micros.to_arrow_datatype(),
            true,
        ));
        Arc::new(Schema::new(fields))
    }

    pub(crate) fn row_count(&self) -> usize {
        self.row_count
    }

    pub(crate) fn append_row(
        &mut self,
        ticker: &str,
        date_value: Option<&Value<'_>>,
        field_name: &str,
        value: Option<Value<'_>>,
    ) {
        self.ticker.append_value(ticker);
        if let Some(date) = self.date.as_mut() {
            append_date32_value_ref(date, date_value);
        }
        self.field.append_value(field_name);
        self.append_typed_value(value);
        self.row_count += 1;
    }

    fn append_typed_value(&mut self, value: Option<Value<'_>>) {
        let time_value = match value.as_ref() {
            Some(Value::Time64Micros(micros)) => Some(*micros),
            Some(Value::Datetime(datetime)) if !datetime.has_date_parts() => {
                Some(datetime.to_time_micros())
            }
            _ => None,
        };
        self.value_time.append_option(time_value);
        match value {
            Some(Value::Float64(v)) => {
                self.value_f64.append_value(v);
                self.value_i64.append_null();
                self.value_str.append_null();
                self.value_bool.append_null();
                self.value_date.append_null();
                self.value_ts.append_null();
            }
            Some(Value::Int64(v)) => {
                self.value_f64.append_null();
                self.value_i64.append_value(v);
                self.value_str.append_null();
                self.value_bool.append_null();
                self.value_date.append_null();
                self.value_ts.append_null();
            }
            Some(Value::Int32(v)) => {
                self.value_f64.append_null();
                self.value_i64.append_value(i64::from(v));
                self.value_str.append_null();
                self.value_bool.append_null();
                self.value_date.append_null();
                self.value_ts.append_null();
            }
            Some(Value::String(s)) | Some(Value::Enum(s)) => {
                self.value_f64.append_null();
                self.value_i64.append_null();
                self.value_str.append_value(s);
                self.value_bool.append_null();
                self.value_date.append_null();
                self.value_ts.append_null();
            }
            Some(Value::Bool(v)) => {
                self.value_f64.append_null();
                self.value_i64.append_null();
                self.value_str.append_null();
                self.value_bool.append_value(v);
                self.value_date.append_null();
                self.value_ts.append_null();
            }
            Some(Value::Date32(days)) => {
                self.value_f64.append_null();
                self.value_i64.append_null();
                self.value_str.append_null();
                self.value_bool.append_null();
                self.value_date.append_value(days);
                self.value_ts.append_null();
            }
            Some(Value::TimestampMicros(micros)) => {
                self.value_f64.append_null();
                self.value_i64.append_null();
                self.value_str.append_null();
                self.value_bool.append_null();
                self.value_date.append_null();
                self.value_ts.append_value(micros);
            }
            Some(Value::Datetime(dt)) => {
                self.value_f64.append_null();
                self.value_i64.append_null();
                self.value_str.append_null();
                self.value_bool.append_null();
                self.value_date.append_null();
                self.value_ts
                    .append_option(dt.has_date_parts().then(|| dt.to_micros()));
            }
            Some(Value::Time64Micros(_)) => {
                self.value_f64.append_null();
                self.value_i64.append_null();
                self.value_str.append_null();
                self.value_bool.append_null();
                self.value_date.append_null();
                self.value_ts.append_null();
            }
            Some(Value::Byte(v)) => {
                self.value_f64.append_null();
                self.value_i64.append_value(i64::from(v));
                self.value_str.append_null();
                self.value_bool.append_null();
                self.value_date.append_null();
                self.value_ts.append_null();
            }
            Some(Value::Null) | None => {
                self.value_f64.append_null();
                self.value_i64.append_null();
                self.value_str.append_null();
                self.value_bool.append_null();
                self.value_date.append_null();
                self.value_ts.append_null();
            }
        }
    }

    pub(crate) fn finish(mut self) -> Result<RecordBatch, BlpError> {
        let mut arrays: Vec<ArrayRef> =
            Vec::with_capacity(if self.date.is_some() { 10 } else { 9 });
        arrays.push(Arc::new(self.ticker.finish()));
        if let Some(mut date) = self.date.take() {
            arrays.push(Arc::new(date.finish()));
        }
        arrays.push(Arc::new(self.field.finish()));
        arrays.push(Arc::new(self.value_f64.finish()));
        arrays.push(Arc::new(self.value_i64.finish()));
        arrays.push(Arc::new(self.value_str.finish()));
        arrays.push(Arc::new(self.value_bool.finish()));
        arrays.push(Arc::new(self.value_date.finish()));
        arrays.push(Arc::new(self.value_ts.finish().with_timezone("UTC")));
        arrays.push(Arc::new(self.value_time.finish()));

        RecordBatch::try_new(self.schema, arrays).map_err(|e| BlpError::Internal {
            detail: format!("build typed long RecordBatch: {e}"),
        })
    }
}

fn append_date32_value_ref(builder: &mut Date32Builder, value: Option<&Value<'_>>) {
    match value {
        Some(Value::Date32(days)) => builder.append_value(*days),
        Some(Value::TimestampMicros(micros)) => {
            builder.append_value((micros / 86_400_000_000) as i32)
        }
        _ => builder.append_null(),
    }
}

struct WideFieldColumn {
    name: String,
    type_hint: Option<ArrowType>,
    builder: Option<TypedBuilder>,
}

pub(crate) struct WideColumns {
    ticker: StringBuilder,
    date: Option<Date32Builder>,
    fields: Vec<WideFieldColumn>,
    row_count: usize,
}

impl WideColumns {
    pub(crate) fn refdata(
        field_names: &[String],
        field_types: &HashMap<String, ArrowType>,
    ) -> Self {
        Self::new(field_names, field_types, false)
    }

    pub(crate) fn histdata(
        field_names: &[String],
        field_types: &HashMap<String, ArrowType>,
    ) -> Self {
        Self::new(field_names, field_types, true)
    }

    fn new(
        field_names: &[String],
        field_types: &HashMap<String, ArrowType>,
        include_date: bool,
    ) -> Self {
        Self {
            ticker: StringBuilder::new(),
            date: include_date.then(Date32Builder::new),
            fields: field_names
                .iter()
                .map(|name| WideFieldColumn {
                    name: name.clone(),
                    type_hint: field_types.get(name).copied(),
                    builder: None,
                })
                .collect(),
            row_count: 0,
        }
    }

    pub(crate) fn append_refdata_row<'a, F>(
        &mut self,
        ticker: &str,
        field_lookup_names: &[Name],
        field_datatypes: &mut [Option<BlpDataType>],
        lookup: F,
    ) where
        F: FnMut(&Name, &mut Option<BlpDataType>) -> Option<Value<'a>>,
    {
        self.ticker.append_value(ticker);
        self.append_field_values(field_lookup_names, field_datatypes, lookup);
        self.row_count += 1;
    }

    pub(crate) fn row_count(&self) -> usize {
        self.row_count
    }

    pub(crate) fn append_histdata_row<'a, F>(
        &mut self,
        ticker: &str,
        date_value: Option<Value<'_>>,
        field_lookup_names: &[Name],
        field_datatypes: &mut [Option<BlpDataType>],
        lookup: F,
    ) where
        F: FnMut(&Name, &mut Option<BlpDataType>) -> Option<Value<'a>>,
    {
        self.ticker.append_value(ticker);
        if let Some(date) = self.date.as_mut() {
            append_date32_value(date, date_value);
        }
        self.append_field_values(field_lookup_names, field_datatypes, lookup);
        self.row_count += 1;
    }

    fn append_field_values<'a, F>(
        &mut self,
        field_lookup_names: &[Name],
        field_datatypes: &mut [Option<BlpDataType>],
        mut lookup: F,
    ) where
        F: FnMut(&Name, &mut Option<BlpDataType>) -> Option<Value<'a>>,
    {
        for index in 0..self.fields.len() {
            let value = match (
                field_lookup_names.get(index),
                field_datatypes.get_mut(index),
            ) {
                (Some(field_lookup_name), Some(field_datatype)) => {
                    lookup(field_lookup_name, field_datatype)
                }
                _ => None,
            };
            self.append_field_value(index, value);
        }
    }

    fn append_field_value(&mut self, index: usize, value: Option<Value<'_>>) {
        let Some(column) = self.fields.get_mut(index) else {
            return;
        };

        if let Some(builder) = column.builder.as_mut() {
            match value {
                Some(value) => builder.append_value(Some(value)),
                None => builder.append_null(),
            }
            return;
        }

        if let Some(value) = value {
            let arrow_type = column
                .type_hint
                .unwrap_or_else(|| ArrowType::from_value(&value));
            let mut builder = TypedBuilder::new(arrow_type);
            for _ in 0..self.row_count {
                builder.append_null();
            }
            builder.append_value(Some(value));
            column.builder = Some(builder);
        }
    }

    pub(crate) fn finish_refdata(self) -> Result<RecordBatch, BlpError> {
        self.finish(false)
    }

    pub(crate) fn finish_histdata(self) -> Result<RecordBatch, BlpError> {
        self.finish(true)
    }

    fn finish(mut self, include_date: bool) -> Result<RecordBatch, BlpError> {
        let mut arrow_fields =
            Vec::with_capacity(self.fields.len() + if include_date { 2 } else { 1 });
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(arrow_fields.capacity());

        arrow_fields.push(Field::new(
            "ticker",
            ArrowType::String.to_arrow_datatype(),
            true,
        ));
        arrays.push(Arc::new(self.ticker.finish()));

        if include_date {
            let Some(mut date) = self.date.take() else {
                return Err(BlpError::Internal {
                    detail: "wide HistoricalData columns missing date builder".to_string(),
                });
            };
            arrow_fields.push(Field::new(
                "date",
                ArrowType::Date32.to_arrow_datatype(),
                true,
            ));
            arrays.push(Arc::new(date.finish()));
        }

        for mut column in self.fields {
            let mut builder = column.builder.take().unwrap_or_else(|| {
                let mut builder = TypedBuilder::new(column.type_hint.unwrap_or(ArrowType::String));
                for _ in 0..self.row_count {
                    builder.append_null();
                }
                builder
            });
            arrow_fields.push(Field::new(&column.name, builder.data_type(), true));
            arrays.push(builder.finish());
        }

        RecordBatch::try_new(Arc::new(Schema::new(arrow_fields)), arrays).map_err(|e| {
            BlpError::Internal {
                detail: format!("build wide RecordBatch: {e}"),
            }
        })
    }
}

pub(crate) fn append_long_value_row<F>(
    columns: &mut ColumnSet,
    long_mode: LongMode,
    field_name: &str,
    value: Option<Value<'_>>,
    dtype: Option<&str>,
    prefix: F,
) where
    F: FnOnce(&mut ColumnSet),
{
    prefix(columns);
    columns.append_str("field", field_name);

    match long_mode {
        LongMode::String => {
            if let Some(value) = value {
                columns.append("value", value);
            } else {
                columns.append_null("value");
            }
        }
        LongMode::WithMetadata => {
            if let Some(ref value) = value {
                let value_str = value_to_string(value);
                columns.append_str("value", value_str.as_ref());
                columns.append_str("dtype", dtype.unwrap_or("null"));
            } else {
                columns.append_null("value");
                columns.append_str("dtype", "null");
            }
        }
        LongMode::Typed => unreachable!("typed long rows are appended by TypedLongColumns"),
    }

    columns.end_row();
}

fn civil_from_days(days: i64) -> (i32, u32, u32) {
    // Howard Hinnant's civil-from-days algorithm. `days` is relative to
    // 1970-01-01, matching Arrow Date32 and Bloomberg date extraction.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = y + i64::from(month <= 2);

    (year as i32, month as u32, day as u32)
}

fn push_padded_u64(out: &mut String, value: u64, width: usize) {
    let mut buffer = itoa::Buffer::new();
    let digits = buffer.format(value);
    for _ in digits.len()..width {
        out.push('0');
    }
    out.push_str(digits);
}

fn push_padded_i64(out: &mut String, value: i64, width: usize) {
    if value < 0 {
        out.push('-');
        push_padded_u64(out, value.unsigned_abs(), width);
    } else {
        push_padded_u64(out, value as u64, width);
    }
}

fn push_date(out: &mut String, days: i64) {
    let (year, month, day) = civil_from_days(days);
    push_padded_i64(out, year as i64, 4);
    out.push('-');
    push_padded_u64(out, month as u64, 2);
    out.push('-');
    push_padded_u64(out, day as u64, 2);
}

pub(crate) fn format_date32(days: i32) -> String {
    let mut out = String::with_capacity(10);
    push_date(&mut out, days as i64);
    out
}

pub(crate) fn format_time64_micros(micros: i64) -> String {
    let total_secs = micros / 1_000_000;
    let frac_us = (micros % 1_000_000).unsigned_abs();
    let h = total_secs / 3600;
    let m = (total_secs % 3600) / 60;
    let s = total_secs % 60;

    let mut out = String::with_capacity(15);
    push_padded_i64(&mut out, h, 2);
    out.push(':');
    push_padded_i64(&mut out, m, 2);
    out.push(':');
    push_padded_i64(&mut out, s, 2);
    out.push('.');
    push_padded_u64(&mut out, frac_us, 6);
    out
}

pub(crate) fn format_timestamp_micros(micros: i64) -> String {
    if micros < 0 {
        return format_timestamp_micros_fallback(micros);
    }

    let secs = micros / 1_000_000;
    let frac_us = (micros % 1_000_000) as u64;
    let days = secs / 86_400;
    let seconds_of_day = secs % 86_400;
    let h = seconds_of_day / 3_600;
    let m = (seconds_of_day % 3_600) / 60;
    let s = seconds_of_day % 60;

    let mut out = String::with_capacity(27);
    push_date(&mut out, days);
    out.push('T');
    push_padded_i64(&mut out, h, 2);
    out.push(':');
    push_padded_i64(&mut out, m, 2);
    out.push(':');
    push_padded_i64(&mut out, s, 2);
    out.push('.');
    push_padded_u64(&mut out, frac_us, 6);
    out.push('Z');
    out
}

fn format_timestamp_micros_fallback(micros: i64) -> String {
    use chrono::DateTime;

    let secs = micros / 1_000_000;
    let nanos = ((micros % 1_000_000) * 1000) as u32;
    if let Some(dt) = DateTime::from_timestamp(secs, nanos) {
        dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
    } else {
        let mut buffer = itoa::Buffer::new();
        let mut out = String::with_capacity(24);
        out.push_str(buffer.format(micros));
        out.push_str("us");
        out
    }
}

pub(crate) fn value_to_string<'a>(value: &'a Value<'a>) -> Cow<'a, str> {
    match value {
        Value::Null => Cow::Borrowed(""),
        Value::Bool(b) => Cow::Owned(b.to_string()),
        Value::Int32(i) => Cow::Owned(i.to_string()),
        Value::Int64(i) => Cow::Owned(i.to_string()),
        Value::Float64(f) => Cow::Owned(f.to_string()),
        Value::String(s) | Value::Enum(s) => Cow::Borrowed(s),
        Value::Date32(days) => Cow::Owned(format_date32(*days)),
        Value::TimestampMicros(micros) => Cow::Owned(format_timestamp_micros(*micros)),
        Value::Datetime(dt) => Cow::Owned(format_timestamp_micros(dt.to_micros())),
        Value::Time64Micros(micros) => Cow::Owned(format_time64_micros(*micros)),
        Value::Byte(b) => Cow::Owned(b.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Date32Array, Float64Array, StringArray};

    #[test]
    fn date_time_formatters_match_expected_strings() {
        assert_eq!(format_date32(0), "1970-01-01");
        assert_eq!(format_date32(1), "1970-01-02");
        assert_eq!(format_timestamp_micros(0), "1970-01-01T00:00:00.000000Z");
        assert_eq!(
            format_timestamp_micros(1_714_639_234_567_890),
            "2024-05-02T08:40:34.567890Z"
        );
        assert_eq!(format_time64_micros(37_234_005_006), "10:20:34.005006");
    }

    #[test]
    fn common_value_type_partial_hints_missing_requested_fields_returns_string() {
        let requested_fields = vec![
            "NAME".to_string(),
            "PX_LAST".to_string(),
            "CRNCY".to_string(),
        ];
        let field_types = HashMap::from([("PX_LAST".to_string(), ArrowType::Float64)]);

        assert_eq!(
            common_value_type(&requested_fields, &field_types),
            ArrowType::String
        );
    }

    #[test]
    fn common_value_type_all_requested_fields_hinted_numeric_returns_float64() {
        let requested_fields = vec!["PX_LAST".to_string(), "VOLUME".to_string()];
        let field_types = HashMap::from([
            ("PX_LAST".to_string(), ArrowType::Float64),
            ("VOLUME".to_string(), ArrowType::Int64),
        ]);

        assert_eq!(
            common_value_type(&requested_fields, &field_types),
            ArrowType::Float64
        );
    }

    #[test]
    fn long_string_columns_refdata_preserve_order_and_nulls() {
        let mut columns = LongStringColumns::refdata(ArrowType::String);
        columns.append_refdata_row("IBM US Equity", "PX_LAST", Some(Value::Float64(123.45)));
        columns.append_refdata_row("IBM US Equity", "BAD_FIELD", None);

        let batch = columns.finish_refdata().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.schema().field(0).name(), "ticker");
        assert_eq!(batch.schema().field(1).name(), "field");
        assert_eq!(batch.schema().field(2).name(), "value");

        let tickers = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let fields = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(tickers.value(0), "IBM US Equity");
        assert_eq!(fields.value(0), "PX_LAST");
        assert_eq!(values.value(0), "123.45");
        assert_eq!(fields.value(1), "BAD_FIELD");
        assert!(values.is_null(1));
    }

    #[test]
    fn long_string_columns_histdata_preserve_date_and_typed_value() {
        let mut columns = LongStringColumns::histdata(ArrowType::Float64);
        columns.append_histdata_row(
            "IBM US Equity",
            Some(Value::Date32(20_000)),
            "PX_LAST",
            Some(Value::Float64(123.45)),
        );
        columns.append_histdata_row("IBM US Equity", Some(Value::Date32(20_001)), "VOLUME", None);

        let batch = columns.finish_histdata().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);
        assert_eq!(batch.schema().field(0).name(), "ticker");
        assert_eq!(batch.schema().field(1).name(), "date");
        assert_eq!(batch.schema().field(2).name(), "field");
        assert_eq!(batch.schema().field(3).name(), "value");

        let dates = batch
            .column(1)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        let fields = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(dates.value(0), 20_000);
        assert_eq!(fields.value(0), "PX_LAST");
        assert_eq!(values.value(0), 123.45);
        assert_eq!(dates.value(1), 20_001);
        assert_eq!(fields.value(1), "VOLUME");
        assert!(values.is_null(1));
    }

    #[test]
    fn datetime_without_date_parts_stays_time_only_in_typed_outputs() {
        use crate::field_cache::BlpFieldType;
        use arrow_array::Time64MicrosecondArray;
        use xbbg_core::test_support::TestEvent;

        let schema = r#"<ServiceDefinition name="xbbg.test.temporal_value" version="1.0.0.0">
            <service name="//xbbg/test/temporal_value" version="1.0.0.0">
                <event name="TemporalValue" eventType="TemporalValueType"/>
            </service>
            <schema><sequenceType name="TemporalValueType">
                <element name="SYNTHETIC_TIME" type="Datetime"/>
            </sequenceType></schema>
        </ServiceDefinition>"#;
        // The JSON formatter rejects incomplete Datetime strings; construct the
        // SDK's actual time-only Datetime representation with its typed setter.
        let datetime = blpapi_sys::blpapi_HighPrecisionDatetime_t {
            datetime: blpapi_sys::blpapi_Datetime_t {
                parts: 112, // hour, minute and second, with no date bits
                hours: 15,
                minutes: 59,
                seconds: 2,
                milliSeconds: 0,
                month: 0,
                day: 0,
                year: 0,
                offset: 0,
            },
            picoseconds: 0,
        };
        let event = TestEvent::subscription(schema, "TemporalValue", |formatter| {
            formatter.datetime("SYNTHETIC_TIME", &datetime);
        });
        let mut messages = event.event().messages();
        let message = messages.next().unwrap();
        let element = message.elements().get_by_str("SYNTHETIC_TIME").unwrap();
        let mut datatype = None;
        for historical in [false, true] {
            let mut columns = if historical {
                TypedLongColumns::histdata()
            } else {
                TypedLongColumns::refdata()
            };
            let value = get_value_cached_datatype(&element, &mut datatype);
            columns.append_row(
                "IBM US Equity",
                Some(&Value::Date32(20_455)),
                "SYNTHETIC_TIME",
                value,
            );
            let batch = columns.finish().unwrap();
            let times = batch
                .column_by_name("value_time")
                .unwrap()
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .unwrap();
            assert_eq!(times.value(0), 57_542_000_000);
            assert!(batch.column_by_name("value_ts").unwrap().is_null(0));
        }
        let fields = vec!["SYNTHETIC_TIME".to_string()];
        let hint = ArrowType::parse(
            BlpFieldType::from_metadata(Some("Datetime"), Some("Time")).to_arrow_type_str(),
        );
        let hints = HashMap::from([("SYNTHETIC_TIME".to_string(), hint)]);
        let names = [Name::get_or_intern("SYNTHETIC_TIME")];
        for historical in [false, true] {
            let mut columns = if historical {
                WideColumns::histdata(&fields, &hints)
            } else {
                WideColumns::refdata(&fields, &hints)
            };
            if historical {
                columns.append_histdata_row(
                    "IBM US Equity",
                    Some(Value::Date32(20_455)),
                    &names,
                    &mut [None],
                    |_, cached| get_value_cached_datatype(&element, cached),
                );
            } else {
                columns.append_refdata_row("IBM US Equity", &names, &mut [None], |_, cached| {
                    get_value_cached_datatype(&element, cached)
                });
            }
            let batch = if historical {
                columns.finish_histdata()
            } else {
                columns.finish_refdata()
            }
            .unwrap();
            let times = batch
                .column_by_name("SYNTHETIC_TIME")
                .unwrap()
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .unwrap();
            assert_eq!(times.value(0), 57_542_000_000);
        }
    }

    fn tiny_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ticker",
            arrow_schema::DataType::Utf8,
            false,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["IBM US Equity"]))],
        )
        .unwrap()
    }

    #[test]
    fn response_metadata_survives_on_zero_row_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ticker",
            arrow_schema::DataType::Utf8,
            false,
        )]));
        let empty = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(Vec::<String>::new()))],
        )
        .unwrap();
        let mut metadata = ResponseMetadata::default();
        metadata
            .eid_data
            .insert("IBM US Equity".to_string(), vec![14005, 35009]);
        metadata
            .eid_data
            .insert("EMPTY US Equity".to_string(), Vec::new());

        let batch = metadata.attach(empty);

        assert_eq!(batch.num_rows(), 0);
        assert_eq!(
            batch
                .schema_ref()
                .metadata()
                .get(METADATA_KEY_EID_DATA)
                .map(String::as_str),
            Some(r#"{"EMPTY US Equity":[],"IBM US Equity":[14005,35009]}"#)
        );
    }

    #[test]
    fn response_metadata_attach_and_union_round_trip() {
        // Shard 1: entitled security with EIDs + a field exception.
        let mut meta1 = ResponseMetadata::default();
        meta1
            .eid_data
            .insert("IBM US Equity".to_string(), vec![14005, 35009]);
        meta1.field_exceptions.insert(
            "IBM US Equity".to_string(),
            vec![FieldExceptionMeta {
                field: "BAD_FIELD".to_string(),
                category: "BAD_FLD".to_string(),
                code: 9,
                subcategory: "NOT_APPLICABLE_TO_REF_DATA".to_string(),
                message: "Field not applicable".to_string(),
            }],
        );
        let batch1 = meta1.attach(tiny_batch());
        assert!(batch1
            .schema_ref()
            .metadata()
            .contains_key(METADATA_KEY_EID_DATA));

        // Shard 2: unentitled security — securityError AND eidData together
        // (the SAPI/B-PIPE case: EIDs are reported for securities the
        // identity cannot see).
        let mut meta2 = ResponseMetadata::default();
        meta2
            .eid_data
            .insert("PRIVATE US Equity".to_string(), vec![9999]);
        meta2.security_errors.insert(
            "PRIVATE US Equity".to_string(),
            SecurityErrorMeta {
                category: "AUTHORIZATION".to_string(),
                code: 17,
                subcategory: "NOT_ENTITLED".to_string(),
                message: "Not entitled to security".to_string(),
            },
        );
        let batch2 = meta2.attach(tiny_batch());

        // Empty metadata attaches nothing.
        let batch3 = ResponseMetadata::default().attach(tiny_batch());
        assert!(batch3.schema_ref().metadata().is_empty());

        // Union across shards preserves every entry from both sides.
        let merged = ResponseMetadata::union_of(&[batch1, batch2, batch3]);
        assert_eq!(
            merged.eid_data.get("IBM US Equity"),
            Some(&vec![14005, 35009])
        );
        assert_eq!(merged.eid_data.get("PRIVATE US Equity"), Some(&vec![9999]));
        let err = merged.security_errors.get("PRIVATE US Equity").unwrap();
        assert_eq!(err.code, 17);
        assert_eq!(err.subcategory, "NOT_ENTITLED");
        let excs = merged.field_exceptions.get("IBM US Equity").unwrap();
        assert_eq!(excs.len(), 1);
        assert_eq!(excs[0].field, "BAD_FIELD");

        // Re-attach of the merged map keeps JSON parseable end to end.
        let final_batch = merged.attach(tiny_batch());
        let re_merged = ResponseMetadata::union_of(std::slice::from_ref(&final_batch));
        assert_eq!(re_merged.eid_data.len(), 2);
        assert_eq!(re_merged.security_errors.len(), 1);
        assert_eq!(re_merged.field_exceptions.len(), 1);
    }
}
