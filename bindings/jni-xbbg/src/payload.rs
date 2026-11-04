//! Native results and their Java representation.
//!
//! Area modules return a [`Payload`]; the bridge converts it to the Java object
//! for the requested [`ResultKind`] (on the completing bridge thread for async
//! calls, on the caller thread for sync calls). A kind the payload cannot
//! satisfy is an INTERNAL error. `Payload::Void` is Java `null` for every kind,
//! which is how a method reports "absent" (`Optional.empty()` in Java).

use arrow::ipc::writer::{FileWriter, StreamWriter};
use arrow::record_batch::RecordBatch;
use jni::Env;
use jni::objects::{JObject, JObjectArray, JString};
use jni::refs::IntoAuto;
use xbbg_host::errors::HostError;

use crate::bridge::{NativeError, java};
use crate::table;

/// Result kind codes shared with Java `internal.ResultKind`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum ResultKind {
    Void,
    Table,
    IpcStream,
    IpcFile,
    Json,
    Handle,
    Bool,
    Str,
    Ticks,
    Delivery,
    TableList,
    TicksList,
    DeliveryList,
}

impl ResultKind {
    const ALL: [Self; 13] = [
        Self::Void,
        Self::Table,
        Self::IpcStream,
        Self::IpcFile,
        Self::Json,
        Self::Handle,
        Self::Bool,
        Self::Str,
        Self::Ticks,
        Self::Delivery,
        Self::TableList,
        Self::TicksList,
        Self::DeliveryList,
    ];

    /// The kind with Java code `code`; anything else is INTERNAL.
    pub(crate) fn from_code(code: i32) -> Result<Self, HostError> {
        usize::try_from(code)
            .ok()
            .and_then(|index| Self::ALL.get(index).copied())
            .ok_or_else(|| HostError::internal(format!("unknown result kind {code}")))
    }

    #[cfg(test)]
    pub(crate) const fn code(self) -> i32 {
        self as i32
    }

    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Void => "VOID",
            Self::Table => "TABLE",
            Self::IpcStream => "IPC_STREAM",
            Self::IpcFile => "IPC_FILE",
            Self::Json => "JSON",
            Self::Handle => "HANDLE",
            Self::Bool => "BOOL",
            Self::Str => "STRING",
            Self::Ticks => "TICKS",
            Self::Delivery => "DELIVERY",
            Self::TableList => "TABLE_LIST",
            Self::TicksList => "TICKS_LIST",
            Self::DeliveryList => "DELIVERY_LIST",
        }
    }

    /// TABLE, IPC_STREAM or IPC_FILE: the kinds a table-producing method honours.
    pub(crate) const fn is_table(self) -> bool {
        matches!(self, Self::Table | Self::IpcStream | Self::IpcFile)
    }

    /// Fail unless the caller asked for a table kind (a table-producing method).
    pub(crate) fn expect_table(self, method: &str) -> Result<(), HostError> {
        if self.is_table() {
            Ok(())
        } else {
            Err(HostError::internal(format!(
                "method '{method}' returns a table, not {}; expected TABLE, IPC_STREAM or IPC_FILE",
                self.name()
            )))
        }
    }

    /// Fail unless the caller asked for `natural`, the method's only kind.
    pub(crate) fn expect(self, natural: Self, method: &str) -> Result<(), HostError> {
        if self == natural {
            Ok(())
        } else {
            Err(HostError::internal(format!(
                "method '{method}' returns {}, not {}",
                natural.name(),
                self.name()
            )))
        }
    }
}

/// A result whose conversion to Java needs the JNI environment (V3 ticks and
/// deliveries). Conversion runs in the completion upcall, right before
/// `NativeBridge.complete` (or before a sync native returns).
pub(crate) trait JavaPayload: Send + 'static {
    fn into_java<'local>(
        self: Box<Self>,
        env: &mut Env<'local>,
    ) -> Result<JObject<'local>, NativeError>;
}

/// A native result before conversion to Java.
pub(crate) enum Payload {
    /// Java `null`.
    Void,
    /// `table.Table`.
    Table(RecordBatch),
    /// `table.Table[]`.
    TableList(Vec<RecordBatch>),
    /// `byte[]` (IPC stream or file bytes).
    Bytes(Vec<u8>),
    /// `String` holding JSON text.
    Json(String),
    /// `Long` handle.
    Handle(u64),
    /// `Boolean`.
    Bool(bool),
    /// `String`.
    Str(String),
    /// Converted by its own implementation.
    Java(Box<dyn JavaPayload>),
}

impl std::fmt::Debug for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Void => f.write_str("Void"),
            Self::Table(batch) => write!(f, "Table({} rows)", batch.num_rows()),
            Self::TableList(batches) => write!(f, "TableList({})", batches.len()),
            Self::Bytes(bytes) => write!(f, "Bytes({})", bytes.len()),
            Self::Json(text) => write!(f, "Json({text})"),
            Self::Handle(id) => write!(f, "Handle({id})"),
            Self::Bool(value) => write!(f, "Bool({value})"),
            Self::Str(text) => write!(f, "Str({text})"),
            Self::Java(_) => f.write_str("Java(..)"),
        }
    }
}

impl Payload {
    /// A table result in the requested table kind: a `Table`, or Arrow IPC
    /// stream / file bytes encoded here, off the JVM.
    pub(crate) fn table(batch: RecordBatch, kind: ResultKind) -> Result<Self, HostError> {
        match kind {
            ResultKind::Table => Ok(Self::Table(batch)),
            ResultKind::IpcStream => Ok(Self::Bytes(ipc_stream(&batch)?)),
            ResultKind::IpcFile => Ok(Self::Bytes(ipc_file(&batch)?)),
            other => Err(HostError::internal(format!(
                "a table result cannot be returned as {}; expected TABLE, IPC_STREAM or IPC_FILE",
                other.name()
            ))),
        }
    }

    /// A JSON result serialized from `value`.
    pub(crate) fn json<T: serde::Serialize + ?Sized>(value: &T) -> Result<Self, HostError> {
        serde_json::to_string(value)
            .map(Self::Json)
            .map_err(|error| HostError::internal(format!("serialize result: {error}")))
    }

    /// Whether this payload is a valid result for `kind`.
    pub(crate) fn matches(&self, kind: ResultKind) -> bool {
        match self {
            Self::Void => true,
            Self::Table(_) => kind == ResultKind::Table,
            Self::TableList(_) => kind == ResultKind::TableList,
            Self::Bytes(_) => matches!(kind, ResultKind::IpcStream | ResultKind::IpcFile),
            Self::Json(_) => kind == ResultKind::Json,
            Self::Handle(_) => kind == ResultKind::Handle,
            Self::Bool(_) => kind == ResultKind::Bool,
            Self::Str(_) => kind == ResultKind::Str,
            // The implementation produces the requested kind's Java type itself.
            Self::Java(_) => kind != ResultKind::Void,
        }
    }

    /// Convert to the Java object for `kind`.
    pub(crate) fn into_java<'local>(
        self,
        env: &mut Env<'local>,
        kind: ResultKind,
    ) -> Result<JObject<'local>, NativeError> {
        if !self.matches(kind) {
            return Err(HostError::internal(format!(
                "native result {self:?} does not match result kind {}",
                kind.name()
            ))
            .into());
        }
        match self {
            Self::Void => Ok(JObject::null()),
            Self::Table(batch) => table::to_java(env, &batch),
            Self::TableList(batches) => table_list(env, &batches),
            Self::Bytes(bytes) => Ok(env.byte_array_from_slice(&bytes)?.into()),
            Self::Json(text) | Self::Str(text) => Ok(JString::new(env, text)?.into()),
            Self::Handle(id) => java()?.boxed_long(env, crate::handles::java_handle(id)),
            Self::Bool(value) => java()?.boxed_bool(env, value),
            Self::Java(payload) => payload.into_java(env),
        }
    }
}

fn table_list<'local>(
    env: &mut Env<'local>,
    batches: &[RecordBatch],
) -> Result<JObject<'local>, NativeError> {
    let java = java()?;
    let array: JObjectArray<'local> = env.new_object_array(
        jsize(batches.len(), "table list")?,
        &java.table_class,
        JObject::null(),
    )?;
    for (index, batch) in batches.iter().enumerate() {
        let table = table::to_java(env, batch)?.auto();
        array.set_element(env, index, &*table)?;
    }
    Ok(array.into())
}

/// A Java array length; larger collections cannot cross the boundary.
pub(crate) fn jsize(len: usize, what: &str) -> Result<i32, HostError> {
    i32::try_from(len).map_err(|_| {
        HostError::internal(format!(
            "{what} length {len} exceeds the Java array length limit"
        ))
    })
}

/// Encode one batch as an Arrow IPC stream (napi `requestRaw` format).
pub(crate) fn ipc_stream(batch: &RecordBatch) -> Result<Vec<u8>, HostError> {
    let mut writer = StreamWriter::try_new(Vec::new(), batch.schema_ref())
        .map_err(|error| HostError::internal(format!("Arrow IPC writer init failed: {error}")))?;
    writer
        .write(batch)
        .map_err(|error| HostError::internal(format!("Arrow IPC write failed: {error}")))?;
    writer
        .finish()
        .map_err(|error| HostError::internal(format!("Arrow IPC finalize failed: {error}")))?;
    writer
        .into_inner()
        .map_err(|error| HostError::internal(format!("Arrow IPC finalize failed: {error}")))
}

/// Encode one batch as an Arrow IPC file (napi `requestIpcFile` format).
pub(crate) fn ipc_file(batch: &RecordBatch) -> Result<Vec<u8>, HostError> {
    let ipc_error =
        |stage: &str, error| HostError::internal(format!("Arrow IPC file {stage} failed: {error}"));
    let mut writer = FileWriter::try_new(Vec::new(), batch.schema_ref())
        .map_err(|error| ipc_error("writer init", error))?;
    writer
        .write(batch)
        .map_err(|error| ipc_error("write", error))?;
    writer
        .into_inner()
        .map_err(|error| ipc_error("finalize", error))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::reader::{FileReader, StreamReader};

    use super::*;

    fn batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ])
        .with_metadata(HashMap::from([(
            "xbbg.bloomberg_request_id".to_string(),
            "sdk-1".to_string(),
        )]));
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec!["A", "B"])),
                Arc::new(Int64Array::from(vec![Some(1), None])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn result_kind_codes_match_the_java_constants() {
        for (code, name) in [
            (0, "VOID"),
            (1, "TABLE"),
            (2, "IPC_STREAM"),
            (3, "IPC_FILE"),
            (4, "JSON"),
            (5, "HANDLE"),
            (6, "BOOL"),
            (7, "STRING"),
            (8, "TICKS"),
            (9, "DELIVERY"),
            (10, "TABLE_LIST"),
            (11, "TICKS_LIST"),
            (12, "DELIVERY_LIST"),
        ] {
            let kind = ResultKind::from_code(code).unwrap();
            assert_eq!((kind.code(), kind.name()), (code, name));
        }
        for code in [-1, 13, i32::MAX] {
            let error = ResultKind::from_code(code).unwrap_err();
            assert_eq!(error.message, format!("unknown result kind {code}"));
        }
    }

    #[test]
    fn natural_kind_mismatch_is_internal() {
        assert!(
            ResultKind::Bool
                .expect(ResultKind::Bool, "isAvailable")
                .is_ok()
        );
        let error = ResultKind::Json
            .expect(ResultKind::Bool, "isAvailable")
            .unwrap_err();
        assert_eq!(error.kind, xbbg_host::errors::HostErrorKind::Internal);
        assert_eq!(error.message, "method 'isAvailable' returns BOOL, not JSON");
    }

    #[test]
    fn table_payload_honours_the_requested_table_kind() {
        assert!(matches!(
            Payload::table(batch(), ResultKind::Table),
            Ok(Payload::Table(_))
        ));

        let Ok(Payload::Bytes(stream)) = Payload::table(batch(), ResultKind::IpcStream) else {
            panic!("stream bytes expected");
        };
        let mut reader = StreamReader::try_new(Cursor::new(stream), None).unwrap();
        let decoded = reader.next().unwrap().unwrap();
        assert_eq!(decoded, batch());
        assert_eq!(
            decoded.schema().metadata()["xbbg.bloomberg_request_id"],
            "sdk-1"
        );

        let Ok(Payload::Bytes(file)) = Payload::table(batch(), ResultKind::IpcFile) else {
            panic!("file bytes expected");
        };
        assert_eq!(&file[..6], b"ARROW1");
        assert_eq!(&file[file.len() - 6..], b"ARROW1");
        let mut reader = FileReader::try_new(Cursor::new(file), None).unwrap();
        assert_eq!(reader.next().unwrap().unwrap(), batch());

        let error = Payload::table(batch(), ResultKind::Json).unwrap_err();
        assert_eq!(
            error.message,
            "a table result cannot be returned as JSON; expected TABLE, IPC_STREAM or IPC_FILE"
        );
    }

    #[test]
    fn payloads_only_match_their_kinds() {
        assert!(Payload::Void.matches(ResultKind::Table));
        assert!(Payload::Void.matches(ResultKind::Json));
        assert!(Payload::Json("{}".into()).matches(ResultKind::Json));
        assert!(!Payload::Json("{}".into()).matches(ResultKind::Str));
        assert!(Payload::Bytes(Vec::new()).matches(ResultKind::IpcFile));
        assert!(!Payload::Bytes(Vec::new()).matches(ResultKind::Table));
        assert!(Payload::Handle(1).matches(ResultKind::Handle));
        assert!(!Payload::Bool(true).matches(ResultKind::Handle));
        assert!(!Payload::Table(batch()).matches(ResultKind::TableList));
        assert!(Payload::TableList(vec![batch()]).matches(ResultKind::TableList));
    }

    #[test]
    fn java_array_lengths_are_bounded() {
        assert_eq!(jsize(5, "x").unwrap(), 5);
        let error = jsize(i32::MAX as usize + 1, "column 'a' values").unwrap_err();
        assert_eq!(
            error.message,
            "column 'a' values length 2147483648 exceeds the Java array length limit"
        );
    }
}
