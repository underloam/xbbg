//! Versioned response diagnostics shared by the engine and native consumers.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use indexmap::{Equivalent, IndexSet};
use serde::ser::{SerializeMap, SerializeSeq, SerializeStruct};
use serde::{Deserialize, Serialize, Serializer};

use crate::engine::state::{DelayedMeta, ResponseMetadata, SecurityErrorMeta};

/// Dictionary-encoded per-security field exceptions in Arrow schema metadata.
pub const METADATA_KEY_FIELD_EXCEPTIONS: &str = "xbbg.field_exceptions";
/// Per-id rejections returned by the Bloomberg field metadata service.
pub const METADATA_KEY_FIELD_ERRORS: &str = "xbbg.field_errors";

/// One complete Bloomberg field exception; all members identify a dictionary entry.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct FieldExceptionMeta {
    pub field: String,
    pub category: String,
    pub code: i32,
    pub subcategory: String,
    pub message: String,
}

/// A borrowed exception allows repeated wire diagnostics to be interned without allocations.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct FieldExceptionRef<'a> {
    pub field: &'a str,
    pub category: &'a str,
    pub code: i32,
    pub subcategory: &'a str,
    pub message: &'a str,
}

impl Equivalent<FieldExceptionMeta> for FieldExceptionRef<'_> {
    fn equivalent(&self, other: &FieldExceptionMeta) -> bool {
        self.field == other.field
            && self.category == other.category
            && self.code == other.code
            && self.subcategory == other.subcategory
            && self.message == other.message
    }
}

/// One field-id refusal, distinct from a per-security field exception.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldErrorMeta {
    pub category: String,
    pub code: i32,
    pub subcategory: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

/// Interned exception payloads and ordered occurrences for each security.
///
/// New output uses the version-1 dictionary envelope. Historical persisted maps
/// remain readable. Occurrence order and duplicates are never deduplicated.
#[derive(Debug, Default)]
pub struct FieldExceptions {
    entries: IndexSet<FieldExceptionMeta>,
    securities: BTreeMap<String, Vec<usize>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    version: u32,
    entries: Vec<FieldExceptionMeta>,
    securities: BTreeMap<String, Vec<usize>>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Encoded {
    Compact(Envelope),
    Legacy(BTreeMap<String, Vec<FieldExceptionMeta>>),
}

impl FieldExceptions {
    /// Decode and validate a compact envelope or historical per-security map.
    pub fn from_json(raw: &str) -> Result<Self, serde_json::Error> {
        let mut result = Self::default();
        match serde_json::from_str::<Encoded>(raw).map_err(|error| explain_rejection(raw, error))? {
            Encoded::Compact(envelope) => {
                if envelope.version != 1 {
                    return Err(unsupported_version(envelope.version));
                }
                if envelope
                    .securities
                    .values()
                    .flatten()
                    .any(|&index| index >= envelope.entries.len())
                {
                    return Err(invalid("field-exception dictionary index out of range"));
                }
                let remap: Vec<usize> = envelope
                    .entries
                    .into_iter()
                    .map(|entry| result.entries.insert_full(entry).0)
                    .collect();
                result.securities = envelope
                    .securities
                    .into_iter()
                    .map(|(security, indices)| {
                        (
                            security,
                            indices.into_iter().map(|index| remap[index]).collect(),
                        )
                    })
                    .collect();
            }
            Encoded::Legacy(securities) => {
                // These two reserved keys together identify an envelope even
                // when its version/member types are malformed.
                if securities.contains_key("entries") && securities.contains_key("securities") {
                    return Err(invalid(
                        "malformed field-exception dictionary envelope: missing field `version`",
                    ));
                }
                for (security, exceptions) in securities {
                    let indices = exceptions
                        .into_iter()
                        .map(|exception| result.entries.insert_full(exception).0)
                        .collect();
                    result.securities.insert(security, indices);
                }
            }
        }
        Ok(result)
    }

    /// Serialize deterministically, independent of dictionary insertion order.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        let mut order: Vec<usize> = (0..self.entries.len()).collect();
        order.sort_unstable_by(|&left, &right| self.entries[left].cmp(&self.entries[right]));
        let mut remap = vec![0; order.len()];
        for (canonical, &original) in order.iter().enumerate() {
            remap[original] = canonical;
        }
        serde_json::to_string(&CanonicalEnvelope {
            diagnostics: self,
            order: &order,
            remap: &remap,
        })
    }

    /// Whether there are no security diagnostic lists.
    pub fn is_empty(&self) -> bool {
        self.securities.is_empty()
    }

    /// Retain one occurrence, interning its complete payload.
    pub fn record(&mut self, ticker: &str, exception: FieldExceptionMeta) {
        let index = self.entries.insert_full(exception).0;
        self.record_index(ticker, index);
    }

    /// Retain a borrowed wire exception, allocating its strings only when new.
    pub fn record_ref(&mut self, ticker: &str, exception: FieldExceptionRef<'_>) {
        let index = self.entries.get_index_of(&exception).unwrap_or_else(|| {
            self.entries
                .insert_full(FieldExceptionMeta {
                    field: exception.field.to_owned(),
                    category: exception.category.to_owned(),
                    code: exception.code,
                    subcategory: exception.subcategory.to_owned(),
                    message: exception.message.to_owned(),
                })
                .0
        });
        self.record_index(ticker, index);
    }

    fn record_index(&mut self, ticker: &str, index: usize) {
        if let Some(indices) = self.securities.get_mut(ticker) {
            indices.push(index);
        } else {
            self.securities.insert(ticker.to_owned(), vec![index]);
        }
    }

    /// Merge another dictionary, remapping local indices and appending occurrences.
    pub fn extend(&mut self, other: Self) {
        if self.is_empty() {
            *self = other;
            return;
        }
        let remap: Vec<usize> = other
            .entries
            .into_iter()
            .map(|entry| self.entries.insert_full(entry).0)
            .collect();
        for (security, indices) in other.securities {
            let remapped = indices.into_iter().map(|index| remap[index]);
            self.securities
                .entry(security)
                .or_default()
                .extend(remapped);
        }
    }

    /// Security names in stable order, without expanding their diagnostics.
    pub fn securities(&self) -> impl Iterator<Item = &str> {
        self.securities.keys().map(String::as_str)
    }

    /// Ordered exception occurrences for one security, borrowing interned entries.
    pub fn for_security(&self, ticker: &str) -> impl Iterator<Item = &FieldExceptionMeta> {
        self.securities
            .get(ticker)
            .into_iter()
            .flatten()
            .map(|&index| &self.entries[index])
    }

    /// Materialize the established public per-security accessor shape.
    pub fn expanded(&self) -> BTreeMap<String, Vec<FieldExceptionMeta>> {
        self.securities
            .keys()
            .map(|security| {
                (
                    security.clone(),
                    self.for_security(security).cloned().collect(),
                )
            })
            .collect()
    }

    /// Materialize JSON directly from borrowed entries, without an intermediate map copy.
    pub fn expanded_json(&self) -> serde_json::Value {
        serde_json::to_value(Expanded(self))
            .expect("field-exception strings and integers serialize")
    }
}

/// Encode FieldInfo refusals as the `xbbg.field_errors` metadata value.
///
/// `None` when there are no refusals: the key is then absent, as in engine
/// results. FieldInfo batches and [`ResultDiagnostics`] both use this writer.
pub fn encode_field_errors(
    errors: &BTreeMap<String, FieldErrorMeta>,
) -> Result<Option<String>, serde_json::Error> {
    if errors.is_empty() {
        return Ok(None);
    }
    serde_json::to_string(errors).map(Some)
}

/// Result diagnostics held as values, in the shapes the result accessors
/// return (`security_errors`, `field_exceptions`, `delayed`, `type_misses`,
/// `field_errors`, `eid_data`, `bloomberg_request_id`).
///
/// [`ResultDiagnostics::into_schema_metadata`] encodes them with the code the
/// engine attaches to its own results, so a result built from values carries
/// byte-identical `xbbg.*` schema metadata to one built from a response.
/// Empty maps leave their key absent.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct ResultDiagnostics {
    /// Entitlement IDs per security (`xbbg.eid_data`).
    pub eid_data: BTreeMap<String, Vec<i64>>,
    /// `securityError` per security (`xbbg.security_errors`).
    pub security_errors: BTreeMap<String, SecurityErrorMeta>,
    /// Ordered field exceptions per security (`xbbg.field_exceptions`). A
    /// security with no exceptions is not represented, as in engine results.
    pub field_exceptions: BTreeMap<String, Vec<FieldExceptionMeta>>,
    /// Delayed-data status per security (`xbbg.delayed`).
    pub delayed: BTreeMap<String, DelayedMeta>,
    /// Conversion misses per field (`xbbg.type_misses`).
    pub type_misses: BTreeMap<String, u64>,
    /// FieldInfo refusals per requested id (`xbbg.field_errors`).
    pub field_errors: BTreeMap<String, FieldErrorMeta>,
    /// SDK-assigned request ID (`xbbg.bloomberg_request_id`).
    pub bloomberg_request_id: Option<String>,
}

impl ResultDiagnostics {
    /// The schema-metadata map the engine would attach for these diagnostics.
    pub fn into_schema_metadata(self) -> Result<HashMap<String, String>, serde_json::Error> {
        let mut field_exceptions = FieldExceptions::default();
        for (security, exceptions) in self.field_exceptions {
            for exception in exceptions {
                field_exceptions.record(&security, exception);
            }
        }
        let carrier = ResponseMetadata::from_diagnostics(
            self.eid_data,
            self.security_errors,
            field_exceptions,
            self.delayed,
            self.type_misses,
            self.bloomberg_request_id.map(Arc::from),
        )
        .attach(RecordBatch::new_empty(Arc::new(Schema::empty())));
        let mut metadata = carrier.schema_ref().metadata().clone();
        if let Some(errors) = encode_field_errors(&self.field_errors)? {
            metadata.insert(METADATA_KEY_FIELD_ERRORS.to_owned(), errors);
        }
        Ok(metadata)
    }
}

fn invalid(message: &str) -> serde_json::Error {
    <serde_json::Error as serde::de::Error>::custom(message)
}

fn unsupported_version(version: impl std::fmt::Display) -> serde_json::Error {
    invalid(&format!(
        "unsupported field-exception metadata version {version}"
    ))
}

/// Untagged decoding only reports that no shape matched. Re-read the members
/// that identify an envelope (error path only) to say which part was rejected.
fn explain_rejection(raw: &str, error: serde_json::Error) -> serde_json::Error {
    #[derive(Deserialize)]
    struct EnvelopeMembers {
        version: Option<serde_json::Value>,
        entries: Option<serde::de::IgnoredAny>,
        securities: Option<serde::de::IgnoredAny>,
    }

    let Ok(members) = serde_json::from_str::<EnvelopeMembers>(raw) else {
        // Not a JSON object, or not JSON at all: serde's own error is precise.
        return error;
    };
    // A historical map could name a security `version`; only treat input as an
    // envelope when its reserved members appear together.
    let envelope_like = match members.version {
        Some(_) => members.entries.is_some() || members.securities.is_some(),
        None => members.entries.is_some() && members.securities.is_some(),
    };
    if !envelope_like {
        return error;
    }
    match members.version {
        Some(version) if version != 1 => unsupported_version(version),
        _ => match serde_json::from_str::<Envelope>(raw) {
            Err(envelope_error) => invalid(&format!(
                "malformed field-exception dictionary envelope: {envelope_error}"
            )),
            Ok(_) => error,
        },
    }
}

struct CanonicalEnvelope<'a> {
    diagnostics: &'a FieldExceptions,
    order: &'a [usize],
    remap: &'a [usize],
}

impl Serialize for CanonicalEnvelope<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut envelope = serializer.serialize_struct("FieldExceptions", 3)?;
        envelope.serialize_field("version", &1_u32)?;
        envelope.serialize_field("entries", &OrderedEntries(self.diagnostics, self.order))?;
        envelope.serialize_field(
            "securities",
            &RemappedSecurities(self.diagnostics, self.remap),
        )?;
        envelope.end()
    }
}

struct OrderedEntries<'a>(&'a FieldExceptions, &'a [usize]);

impl Serialize for OrderedEntries<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut entries = serializer.serialize_seq(Some(self.1.len()))?;
        for &index in self.1 {
            entries.serialize_element(&self.0.entries[index])?;
        }
        entries.end()
    }
}

struct RemappedSecurities<'a>(&'a FieldExceptions, &'a [usize]);

impl Serialize for RemappedSecurities<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut securities = serializer.serialize_map(Some(self.0.securities.len()))?;
        for (security, indices) in &self.0.securities {
            securities.serialize_entry(security, &RemappedIndices(indices, self.1))?;
        }
        securities.end()
    }
}

struct RemappedIndices<'a>(&'a [usize], &'a [usize]);

impl Serialize for RemappedIndices<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut indices = serializer.serialize_seq(Some(self.0.len()))?;
        for &index in self.0 {
            indices.serialize_element(&self.1[index])?;
        }
        indices.end()
    }
}

struct Expanded<'a>(&'a FieldExceptions);

impl Serialize for Expanded<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut securities = serializer.serialize_map(Some(self.0.securities.len()))?;
        for (security, indices) in &self.0.securities {
            securities.serialize_entry(security, &ExpandedEntries(self.0, indices))?;
        }
        securities.end()
    }
}

struct ExpandedEntries<'a>(&'a FieldExceptions, &'a [usize]);

impl Serialize for ExpandedEntries<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut entries = serializer.serialize_seq(Some(self.1.len()))?;
        for &index in self.1 {
            entries.serialize_element(&self.0.entries[index])?;
        }
        entries.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn exception(field: &str, code: i32) -> FieldExceptionMeta {
        FieldExceptionMeta {
            field: field.into(),
            category: "BAD_FLD".into(),
            code,
            subcategory: "NOT_APPLICABLE_TO_REF_DATA".into(),
            message: "Synthetic field not applicable".into(),
        }
    }

    #[test]
    fn dictionary_round_trip_preserves_payload_identity_order_and_duplicate_occurrences() {
        let first = exception("FIELD_A", 9);
        let second = exception("FIELD_A", 10);
        let mut diagnostics = FieldExceptions::default();
        for ticker in ["SYN_A", "SYN_B"] {
            diagnostics.record(ticker, second.clone());
            diagnostics.record_ref(
                ticker,
                FieldExceptionRef {
                    field: &first.field,
                    category: &first.category,
                    code: first.code,
                    subcategory: &first.subcategory,
                    message: &first.message,
                },
            );
            diagnostics.record(ticker, second.clone());
        }
        let raw = diagnostics.to_json().unwrap();
        let encoded: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(encoded["entries"], json!([first, second]));
        assert_eq!(encoded["securities"]["SYN_A"], json!([1, 0, 1]));
        let restored = FieldExceptions::from_json(&raw).unwrap();
        assert_eq!(restored.expanded(), diagnostics.expanded());
        assert_eq!(
            restored.expanded_json()["SYN_B"],
            json!([second, first, second])
        );
    }

    #[test]
    fn merge_remaps_local_dictionaries_without_deduplicating_occurrences() {
        let mut first = FieldExceptions::default();
        first.record("SYN_A", exception("FIELD_Z", 9));
        let mut second = FieldExceptions::default();
        second.record("SYN_B", exception("FIELD_A", 9));
        second.record("SYN_A", exception("FIELD_A", 9));
        second.record("SYN_A", exception("FIELD_Z", 9));
        first.extend(second);
        assert_eq!(
            first.expanded_json(),
            json!({
                "SYN_A": [exception("FIELD_Z", 9), exception("FIELD_A", 9), exception("FIELD_Z", 9)],
                "SYN_B": [exception("FIELD_A", 9)],
            })
        );
        assert_eq!(
            FieldExceptions::from_json(&first.to_json().unwrap())
                .unwrap()
                .expanded(),
            first.expanded()
        );
    }

    #[test]
    fn historical_maps_remain_readable_and_new_writes_are_canonical() {
        let legacy =
            json!({"SYN_B": [exception("FIELD_B", 9)], "SYN_A": [exception("FIELD_A", 9)]});
        let decoded = FieldExceptions::from_json(&legacy.to_string()).unwrap();
        let mut other = FieldExceptions::default();
        other.record("SYN_A", exception("FIELD_A", 9));
        other.record("SYN_B", exception("FIELD_B", 9));
        assert_eq!(decoded.to_json().unwrap(), other.to_json().unwrap());
        assert_eq!(decoded.expanded_json(), legacy);
        assert_eq!(
            FieldExceptions::from_json(r#"{"SYN_EMPTY":[]}"#)
                .unwrap()
                .expanded_json(),
            json!({"SYN_EMPTY": []})
        );
    }

    #[test]
    fn invalid_versions_and_dictionary_references_are_rejected() {
        for index in [json!(-1), json!(0.5), json!(1), json!("0"), json!(null)] {
            let raw = json!({"version": 1, "entries": [exception("FIELD_A", 9)], "securities": {"SYN_A": [index]}});
            assert!(
                FieldExceptions::from_json(&raw.to_string()).is_err(),
                "accepted {raw}"
            );
        }
        for raw in [
            json!({"version": 2, "entries": [], "securities": {}}),
            json!({"version": 1, "entries": [], "securities": {"SYN_A": [0]}}),
            json!({"version": 1, "entries": [{"field": "INCOMPLETE"}], "securities": {}}),
            json!({"entries": [], "securities": {}}),
            json!({"entries": [], "securities": []}),
            json!({"version": [], "entries": [], "securities": []}),
        ] {
            assert!(
                FieldExceptions::from_json(&raw.to_string()).is_err(),
                "accepted {raw}"
            );
        }
    }

    #[test]
    fn rejected_envelopes_name_the_version_or_member_at_fault() {
        let message = |raw: serde_json::Value| {
            FieldExceptions::from_json(&raw.to_string())
                .unwrap_err()
                .to_string()
        };
        for raw in [
            json!({"version": 2, "entries": [], "securities": {}}),
            json!({"version": 2, "entries": [], "securities": {}, "future": true}),
        ] {
            let message = message(raw);
            assert!(
                message.contains("unsupported field-exception metadata version 2"),
                "{message}"
            );
        }
        for raw in [
            json!({"version": 1, "entries": [exception("FIELD_A", 9)], "securities": {"SYN_A": [-1]}}),
            json!({"entries": [], "securities": {}}),
        ] {
            let message = message(raw);
            assert!(
                message.contains("malformed field-exception dictionary envelope"),
                "{message}"
            );
            assert!(!message.contains("did not match any variant"), "{message}");
        }
    }

    fn engine_metadata(metadata: ResponseMetadata) -> HashMap<String, String> {
        metadata
            .attach(RecordBatch::new_empty(Arc::new(Schema::empty())))
            .schema_ref()
            .metadata()
            .clone()
    }

    fn security_error() -> SecurityErrorMeta {
        SecurityErrorMeta {
            category: "BAD_SEC".into(),
            code: 15,
            subcategory: "INVALID_SECURITY".into(),
            message: "Synthetic unknown security".into(),
        }
    }

    fn field_error() -> FieldErrorMeta {
        FieldErrorMeta {
            category: "NO_AUTH".into(),
            code: -4,
            subcategory: "NOT_ENTITLED".into(),
            message: "Synthetic refusal".into(),
            source: Some("apiflds@synthetic".into()),
        }
    }

    #[test]
    fn result_diagnostics_encode_each_key_like_the_engine() {
        use crate::engine::state::{
            METADATA_KEY_BLOOMBERG_REQUEST_ID, METADATA_KEY_DELAYED, METADATA_KEY_EID_DATA,
            METADATA_KEY_SECURITY_ERRORS, METADATA_KEY_TYPE_MISSES,
        };

        let occurrences = vec![
            exception("FIELD_B", 9),
            exception("FIELD_A", 9),
            exception("FIELD_B", 9),
        ];
        let mut diagnostics = ResultDiagnostics::default();
        diagnostics
            .eid_data
            .insert("TEST Equity".into(), vec![14001, 14002]);
        diagnostics
            .security_errors
            .insert("NOTASECURITY1 Curncy".into(), security_error());
        diagnostics
            .field_exceptions
            .insert("TEST Equity".into(), occurrences.clone());
        diagnostics.delayed.insert(
            "TEST Equity".into(),
            DelayedMeta {
                delayed: true,
                delay_minutes: Some(15),
            },
        );
        diagnostics.type_misses.insert("FIELD_A".into(), 2);
        diagnostics
            .field_errors
            .insert("REFUSED_ALIAS".into(), field_error());
        diagnostics.bloomberg_request_id = Some("sdk-synthetic-1".into());
        let delayed = diagnostics.delayed.clone();
        let eid_json = serde_json::to_string(&diagnostics.eid_data).unwrap();
        let field_errors = diagnostics.field_errors.clone();
        let encoded = diagnostics.into_schema_metadata().unwrap();

        let mut errors = ResponseMetadata::default();
        errors.record_security_error("NOTASECURITY1 Curncy", security_error());
        let mut misses = ResponseMetadata::default();
        misses.record_type_miss("FIELD_A");
        misses.record_type_miss("FIELD_A");
        let id = ResponseMetadata::attach_bloomberg_request_id(
            RecordBatch::new_empty(Arc::new(Schema::empty())),
            "sdk-synthetic-1",
        );
        let mut exceptions = FieldExceptions::default();
        for occurrence in occurrences {
            exceptions.record("TEST Equity", occurrence);
        }

        let expected = HashMap::from([
            (METADATA_KEY_EID_DATA.to_owned(), eid_json),
            (
                METADATA_KEY_SECURITY_ERRORS.to_owned(),
                engine_metadata(errors)[METADATA_KEY_SECURITY_ERRORS].clone(),
            ),
            (
                METADATA_KEY_FIELD_EXCEPTIONS.to_owned(),
                exceptions.to_json().unwrap(),
            ),
            (
                METADATA_KEY_DELAYED.to_owned(),
                engine_metadata(ResponseMetadata::with_delayed(delayed))[METADATA_KEY_DELAYED]
                    .clone(),
            ),
            (
                METADATA_KEY_TYPE_MISSES.to_owned(),
                engine_metadata(misses)[METADATA_KEY_TYPE_MISSES].clone(),
            ),
            (
                METADATA_KEY_FIELD_ERRORS.to_owned(),
                encode_field_errors(&field_errors).unwrap().unwrap(),
            ),
            (
                METADATA_KEY_BLOOMBERG_REQUEST_ID.to_owned(),
                id.schema_ref().metadata()[METADATA_KEY_BLOOMBERG_REQUEST_ID].clone(),
            ),
        ]);
        assert_eq!(encoded, expected);
    }

    #[test]
    fn result_diagnostics_keep_field_exception_occurrences_across_a_round_trip() {
        let occurrences = vec![
            exception("FIELD_B", 9),
            exception("FIELD_A", 9),
            exception("FIELD_B", 9),
        ];
        let mut diagnostics = ResultDiagnostics::default();
        diagnostics
            .field_exceptions
            .insert("TEST Equity".into(), occurrences.clone());
        let metadata = diagnostics.into_schema_metadata().unwrap();
        let restored =
            FieldExceptions::from_json(&metadata[METADATA_KEY_FIELD_EXCEPTIONS]).unwrap();
        assert_eq!(restored.expanded()["TEST Equity"], occurrences);
    }

    #[test]
    fn empty_result_diagnostics_attach_no_keys() {
        assert!(
            ResultDiagnostics::default()
                .into_schema_metadata()
                .unwrap()
                .is_empty()
        );
    }
}
