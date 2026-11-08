//! `volSurface` preset defaulting and field-spec encoding.
//!
//! `xbbg-recipes` takes presets and field specs as strings. A field spec is
//! either a bare Bloomberg field (metric, tenor, point type and point are then
//! inferred from the field name) or `field|metric|tenor|pointType|point`,
//! where an empty part means "infer it". Hosts send the `@xbbg/core` shapes:
//!
//! - `preset`: absent → [`DEFAULT_PRESET`]; `null` → no preset; a name →
//!   that preset; an array → those presets (empty: none).
//! - `fields`: an array → those field names unchanged; an object → one spec
//!   per entry, in object order, from [`VolFieldSpec`] (`null` → the bare
//!   field); `null` or absent → no custom fields.
//!
//! With neither presets nor fields the recipe itself falls back to
//! [`DEFAULT_PRESET`].

use std::fmt;

use serde::Deserialize;
use serde::de::{self, Deserializer, MapAccess, SeqAccess, Visitor};

/// The preset JavaScript applies when `preset` is not given.
pub const DEFAULT_PRESET: &str = "MONEYNESS_30D";

/// One field's surface coordinates; a missing part is inferred by the recipe.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VolFieldSpec {
    #[serde(default)]
    pub metric: Option<String>,
    #[serde(default)]
    pub tenor: Option<String>,
    #[serde(default)]
    pub point_type: Option<String>,
    #[serde(default)]
    pub point: Option<f64>,
}

/// Encode one field spec in the recipe wire form.
///
/// No spec gives the bare field. Otherwise the result is
/// `field|metric|tenor|pointType|point` with missing parts empty; the point
/// uses the shortest decimal form (`25`, `-25`, `97.5`).
pub fn encode_field_spec(field: &str, spec: Option<&VolFieldSpec>) -> String {
    let Some(spec) = spec else {
        return field.to_string();
    };
    let point = spec
        .point
        .map(|point| point.to_string())
        .unwrap_or_default();
    [
        field,
        spec.metric.as_deref().unwrap_or_default(),
        spec.tenor.as_deref().unwrap_or_default(),
        spec.point_type.as_deref().unwrap_or_default(),
        point.as_str(),
    ]
    .join("|")
}

pub(super) fn default_presets() -> Option<Vec<String>> {
    Some(vec![DEFAULT_PRESET.to_string()])
}

/// Deserialize a present `preset` value (absence is handled by the serde default).
pub(super) fn presets<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Vec<String>>, D::Error> {
    struct PresetVisitor;

    impl<'de> Visitor<'de> for PresetVisitor {
        type Value = Option<Vec<String>>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a preset name, an array of preset names, or null")
        }

        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_some<D2: Deserializer<'de>>(self, inner: D2) -> Result<Self::Value, D2::Error> {
            inner.deserialize_any(self)
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
            Ok(Some(vec![value.to_string()]))
        }

        fn visit_string<E: de::Error>(self, value: String) -> Result<Self::Value, E> {
            Ok(Some(vec![value]))
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut presets = Vec::with_capacity(seq.size_hint().unwrap_or(0));
            while let Some(preset) = seq.next_element::<String>()? {
                presets.push(preset);
            }
            Ok(Some(presets))
        }
    }

    deserializer.deserialize_any(PresetVisitor)
}

/// Deserialize a present `fields` value into encoded field specs.
pub(super) fn field_specs<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Vec<String>>, D::Error> {
    struct FieldSpecsVisitor;

    impl<'de> Visitor<'de> for FieldSpecsVisitor {
        type Value = Option<Vec<String>>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("an array of field names, an object of field specs, or null")
        }

        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_some<D2: Deserializer<'de>>(self, inner: D2) -> Result<Self::Value, D2::Error> {
            inner.deserialize_any(self)
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut fields = Vec::with_capacity(seq.size_hint().unwrap_or(0));
            while let Some(field) = seq.next_element::<String>()? {
                fields.push(field);
            }
            Ok(Some(fields))
        }

        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            let mut specs = Vec::with_capacity(map.size_hint().unwrap_or(0));
            while let Some(field) = map.next_key::<String>()? {
                let spec = map.next_value::<Option<VolFieldSpec>>()?;
                specs.push(encode_field_spec(&field, spec.as_ref()));
            }
            Ok(Some(specs))
        }
    }

    deserializer.deserialize_any(FieldSpecsVisitor)
}
