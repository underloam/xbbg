//! JSON value helpers shared by the planners: insertion-ordered objects, scalar
//! option values (`string | number | boolean`) and JavaScript `String(value)`
//! formatting, so every host stringifies option values exactly like the TS layer.

use std::fmt;
use std::marker::PhantomData;

use serde::de::{self, Deserialize, Deserializer, MapAccess, Visitor};

/// A scalar option value: the TS `PrimitiveValue` (`string | number | boolean`).
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Scalar {
    Str(String),
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
}

impl Scalar {
    /// The value as JavaScript `String(value)` renders it.
    pub(crate) fn to_js_string(&self) -> String {
        match self {
            Self::Str(text) => text.clone(),
            Self::Bool(flag) => flag.to_string(),
            Self::Int(number) => number.to_string(),
            Self::UInt(number) => number.to_string(),
            Self::Float(number) => js_number_string(*number),
        }
    }

    /// The text of a string value; `None` for numbers and booleans (TS `stringOrUndef`).
    pub(crate) fn as_str(&self) -> Option<&str> {
        match self {
            Self::Str(text) => Some(text),
            _ => None,
        }
    }
}

impl<'de> Deserialize<'de> for Scalar {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ScalarVisitor;

        impl Visitor<'_> for ScalarVisitor {
            type Value = Scalar;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string, number or boolean")
            }

            fn visit_bool<E: de::Error>(self, value: bool) -> Result<Scalar, E> {
                Ok(Scalar::Bool(value))
            }

            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Scalar, E> {
                Ok(Scalar::Int(value))
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Scalar, E> {
                Ok(Scalar::UInt(value))
            }

            fn visit_f64<E: de::Error>(self, value: f64) -> Result<Scalar, E> {
                Ok(Scalar::Float(value))
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Scalar, E> {
                Ok(Scalar::Str(value.to_owned()))
            }

            fn visit_string<E: de::Error>(self, value: String) -> Result<Scalar, E> {
                Ok(Scalar::Str(value))
            }
        }

        deserializer.deserialize_any(ScalarVisitor)
    }
}

/// A JSON object read in document order (JS object insertion order).
///
/// A repeated key keeps its first position and takes the last value, like
/// `JSON.parse`. JavaScript's rule that integer-like keys sort first is not
/// reproduced: hosts send insertion order and the planners keep it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Ordered<V>(Vec<(String, V)>);

impl<V> Default for Ordered<V> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<V> Ordered<V> {
    pub(crate) fn from_entries(entries: Vec<(String, V)>) -> Self {
        let mut ordered = Self::default();
        for (key, value) in entries {
            ordered.upsert(key, value);
        }
        ordered
    }

    pub(crate) fn get(&self, key: &str) -> Option<&V> {
        self.0
            .iter()
            .find(|(existing, _)| existing == key)
            .map(|(_, value)| value)
    }

    /// Replace the value of an existing key in place, else append (JS assignment).
    pub(crate) fn upsert(&mut self, key: String, value: V) {
        match self.0.iter_mut().find(|(existing, _)| *existing == key) {
            Some((_, slot)) => *slot = value,
            None => self.0.push((key, value)),
        }
    }

    /// Remove a key (JS `delete`).
    pub(crate) fn remove(&mut self, key: &str) {
        self.0.retain(|(existing, _)| existing != key);
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&str, &V)> {
        self.0.iter().map(|(key, value)| (key.as_str(), value))
    }

    pub(crate) fn into_entries(self) -> Vec<(String, V)> {
        self.0
    }
}

impl<'de, V: Deserialize<'de>> Deserialize<'de> for Ordered<V> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct OrderedVisitor<V>(PhantomData<V>);

        impl<'de, V: Deserialize<'de>> Visitor<'de> for OrderedVisitor<V> {
            type Value = Ordered<V>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON object")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Ordered<V>, A::Error> {
                let mut ordered = Ordered::default();
                while let Some((key, value)) = access.next_entry::<String, V>()? {
                    ordered.upsert(key, value);
                }
                Ok(ordered)
            }
        }

        deserializer.deserialize_map(OrderedVisitor(PhantomData))
    }
}

/// ECMAScript `Number::toString(10)` for a finite or non-finite `f64`.
///
/// Uses the shortest round-trip digits (Rust's `{:e}`), then applies the
/// ECMAScript layout rules: plain digits up to 21 integer digits, plain
/// fractions down to `1e-6`, exponent form (`1e+21`, `1.5e-7`) otherwise.
pub(crate) fn js_number_string(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_owned();
    }
    if value == 0.0 {
        return "0".to_owned();
    }
    if value.is_infinite() {
        return if value > 0.0 { "Infinity" } else { "-Infinity" }.to_owned();
    }
    if value < 0.0 {
        return format!("-{}", js_number_string(-value));
    }

    let scientific = format!("{value:e}");
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("LowerExp output always contains an exponent");
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let exponent: i32 = exponent
        .parse()
        .expect("LowerExp output has an integer exponent");
    let k = i32::try_from(digits.len()).expect("f64 has at most 17 significant digits");
    let n = exponent + 1;

    if k <= n && n <= 21 {
        let zeros = usize::try_from(n - k).expect("n >= k");
        format!("{digits}{}", "0".repeat(zeros))
    } else if 0 < n && n <= 21 {
        let split = usize::try_from(n).expect("n > 0");
        format!("{}.{}", &digits[..split], &digits[split..])
    } else if -6 < n && n <= 0 {
        let zeros = usize::try_from(-n).expect("n <= 0");
        format!("0.{}{digits}", "0".repeat(zeros))
    } else {
        let shown = n - 1;
        let sign = if shown >= 0 { '+' } else { '-' };
        let magnitude = shown.unsigned_abs();
        if k == 1 {
            format!("{digits}e{sign}{magnitude}")
        } else {
            format!("{}.{}e{sign}{magnitude}", &digits[..1], &digits[1..])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn js_number_string_matches_ecmascript_layout() {
        let cases: &[(f64, &str)] = &[
            (0.0, "0"),
            (-0.0, "0"),
            (1.0, "1"),
            (2.0, "2"),
            (40.0, "40"),
            (-40.5, "-40.5"),
            (0.1, "0.1"),
            (1.5, "1.5"),
            (100.0, "100"),
            (123.456, "123.456"),
            (0.000_001, "0.000001"),
            (0.000_000_1, "1e-7"),
            (1.5e-7, "1.5e-7"),
            (1e20, "100000000000000000000"),
            (1e21, "1e+21"),
            (1.234_5e21, "1.2345e+21"),
            (123_456_789_012_345_680_000.0, "123456789012345680000"),
            (f64::MAX, "1.7976931348623157e+308"),
            (5e-324, "5e-324"),
            (f64::NAN, "NaN"),
            (f64::INFINITY, "Infinity"),
            (f64::NEG_INFINITY, "-Infinity"),
        ];
        for (value, expected) in cases {
            assert_eq!(js_number_string(*value), *expected, "{value:e}");
        }
    }

    #[test]
    fn scalars_render_like_js_string() {
        let values: Vec<Scalar> =
            serde_json::from_str(r#"["x", true, false, 7, -3, 18446744073709551615, 2.0, 0.25]"#)
                .unwrap();
        let rendered: Vec<String> = values.iter().map(Scalar::to_js_string).collect();
        assert_eq!(
            rendered,
            [
                "x",
                "true",
                "false",
                "7",
                "-3",
                "18446744073709551615",
                "2",
                "0.25"
            ]
        );
        assert_eq!(values[0].as_str(), Some("x"));
        assert_eq!(values[3].as_str(), None);
    }

    #[test]
    fn scalars_reject_null_and_containers() {
        for json in ["null", "[]", "{}"] {
            let error = serde_json::from_str::<Scalar>(json).unwrap_err();
            assert!(
                error.to_string().contains("a string, number or boolean"),
                "{json}: {error}"
            );
        }
    }

    #[test]
    fn ordered_objects_keep_document_order_and_json_parse_duplicates() {
        let ordered: Ordered<Scalar> =
            serde_json::from_str(r#"{"b": 1, "a": "x", "b": 2, "10": true}"#).unwrap();
        let keys: Vec<&str> = ordered.iter().map(|(key, _)| key).collect();
        assert_eq!(keys, ["b", "a", "10"]);
        assert_eq!(ordered.get("b"), Some(&Scalar::UInt(2)));

        let mut edited = ordered;
        edited.upsert("a".to_owned(), Scalar::Bool(false));
        edited.upsert("c".to_owned(), Scalar::Int(-1));
        edited.remove("b");
        let entries = edited.into_entries();
        assert_eq!(
            entries,
            [
                ("a".to_owned(), Scalar::Bool(false)),
                ("10".to_owned(), Scalar::Bool(true)),
                ("c".to_owned(), Scalar::Int(-1)),
            ]
        );
    }

    #[test]
    fn optional_values_read_null_as_undefined() {
        let ordered: Ordered<Option<Scalar>> =
            serde_json::from_str(r#"{"period": null, "x": "y"}"#).unwrap();
        assert_eq!(ordered.get("period"), Some(&None));
        assert_eq!(ordered.get("x"), Some(&Some(Scalar::Str("y".to_owned()))));
        assert!(serde_json::from_str::<Ordered<Scalar>>("[1]").is_err());
    }
}
