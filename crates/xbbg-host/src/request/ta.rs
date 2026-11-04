//! `bta`: technical-analysis `studyRequest` building (TS `buildTaRequest`) and the
//! study vocabulary from `defs/bloomberg.toml` `[technical_analysis]` (the source
//! the TS `TA_STUDIES`/`TA_DEFAULTS` are generated from; a test keeps them equal).

use serde::Deserialize;
use serde::de::{self, Deserializer};

use super::typed::Kwargs;
use super::values::{Ordered, Scalar};
use super::{RequestInput, StringPair};

/// Study alias → Bloomberg study-attributes element (`[technical_analysis.studies]`).
const TA_STUDIES: &[(&str, &str)] = &[
    ("smavg", "smavgStudyAttributes"),
    ("sma", "smavgStudyAttributes"),
    ("emavg", "emavgStudyAttributes"),
    ("ema", "emavgStudyAttributes"),
    ("wmavg", "wmavgStudyAttributes"),
    ("wma", "wmavgStudyAttributes"),
    ("vmavg", "vmavgStudyAttributes"),
    ("vma", "vmavgStudyAttributes"),
    ("tmavg", "tmavgStudyAttributes"),
    ("tma", "tmavgStudyAttributes"),
    ("ipmavg", "ipmavgStudyAttributes"),
    ("rsi", "rsiStudyAttributes"),
    ("macd", "macdStudyAttributes"),
    ("mao", "maoStudyAttributes"),
    ("momentum", "momentumStudyAttributes"),
    ("mom", "momentumStudyAttributes"),
    ("roc", "rocStudyAttributes"),
    ("boll", "bollStudyAttributes"),
    ("bb", "bollStudyAttributes"),
    ("kltn", "kltnStudyAttributes"),
    ("keltner", "kltnStudyAttributes"),
    ("mae", "maeStudyAttributes"),
    ("te", "teStudyAttributes"),
    ("al", "alStudyAttributes"),
    ("dmi", "dmiStudyAttributes"),
    ("adx", "dmiStudyAttributes"),
    ("tas", "tasStudyAttributes"),
    ("stoch", "tasStudyAttributes"),
    ("trender", "trenderStudyAttributes"),
    ("ptps", "ptpsStudyAttributes"),
    ("parabolic", "ptpsStudyAttributes"),
    ("sar", "ptpsStudyAttributes"),
    ("chko", "chkoStudyAttributes"),
    ("ado", "adoStudyAttributes"),
    ("vat", "vatStudyAttributes"),
    ("tvat", "tvatStudyAttributes"),
    ("atr", "atrStudyAttributes"),
    ("hurst", "hurstStudyAttributes"),
    ("fg", "fgStudyAttributes"),
    ("fear_greed", "fgStudyAttributes"),
    ("goc", "gocStudyAttributes"),
    ("ichimoku", "gocStudyAttributes"),
    ("cmci", "cmciStudyAttributes"),
    ("wlpr", "wlprStudyAttributes"),
    ("williams", "wlprStudyAttributes"),
    ("maxmin", "maxminStudyAttributes"),
    ("rex", "rexStudyAttributes"),
    ("etd", "etdStudyAttributes"),
    ("pd", "pdStudyAttributes"),
    ("rv", "rvStudyAttributes"),
    ("pivot", "pivotStudyAttributes"),
    ("or", "orStudyAttributes"),
    ("pcr", "pcrStudyAttributes"),
    ("bs", "bsStudyAttributes"),
];

/// Default study parameters in defs order, values as JS `String(value)` renders
/// them (`[technical_analysis.defaults.<attr>]`).
const TA_DEFAULTS: &[(&str, &[(&str, &str)])] = &[
    (
        "smavgStudyAttributes",
        &[("period", "20"), ("priceSourceClose", "PX_LAST")],
    ),
    (
        "emavgStudyAttributes",
        &[("period", "20"), ("priceSourceClose", "PX_LAST")],
    ),
    (
        "wmavgStudyAttributes",
        &[("period", "20"), ("priceSourceClose", "PX_LAST")],
    ),
    (
        "vmavgStudyAttributes",
        &[("period", "20"), ("priceSourceClose", "PX_LAST")],
    ),
    (
        "tmavgStudyAttributes",
        &[("period", "20"), ("priceSourceClose", "PX_LAST")],
    ),
    (
        "rsiStudyAttributes",
        &[("period", "14"), ("priceSourceClose", "PX_LAST")],
    ),
    (
        "macdStudyAttributes",
        &[
            ("maPeriod1", "12"),
            ("maPeriod2", "26"),
            ("sigPeriod", "9"),
            ("priceSourceClose", "PX_LAST"),
        ],
    ),
    (
        "bollStudyAttributes",
        &[
            ("period", "20"),
            ("upperBand", "2"),
            ("lowerBand", "2"),
            ("priceSourceClose", "PX_LAST"),
        ],
    ),
    (
        "dmiStudyAttributes",
        &[
            ("period", "14"),
            ("priceSourceHigh", "PX_HIGH"),
            ("priceSourceLow", "PX_LOW"),
            ("priceSourceClose", "PX_LAST"),
        ],
    ),
    (
        "atrStudyAttributes",
        &[
            ("maType", "Simple"),
            ("period", "14"),
            ("priceSourceHigh", "PX_HIGH"),
            ("priceSourceLow", "PX_LOW"),
            ("priceSourceClose", "PX_LAST"),
        ],
    ),
    (
        "tasStudyAttributes",
        &[
            ("periodK", "14"),
            ("periodD", "3"),
            ("periodDS", "3"),
            ("periodDSS", "3"),
            ("priceSourceHigh", "PX_HIGH"),
            ("priceSourceLow", "PX_LOW"),
            ("priceSourceClose", "PX_LAST"),
        ],
    ),
];

/// Study parameters: values may be `null`, which is JS `undefined` (a `null`
/// parameter unsets a default of the same name).
type Params = Ordered<Option<Scalar>>;

/// The `study` argument: a study name, or a study object (TS `RawStudy`:
/// `studyType`/`study` name it, `calcInterval` sets the periodicity, `interval`
/// the intraday bar size, `length` aliases `period`, other keys are attributes).
#[derive(Debug)]
pub(super) enum StudyArg {
    Name(String),
    Object(Params),
}

impl<'de> Deserialize<'de> for StudyArg {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct StudyVisitor;

        impl<'de> de::Visitor<'de> for StudyVisitor {
            type Value = StudyArg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a study name or a study object")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<StudyArg, E> {
                Ok(StudyArg::Name(value.to_owned()))
            }

            fn visit_string<E: de::Error>(self, value: String) -> Result<StudyArg, E> {
                Ok(StudyArg::Name(value))
            }

            fn visit_map<A: de::MapAccess<'de>>(self, access: A) -> Result<StudyArg, A::Error> {
                Params::deserialize(de::value::MapAccessDeserializer::new(access))
                    .map(StudyArg::Object)
            }
        }

        deserializer.deserialize_any(StudyVisitor)
    }
}

/// `bta` arguments; `startDate`/`endDate` and the `kwargs` date keys are
/// host-formatted `YYYYMMDD` dates.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct BtaArgs {
    ticker: String,
    study: StudyArg,
    study_params: Option<Params>,
    kwargs: Option<Kwargs>,
    start_date: Option<String>,
    end_date: Option<String>,
    periodicity: Option<String>,
    interval: Option<Scalar>,
    format: Option<String>,
}

/// TS `getStudyAttrName`: lowercase, `-` and space → `_`, alias lookup, else the
/// normalized name itself when it already ends in `studyattributes`, else
/// `<normalized>StudyAttributes`.
fn study_attr_name(study: &str) -> String {
    let normalized = study.to_lowercase().replace(['-', ' '], "_");
    if let Some((_, attr)) = TA_STUDIES.iter().find(|(alias, _)| *alias == normalized) {
        return (*attr).to_owned();
    }
    if normalized.ends_with("studyattributes") {
        return normalized;
    }
    format!("{normalized}StudyAttributes")
}

fn study_defaults(attr: &str) -> Params {
    let entries = TA_DEFAULTS
        .iter()
        .find(|(name, _)| *name == attr)
        .map(|(_, params)| {
            params
                .iter()
                .map(|(key, value)| ((*key).to_owned(), Some(Scalar::Str((*value).to_owned()))))
                .collect()
        })
        .unwrap_or_default();
    Ordered::from_entries(entries)
}

/// `length` aliases `period` within one parameter source: it fills an undefined
/// `period` (in place, or appended), then is removed.
fn alias_length_to_period(params: &mut Params) {
    if let Some(Some(length)) = params.get("length").cloned()
        && !matches!(params.get("period"), Some(Some(_)))
    {
        params.upsert("period".to_owned(), Some(length));
    }
    params.remove("length");
}

fn defined<'a>(params: &'a Params, key: &str) -> Option<&'a Scalar> {
    params.get(key).and_then(Option::as_ref)
}

/// A host-formatted date; blank is JS `formatDate`'s `undefined`.
fn non_blank(value: Option<String>) -> Option<String> {
    value.filter(|text| !text.trim().is_empty())
}

/// `Engine.bta`: `studyRequest` on `//blp/tasvc` with the `generic` extractor and
/// the elements `buildTaRequest` produces (module docs list the B7 decisions).
pub(super) fn bta(args: BtaArgs) -> RequestInput {
    let (mut study, name) = match args.study {
        StudyArg::Name(name) => (
            Ordered::from_entries(vec![(
                "studyType".to_owned(),
                Some(Scalar::Str(name.clone())),
            )]),
            Some(name),
        ),
        StudyArg::Object(params) => (params, None),
    };
    let study_type = defined(&study, "studyType")
        .or_else(|| defined(&study, "study"))
        .map(Scalar::to_js_string)
        .or(name)
        .unwrap_or_default();
    let attr = study_attr_name(&study_type);

    let mut kwargs = args.kwargs.unwrap_or_default();
    let kwarg_text =
        |kwargs: &Kwargs, key: &str| kwargs.get(key).and_then(Scalar::as_str).map(str::to_owned);
    let start_date = non_blank(
        kwarg_text(&kwargs, "startDate")
            .or_else(|| kwarg_text(&kwargs, "start_date"))
            .or(args.start_date),
    );
    let end_date = non_blank(
        kwarg_text(&kwargs, "endDate")
            .or_else(|| kwarg_text(&kwargs, "end_date"))
            .or(args.end_date),
    );
    let periodicity = kwarg_text(&kwargs, "periodicitySelection")
        .or_else(|| kwarg_text(&kwargs, "periodicity"))
        .or_else(|| defined(&study, "calcInterval").map(Scalar::to_js_string))
        .or(args.periodicity)
        .unwrap_or_else(|| "DAILY".to_owned())
        .to_uppercase();
    let interval = kwargs
        .get("interval")
        .cloned()
        .or_else(|| defined(&study, "interval").cloned())
        .or(args.interval);

    for key in [
        "startDate",
        "start_date",
        "endDate",
        "end_date",
        "periodicitySelection",
        "periodicity",
        "interval",
    ] {
        kwargs.remove(key);
    }
    for key in ["studyType", "study", "calcInterval", "interval"] {
        study.remove(key);
    }
    alias_length_to_period(&mut study);

    let mut params = study_defaults(&attr);
    let mut study_params = args.study_params.unwrap_or_default();
    alias_length_to_period(&mut study_params);
    for (key, value) in study_params
        .into_entries()
        .into_iter()
        .chain(study.into_entries())
    {
        params.upsert(key, value);
    }
    alias_length_to_period(&mut params);
    params.remove("calcInterval");

    let mut elements = vec![StringPair::new("priceSource.securityName", args.ticker)];
    if periodicity == "INTRADAY" {
        let prefix = "priceSource.dataRange.intraday";
        if let Some(start) = start_date {
            elements.push(StringPair::new(format!("{prefix}.startDate"), start));
        }
        if let Some(end) = end_date {
            elements.push(StringPair::new(format!("{prefix}.endDate"), end));
        }
        elements.push(StringPair::new(format!("{prefix}.eventType"), "TRADE"));
        if let Some(interval) = interval {
            elements.push(StringPair::new(
                format!("{prefix}.interval"),
                interval.to_js_string(),
            ));
        }
    } else {
        let prefix = "priceSource.dataRange.historical";
        if let Some(start) = start_date {
            elements.push(StringPair::new(format!("{prefix}.startDate"), start));
        }
        if let Some(end) = end_date {
            elements.push(StringPair::new(format!("{prefix}.endDate"), end));
        }
        elements.push(StringPair::new(
            format!("{prefix}.periodicitySelection"),
            periodicity,
        ));
    }
    for (key, value) in params.iter() {
        if let Some(value) = value {
            elements.push(StringPair::new(
                format!("studyAttributes.{attr}.{key}"),
                value.to_js_string(),
            ));
        }
    }
    for (key, value) in kwargs.iter() {
        elements.push(StringPair::new(key, value.to_js_string()));
    }

    RequestInput {
        service: "//blp/tasvc".to_owned(),
        operation: Some("studyRequest".to_owned()),
        extractor: Some("generic".to_owned()),
        elements: Some(elements),
        format: args.format,
        ..RequestInput::default()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;
    use crate::errors::HostErrorKind;
    use crate::request::plan;
    use crate::request::values::js_number_string;

    /// `args` is `json!` (keys sorted) or a raw JSON string where key order matters.
    fn elements(args: impl std::fmt::Display) -> Vec<(String, String)> {
        let args = args.to_string();
        plan("bta", &args)
            .unwrap_or_else(|error| panic!("{args}: {error:?}"))
            .elements
            .unwrap()
            .into_iter()
            .map(|pair| (pair.key, pair.value))
            .collect()
    }

    fn pairs(entries: &[(&str, &str)]) -> Vec<(String, String)> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn merges_shared_study_defaults_with_caller_parameters() {
        let planned = plan(
            "bta",
            &json!({"ticker": "SYNTHETIC Equity", "study": "rsi", "studyParams": {"period": 7}})
                .to_string(),
        )
        .unwrap();
        let expected = json!({
            "service": "//blp/tasvc",
            "operation": "studyRequest",
            "extractor": "generic",
            "elements": [
                {"key": "priceSource.securityName", "value": "SYNTHETIC Equity"},
                {"key": "priceSource.dataRange.historical.periodicitySelection", "value": "DAILY"},
                {"key": "studyAttributes.rsiStudyAttributes.period", "value": "7"},
                {"key": "studyAttributes.rsiStudyAttributes.priceSourceClose", "value": "PX_LAST"}
            ]
        });
        assert_eq!(
            serde_json::from_str::<Value>(&planned.to_json()).unwrap(),
            expected
        );
    }

    #[test]
    fn preserves_normalization_and_custom_study_fallback() {
        for (study, attr) in [
            ("SMA", "smavgStudyAttributes"),
            ("Fear Greed", "fgStudyAttributes"),
            ("FEAR-GREED", "fgStudyAttributes"),
            ("custom", "customStudyAttributes"),
            ("customStudyAttributes", "customstudyattributes"),
        ] {
            let planned = elements(
                json!({"ticker": "SYNTHETIC Equity", "study": study, "studyParams": {"period": 7}}),
            );
            assert!(
                planned.contains(&(format!("studyAttributes.{attr}.period"), "7".to_owned())),
                "{study}: {planned:?}"
            );
        }
    }

    #[test]
    fn intraday_periodicity_builds_the_intraday_data_range() {
        assert_eq!(
            elements(json!({
                "ticker": "IBM US Equity",
                "study": {"studyType": "sma", "calcInterval": "intraday", "interval": 5, "length": 10},
                "startDate": "20240102",
                "endDate": "20240103"
            })),
            pairs(&[
                ("priceSource.securityName", "IBM US Equity"),
                ("priceSource.dataRange.intraday.startDate", "20240102"),
                ("priceSource.dataRange.intraday.endDate", "20240103"),
                ("priceSource.dataRange.intraday.eventType", "TRADE"),
                ("priceSource.dataRange.intraday.interval", "5"),
                ("studyAttributes.smavgStudyAttributes.period", "10"),
                (
                    "studyAttributes.smavgStudyAttributes.priceSourceClose",
                    "PX_LAST"
                ),
            ])
        );
    }

    #[test]
    fn kwargs_beat_options_and_leftovers_become_raw_elements() {
        assert_eq!(
            elements(
                r#"{
                    "ticker": "T",
                    "study": "macd",
                    "kwargs": {
                        "start_date": "20230101",
                        "endDate": "20230201",
                        "periodicity": "weekly",
                        "nonTradingDayFillOption": "ALL_CALENDAR_DAYS",
                        "adjustmentSplit": true
                    },
                    "startDate": "20990101",
                    "endDate": "20990201",
                    "periodicity": "MONTHLY"
                }"#
            ),
            pairs(&[
                ("priceSource.securityName", "T"),
                ("priceSource.dataRange.historical.startDate", "20230101"),
                ("priceSource.dataRange.historical.endDate", "20230201"),
                (
                    "priceSource.dataRange.historical.periodicitySelection",
                    "WEEKLY"
                ),
                ("studyAttributes.macdStudyAttributes.maPeriod1", "12"),
                ("studyAttributes.macdStudyAttributes.maPeriod2", "26"),
                ("studyAttributes.macdStudyAttributes.sigPeriod", "9"),
                (
                    "studyAttributes.macdStudyAttributes.priceSourceClose",
                    "PX_LAST"
                ),
                ("nonTradingDayFillOption", "ALL_CALENDAR_DAYS"),
                ("adjustmentSplit", "true"),
            ])
        );
    }

    #[test]
    fn periodicity_precedence_and_number_kwargs_follow_string_or_undefined() {
        // kwargs.periodicitySelection > kwargs.periodicity > study.calcInterval > option.
        let study = json!({"studyType": "rsi", "calcInterval": "monthly"});
        let historical = |args: Value| {
            elements(args)
                .into_iter()
                .find(|(key, _)| key.ends_with("periodicitySelection"))
                .map(|(_, value)| value)
                .unwrap()
        };
        assert_eq!(
            historical(
                json!({"ticker": "T", "study": study, "periodicity": "weekly",
                "kwargs": {"periodicitySelection": "quarterly", "periodicity": "yearly"}})
            ),
            "QUARTERLY"
        );
        assert_eq!(
            historical(
                json!({"ticker": "T", "study": study, "periodicity": "weekly",
                "kwargs": {"periodicity": "yearly"}})
            ),
            "YEARLY"
        );
        assert_eq!(
            historical(json!({"ticker": "T", "study": study, "periodicity": "weekly"})),
            "MONTHLY"
        );
        assert_eq!(
            historical(json!({"ticker": "T", "study": "rsi", "periodicity": "weekly"})),
            "WEEKLY"
        );
        // A non-string kwarg date is not a date (TS stringOrUndef) but is still consumed.
        assert_eq!(
            elements(
                json!({"ticker": "T", "study": "custom", "kwargs": {"startDate": 20240101},
                "startDate": "20230101"})
            ),
            pairs(&[
                ("priceSource.securityName", "T"),
                ("priceSource.dataRange.historical.startDate", "20230101"),
                (
                    "priceSource.dataRange.historical.periodicitySelection",
                    "DAILY"
                ),
            ])
        );
        // A blank kwarg date wins and formats to nothing, like formatDate('  ').
        assert_eq!(
            elements(
                json!({"ticker": "T", "study": "custom", "kwargs": {"endDate": "  "},
                "endDate": "20230101"})
            ),
            pairs(&[
                ("priceSource.securityName", "T"),
                (
                    "priceSource.dataRange.historical.periodicitySelection",
                    "DAILY"
                ),
            ])
        );
    }

    #[test]
    fn interval_is_consumed_by_the_intraday_range_only() {
        // B7 decision: kwargs/study `interval` never leaks into raw elements or attributes.
        assert_eq!(
            elements(
                json!({"ticker": "T", "study": {"study": "rsi", "interval": "15"},
                "kwargs": {"interval": 30, "periodicity": "intraday"}, "interval": 60})
            ),
            pairs(&[
                ("priceSource.securityName", "T"),
                ("priceSource.dataRange.intraday.eventType", "TRADE"),
                ("priceSource.dataRange.intraday.interval", "30"),
                ("studyAttributes.rsiStudyAttributes.period", "14"),
                (
                    "studyAttributes.rsiStudyAttributes.priceSourceClose",
                    "PX_LAST"
                ),
            ])
        );
        assert!(
            !elements(json!({"ticker": "T", "study": {"studyType": "rsi", "interval": 15}}))
                .iter()
                .any(|(key, _)| key.contains("interval")),
        );
        let options_interval = elements(
            json!({"ticker": "T", "study": "rsi", "periodicity": "INTRADAY", "interval": 60}),
        );
        assert!(options_interval.contains(&(
            "priceSource.dataRange.intraday.interval".to_owned(),
            "60".to_owned()
        )));
    }

    #[test]
    fn study_params_length_aliases_period_and_study_object_wins() {
        // B7 decision: studyParams.length overrides the default period.
        let attribute = |args: Value, name: &str| {
            elements(args)
                .into_iter()
                .find(|(key, _)| key == &format!("studyAttributes.bollStudyAttributes.{name}"))
                .map(|(_, value)| value)
        };
        assert_eq!(
            attribute(
                json!({"ticker": "T", "study": "boll", "studyParams": {"length": 30}}),
                "period"
            ),
            Some("30".to_owned())
        );
        assert_eq!(
            attribute(
                json!({"ticker": "T", "study": "boll", "studyParams": {"length": 30, "period": 25}}),
                "period"
            ),
            Some("25".to_owned())
        );
        assert_eq!(
            attribute(
                json!({"ticker": "T", "study": {"study": "boll", "period": 40}, "studyParams": {"period": 25}}),
                "period"
            ),
            Some("40".to_owned())
        );
        assert_eq!(
            attribute(
                json!({"ticker": "T", "study": {"study": "boll", "length": 50}, "studyParams": {"period": 25}}),
                "period"
            ),
            Some("50".to_owned())
        );
        // Defaults render JS numbers ("2", not "2.0"); a null parameter unsets a default.
        assert_eq!(
            attribute(json!({"ticker": "T", "study": "boll"}), "upperBand"),
            Some("2".to_owned())
        );
        assert_eq!(
            attribute(
                json!({"ticker": "T", "study": "boll", "studyParams": {"lowerBand": null}}),
                "lowerBand"
            ),
            None
        );
        assert_eq!(
            attribute(
                json!({"ticker": "T", "study": {"study": "boll", "upperBand": 2.5}}),
                "upperBand"
            ),
            Some("2.5".to_owned())
        );
    }

    #[test]
    fn parameter_order_follows_defaults_then_params_then_study() {
        assert_eq!(
            elements(
                r#"{
                    "ticker": "T",
                    "study": {"studyType": "dmi", "zeta": "z", "priceSourceLow": "PX_OPEN", "length": 9, "beta": 2},
                    "studyParams": {"priceSourceHigh": "PX_BID", "alpha": 1}
                }"#
            ),
            pairs(&[
                ("priceSource.securityName", "T"),
                (
                    "priceSource.dataRange.historical.periodicitySelection",
                    "DAILY"
                ),
                ("studyAttributes.dmiStudyAttributes.period", "9"),
                (
                    "studyAttributes.dmiStudyAttributes.priceSourceHigh",
                    "PX_BID"
                ),
                (
                    "studyAttributes.dmiStudyAttributes.priceSourceLow",
                    "PX_OPEN"
                ),
                (
                    "studyAttributes.dmiStudyAttributes.priceSourceClose",
                    "PX_LAST"
                ),
                ("studyAttributes.dmiStudyAttributes.alpha", "1"),
                ("studyAttributes.dmiStudyAttributes.zeta", "z"),
                ("studyAttributes.dmiStudyAttributes.beta", "2"),
            ])
        );
    }

    #[test]
    fn study_objects_without_a_name_use_the_empty_study() {
        assert_eq!(
            elements(json!({"ticker": "T", "study": {"period": 3}})),
            pairs(&[
                ("priceSource.securityName", "T"),
                (
                    "priceSource.dataRange.historical.periodicitySelection",
                    "DAILY"
                ),
                ("studyAttributes.StudyAttributes.period", "3"),
            ])
        );
        assert_eq!(
            elements(json!({"ticker": "T", "study": {"study": "WMA", "studyType": null}}))[2],
            (
                "studyAttributes.wmavgStudyAttributes.period".to_owned(),
                "20".to_owned()
            )
        );
    }

    #[test]
    fn bta_forwards_format_and_rejects_bad_studies() {
        let planned = plan(
            "bta",
            &json!({"ticker": "T", "study": "rsi", "format": "long"}).to_string(),
        )
        .unwrap();
        assert_eq!(planned.format.as_deref(), Some("long"));
        for study in [json!(3), json!(["rsi"]), json!(null)] {
            let error =
                plan("bta", &json!({"ticker": "T", "study": study}).to_string()).unwrap_err();
            assert_eq!(error.kind, HostErrorKind::Argument, "{study}");
        }
    }

    type Entries = Vec<(String, String)>;

    /// Parse `[technical_analysis.*]` from the defs source the TS constants are
    /// generated from: `key = "text"` or `key = <number>` lines, `#` comments.
    fn defs_technical_analysis() -> (Entries, Vec<(String, Entries)>) {
        let defs = include_str!("../../../../defs/bloomberg.toml");
        let mut studies = Vec::new();
        let mut defaults: Vec<(String, Entries)> = Vec::new();
        let mut section = String::new();
        for line in defs.lines() {
            let line = line.split('#').next().unwrap().trim();
            if line.is_empty() {
                continue;
            }
            if let Some(header) = line
                .strip_prefix('[')
                .and_then(|rest| rest.strip_suffix(']'))
            {
                section = header.to_owned();
                if let Some(attr) = section.strip_prefix("technical_analysis.defaults.") {
                    defaults.push((attr.to_owned(), Vec::new()));
                }
                continue;
            }
            let Some((key, value)) = line.split_once('=') else {
                continue;
            };
            let is_studies = section == "technical_analysis.studies";
            if !is_studies && !section.starts_with("technical_analysis.defaults.") {
                continue;
            }
            let key = key.trim().to_owned();
            let value = value.trim();
            let value = match value
                .strip_prefix('"')
                .and_then(|rest| rest.strip_suffix('"'))
            {
                Some(text) => text.to_owned(),
                None => js_number_string(value.parse::<f64>().expect("numeric default")),
            };
            if is_studies {
                studies.push((key, value));
            } else {
                defaults.last_mut().unwrap().1.push((key, value));
            }
        }
        (studies, defaults)
    }

    #[test]
    fn embedded_vocabulary_matches_defs() {
        let (studies, defaults) = defs_technical_analysis();
        let embedded_studies: Vec<(String, String)> = TA_STUDIES
            .iter()
            .map(|(alias, attr)| ((*alias).to_owned(), (*attr).to_owned()))
            .collect();
        assert_eq!(embedded_studies, studies);
        assert_eq!(studies.len(), 54);

        let embedded_defaults: Vec<(String, Vec<(String, String)>)> = TA_DEFAULTS
            .iter()
            .map(|(attr, params)| {
                (
                    (*attr).to_owned(),
                    params
                        .iter()
                        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                        .collect(),
                )
            })
            .collect();
        assert_eq!(embedded_defaults, defaults);
        assert_eq!(defaults.len(), 11);
    }
}
