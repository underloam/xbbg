use serde_json::{Value, json};

use super::*;
use crate::errors::HostErrorKind;

fn parse(method: &str, args: Value) -> Result<RecipeCall, HostError> {
    RecipeCall::parse(method, &args.to_string())
}

fn parsed(method: &str, args: Value) -> RecipeCall {
    parse(method, args).unwrap_or_else(|error| panic!("{method}: {error}"))
}

fn rejected(method: &str, args: Value) -> HostError {
    match parse(method, args) {
        Ok(call) => panic!("{method} unexpectedly parsed: {call:?}"),
        Err(error) => error,
    }
}

fn described(method: &str, args: Value) -> Value {
    let call = parsed(method, args);
    serde_json::from_str(&call.to_json().expect("serializes")).expect("valid JSON")
}

fn minimal_args(method: &str) -> Value {
    match method {
        "bqr" | "corporateBonds" => json!({"ticker": "IBM US Equity"}),
        "yas" => json!({"tickers": ["T 4 1/4 11/15/34 Govt"], "fields": ["YAS_BOND_YLD"]}),
        "preferreds" => json!({"equityTicker": "BAC US Equity"}),
        "futTicker" | "activeFutures" | "futuresCurve" | "cdxTicker" | "activeCdx" => {
            json!({"genTicker": "ES1 Index"})
        }
        "dividend"
        | "dividendYield"
        | "turnover"
        | "volSurface"
        | "delayStatus"
        | "etfNavRelationships"
        | "etfNavSnapshot"
        | "etfNavHistory" => {
            json!({"tickers": ["SPY US Equity"]})
        }
        "etfHoldings" => json!({"etfTicker": "SPY US Equity"}),
        "indexMembers" => json!({"index": "INDU Index"}),
        "resolveIsins" => json!({"isins": ["US0378331005"]}),
        "resolveVenues" | "auctionSnapshot" => json!({"securities": ["VOD LN Equity"]}),
        "issuerIsins" => json!({"bondIsins": ["US037833AK68"]}),
        "currencyConversion" => json!({"ticker": "VOD LN Equity", "targetCcy": "USD"}),
        other => panic!("no minimal args for {other}"),
    }
}

#[test]
fn every_method_parses_minimal_args_and_round_trips_its_name() {
    assert_eq!(METHODS.len(), 24);
    for method in METHODS {
        let call = parsed(method, minimal_args(method));
        assert_eq!(call.method(), *method);
        let json = described(method, minimal_args(method));
        assert_eq!(json["method"], *method, "serialized tag of {method}");
    }
}

#[test]
fn unknown_method_is_internal() {
    let error = rejected("bdp", json!({}));
    assert_eq!(error.kind, HostErrorKind::Internal);
    assert_eq!(error.message, "unknown engine method 'bdp'");
}

#[test]
fn unknown_fields_and_missing_required_fields_are_argument_errors() {
    let unknown = rejected("bqr", json!({"ticker": "X", "backend": "json"}));
    assert_eq!(unknown.kind, HostErrorKind::Argument);
    assert_eq!(unknown.message, "unknown field 'backend'");

    let missing = rejected("currencyConversion", json!({"ticker": "X"}));
    assert_eq!(missing.kind, HostErrorKind::Argument);
    assert_eq!(missing.message, "missing field 'targetCcy'");

    let wrong_type = rejected("yas", json!({"tickers": "X", "fields": ["F"]}));
    assert_eq!(wrong_type.kind, HostErrorKind::Argument);
    assert_eq!(
        wrong_type.message,
        "invalid type: string \"X\", expected a sequence"
    );

    let fractional = rejected(
        "futuresCurve",
        json!({"genTicker": "ES1 Index", "maxContracts": 1.5}),
    );
    assert_eq!(fractional.kind, HostErrorKind::Argument);
}

#[test]
fn bqr_defaults_broker_codes_and_leaves_datetimes_to_the_recipe() {
    let RecipeCall::Bqr(args) = parsed("bqr", json!({"ticker": "XS1 Corp"})) else {
        panic!("not bqr");
    };
    assert!(args.include_broker_codes);
    assert_eq!(args.start_datetime, None);
    assert_eq!(args.end_datetime, None);
    assert_eq!(args.event_types, None);

    let RecipeCall::Bqr(args) = parsed(
        "bqr",
        json!({"ticker": "XS1 Corp", "includeBrokerCodes": null, "startDatetime": "2024-01-15 09:00"}),
    ) else {
        panic!("not bqr");
    };
    assert!(
        args.include_broker_codes,
        "null keeps the JavaScript default"
    );
    assert_eq!(args.start_datetime.as_deref(), Some("2024-01-15 09:00"));

    let RecipeCall::Bqr(args) = parsed(
        "bqr",
        json!({"ticker": "XS1 Corp", "includeBrokerCodes": false, "eventTypes": ["TRADE"]}),
    ) else {
        panic!("not bqr");
    };
    assert!(!args.include_broker_codes);
    assert_eq!(args.event_types, Some(vec!["TRADE".to_string()]));
}

#[test]
fn yas_accepts_every_yield_type_code_and_rejects_the_rest_untruncated() {
    for code in 1..=9 {
        let call = parsed(
            "yas",
            json!({"tickers": ["X Govt"], "fields": ["YAS_BOND_YLD"], "yieldType": code}),
        );
        let RecipeCall::Yas(args) = call else {
            panic!("not yas");
        };
        assert_eq!(args.yield_type, Some(code));
        assert_eq!(yield_type(code).expect("valid") as u8, code as u8);
    }
    for code in [0_i64, 10, 257, -1, 300] {
        let error = rejected(
            "yas",
            json!({"tickers": ["X Govt"], "fields": ["YAS_BOND_YLD"], "yieldType": code}),
        );
        assert_eq!(error.kind, HostErrorKind::Validation, "code {code}");
        assert_eq!(
            error.message,
            format!(
                "unknown yield type '{code}': expected YTM, YTC, YTR, YTP, YTW, YTWR, EYTW, EYTWR, or YTAL"
            )
        );
        assert_eq!(error.context.element.as_deref(), Some("yieldType"));
    }
}

#[test]
fn missing_or_null_required_dates_become_empty_like_javascript() {
    let RecipeCall::FutTicker(args) = parsed("futTicker", json!({"genTicker": "ES1 Index"})) else {
        panic!("not futTicker");
    };
    assert_eq!(args.dt, "");
    assert_eq!(args.freq, None);

    let RecipeCall::Dividend(args) = parsed(
        "dividend",
        json!({"tickers": ["IBM US Equity"], "startDate": null, "endDate": "20241231", "dvdType": "all"}),
    ) else {
        panic!("not dividend");
    };
    assert_eq!(args.start_date, "");
    assert_eq!(args.end_date, "20241231");
    assert_eq!(args.dvd_type.as_deref(), Some("all"));

    let RecipeCall::CurrencyConversion(args) = parsed(
        "currencyConversion",
        json!({"ticker": "VOD LN Equity", "targetCcy": "USD", "startDate": "20240101"}),
    ) else {
        panic!("not currencyConversion");
    };
    assert_eq!(args.start_date, "20240101");
    assert_eq!(args.end_date, "");
}

#[test]
fn boolean_options_take_their_javascript_defaults() {
    let json = described("cdxTicker", json!({"genTicker": "CDX IG CDSI GEN 5Y Corp"}));
    assert_eq!(json["versionless"], false);
    let json = described(
        "activeCdx",
        json!({"genTicker": "CDX IG CDSI GEN 5Y Corp", "dt": "20260325", "lookbackDays": 10, "versionless": true}),
    );
    assert_eq!(json["versionless"], true);
    assert_eq!(json["lookbackDays"], 10);
    assert_eq!(json["dt"], "20260325");
    let json = described("delayStatus", json!({"tickers": ["IBM US Equity"]}));
    assert_eq!(json["checkIdentity"], false);
    let json = described(
        "delayStatus",
        json!({"tickers": ["IBM US Equity"], "checkIdentity": true}),
    );
    assert_eq!(json["checkIdentity"], true);
}

#[test]
fn vol_surface_preset_absent_null_single_and_list() {
    let absent = described("volSurface", json!({"tickers": ["SPX Index"]}));
    assert_eq!(absent["presets"], json!(["MONEYNESS_30D"]));
    assert_eq!(absent["fieldSpecs"], Value::Null);
    assert_eq!(absent["asDecimal"], true);
    assert_eq!(absent["includeDerived"], false);
    assert_eq!(absent["startDate"], "");

    let null = described(
        "volSurface",
        json!({"tickers": ["SPX Index"], "preset": null}),
    );
    assert_eq!(null["presets"], Value::Null, "null selects no preset");

    let single = described(
        "volSurface",
        json!({"tickers": ["SPX Index"], "preset": "DELTA_1M_2M"}),
    );
    assert_eq!(single["presets"], json!(["DELTA_1M_2M"]));

    let list = described(
        "volSurface",
        json!({"tickers": ["SPX Index"], "preset": ["MONEYNESS_3M", "MONEYNESS_6M"]}),
    );
    assert_eq!(list["presets"], json!(["MONEYNESS_3M", "MONEYNESS_6M"]));

    let empty = described(
        "volSurface",
        json!({"tickers": ["SPX Index"], "preset": []}),
    );
    assert_eq!(empty["presets"], json!([]));

    let error = rejected("volSurface", json!({"tickers": ["SPX Index"], "preset": 5}));
    assert_eq!(error.kind, HostErrorKind::Argument);
    assert!(
        error
            .message
            .contains("a preset name, an array of preset names, or null"),
        "{}",
        error.message
    );
}

#[test]
fn vol_surface_field_specs_keep_lists_and_encode_maps_in_order() {
    let list = described(
        "volSurface",
        json!({"tickers": ["SPX Index"], "fields": ["3MTH_IMPVOL_100.0%MNY_DF", "PX_LAST"]}),
    );
    assert_eq!(
        list["fieldSpecs"],
        json!(["3MTH_IMPVOL_100.0%MNY_DF", "PX_LAST"])
    );

    // Raw text: `json!` objects sort their keys, which would hide the ordering.
    let call = RecipeCall::parse(
        "volSurface",
        r#"{"tickers":["SPX Index"],"preset":[],"fields":{
            "ZETA_FIELD":{"metric":"implied_volatility","tenor":"1M","pointType":"delta_put","point":-25},
            "ALPHA_FIELD":null,
            "MID_FIELD":{"tenor":"3M"},
            "POINT_FIELD":{"point":97.5}}}"#,
    )
    .expect("parses");
    let map: Value = serde_json::from_str(&call.to_json().expect("serializes")).expect("JSON");
    assert_eq!(
        map["fieldSpecs"],
        json!([
            "ZETA_FIELD|implied_volatility|1M|delta_put|-25",
            "ALPHA_FIELD",
            "MID_FIELD||3M||",
            "POINT_FIELD||||97.5",
        ]),
        "object order is kept, missing parts are empty, null spec is the bare field"
    );

    let null = described(
        "volSurface",
        json!({"tickers": ["SPX Index"], "fields": null}),
    );
    assert_eq!(null["fieldSpecs"], Value::Null);

    let unknown = rejected(
        "volSurface",
        json!({"tickers": ["SPX Index"], "fields": {"F": {"pointtype": "delta"}}}),
    );
    assert_eq!(unknown.kind, HostErrorKind::Argument);
    assert_eq!(unknown.message, "unknown field 'pointtype'");
}

#[test]
fn encode_field_spec_matches_the_javascript_encoding() {
    assert_eq!(vol::encode_field_spec("F", None), "F");
    assert_eq!(
        vol::encode_field_spec("F", Some(&vol::VolFieldSpec::default())),
        "F||||"
    );
    let spec = vol::VolFieldSpec {
        metric: Some("rate".to_string()),
        tenor: None,
        point_type: Some("moneyness".to_string()),
        point: Some(100.0),
    };
    assert_eq!(
        vol::encode_field_spec("F", Some(&spec)),
        "F|rate||moneyness|100"
    );
}

#[test]
fn etf_nav_tickers_are_trimmed() {
    let RecipeCall::EtfNavSnapshot(args) = parsed(
        "etfNavSnapshot",
        json!({"tickers": ["  SPY US Equity ", "QQQ US Equity"]}),
    ) else {
        panic!("not etfNavSnapshot");
    };
    assert_eq!(args.tickers, ["SPY US Equity", "QQQ US Equity"]);

    let RecipeCall::EtfNavHistory(args) = parsed(
        "etfNavHistory",
        json!({"tickers": ["\tSPY US Equity"], "startDate": "20240101", "endDate": "20240131"}),
    ) else {
        panic!("not etfNavHistory");
    };
    assert_eq!(args.tickers, ["SPY US Equity"]);
    assert_eq!(args.start_date, "20240101");

    let RecipeCall::DelayStatus(args) =
        parsed("delayStatus", json!({"tickers": [" IBM US Equity "]}))
    else {
        panic!("not delayStatus");
    };
    assert_eq!(
        args.tickers,
        [" IBM US Equity "],
        "only ETF NAV inputs trim"
    );
}

#[test]
fn auction_recipes_default_fields_and_pcs_overrides() {
    let RecipeCall::AuctionSnapshot(args) = parsed(
        "auctionSnapshot",
        json!({"securities": ["VOD LN Equity"], "fields": null}),
    ) else {
        panic!("not auctionSnapshot");
    };
    assert!(args.fields.is_empty(), "empty selects the default group");
    assert!(args.pcs_overrides.is_empty());

    let RecipeCall::ResolveVenues(args) = parsed(
        "resolveVenues",
        json!({"securities": ["ABC LN Equity"], "pcsOverrides": {"London": "LSE1"}}),
    ) else {
        panic!("not resolveVenues");
    };
    assert_eq!(
        args.pcs_overrides.get("London").map(String::as_str),
        Some("LSE1")
    );
    assert_eq!(
        hash_map(args.pcs_overrides)
            .get("London")
            .map(String::as_str),
        Some("LSE1")
    );
}

#[test]
fn optional_lists_and_numbers_pass_through_unchanged() {
    let json = described(
        "futuresCurve",
        json!({"genTicker": "CL1 Comdty", "asof": "20240115", "chainField": "FUT_CHAIN", "fields": ["OPEN_INT"], "maxContracts": 6}),
    );
    assert_eq!(json["asof"], "20240115");
    assert_eq!(json["chainField"], "FUT_CHAIN");
    assert_eq!(json["fields"], json!(["OPEN_INT"]));
    assert_eq!(json["maxContracts"], 6);

    let json = described(
        "turnover",
        json!({"tickers": ["VOD LN Equity"], "ccy": "USD", "factor": 1e6}),
    );
    assert_eq!(json["ccy"], "USD");
    assert_eq!(json["factor"], 1e6);
    assert_eq!(json["startDate"], "");

    let json = described(
        "dividendYield",
        json!({"tickers": ["IBM US Equity"], "startDate": "20240101", "endDate": "20241231", "dividendTypes": ["Regular Cash"], "windowDays": 90}),
    );
    assert_eq!(json["dividendTypes"], json!(["Regular Cash"]));
    assert_eq!(json["windowDays"], 90);

    let json = described(
        "indexMembers",
        json!({"index": "INDU Index", "field": "INDX_MEMBERS", "asof": "20240101"}),
    );
    assert_eq!(json["field"], "INDX_MEMBERS");
    assert_eq!(json["asof"], "20240101");
}
