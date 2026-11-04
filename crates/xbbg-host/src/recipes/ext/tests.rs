use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use serde_json::{Value, json};

use super::*;
use crate::errors::HostErrorKind;

fn call_json(function: &str, args: Value) -> Value {
    match call(function, &args.to_string()) {
        Ok(ExtValue::Json(text)) => serde_json::from_str(&text).expect("valid JSON"),
        other => panic!("{function}: expected JSON, got {other:?}"),
    }
}

fn call_str(function: &str, args: Value) -> String {
    match call(function, &args.to_string()) {
        Ok(ExtValue::Str(text)) => text,
        other => panic!("{function}: expected a string, got {other:?}"),
    }
}

fn call_bool(function: &str, args: Value) -> bool {
    match call(function, &args.to_string()) {
        Ok(ExtValue::Bool(value)) => value,
        other => panic!("{function}: expected a bool, got {other:?}"),
    }
}

fn call_err(function: &str, args: Value) -> HostError {
    match call(function, &args.to_string()) {
        Ok(value) => panic!("{function}: unexpectedly returned {value:?}"),
        Err(error) => error,
    }
}

fn assert_argument(error: &HostError, message: &str) {
    assert_eq!(error.kind, HostErrorKind::Argument, "{}", error.message);
    assert_eq!(error.message, message);
}

/// Sample arguments that succeed, for every function in [`FUNCTIONS`].
fn sample_args(function: &str) -> Value {
    match function {
        "parseDate" => json!({"dateStr": "2024-01-15"}),
        "fmtDate" => json!({"year": 2024, "month": 1, "day": 15}),
        "parseTicker" | "isSpecificContract" | "validateGenericTicker" => {
            json!({"ticker": "ES1 Index"})
        }
        "buildFuturesTicker" => {
            json!({"prefix": "ES", "monthCode": "H", "year": "24", "asset": "Index"})
        }
        "normalizeTickers" | "filterEquityTickers" => json!({"tickers": ["IBM US Equity"]}),
        "generateFuturesCandidates" => {
            json!({"genTicker": "ES1 Index", "year": 2024, "month": 1, "day": 15})
        }
        "contractIndex" => json!({"genTicker": "ES2 Index"}),
        "filterCandidatesByCycle" => json!({"candidates": [], "cycle": "HMUZ"}),
        "filterValidContracts" => json!({"contracts": [], "year": 2024, "month": 1, "day": 15}),
        "parseCdxTicker" | "previousCdxSeries" => json!({"ticker": "CDX IG CDSI S45 5Y Corp"}),
        "cdxGenToSpecific" => json!({"genTicker": "CDX IG CDSI GEN 5Y Corp", "series": 45}),
        "auctionFieldGroup" => json!({"name": "quotes"}),
        "imbalanceSide" => json!({"code": "BUY"}),
        "buildFxPair" => json!({"fromCcy": "EUR", "toCcy": "USD"}),
        "sameCurrency" => json!({"ccy1": "USD", "ccy2": "usd"}),
        "currenciesNeedingConversion" => json!({"currencies": ["EUR"], "target": "USD"}),
        "renameDividendColumns" | "renameEtfColumns" => json!({"columns": ["Ex-Date"]}),
        "getMonthCode" => json!({"monthName": "Mar"}),
        "getMonthName" => json!({"code": "H"}),
        "getDvdType" => json!({"typ": "all"}),
        "buildYasOverrides" => json!({"yieldType": 1}),
        "buildEarningHeaderRename" => json!({"headerRow": [], "dataColumns": []}),
        "calculateLevelPercentages" => json!({"values": [], "levels": []}),
        "buildPreferredsQuery" => json!({"equityTicker": "BAC"}),
        "buildCorporateBondsQuery" => json!({"ticker": "AAPL"}),
        "buildEtfHoldingsQuery" => json!({"etfTicker": "SPY"}),
        "defaultTurnoverDates" | "defaultBqrDatetimes" | "getMarketRule" => json!({}),
        "deriveSessions" => json!({"dayStart": "09:30", "dayEnd": "16:00"}),
        "inferTimezone" => json!({"countryIso": "US"}),
        "getExchangeOverride" => json!({"ticker": "NO_OVERRIDE_SAMPLE Equity"}),
        "clearExchangeOverride" => json!({"ticker": "NO_OVERRIDE_SAMPLE Equity"}),
        "sessionTimesToUtc" => json!({
            "startTime": "09:30", "endTime": "16:00", "exchangeTz": "UTC", "date": "2024-01-15"
        }),
        // Mutating or zero-argument functions are exercised separately.
        "setExchangeOverride"
        | "listExchangeOverrides"
        | "auctionZeroPriceFields"
        | "getFuturesMonths"
        | "getDvdTypes"
        | "getDvdCols"
        | "getEtfCols" => Value::Null,
        other => panic!("no sample for {other}"),
    }
}

#[test]
fn registry_lists_all_48_napi_functions_once() {
    let mut names = HashSet::new();
    for (name, _) in FUNCTIONS {
        assert!(names.insert(*name), "duplicate {name}");
    }
    for name in BYTE_FUNCTIONS {
        assert!(names.insert(*name), "duplicate {name}");
    }
    assert_eq!(names.len(), 48);
    assert_eq!(kind_of("fmtDate"), Some(ExtKind::Str));
    assert_eq!(
        kind_of("pivotToWide"),
        None,
        "byte functions go through call_bytes"
    );
    assert_eq!(kind_of("nope"), None);
}

#[test]
fn every_function_returns_its_declared_kind() {
    for (name, kind) in FUNCTIONS {
        let args = sample_args(name);
        let args = if args.is_null() { json!({}) } else { args };
        if *name == "setExchangeOverride" {
            continue;
        }
        let value = call(name, &args.to_string()).unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(value.kind(), *kind, "{name}");
    }
}

#[test]
fn unknown_functions_and_fields_are_rejected() {
    let error = call_err("nope", json!({}));
    assert_eq!(error.kind, HostErrorKind::Internal);
    assert_eq!(error.message, "unknown ext method 'nope'");

    let error = call_bytes("nope", &[]).expect_err("unknown");
    assert_eq!(error.message, "unknown ext method 'nope'");

    assert_argument(
        &call_err("parseDate", json!({"dateStr": "2024-01-15", "fmt": "x"})),
        "unknown field 'fmt'",
    );
    assert_argument(&call_err("parseDate", json!({})), "missing field 'dateStr'");
    assert_argument(
        &call_err("getDvdTypes", json!({"typ": "all"})),
        "unknown field 'typ'",
    );
}

#[test]
fn parse_date_and_fmt_date() {
    for input in [
        "2024-01-15",
        "20240115",
        "2024/01/15",
        "15-01-2024",
        "15/01/2024",
    ] {
        assert_eq!(
            call_json("parseDate", json!({"dateStr": input})),
            json!([2024, 1, 15]),
            "{input}"
        );
    }
    assert_argument(
        &call_err("parseDate", json!({"dateStr": "Jan 15"})),
        "failed to parse date 'Jan 15': expected format YYYY-MM-DD, YYYYMMDD, or YYYY/MM/DD",
    );

    assert_eq!(
        call_str("fmtDate", json!({"year": 2024, "month": 1, "day": 5})),
        "20240105"
    );
    assert_eq!(
        call_str(
            "fmtDate",
            json!({"year": 2024, "month": 1, "day": 5, "fmt": "%d/%m/%Y"})
        ),
        "05/01/2024"
    );
    assert_argument(
        &call_err("fmtDate", json!({"year": 2023, "month": 2, "day": 29})),
        "invalid date: 2023-2-29",
    );
    assert_argument(
        &call_err(
            "fmtDate",
            json!({"year": 2024, "month": 1, "day": 5, "fmt": "%H:%M"}),
        ),
        "invalid date format '%H:%M'",
    );
    assert_eq!(
        call_err("fmtDate", json!({"year": 2024, "month": -1, "day": 5})).kind,
        HostErrorKind::Argument
    );
}

#[test]
fn ticker_utilities() {
    assert_eq!(
        call_json("parseTicker", json!({"ticker": "ES1 Index"})),
        json!({"prefix": "ES", "index": 1, "asset": "Index"})
    );
    assert_eq!(
        call_json("parseTicker", json!({"ticker": "SPY1 US Equity"})),
        json!({"prefix": "SPY", "index": 1, "asset": "Equity", "exchange": "US"})
    );
    assert_argument(
        &call_err("parseTicker", json!({"ticker": "AAPL"})),
        "invalid ticker format 'AAPL': expected 'PREFIX ASSET' (e.g., 'ES1 Index')",
    );
    assert!(call_bool(
        "isSpecificContract",
        json!({"ticker": "ESH24 Index"})
    ));
    assert!(!call_bool(
        "isSpecificContract",
        json!({"ticker": "ES1 Index"})
    ));
    assert_eq!(
        call_str(
            "buildFuturesTicker",
            json!({"prefix": "CL", "monthCode": "Z", "year": "25", "asset": "Comdty"})
        ),
        "CLZ25 Comdty"
    );
    let tickers = json!([
        "IBM US Equity",
        "ES1 Index",
        "X=Y US Equity",
        "VOD LN Equity"
    ]);
    assert_eq!(
        call_json("normalizeTickers", json!({"tickers": tickers})),
        tickers,
        "identity"
    );
    assert_eq!(
        call_json("filterEquityTickers", json!({"tickers": tickers})),
        json!(["IBM US Equity", "VOD LN Equity"])
    );
}

#[test]
fn futures_resolution() {
    let candidates = call_json(
        "generateFuturesCandidates",
        json!({"genTicker": "ES1 Index", "year": 2024, "month": 1, "day": 15, "freq": "Q"}),
    );
    let candidates = candidates.as_array().expect("array");
    assert_eq!(candidates.len(), 4, "count defaults to 4");
    assert_eq!(candidates[0]["year"], 2024);
    assert_eq!(candidates[0]["month"], 3);
    assert!(
        candidates[0]["ticker"]
            .as_str()
            .is_some_and(|ticker| ticker.starts_with("ESH"))
    );

    let monthly = call_json(
        "generateFuturesCandidates",
        json!({"genTicker": "CL1 Comdty", "year": 2024, "month": 1, "day": 15, "freq": "bogus", "count": 2}),
    );
    assert_eq!(monthly.as_array().map(Vec::len), Some(2));
    assert_eq!(monthly[1]["month"], 2, "unknown freq falls back to monthly");

    assert_argument(
        &call_err(
            "generateFuturesCandidates",
            json!({"genTicker": "ES1 Index", "year": 2024, "month": 1, "day": 15, "count": 10001}),
        ),
        "futures candidate count 10001 exceeds the maximum of 10000",
    );
    assert_argument(
        &call_err(
            "generateFuturesCandidates",
            json!({"genTicker": "ES1 Index", "year": 2024, "month": 13, "day": 1}),
        ),
        "invalid date: 2024-13-1",
    );

    assert_eq!(
        call(
            "validateGenericTicker",
            &json!({"ticker": "ES1 Index"}).to_string()
        ),
        Ok(ExtValue::Void)
    );
    assert_argument(
        &call_err("validateGenericTicker", json!({"ticker": "ESH24 Index"})),
        "'ESH24 Index' appears to be a specific contract, not generic. Use generic ticker like 'ES1 Index' instead",
    );
    assert_eq!(
        call_json("contractIndex", json!({"genTicker": "ES3 Index"})),
        json!(2)
    );

    assert_eq!(
        call_json(
            "filterCandidatesByCycle",
            json!({
                "candidates": [
                    {"ticker": "ESF24 Index", "year": 2024, "month": 1},
                    {"ticker": "ESH24 Index", "year": 2024, "month": 3},
                    {"ticker": "ESM24 Index", "year": 2024, "month": 6}
                ],
                "cycle": "hm"
            })
        ),
        json!([
            {"ticker": "ESH24 Index", "year": 2024, "month": 3},
            {"ticker": "ESM24 Index", "year": 2024, "month": 6}
        ])
    );

    assert_eq!(
        call_json(
            "filterValidContracts",
            json!({
                "contracts": [
                    {"key": "ESM24 Index", "value": "2024-06-21"},
                    {"key": "ESZ23 Index", "value": "2023-12-15"},
                    {"key": "ESH24 Index", "value": "20240315"},
                    {"key": "BAD Index", "value": "soon"}
                ],
                "year": 2024, "month": 1, "day": 15
            })
        ),
        json!(["ESH24 Index", "ESM24 Index"])
    );
}

#[test]
fn cdx_utilities() {
    assert_eq!(
        call_json(
            "parseCdxTicker",
            json!({"ticker": "CDX IG CDSI GEN 5Y Corp"})
        ),
        json!({"index": "CDX IG CDSI", "series": "GEN", "tenor": "5Y", "asset": "Corp", "isGeneric": true})
    );
    assert_eq!(
        call_json(
            "parseCdxTicker",
            json!({"ticker": "CDX HY CDSI S45 V2 5Y Corp"})
        ),
        json!({"index": "CDX HY CDSI", "series": "S45", "version": 2, "tenor": "5Y", "asset": "Corp", "isGeneric": false, "seriesNum": 45})
    );
    assert_argument(
        &call_err("parseCdxTicker", json!({"ticker": "CDX IG 5Y"})),
        "invalid ticker format 'CDX IG 5Y': expected 'PREFIX ASSET' (e.g., 'ES1 Index')",
    );
    assert_eq!(
        call_json(
            "previousCdxSeries",
            json!({"ticker": "CDX IG CDSI S45 V3 5Y Corp"})
        ),
        json!("CDX IG CDSI S44 5Y Corp")
    );
    assert_eq!(
        call_json(
            "previousCdxSeries",
            json!({"ticker": "CDX IG CDSI GEN 5Y Corp"})
        ),
        Value::Null
    );
    assert_eq!(
        call_str(
            "cdxGenToSpecific",
            json!({"genTicker": "CDX IG CDSI GEN 5Y Corp", "series": 46})
        ),
        "CDX IG CDSI S46 5Y Corp"
    );
    assert_argument(
        &call_err(
            "cdxGenToSpecific",
            json!({"genTicker": "CDX IG CDSI GEN 5Y Corp", "series": 0}),
        ),
        "invalid input: CDX series must be positive",
    );
    assert_eq!(
        call_err(
            "cdxGenToSpecific",
            json!({"genTicker": "CDX IG CDSI GEN 5Y Corp", "series": -1})
        )
        .kind,
        HostErrorKind::Argument
    );
}

#[test]
fn auction_utilities() {
    assert_eq!(
        call_json("auctionFieldGroup", json!({"name": "  Quotes "})),
        json!(["BID", "ASK", "BID_SIZE", "ASK_SIZE"])
    );
    let default = call_json("auctionFieldGroup", json!({"name": "DEFAULT"}));
    assert_eq!(default.as_array().map(Vec::len), Some(39));
    assert_eq!(
        call_json("auctionFieldGroup", json!({"name": "bogus"})),
        Value::Null
    );
    assert_eq!(
        call_json("auctionZeroPriceFields", json!({})),
        json!([
            "THEO_PRICE",
            "INDICATIVE_NEAR",
            "INDICATIVE_FAR",
            "IMBALANCE_BUY",
            "IMBALANCE_SELL",
            "REFERENCE_PRICE_RT"
        ])
    );
    for (code, side) in [
        (" buy ", json!("buy")),
        ("MBUY", json!("buy")),
        ("rbuy", json!("buy")),
        ("SELL", json!("sell")),
        ("MSEL", json!("sell")),
        ("RSEL", json!("sell")),
        ("NOIM", json!("none")),
        ("nimb", json!("none")),
        ("INOR", Value::Null),
        ("NODS", Value::Null),
        ("", Value::Null),
        ("N.A.", Value::Null),
    ] {
        assert_eq!(
            call_json("imbalanceSide", json!({"code": code})),
            side,
            "{code:?}"
        );
    }
}

#[test]
fn currency_and_column_utilities() {
    assert_eq!(
        call_json("buildFxPair", json!({"fromCcy": "GBp", "toCcy": "usd"})),
        json!({"fxPair": "USDGBP Curncy", "factor": 100.0, "fromCcy": "GBP", "toCcy": "USD"})
    );
    assert!(call_bool(
        "sameCurrency",
        json!({"ccy1": "GBP", "ccy2": "GBp"})
    ));
    assert!(!call_bool(
        "sameCurrency",
        json!({"ccy1": "USD", "ccy2": "EUR"})
    ));
    assert_eq!(
        call_json(
            "currenciesNeedingConversion",
            json!({"currencies": ["usd", "GBP", "eur", "gbp"], "target": "USD"})
        ),
        json!(["EUR", "GBP"])
    );
    assert_eq!(
        call_json(
            "renameDividendColumns",
            json!({"columns": ["ticker", "Declared Date", "Ex-Date"]})
        ),
        json!([
            {"key": "Declared Date", "value": "dec_date"},
            {"key": "Ex-Date", "value": "ex_date"}
        ])
    );
    assert_eq!(
        call_json(
            "renameEtfColumns",
            json!({"columns": ["Holding Ticker", "other"]})
        ),
        json!([{"key": "Holding Ticker", "value": "ticker"}])
    );
}

#[test]
fn constant_tables() {
    assert_eq!(
        call_json("getMonthCode", json!({"monthName": "Mar"})),
        json!("H")
    );
    assert_eq!(
        call_json("getMonthCode", json!({"monthName": "March"})),
        Value::Null,
        "exact key lookup"
    );
    assert_eq!(
        call_json("getMonthName", json!({"code": "Z"})),
        json!("Dec")
    );
    assert_eq!(call_json("getMonthName", json!({"code": "z"})), Value::Null);
    assert_eq!(
        call_json("getDvdType", json!({"typ": "all"})),
        json!("DVD_Hist_All")
    );
    assert_eq!(call_json("getDvdType", json!({"typ": "ALL"})), Value::Null);

    let months = call_json("getFuturesMonths", json!({}));
    let months = months.as_array().expect("array");
    assert_eq!(months.len(), 12);
    assert!(months.contains(&json!({"key": "Jan", "value": "F"})));
    let dvd_types = call_json("getDvdTypes", json!({}));
    assert!(
        dvd_types
            .as_array()
            .expect("array")
            .contains(&json!({"key": "all", "value": "DVD_Hist_All"}))
    );
    let dvd_cols = call_json("getDvdCols", json!({}));
    assert!(
        dvd_cols
            .as_array()
            .expect("array")
            .contains(&json!({"key": "Ex-Date", "value": "ex_date"}))
    );
    let etf_cols = call_json("getEtfCols", json!({}));
    assert!(
        etf_cols
            .as_array()
            .expect("array")
            .contains(&json!({"key": "Holding Name", "value": "name"}))
    );
}

#[test]
fn yas_overrides() {
    assert_eq!(
        call_json(
            "buildYasOverrides",
            json!({"settleDt": "20240115", "yieldType": 5, "spread": 25, "yieldVal": 4.25, "price": 99.5, "benchmark": "T 4 11/15/34 Govt"})
        ),
        json!([
            {"key": "YAS_SETTLE_DT", "value": "20240115"},
            {"key": "YAS_YLD_FLAG", "value": "5"},
            {"key": "YAS_YLD_SPREAD", "value": "25"},
            {"key": "YAS_BOND_YLD", "value": "4.25"},
            {"key": "YAS_BOND_PX", "value": "99.5"},
            {"key": "YAS_BNCHMRK_BOND", "value": "T 4 11/15/34 Govt"}
        ])
    );
    assert_eq!(call_json("buildYasOverrides", json!({})), json!([]));
    for code in [0, 10, 257] {
        assert_argument(
            &call_err("buildYasOverrides", json!({"yieldType": code})),
            &format!(
                "unknown yield type '{code}': expected YTM, YTC, YTR, YTP, YTW, YTWR, EYTW, EYTWR, or YTAL"
            ),
        );
    }
}

#[test]
fn earnings_utilities() {
    assert_eq!(
        call_json(
            "buildEarningHeaderRename",
            json!({
                "headerRow": [
                    {"key": "Period 1 Header", "value": "FY 2023"},
                    {"key": "level Header", "value": "Level"}
                ],
                "dataColumns": ["ticker", "Period 1 Value", "Period 2 Value", "level"]
            })
        ),
        json!([
            {"key": "Period 1 Value", "value": "fy2023"},
            {"key": "level", "value": "level"}
        ])
    );
    assert_eq!(
        call_json(
            "calculateLevelPercentages",
            json!({"values": [60.0, 40.0, 30.0, 10.0, null], "levels": [1, 1, 2, 2, 2]})
        ),
        json!([60.0, 40.0, 75.0, 25.0, null])
    );
    assert_eq!(
        call_json(
            "calculateLevelPercentages",
            json!({"values": [1.0, 2.0], "levels": [1]})
        ),
        json!([null, null]),
        "mismatched lengths give nulls"
    );
}

#[test]
fn bql_query_builders() {
    assert_eq!(
        call_str("buildPreferredsQuery", json!({"equityTicker": "BAC"})),
        "get(id, name) for(filter(debt(['BAC US Equity'], CONSOLIDATEDUPLICATES='N'), SRCH_ASSET_CLASS=='Preferreds'))"
    );
    assert_eq!(
        call_str(
            "buildCorporateBondsQuery",
            json!({"ticker": "AAPL", "ccy": "USD", "extraFields": ["cpn", "ID"]})
        ),
        "get(id, cpn) for(filter(debt(['AAPL US Equity'], CONSOLIDATEDUPLICATES='N'), SRCH_ASSET_CLASS=='Corporates' AND CRNCY=='USD'))"
    );
    assert_eq!(
        call_str(
            "buildEtfHoldingsQuery",
            json!({"etfTicker": "SPY", "extraFields": ["name"]})
        ),
        "get(id_isin, weights, id().position, name) for(holdings('SPY US Equity'))"
    );
}

#[test]
fn default_ranges() {
    assert_eq!(
        call_json("defaultTurnoverDates", json!({"endDate": "2024-06-15"})),
        json!({"start": "2024-05-16", "end": "2024-06-15"})
    );
    assert_eq!(
        call_json(
            "defaultTurnoverDates",
            json!({"startDate": "20240101", "endDate": "20240131"})
        ),
        json!({"start": "2024-01-01", "end": "2024-01-31"})
    );
    assert_eq!(
        call_json(
            "defaultBqrDatetimes",
            json!({"startDatetime": "2024-01-15 09:00", "endDatetime": "2024-01-15T10:00"})
        ),
        json!({"start": "2024-01-15T09:00:00", "end": "2024-01-15T10:00:00"})
    );
    assert_eq!(
        call_json(
            "defaultBqrDatetimes",
            json!({"endDatetime": "2024-01-15 10:30"})
        ),
        json!({"start": "2024-01-15T09:30:00", "end": "2024-01-15T10:30:00"})
    );
    let now = call_json("defaultBqrDatetimes", json!({}));
    let start = now["start"].as_str().expect("start");
    let end = now["end"].as_str().expect("end");
    assert_eq!((start.len(), end.len()), (19, 19));
    assert!(start < end);
}

#[test]
fn market_sessions() {
    assert_eq!(
        call_json(
            "deriveSessions",
            json!({"dayStart": "09:30", "dayEnd": "16:00", "mic": "XNYS", "exchCode": "US"})
        ),
        json!({
            "day": {"start": "09:30", "end": "16:00"},
            "allday": {"start": "04:00", "end": "20:00"},
            "pre": {"start": "04:00", "end": "09:30"},
            "post": {"start": "16:01", "end": "20:00"}
        })
    );
    assert_eq!(
        call_json(
            "deriveSessions",
            json!({"dayStart": "09:00", "dayEnd": "15:00", "mic": "XTKS"})
        )["am"],
        json!({"start": "09:00", "end": "11:30"})
    );
    assert_eq!(
        call_json("deriveSessions", json!({"dayStart": "", "dayEnd": "16:00"})),
        json!({})
    );
    assert_eq!(
        call_json("getMarketRule", json!({"mic": "XNYS"})),
        json!({"preMinutes": 330, "postMinutes": 240, "isContinuous": false})
    );
    assert_eq!(
        call_json("getMarketRule", json!({"mic": "XXXX", "exchCode": "ZZ"})),
        Value::Null
    );
    assert_eq!(
        call_json("inferTimezone", json!({"countryIso": " jp "})),
        json!("Asia/Tokyo")
    );
    assert_eq!(
        call_json("inferTimezone", json!({"countryIso": "QQ"})),
        Value::Null
    );
}

#[test]
fn session_times_to_utc() {
    assert_eq!(
        call_json(
            "sessionTimesToUtc",
            json!({"startTime": "09:30", "endTime": "16:00", "exchangeTz": "America/New_York", "date": "2024-01-15"})
        ),
        json!({"start": "2024-01-15T14:30:00", "end": "2024-01-15T21:00:00"})
    );
    assert_argument(
        &call_err(
            "sessionTimesToUtc",
            json!({"startTime": "09:30", "endTime": "16:00", "exchangeTz": "UTC", "date": "20240115"}),
        ),
        "invalid date '20240115', expected YYYY-MM-DD",
    );
    assert_argument(
        &call_err(
            "sessionTimesToUtc",
            json!({"startTime": "09:30", "endTime": "16:00", "exchangeTz": "Mars/Base", "date": "2024-01-15"}),
        ),
        "invalid input: invalid timezone: Mars/Base",
    );
    assert_argument(
        &call_err(
            "sessionTimesToUtc",
            json!({"startTime": "9h", "endTime": "16:00", "exchangeTz": "UTC", "date": "2024-01-15"}),
        ),
        "invalid input: invalid time: 9h",
    );
}

/// The registry is process-global, so the whole lifecycle runs in one test
/// with tickers no other test uses.
#[test]
fn exchange_override_lifecycle() {
    let ticker = "XBBG_HOST_EXT_TEST Equity";
    let other = "XBBG_HOST_EXT_OTHER Equity";

    assert_argument(
        &call_err(
            "setExchangeOverride",
            json!({"ticker": ticker, "input": {}}),
        ),
        "invalid input: override patch must include at least one field",
    );
    assert_argument(
        &call_err(
            "setExchangeOverride",
            json!({"ticker": "  ", "input": {"timezone": "UTC"}}),
        ),
        "invalid input: ticker cannot be empty",
    );
    assert_argument(
        &call_err(
            "setExchangeOverride",
            json!({"ticker": ticker, "input": {"day": {"start": "09:00"}}}),
        ),
        "missing field 'end'",
    );

    let set = |ticker: &str, input: Value| {
        call(
            "setExchangeOverride",
            &json!({"ticker": ticker, "input": input}).to_string(),
        )
    };
    assert_eq!(
        set(
            ticker,
            json!({"timezone": "Europe/London", "mic": "XLON", "exchCode": "LN"})
        ),
        Ok(ExtValue::Void)
    );
    assert_eq!(
        set(
            ticker,
            json!({"day": {"start": "08:00", "end": "16:30"}, "pre": {"start": "07:00", "end": "08:00"}})
        ),
        Ok(ExtValue::Void)
    );
    assert_eq!(
        call_json("getExchangeOverride", json!({"ticker": ticker})),
        json!({
            "ticker": ticker,
            "mic": "XLON",
            "exchCode": "LN",
            "timezone": "Europe/London",
            "source": "override",
            "day": {"start": "08:00", "end": "16:30"},
            "pre": {"start": "07:00", "end": "08:00"}
        }),
        "a later patch merges into the earlier one"
    );
    assert_eq!(
        set(other, json!({"timezone": "Asia/Tokyo"})),
        Ok(ExtValue::Void)
    );
    let listed = call_json("listExchangeOverrides", json!({}));
    let listed_tickers = listed
        .as_array()
        .expect("array")
        .iter()
        .filter_map(|info| info["ticker"].as_str())
        .filter(|name| name.starts_with("XBBG_HOST_EXT_"))
        .collect::<Vec<_>>();
    assert_eq!(listed_tickers, [other, ticker], "sorted by ticker");

    assert_eq!(
        call(
            "clearExchangeOverride",
            &json!({"ticker": other}).to_string()
        ),
        Ok(ExtValue::Void)
    );
    assert_eq!(
        call_json("getExchangeOverride", json!({"ticker": other})),
        Value::Null
    );
    assert_eq!(
        call("clearExchangeOverride", &json!({}).to_string()),
        Ok(ExtValue::Void)
    );
    assert_eq!(
        call_json("getExchangeOverride", json!({"ticker": ticker})),
        Value::Null,
        "no ticker clears every override"
    );
}

fn ipc(batch: &RecordBatch) -> Vec<u8> {
    batch_to_ipc_stream(batch).expect("IPC")
}

fn long_batch(key: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(key, DataType::Utf8, false),
        Field::new("field", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                "AAPL US Equity",
                "AAPL US Equity",
                "MSFT US Equity",
            ])),
            Arc::new(StringArray::from(vec!["PX_LAST", "VOLUME", "PX_LAST"])),
            Arc::new(Float64Array::from(vec![
                Some(150.0),
                Some(1e6),
                Some(300.0),
            ])),
        ],
    )
    .expect("batch")
}

fn read_ipc(bytes: &[u8]) -> RecordBatch {
    ipc_to_batch(bytes).expect("readable IPC")
}

#[test]
fn pivot_to_wide_round_trips_ipc() {
    let wide = read_ipc(&call_bytes("pivotToWide", &ipc(&long_batch("ticker"))).expect("pivot"));
    let names = wide
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    assert_eq!(names, ["ticker", "PX_LAST", "VOLUME"]);
    assert_eq!(wide.num_rows(), 2);
    let volume = wide
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("f64");
    assert_eq!(volume.value(0), 1e6);
    assert!(volume.is_null(1));

    let security =
        read_ipc(&call_bytes("pivotToWide", &ipc(&long_batch("security"))).expect("pivot"));
    assert_eq!(security.schema().field(0).name(), "security");

    assert_eq!(
        call_bytes("isLongFormat", &ipc(&long_batch("ticker"))),
        Ok(vec![1])
    );
    assert_eq!(call_bytes("isLongFormat", &ipc(&wide)), Ok(vec![0]));
    let unchanged = read_ipc(&call_bytes("pivotToWide", &ipc(&wide)).expect("wide input"));
    assert_eq!(unchanged, wide, "wide input is returned unchanged");
}

#[test]
fn arrow_input_errors_are_argument_errors() {
    let error = call_bytes("pivotToWide", b"not arrow").expect_err("garbage");
    assert_eq!(error.kind, HostErrorKind::Argument);
    assert!(
        error.message.starts_with("invalid Arrow IPC: "),
        "{}",
        error.message
    );

    let schema = Arc::new(Schema::new(vec![Field::new(
        "ticker",
        DataType::Utf8,
        false,
    )]));
    let mut empty = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut empty, &schema).expect("writer");
        writer.finish().expect("finish");
    }
    assert_argument(
        &call_bytes("isLongFormat", &empty).expect_err("no batch"),
        "empty Arrow IPC stream",
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("ticker", DataType::Int64, false),
        Field::new("field", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["F"])),
            Arc::new(StringArray::from(vec!["v"])),
        ],
    )
    .expect("batch");
    assert_argument(
        &call_bytes("pivotToWide", &ipc(&batch)).expect_err("non-string key"),
        "missing required column 'ticker must be string'",
    );
}
