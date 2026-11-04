package io.github.underloam.xbbg.ext;

import io.github.underloam.xbbg.internal.Json;
import io.github.underloam.xbbg.internal.JsonFields;
import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import io.github.underloam.xbbg.internal.NativeBridge;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Synchronous, engine-free utilities: the {@code @xbbg/core} {@code ext} namespace with the same
 * function names. Every call runs in the native library (loading it on first use) and returns
 * quickly; nothing touches Bloomberg.
 *
 * <p>Rejected input throws {@link IllegalArgumentException} with the native message; a {@code null}
 * required argument throws {@link NullPointerException}. Lists and maps are unmodifiable; maps keep
 * the native order. The asynchronous {@code ext.cdx} helpers are in {@link Cdx}, the auction
 * helpers in {@link AuctionFields}.
 */
public final class Ext {
  private Ext() {}

  // ── Dates ──────────────────────────────────────────────────────────────

  /**
   * Parses a date in one of the formats {@code YYYY-MM-DD}, {@code YYYYMMDD}, {@code YYYY/MM/DD},
   * {@code DD-MM-YYYY} or {@code DD/MM/YYYY} (surrounding whitespace ignored).
   *
   * @param dateStr the date text
   * @return the date (JavaScript returns {@code [year, month, day]})
   * @throws IllegalArgumentException if no format matches
   */
  public static LocalDate parseDate(String dateStr) {
    Objects.requireNonNull(dateStr, "dateStr");
    List<Object> parts =
        JsonFields.array(
            ExtCalls.json("parseDate", Json.object().put("dateStr", dateStr)), "parseDate");
    if (parts.size() != 3) {
      throw JsonFields.malformed("parseDate", "must hold year, month and day");
    }
    return LocalDate.of(
        ExtCalls.intOf(parts.get(0), "parseDate"),
        ExtCalls.intOf(parts.get(1), "parseDate"),
        ExtCalls.intOf(parts.get(2), "parseDate"));
  }

  /**
   * Formats a date as {@code YYYYMMDD}.
   *
   * @param year the year
   * @param month the month, 1–12
   * @param day the day of the month
   * @return the formatted date
   * @throws IllegalArgumentException for an invalid date ({@code invalid date: y-m-d})
   */
  public static String fmtDate(int year, int month, int day) {
    return fmtDate(year, month, day, null);
  }

  /**
   * Formats a date with a chrono {@code strftime} pattern.
   *
   * @param year the year
   * @param month the month, 1–12
   * @param day the day of the month
   * @param fmt the pattern, for example {@code "%Y-%m-%d"}; {@code null} means {@code "%Y%m%d"}
   * @return the formatted date
   * @throws IllegalArgumentException for an invalid date or a pattern a date cannot render
   */
  public static String fmtDate(int year, int month, int day, String fmt) {
    return ExtCalls.string(
        "fmtDate",
        Json.object().put("year", year).put("month", month).put("day", day).put("fmt", fmt));
  }

  /**
   * Computes a turnover date range: the end defaults to yesterday (local time) and the start to 30
   * days before the end.
   *
   * @return the range as {@code YYYY-MM-DD} dates
   */
  public static TimeRange defaultTurnoverDates() {
    return defaultTurnoverDates(null, null);
  }

  /**
   * Computes a turnover date range. A missing or unparsable end becomes yesterday (local time); a
   * missing or unparsable start becomes 30 days before the end.
   *
   * @param startDate the start date, or {@code null}
   * @param endDate the end date, or {@code null}
   * @return the range as {@code YYYY-MM-DD} dates
   */
  public static TimeRange defaultTurnoverDates(String startDate, String endDate) {
    return TimeRange.fromJson(
        JsonFields.object(
            ExtCalls.json(
                "defaultTurnoverDates",
                Json.object().put("startDate", startDate).put("endDate", endDate)),
            "defaultTurnoverDates"));
  }

  /**
   * Computes a BQR datetime range: the end defaults to now (local time) and the start to one hour
   * before the end.
   *
   * @return the range as {@code YYYY-MM-DDTHH:MM:SS} datetimes
   */
  public static TimeRange defaultBqrDatetimes() {
    return defaultBqrDatetimes(null, null);
  }

  /**
   * Computes a BQR datetime range. A missing end becomes now (local time), a missing start one hour
   * before the end; {@code YYYY-MM-DD HH:MM} inputs gain {@code T} and {@code :00}.
   *
   * @param startDatetime the start, or {@code null}
   * @param endDatetime the end, or {@code null}
   * @return the range as {@code YYYY-MM-DDTHH:MM:SS} datetimes
   */
  public static TimeRange defaultBqrDatetimes(String startDatetime, String endDatetime) {
    return TimeRange.fromJson(
        JsonFields.object(
            ExtCalls.json(
                "defaultBqrDatetimes",
                Json.object().put("startDatetime", startDatetime).put("endDatetime", endDatetime)),
            "defaultBqrDatetimes"));
  }

  // ── Arrow tables ───────────────────────────────────────────────────────

  /**
   * Pivots a long {@code (ticker|security, field, value)} table to one column per field, keys and
   * fields in first-appearance order (a repeated pair keeps its last value). Any other table is
   * returned unchanged.
   *
   * @param ipcStream an Arrow IPC stream; only its first batch is read
   * @return the wide table as an Arrow IPC stream
   * @throws IllegalArgumentException if the bytes are not an Arrow IPC stream with a batch, or the
   *     key or {@code field} column is not text
   */
  public static byte[] pivotToWide(byte[] ipcStream) {
    return NativeBridge.extCallBytes("pivotToWide", Objects.requireNonNull(ipcStream, "ipcStream"));
  }

  /**
   * Whether a table has exactly the long columns {@code ticker} or {@code security}, {@code field}
   * and {@code value}.
   *
   * @param ipcStream an Arrow IPC stream; only its first batch is read
   * @return {@code true} for a long table
   * @throws IllegalArgumentException if the bytes are not an Arrow IPC stream with a batch
   */
  public static boolean isLongFormat(byte[] ipcStream) {
    byte[] answer =
        NativeBridge.extCallBytes("isLongFormat", Objects.requireNonNull(ipcStream, "ipcStream"));
    if (answer.length != 1 || (answer[0] != 0 && answer[0] != 1)) {
      throw JsonFields.malformed("isLongFormat", "must be one byte, 0 or 1");
    }
    return answer[0] == 1;
  }

  // ── Tickers ────────────────────────────────────────────────────────────

  /**
   * Splits a generic ticker such as {@code "ES1 Index"} or {@code "SPY1 US Equity"}.
   *
   * @param ticker the ticker
   * @return its components
   * @throws IllegalArgumentException if the ticker is not {@code PREFIX<n> [EXCHANGE] ASSET}
   */
  public static TickerParts parseTicker(String ticker) {
    return TickerParts.fromJson(
        JsonFields.object(ExtCalls.json("parseTicker", tickerArgs(ticker)), "parseTicker"));
  }

  /**
   * Whether a ticker names a specific contract ({@code "ESH24 Index"}) rather than a generic one.
   *
   * @param ticker the ticker
   * @return {@code true} for a specific contract
   */
  public static boolean isSpecificContract(String ticker) {
    return ExtCalls.bool("isSpecificContract", tickerArgs(ticker));
  }

  /**
   * Builds a futures ticker: {@code prefix + monthCode + year + " " + asset}.
   *
   * @param prefix the root symbol, for example {@code "ES"}
   * @param monthCode the month code, for example {@code "H"}
   * @param year the year digits, for example {@code "24"}
   * @param asset the yellow key, for example {@code "Index"}
   * @return the ticker
   */
  public static String buildFuturesTicker(
      String prefix, String monthCode, String year, String asset) {
    return ExtCalls.string(
        "buildFuturesTicker",
        Json.object()
            .put("prefix", Objects.requireNonNull(prefix, "prefix"))
            .put("monthCode", Objects.requireNonNull(monthCode, "monthCode"))
            .put("year", Objects.requireNonNull(year, "year"))
            .put("asset", Objects.requireNonNull(asset, "asset")));
  }

  /**
   * Returns the tickers unchanged (the JavaScript function is an identity).
   *
   * @param tickers the tickers
   * @return the same tickers, in order
   */
  public static List<String> normalizeTickers(Collection<String> tickers) {
    return JsonFields.stringList(
        ExtCalls.json("normalizeTickers", tickersArgs(tickers)), "normalizeTickers");
  }

  /**
   * Keeps the tickers that contain {@code Equity} and no {@code =}.
   *
   * @param tickers the tickers
   * @return the equity tickers, in input order
   */
  public static List<String> filterEquityTickers(Collection<String> tickers) {
    return JsonFields.stringList(
        ExtCalls.json("filterEquityTickers", tickersArgs(tickers)), "filterEquityTickers");
  }

  // ── Futures ────────────────────────────────────────────────────────────

  /**
   * Generates four monthly contract candidates for a generic futures ticker.
   *
   * @param genTicker the generic ticker, for example {@code "ES1 Index"}
   * @param year the reference year
   * @param month the reference month, 1–12
   * @param day the reference day
   * @return the candidates, from the reference month on
   * @throws IllegalArgumentException for an invalid date or a specific or unparsable ticker
   */
  public static List<FuturesCandidate> generateFuturesCandidates(
      String genTicker, int year, int month, int day) {
    return generateFuturesCandidates(genTicker, year, month, day, null, null);
  }

  /**
   * Generates contract candidates for a generic futures ticker.
   *
   * @param genTicker the generic ticker, for example {@code "ES1 Index"}
   * @param year the reference year
   * @param month the reference month, 1–12
   * @param day the reference day
   * @param freq {@code "Q"} or {@code "QE"} for quarterly contracts; anything else, or {@code
   *     null}, is monthly
   * @param count the number of candidates (default 4, at most 10 000), or {@code null}
   * @return the candidates, from the reference month on
   * @throws IllegalArgumentException for an invalid date, a negative or too large count, or a
   *     specific or unparsable ticker
   */
  public static List<FuturesCandidate> generateFuturesCandidates(
      String genTicker, int year, int month, int day, String freq, Integer count) {
    return candidates(
        ExtCalls.json(
            "generateFuturesCandidates",
            Json.object()
                .put("genTicker", Objects.requireNonNull(genTicker, "genTicker"))
                .put("year", year)
                .put("month", month)
                .put("day", day)
                .put("freq", freq)
                .put("count", count)),
        "generateFuturesCandidates");
  }

  /**
   * Fails unless the ticker is generic.
   *
   * @param ticker the ticker
   * @throws IllegalArgumentException if the ticker names a specific contract
   */
  public static void validateGenericTicker(String ticker) {
    ExtCalls.run("validateGenericTicker", tickerArgs(ticker));
  }

  /**
   * Returns the 0-based contract position of a generic ticker ({@code "ES2 Index"} → 1).
   *
   * @param genTicker the generic ticker
   * @return the position
   * @throws IllegalArgumentException if the ticker cannot be parsed
   */
  public static int contractIndex(String genTicker) {
    return ExtCalls.intOf(
        ExtCalls.json(
            "contractIndex",
            Json.object().put("genTicker", Objects.requireNonNull(genTicker, "genTicker"))),
        "contractIndex");
  }

  /**
   * Keeps the candidates whose contract month code appears in a cycle string (case-insensitive).
   *
   * @param candidates the candidates
   * @param cycle month codes, for example {@code "HMUZ"}
   * @return the matching candidates, in input order
   */
  public static List<FuturesCandidate> filterCandidatesByCycle(
      Collection<FuturesCandidate> candidates, String cycle) {
    List<Map<String, Object>> json = new ArrayList<Map<String, Object>>();
    for (FuturesCandidate candidate : ExtCalls.copy(candidates, "candidates")) {
      json.add(Objects.requireNonNull(candidate, "candidate").toJson());
    }
    return candidates(
        ExtCalls.json(
            "filterCandidatesByCycle",
            Json.object()
                .put("candidates", json)
                .put("cycle", Objects.requireNonNull(cycle, "cycle"))),
        "filterCandidatesByCycle");
  }

  /**
   * Keeps the contracts maturing after a reference date, sorted by maturity (ties keep their
   * order). Unparsable maturities are dropped.
   *
   * @param contracts contract ticker → maturity date, in order
   * @param year the reference year
   * @param month the reference month, 1–12
   * @param day the reference day
   * @return the surviving tickers
   * @throws IllegalArgumentException for an invalid reference date
   */
  public static List<String> filterValidContracts(
      Map<String, String> contracts, int year, int month, int day) {
    return JsonFields.stringList(
        ExtCalls.json(
            "filterValidContracts",
            Json.object()
                .put(
                    "contracts",
                    ExtCalls.pairsJson(Objects.requireNonNull(contracts, "contracts"), "contracts"))
                .put("year", year)
                .put("month", month)
                .put("day", day)),
        "filterValidContracts");
  }

  // ── CDX ────────────────────────────────────────────────────────────────

  /**
   * Splits a CDX ticker such as {@code "CDX IG CDSI GEN 5Y Corp"} or {@code "CDX IG CDSI S45 V2 5Y
   * Corp"}.
   *
   * @param ticker the ticker
   * @return its components
   * @throws IllegalArgumentException if the ticker is not a CDX ticker
   */
  public static CdxTickerInfo parseCdxTicker(String ticker) {
    return CdxTickerInfo.fromJson(
        JsonFields.object(ExtCalls.json("parseCdxTicker", tickerArgs(ticker)), "parseCdxTicker"));
  }

  /**
   * Returns the versionless ticker of the preceding series.
   *
   * @param ticker a specific CDX ticker
   * @return the previous series, or empty for a generic ticker or series 1
   * @throws IllegalArgumentException if the ticker is not a CDX ticker
   */
  public static Optional<String> previousCdxSeries(String ticker) {
    return ExtCalls.optionalString(
        ExtCalls.json("previousCdxSeries", tickerArgs(ticker)), "previousCdxSeries");
  }

  /**
   * Replaces the {@code GEN} token of a generic CDX ticker with a series.
   *
   * @param genTicker the generic ticker
   * @param series the series number, at least 1
   * @return the versionless specific ticker
   * @throws IllegalArgumentException for a specific ticker, a non-CDX ticker or a series outside 1
   *     to 2<sup>32</sup> − 1
   */
  public static String cdxGenToSpecific(String genTicker, long series) {
    return ExtCalls.string(
        "cdxGenToSpecific",
        Json.object()
            .put("genTicker", Objects.requireNonNull(genTicker, "genTicker"))
            .put("series", series));
  }

  // ── Currency ───────────────────────────────────────────────────────────

  /**
   * Builds the FX ticker that converts {@code fromCcy} into {@code toCcy}.
   *
   * @param fromCcy the source currency; a lower-case last letter ({@code GBp}) means a minor unit
   * @param toCcy the target currency
   * @return the FX ticker and factor
   */
  public static FxPairInfo buildFxPair(String fromCcy, String toCcy) {
    return FxPairInfo.fromJson(
        JsonFields.object(
            ExtCalls.json(
                "buildFxPair",
                Json.object()
                    .put("fromCcy", Objects.requireNonNull(fromCcy, "fromCcy"))
                    .put("toCcy", Objects.requireNonNull(toCcy, "toCcy"))),
            "buildFxPair"));
  }

  /**
   * Whether two currency codes are equal ignoring case ({@code GBP} and {@code GBp} match).
   *
   * @param ccy1 a currency
   * @param ccy2 another currency
   * @return {@code true} if they match
   */
  public static boolean sameCurrency(String ccy1, String ccy2) {
    return ExtCalls.bool(
        "sameCurrency",
        Json.object()
            .put("ccy1", Objects.requireNonNull(ccy1, "ccy1"))
            .put("ccy2", Objects.requireNonNull(ccy2, "ccy2")));
  }

  /**
   * Returns the distinct currencies that differ from the target, upper-cased and sorted.
   *
   * @param currencies the currencies
   * @param target the target currency
   * @return the currencies needing conversion
   */
  public static List<String> currenciesNeedingConversion(
      Collection<String> currencies, String target) {
    return JsonFields.stringList(
        ExtCalls.json(
            "currenciesNeedingConversion",
            Json.object()
                .put("currencies", ExtCalls.copy(currencies, "currencies"))
                .put("target", Objects.requireNonNull(target, "target"))),
        "currenciesNeedingConversion");
  }

  // ── Columns and constants ──────────────────────────────────────────────

  /**
   * Maps the Bloomberg dividend column names among {@code columns} to clean names.
   *
   * @param columns column names
   * @return original → clean name, for the known columns in input order
   */
  public static Map<String, String> renameDividendColumns(Collection<String> columns) {
    return ExtCalls.pairs(
        ExtCalls.json("renameDividendColumns", columnsArgs(columns)), "renameDividendColumns");
  }

  /**
   * Maps the Bloomberg ETF holdings column names among {@code columns} to clean names.
   *
   * @param columns column names
   * @return original → clean name, for the known columns in input order
   */
  public static Map<String, String> renameEtfColumns(Collection<String> columns) {
    return ExtCalls.pairs(
        ExtCalls.json("renameEtfColumns", columnsArgs(columns)), "renameEtfColumns");
  }

  /**
   * Returns the futures month code of a three-letter month name (exact key, for example {@code
   * "Mar"} → {@code "H"}).
   *
   * @param monthName the month name
   * @return the code, or empty
   */
  public static Optional<String> getMonthCode(String monthName) {
    return ExtCalls.optionalString(
        ExtCalls.json(
            "getMonthCode",
            Json.object().put("monthName", Objects.requireNonNull(monthName, "monthName"))),
        "getMonthCode");
  }

  /**
   * Returns the three-letter month name of a futures month code (exact key, for example {@code "H"}
   * → {@code "Mar"}).
   *
   * @param code the month code
   * @return the month name, or empty
   */
  public static Optional<String> getMonthName(String code) {
    return ExtCalls.optionalString(
        ExtCalls.json(
            "getMonthName", Json.object().put("code", Objects.requireNonNull(code, "code"))),
        "getMonthName");
  }

  /**
   * Returns every month name → futures month code.
   *
   * @return the table in native order
   */
  public static Map<String, String> getFuturesMonths() {
    return ExtCalls.pairs(ExtCalls.json("getFuturesMonths", Json.object()), "getFuturesMonths");
  }

  /**
   * Returns the Bloomberg field of a dividend type alias (exact key, for example {@code "all"} →
   * {@code "DVD_Hist_All"}).
   *
   * @param typ the alias
   * @return the field, or empty
   */
  public static Optional<String> getDvdType(String typ) {
    return ExtCalls.optionalString(
        ExtCalls.json("getDvdType", Json.object().put("typ", Objects.requireNonNull(typ, "typ"))),
        "getDvdType");
  }

  /**
   * Returns every dividend type alias → Bloomberg field.
   *
   * @return the table in native order
   */
  public static Map<String, String> getDvdTypes() {
    return ExtCalls.pairs(ExtCalls.json("getDvdTypes", Json.object()), "getDvdTypes");
  }

  /**
   * Returns every Bloomberg dividend column → clean name.
   *
   * @return the table in native order
   */
  public static Map<String, String> getDvdCols() {
    return ExtCalls.pairs(ExtCalls.json("getDvdCols", Json.object()), "getDvdCols");
  }

  /**
   * Returns every Bloomberg ETF holdings column → clean name.
   *
   * @return the table in native order
   */
  public static Map<String, String> getEtfCols() {
    return ExtCalls.pairs(ExtCalls.json("getEtfCols", Json.object()), "getEtfCols");
  }

  // ── Fixed income and earnings ──────────────────────────────────────────

  /**
   * Builds the YAS overrides for the given values; {@code null} arguments are left out.
   *
   * @param settleDt {@code YAS_SETTLE_DT}
   * @param yieldType {@code YAS_YLD_FLAG}: 1 = YTM, 2 = YTC, 3 = YTR, 4 = YTP, 5 = YTW, 6 = YTWR, 7
   *     = EYTW, 8 = EYTWR, 9 = YTAL
   * @param spread {@code YAS_YLD_SPREAD}
   * @param yieldVal {@code YAS_BOND_YLD}
   * @param price {@code YAS_BOND_PX}
   * @param benchmark {@code YAS_BNCHMRK_BOND}
   * @return override name → value, in that order
   * @throws IllegalArgumentException if {@code yieldType} is outside 1–9
   */
  public static Map<String, String> buildYasOverrides(
      String settleDt,
      Integer yieldType,
      Double spread,
      Double yieldVal,
      Double price,
      String benchmark) {
    return ExtCalls.pairs(
        ExtCalls.json(
            "buildYasOverrides",
            Json.object()
                .put("settleDt", settleDt)
                .put("yieldType", yieldType)
                .put("spread", spread)
                .put("yieldVal", yieldVal)
                .put("price", price)
                .put("benchmark", benchmark)),
        "buildYasOverrides");
  }

  /**
   * Maps earnings data columns to names derived from the header row ({@code "Period 1 Value"} with
   * header {@code "FY 2023"} → {@code "fy2023"}).
   *
   * @param headerRow header column → header value
   * @param dataColumns data column names
   * @return data column → new name, in data-column order
   */
  public static Map<String, String> buildEarningHeaderRename(
      Map<String, String> headerRow, Collection<String> dataColumns) {
    return ExtCalls.pairs(
        ExtCalls.json(
            "buildEarningHeaderRename",
            Json.object()
                .put(
                    "headerRow",
                    ExtCalls.pairsJson(Objects.requireNonNull(headerRow, "headerRow"), "headerRow"))
                .put("dataColumns", ExtCalls.copy(dataColumns, "dataColumns"))),
        "buildEarningHeaderRename");
  }

  /**
   * Computes each row's percentage of its level: level-1 rows of the level-1 total, level-2 rows of
   * their parent group. Mismatched lengths give all nulls.
   *
   * @param values the values ({@code null} for missing)
   * @param levels the hierarchy levels ({@code null} for missing)
   * @return the percentages (0–100), {@code null} where undefined
   */
  public static List<Double> calculateLevelPercentages(List<Double> values, List<Long> levels) {
    List<Object> parsed =
        JsonFields.array(
            ExtCalls.json(
                "calculateLevelPercentages",
                Json.object()
                    .put("values", ExtCalls.copy(values, "values"))
                    .put("levels", ExtCalls.copy(levels, "levels"))),
            "calculateLevelPercentages");
    List<Double> result = new ArrayList<Double>(parsed.size());
    for (Object item : parsed) {
      result.add(item == null ? null : JsonFields.doubleOf(item, "calculateLevelPercentages"));
    }
    return Collections.unmodifiableList(result);
  }

  // ── BQL queries ────────────────────────────────────────────────────────

  /**
   * Builds the BQL query behind {@code preferreds}.
   *
   * @param equityTicker the company equity ticker ({@code " US Equity"} is appended when it has no
   *     space)
   * @return the BQL text
   */
  public static String buildPreferredsQuery(String equityTicker) {
    return buildPreferredsQuery(equityTicker, null);
  }

  /**
   * Builds the BQL query behind {@code preferreds} with extra fields after {@code id, name}.
   *
   * @param equityTicker the company equity ticker
   * @param extraFields extra fields, or {@code null}
   * @return the BQL text
   */
  public static String buildPreferredsQuery(String equityTicker, Collection<String> extraFields) {
    return ExtCalls.string(
        "buildPreferredsQuery",
        Json.object()
            .put("equityTicker", Objects.requireNonNull(equityTicker, "equityTicker"))
            .put("extraFields", extraFields));
  }

  /**
   * Builds the BQL query behind {@code corporateBonds} for every currency.
   *
   * @param ticker the company equity ticker
   * @return the BQL text
   */
  public static String buildCorporateBondsQuery(String ticker) {
    return buildCorporateBondsQuery(ticker, null, null);
  }

  /**
   * Builds the BQL query behind {@code corporateBonds}.
   *
   * @param ticker the company equity ticker ({@code " US Equity"} is appended when it has no space)
   * @param ccy the currency filter, or {@code null} for every currency
   * @param extraFields extra fields after {@code id}, or {@code null}
   * @return the BQL text
   */
  public static String buildCorporateBondsQuery(
      String ticker, String ccy, Collection<String> extraFields) {
    return ExtCalls.string(
        "buildCorporateBondsQuery",
        Json.object()
            .put("ticker", Objects.requireNonNull(ticker, "ticker"))
            .put("ccy", ccy)
            .put("extraFields", extraFields));
  }

  /**
   * Builds the BQL query behind {@code etfHoldings}.
   *
   * @param etfTicker the ETF ticker
   * @return the BQL text
   */
  public static String buildEtfHoldingsQuery(String etfTicker) {
    return buildEtfHoldingsQuery(etfTicker, null);
  }

  /**
   * Builds the BQL query behind {@code etfHoldings} with extra fields after {@code id_isin,
   * weights, id().position}.
   *
   * @param etfTicker the ETF ticker ({@code " US Equity"} is appended when it has no space)
   * @param extraFields extra fields, or {@code null}
   * @return the BQL text
   */
  public static String buildEtfHoldingsQuery(String etfTicker, Collection<String> extraFields) {
    return ExtCalls.string(
        "buildEtfHoldingsQuery",
        Json.object()
            .put("etfTicker", Objects.requireNonNull(etfTicker, "etfTicker"))
            .put("extraFields", extraFields));
  }

  // ── Markets and sessions ───────────────────────────────────────────────

  /**
   * Derives session windows from regular trading hours without a market rule.
   *
   * @param dayStart the open, {@code "HH:MM"}
   * @param dayEnd the close, {@code "HH:MM"}
   * @return the windows (empty if a time is unparsable)
   */
  public static SessionWindows deriveSessions(String dayStart, String dayEnd) {
    return deriveSessions(dayStart, dayEnd, null, null);
  }

  /**
   * Derives session windows from regular trading hours and the market rule of a MIC or exchange
   * code.
   *
   * @param dayStart the open, {@code "HH:MM"}
   * @param dayEnd the close, {@code "HH:MM"}
   * @param mic the market identifier code, or {@code null}
   * @param exchCode the Bloomberg exchange code, or {@code null}
   * @return the windows (empty if a time is unparsable)
   */
  public static SessionWindows deriveSessions(
      String dayStart, String dayEnd, String mic, String exchCode) {
    return SessionWindows.fromJson(
        JsonFields.object(
            ExtCalls.json(
                "deriveSessions",
                Json.object()
                    .put("dayStart", Objects.requireNonNull(dayStart, "dayStart"))
                    .put("dayEnd", Objects.requireNonNull(dayEnd, "dayEnd"))
                    .put("mic", mic)
                    .put("exchCode", exchCode)),
            "deriveSessions"));
  }

  /**
   * Looks up the market rule of a MIC, falling back to a Bloomberg exchange code.
   *
   * @param mic the market identifier code, or {@code null}
   * @param exchCode the exchange code, or {@code null}
   * @return the rule, or empty
   */
  public static Optional<MarketRule> getMarketRule(String mic, String exchCode) {
    Object parsed =
        ExtCalls.json("getMarketRule", Json.object().put("mic", mic).put("exchCode", exchCode));
    return parsed == null
        ? Optional.<MarketRule>empty()
        : Optional.of(MarketRule.fromJson(JsonFields.object(parsed, "getMarketRule")));
  }

  /**
   * Returns the IANA timezone of a country (ISO code, case-insensitive).
   *
   * @param countryIso the country code, for example {@code "JP"}
   * @return the timezone, or empty
   */
  public static Optional<String> inferTimezone(String countryIso) {
    return ExtCalls.optionalString(
        ExtCalls.json(
            "inferTimezone",
            Json.object().put("countryIso", Objects.requireNonNull(countryIso, "countryIso"))),
        "inferTimezone");
  }

  /**
   * Sets or merges a process-wide exchange override for a ticker (it also applies to the engine's
   * exchange lookups in this process).
   *
   * @param ticker the ticker (trimmed; must not be blank)
   * @param override the patch; it must set at least one member
   * @throws IllegalArgumentException for a blank ticker or an empty patch
   */
  public static void setExchangeOverride(String ticker, ExchangeOverride override) {
    ExtCalls.run(
        "setExchangeOverride",
        Json.object()
            .put("ticker", Objects.requireNonNull(ticker, "ticker"))
            .put("input", Objects.requireNonNull(override, "override").toJson()));
  }

  /**
   * Returns the override of a ticker.
   *
   * @param ticker the ticker
   * @return the materialized override, or empty
   */
  public static Optional<ExchangeInfo> getExchangeOverride(String ticker) {
    Object parsed = ExtCalls.json("getExchangeOverride", tickerArgs(ticker));
    return parsed == null
        ? Optional.<ExchangeInfo>empty()
        : Optional.of(ExchangeInfo.fromJson(JsonFields.object(parsed, "getExchangeOverride")));
  }

  /** Removes every exchange override. */
  public static void clearExchangeOverride() {
    ExtCalls.run("clearExchangeOverride", Json.object());
  }

  /**
   * Removes the override of one ticker. A blank ticker removes every override, like the native
   * function.
   *
   * @param ticker the ticker
   */
  public static void clearExchangeOverride(String ticker) {
    ExtCalls.run("clearExchangeOverride", tickerArgs(ticker));
  }

  /**
   * Returns every exchange override.
   *
   * @return the overrides, sorted by ticker
   */
  public static List<ExchangeInfo> listExchangeOverrides() {
    List<Object> parsed =
        JsonFields.array(
            ExtCalls.json("listExchangeOverrides", Json.object()), "listExchangeOverrides");
    List<ExchangeInfo> result = new ArrayList<ExchangeInfo>(parsed.size());
    for (Object item : parsed) {
      result.add(ExchangeInfo.fromJson(JsonFields.object(item, "listExchangeOverrides")));
    }
    return Collections.unmodifiableList(result);
  }

  /**
   * Converts exchange-local session times on a date to UTC.
   *
   * @param startTime the local start, {@code "HH:MM"} or {@code "HH:MM:SS"}
   * @param endTime the local end
   * @param exchangeTz the IANA timezone of the exchange, or {@code "UTC"}
   * @param date the date, {@code YYYY-MM-DD}
   * @return the UTC range as {@code YYYY-MM-DDTHH:MM:SS} datetimes
   * @throws IllegalArgumentException for an invalid date, time or timezone, or a local time that
   *     does not exist or is ambiguous
   */
  public static TimeRange sessionTimesToUtc(
      String startTime, String endTime, String exchangeTz, String date) {
    return TimeRange.fromJson(
        JsonFields.object(
            ExtCalls.json(
                "sessionTimesToUtc",
                Json.object()
                    .put("startTime", Objects.requireNonNull(startTime, "startTime"))
                    .put("endTime", Objects.requireNonNull(endTime, "endTime"))
                    .put("exchangeTz", Objects.requireNonNull(exchangeTz, "exchangeTz"))
                    .put("date", Objects.requireNonNull(date, "date"))),
            "sessionTimesToUtc"));
  }

  private static JsonObjectBuilder tickerArgs(String ticker) {
    return Json.object().put("ticker", Objects.requireNonNull(ticker, "ticker"));
  }

  private static JsonObjectBuilder tickersArgs(Collection<String> tickers) {
    return Json.object().put("tickers", ExtCalls.copy(tickers, "tickers"));
  }

  private static JsonObjectBuilder columnsArgs(Collection<String> columns) {
    return Json.object().put("columns", ExtCalls.copy(columns, "columns"));
  }

  private static List<FuturesCandidate> candidates(Object parsed, String what) {
    List<Object> items = JsonFields.array(parsed, what);
    List<FuturesCandidate> result = new ArrayList<FuturesCandidate>(items.size());
    for (Object item : items) {
      result.add(FuturesCandidate.fromJson(JsonFields.object(item, what)));
    }
    return Collections.unmodifiableList(result);
  }
}
