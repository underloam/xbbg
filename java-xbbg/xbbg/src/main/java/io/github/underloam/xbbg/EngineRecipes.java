package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonValue;
import io.github.underloam.xbbg.table.Table;
import java.time.temporal.TemporalAccessor;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Recipes: multi-request workflows the engine computes ({@code bqr}, {@code yas}, {@code
 * futTicker}, {@code volSurface}, {@code delayStatus}, ...), the Java form of the {@code
 * @xbbg/core} {@code Engine} recipe methods of the same names.
 *
 * <p>Each recipe sends one argument object (the JavaScript positional arguments and options as
 * named fields), which the engine validates and completes with the JavaScript defaults before any
 * Bloomberg request. Results are tables: a {@link Table} by default, or any {@link Backend} form,
 * including Arrow IPC bytes. Where JavaScript accepts {@code string | string[]} there are {@code
 * String} and {@code Collection<String>} overloads; positional dates take {@code YYYY-MM-DD} or
 * {@code YYYYMMDD} text, or any {@code java.time} value carrying a date. A {@code null} date is
 * sent empty, as JavaScript sends a missing date: {@code dividend} then leaves that end of the
 * range open and {@code turnover} uses its default range, while recipes that need the date fail.
 *
 * <p>Null required arguments fail fast with {@link NullPointerException} and ambiguous date text
 * with {@link IllegalArgumentException}. Malformed argument objects complete the future with
 * {@link IllegalArgumentException}, rejected values (such as a YAS yield type outside 1–9) with
 * {@link BlpValidationException}, and request failures with the engine's exception ({@link
 * BlpRequestException}, ...). The ticker key-column deprecation warning applies as for requests.
 */
abstract class EngineRecipes extends EngineRequests {
  EngineRecipes(long handle, KeyColumn keyColumn) {
    super(handle, keyColumn);
  }

  // ── bqr ────────────────────────────────────────────────────────────────────────────────────────

  /**
   * Bloomberg Quote Request: dealer quotes with broker codes, from intraday ticks.
   *
   * @param ticker the security, for example a fixed-income ticker with a dealer pricing source such
   *     as {@code "/isin/US037833FB15@MSG1 Corp"}
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> bqr(String ticker) {
    return runBqr(ticker, BqrOptions.defaults(), Backend.TABLE);
  }

  /**
   * Bloomberg Quote Request: dealer quotes with broker codes, from intraday ticks.
   *
   * @param ticker the security, for example a fixed-income ticker with a dealer pricing source such
   *     as {@code "/isin/US037833FB15@MSG1 Corp"}
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> bqr(String ticker, BqrOptions options) {
    return runBqr(ticker, Objects.requireNonNull(options, "options"), Backend.TABLE);
  }

  /**
   * Bloomberg Quote Request: dealer quotes with broker codes, from intraday ticks.
   *
   * @param ticker the security, for example a fixed-income ticker with a dealer pricing source such
   *     as {@code "/isin/US037833FB15@MSG1 Corp"}
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> bqr(String ticker, Backend<T> backend) {
    return runBqr(ticker, BqrOptions.defaults(), Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Bloomberg Quote Request: dealer quotes with broker codes, from intraday ticks.
   *
   * <p>Rows are sorted by time; Bloomberg names become {@code event_type} , {@code price} , {@code
   * broker_buy} , {@code broker_sell} , {@code spread_price} , {@code condition_codes} and {@code
   * exchange} . Without datetimes the window is the last hour (end = now). Non-empty quotes without
   * broker codes fail with {@link BlpInternalException} unless {@link
   * BqrOptions.Builder#includeBrokerCodes(Boolean)} is {@code false} .
   *
   * @param ticker the security, for example a fixed-income ticker with a dealer pricing source such
   *     as {@code "/isin/US037833FB15@MSG1 Corp"}
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> bqr(String ticker, BqrOptions options, Backend<T> backend) {
    return runBqr(
        ticker,
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  private <T> CompletableFuture<T> runBqr(String ticker, BqrOptions options, Backend<T> backend) {
    return recipe("bqr", RecipeArgs.bqr(ticker, options), backend);
  }

  // ── yas ────────────────────────────────────────────────────────────────────────────────────────

  /**
   * Yield and spread analysis: a reference-data request with the YAS overrides of the options.
   *
   * @param tickers the securities
   * @param fields the YAS fields, for example {@code YAS_BOND_YLD}
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> yas(Collection<String> tickers, Collection<String> fields) {
    return runYas(
        RequestArgs.strings("tickers", tickers),
        RequestArgs.strings("fields", fields),
        YasOptions.defaults(),
        Backend.TABLE);
  }

  /**
   * Yield and spread analysis: a reference-data request with the YAS overrides of the options.
   *
   * @param tickers the securities
   * @param fields the YAS fields, for example {@code YAS_BOND_YLD}
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> yas(
      Collection<String> tickers, Collection<String> fields, YasOptions options) {
    return runYas(
        RequestArgs.strings("tickers", tickers),
        RequestArgs.strings("fields", fields),
        Objects.requireNonNull(options, "options"),
        Backend.TABLE);
  }

  /**
   * Yield and spread analysis: a reference-data request with the YAS overrides of the options.
   *
   * @param tickers the securities
   * @param fields the YAS fields, for example {@code YAS_BOND_YLD}
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> yas(
      Collection<String> tickers, Collection<String> fields, Backend<T> backend) {
    return runYas(
        RequestArgs.strings("tickers", tickers),
        RequestArgs.strings("fields", fields),
        YasOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Yield and spread analysis: a reference-data request with the YAS overrides of the options.
   *
   * <p>A yield type outside 1–9 fails with {@link BlpValidationException} before any request.
   *
   * @param tickers the securities
   * @param fields the YAS fields, for example {@code YAS_BOND_YLD}
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> yas(
      Collection<String> tickers,
      Collection<String> fields,
      YasOptions options,
      Backend<T> backend) {
    return runYas(
        RequestArgs.strings("tickers", tickers),
        RequestArgs.strings("fields", fields),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Yield and spread analysis: a reference-data request with the YAS overrides of the options.
   *
   * @param tickers the securities
   * @param field the YAS field
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> yas(Collection<String> tickers, String field) {
    return runYas(
        RequestArgs.strings("tickers", tickers),
        RequestArgs.single("field", field),
        YasOptions.defaults(),
        Backend.TABLE);
  }

  /**
   * Yield and spread analysis: a reference-data request with the YAS overrides of the options.
   *
   * @param tickers the securities
   * @param field the YAS field
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> yas(
      Collection<String> tickers, String field, YasOptions options) {
    return runYas(
        RequestArgs.strings("tickers", tickers),
        RequestArgs.single("field", field),
        Objects.requireNonNull(options, "options"),
        Backend.TABLE);
  }

  /**
   * Yield and spread analysis: a reference-data request with the YAS overrides of the options.
   *
   * @param tickers the securities
   * @param field the YAS field
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> yas(
      Collection<String> tickers, String field, Backend<T> backend) {
    return runYas(
        RequestArgs.strings("tickers", tickers),
        RequestArgs.single("field", field),
        YasOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Yield and spread analysis: a reference-data request with the YAS overrides of the options.
   *
   * <p>A yield type outside 1–9 fails with {@link BlpValidationException} before any request.
   *
   * @param tickers the securities
   * @param field the YAS field
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> yas(
      Collection<String> tickers, String field, YasOptions options, Backend<T> backend) {
    return runYas(
        RequestArgs.strings("tickers", tickers),
        RequestArgs.single("field", field),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Yield and spread analysis: a reference-data request with the YAS overrides of the options.
   *
   * @param ticker the security
   * @param fields the YAS fields, for example {@code YAS_BOND_YLD}
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> yas(String ticker, Collection<String> fields) {
    return runYas(
        RequestArgs.single("ticker", ticker),
        RequestArgs.strings("fields", fields),
        YasOptions.defaults(),
        Backend.TABLE);
  }

  /**
   * Yield and spread analysis: a reference-data request with the YAS overrides of the options.
   *
   * @param ticker the security
   * @param fields the YAS fields, for example {@code YAS_BOND_YLD}
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> yas(
      String ticker, Collection<String> fields, YasOptions options) {
    return runYas(
        RequestArgs.single("ticker", ticker),
        RequestArgs.strings("fields", fields),
        Objects.requireNonNull(options, "options"),
        Backend.TABLE);
  }

  /**
   * Yield and spread analysis: a reference-data request with the YAS overrides of the options.
   *
   * @param ticker the security
   * @param fields the YAS fields, for example {@code YAS_BOND_YLD}
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> yas(
      String ticker, Collection<String> fields, Backend<T> backend) {
    return runYas(
        RequestArgs.single("ticker", ticker),
        RequestArgs.strings("fields", fields),
        YasOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Yield and spread analysis: a reference-data request with the YAS overrides of the options.
   *
   * <p>A yield type outside 1–9 fails with {@link BlpValidationException} before any request.
   *
   * @param ticker the security
   * @param fields the YAS fields, for example {@code YAS_BOND_YLD}
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> yas(
      String ticker, Collection<String> fields, YasOptions options, Backend<T> backend) {
    return runYas(
        RequestArgs.single("ticker", ticker),
        RequestArgs.strings("fields", fields),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Yield and spread analysis: a reference-data request with the YAS overrides of the options.
   *
   * @param ticker the security
   * @param field the YAS field
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> yas(String ticker, String field) {
    return runYas(
        RequestArgs.single("ticker", ticker),
        RequestArgs.single("field", field),
        YasOptions.defaults(),
        Backend.TABLE);
  }

  /**
   * Yield and spread analysis: a reference-data request with the YAS overrides of the options.
   *
   * @param ticker the security
   * @param field the YAS field
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> yas(String ticker, String field, YasOptions options) {
    return runYas(
        RequestArgs.single("ticker", ticker),
        RequestArgs.single("field", field),
        Objects.requireNonNull(options, "options"),
        Backend.TABLE);
  }

  /**
   * Yield and spread analysis: a reference-data request with the YAS overrides of the options.
   *
   * @param ticker the security
   * @param field the YAS field
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> yas(String ticker, String field, Backend<T> backend) {
    return runYas(
        RequestArgs.single("ticker", ticker),
        RequestArgs.single("field", field),
        YasOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Yield and spread analysis: a reference-data request with the YAS overrides of the options.
   *
   * <p>A yield type outside 1–9 fails with {@link BlpValidationException} before any request.
   *
   * @param ticker the security
   * @param field the YAS field
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> yas(
      String ticker, String field, YasOptions options, Backend<T> backend) {
    return runYas(
        RequestArgs.single("ticker", ticker),
        RequestArgs.single("field", field),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  private <T> CompletableFuture<T> runYas(
      List<String> tickers, List<String> fields, YasOptions options, Backend<T> backend) {
    return recipe("yas", RecipeArgs.yas(tickers, fields, options), backend);
  }

  // ── preferreds ─────────────────────────────────────────────────────────────────────────────────

  /**
   * Preferred stocks issued by a company, via BQL.
   *
   * @param equityTicker the company equity ticker ({@code " US Equity"} is appended when it has no
   *     space)
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> preferreds(String equityTicker) {
    return runPreferreds(equityTicker, PreferredsOptions.defaults(), Backend.TABLE);
  }

  /**
   * Preferred stocks issued by a company, via BQL.
   *
   * @param equityTicker the company equity ticker ({@code " US Equity"} is appended when it has no
   *     space)
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> preferreds(String equityTicker, PreferredsOptions options) {
    return runPreferreds(equityTicker, Objects.requireNonNull(options, "options"), Backend.TABLE);
  }

  /**
   * Preferred stocks issued by a company, via BQL.
   *
   * @param equityTicker the company equity ticker ({@code " US Equity"} is appended when it has no
   *     space)
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> preferreds(String equityTicker, Backend<T> backend) {
    return runPreferreds(
        equityTicker, PreferredsOptions.defaults(), Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Preferred stocks issued by a company, via BQL.
   *
   * @param equityTicker the company equity ticker ({@code " US Equity"} is appended when it has no
   *     space)
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> preferreds(
      String equityTicker, PreferredsOptions options, Backend<T> backend) {
    return runPreferreds(
        equityTicker,
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  private <T> CompletableFuture<T> runPreferreds(
      String equityTicker, PreferredsOptions options, Backend<T> backend) {
    return recipe("preferreds", RecipeArgs.preferreds(equityTicker, options), backend);
  }

  // ── corporateBonds ─────────────────────────────────────────────────────────────────────────────

  /**
   * Corporate bonds issued by a company, via BQL.
   *
   * @param ticker the company equity ticker ({@code " US Equity"} is appended when it has no space)
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> corporateBonds(String ticker) {
    return runCorporateBonds(ticker, CorporateBondsOptions.defaults(), Backend.TABLE);
  }

  /**
   * Corporate bonds issued by a company, via BQL.
   *
   * @param ticker the company equity ticker ({@code " US Equity"} is appended when it has no space)
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> corporateBonds(String ticker, CorporateBondsOptions options) {
    return runCorporateBonds(ticker, Objects.requireNonNull(options, "options"), Backend.TABLE);
  }

  /**
   * Corporate bonds issued by a company, via BQL.
   *
   * @param ticker the company equity ticker ({@code " US Equity"} is appended when it has no space)
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> corporateBonds(String ticker, Backend<T> backend) {
    return runCorporateBonds(
        ticker, CorporateBondsOptions.defaults(), Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Corporate bonds issued by a company, via BQL.
   *
   * @param ticker the company equity ticker ({@code " US Equity"} is appended when it has no space)
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> corporateBonds(
      String ticker, CorporateBondsOptions options, Backend<T> backend) {
    return runCorporateBonds(
        ticker,
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  private <T> CompletableFuture<T> runCorporateBonds(
      String ticker, CorporateBondsOptions options, Backend<T> backend) {
    return recipe("corporateBonds", RecipeArgs.corporateBonds(ticker, options), backend);
  }

  // ── futTicker ──────────────────────────────────────────────────────────────────────────────────

  /**
   * Resolves a generic futures ticker to the specific contract for a date.
   *
   * @param genTicker the generic futures ticker, for example {@code "ES1 Index"}
   * @param dt the reference date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> futTicker(String genTicker, String dt) {
    return runFutTicker(
        genTicker, Dates.formatDate(dt), FuturesResolveOptions.defaults(), Backend.TABLE);
  }

  /**
   * Resolves a generic futures ticker to the specific contract for a date.
   *
   * @param genTicker the generic futures ticker, for example {@code "ES1 Index"}
   * @param dt the reference date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> futTicker(
      String genTicker, String dt, FuturesResolveOptions options) {
    return runFutTicker(
        genTicker, Dates.formatDate(dt), Objects.requireNonNull(options, "options"), Backend.TABLE);
  }

  /**
   * Resolves a generic futures ticker to the specific contract for a date.
   *
   * @param genTicker the generic futures ticker, for example {@code "ES1 Index"}
   * @param dt the reference date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> futTicker(String genTicker, String dt, Backend<T> backend) {
    return runFutTicker(
        genTicker,
        Dates.formatDate(dt),
        FuturesResolveOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Resolves a generic futures ticker to the specific contract for a date.
   *
   * <p>Returns one row with the {@code ticker} column. Contracts expiring on or before the date are
   * skipped.
   *
   * @param genTicker the generic futures ticker, for example {@code "ES1 Index"}
   * @param dt the reference date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> futTicker(
      String genTicker, String dt, FuturesResolveOptions options, Backend<T> backend) {
    return runFutTicker(
        genTicker,
        Dates.formatDate(dt),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Resolves a generic futures ticker to the specific contract for a date.
   *
   * @param genTicker the generic futures ticker, for example {@code "ES1 Index"}
   * @param dt the reference date
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> futTicker(String genTicker, TemporalAccessor dt) {
    return runFutTicker(
        genTicker, Dates.formatDate(dt), FuturesResolveOptions.defaults(), Backend.TABLE);
  }

  /**
   * Resolves a generic futures ticker to the specific contract for a date.
   *
   * @param genTicker the generic futures ticker, for example {@code "ES1 Index"}
   * @param dt the reference date
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> futTicker(
      String genTicker, TemporalAccessor dt, FuturesResolveOptions options) {
    return runFutTicker(
        genTicker, Dates.formatDate(dt), Objects.requireNonNull(options, "options"), Backend.TABLE);
  }

  /**
   * Resolves a generic futures ticker to the specific contract for a date.
   *
   * @param genTicker the generic futures ticker, for example {@code "ES1 Index"}
   * @param dt the reference date
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> futTicker(
      String genTicker, TemporalAccessor dt, Backend<T> backend) {
    return runFutTicker(
        genTicker,
        Dates.formatDate(dt),
        FuturesResolveOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Resolves a generic futures ticker to the specific contract for a date.
   *
   * <p>Returns one row with the {@code ticker} column. Contracts expiring on or before the date are
   * skipped.
   *
   * @param genTicker the generic futures ticker, for example {@code "ES1 Index"}
   * @param dt the reference date
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> futTicker(
      String genTicker, TemporalAccessor dt, FuturesResolveOptions options, Backend<T> backend) {
    return runFutTicker(
        genTicker,
        Dates.formatDate(dt),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  private <T> CompletableFuture<T> runFutTicker(
      String genTicker, String dt, FuturesResolveOptions options, Backend<T> backend) {
    return recipe("futTicker", RecipeArgs.futuresResolve(genTicker, dt, options), backend);
  }

  // ── activeFutures ──────────────────────────────────────────────────────────────────────────────

  /**
   * Resolves the most active futures contract around a date.
   *
   * @param genTicker the generic futures ticker, for example {@code "ES1 Index"}
   * @param dt the reference date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> activeFutures(String genTicker, String dt) {
    return runActiveFutures(
        genTicker, Dates.formatDate(dt), FuturesResolveOptions.defaults(), Backend.TABLE);
  }

  /**
   * Resolves the most active futures contract around a date.
   *
   * @param genTicker the generic futures ticker, for example {@code "ES1 Index"}
   * @param dt the reference date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> activeFutures(
      String genTicker, String dt, FuturesResolveOptions options) {
    return runActiveFutures(
        genTicker, Dates.formatDate(dt), Objects.requireNonNull(options, "options"), Backend.TABLE);
  }

  /**
   * Resolves the most active futures contract around a date.
   *
   * @param genTicker the generic futures ticker, for example {@code "ES1 Index"}
   * @param dt the reference date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> activeFutures(String genTicker, String dt, Backend<T> backend) {
    return runActiveFutures(
        genTicker,
        Dates.formatDate(dt),
        FuturesResolveOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Resolves the most active futures contract around a date.
   *
   * <p>Within the front contract's maturity month the contract with the higher recent volume wins;
   * returns one {@code ticker} row.
   *
   * @param genTicker the generic futures ticker, for example {@code "ES1 Index"}
   * @param dt the reference date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> activeFutures(
      String genTicker, String dt, FuturesResolveOptions options, Backend<T> backend) {
    return runActiveFutures(
        genTicker,
        Dates.formatDate(dt),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Resolves the most active futures contract around a date.
   *
   * @param genTicker the generic futures ticker, for example {@code "ES1 Index"}
   * @param dt the reference date
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> activeFutures(String genTicker, TemporalAccessor dt) {
    return runActiveFutures(
        genTicker, Dates.formatDate(dt), FuturesResolveOptions.defaults(), Backend.TABLE);
  }

  /**
   * Resolves the most active futures contract around a date.
   *
   * @param genTicker the generic futures ticker, for example {@code "ES1 Index"}
   * @param dt the reference date
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> activeFutures(
      String genTicker, TemporalAccessor dt, FuturesResolveOptions options) {
    return runActiveFutures(
        genTicker, Dates.formatDate(dt), Objects.requireNonNull(options, "options"), Backend.TABLE);
  }

  /**
   * Resolves the most active futures contract around a date.
   *
   * @param genTicker the generic futures ticker, for example {@code "ES1 Index"}
   * @param dt the reference date
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> activeFutures(
      String genTicker, TemporalAccessor dt, Backend<T> backend) {
    return runActiveFutures(
        genTicker,
        Dates.formatDate(dt),
        FuturesResolveOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Resolves the most active futures contract around a date.
   *
   * <p>Within the front contract's maturity month the contract with the higher recent volume wins;
   * returns one {@code ticker} row.
   *
   * @param genTicker the generic futures ticker, for example {@code "ES1 Index"}
   * @param dt the reference date
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> activeFutures(
      String genTicker, TemporalAccessor dt, FuturesResolveOptions options, Backend<T> backend) {
    return runActiveFutures(
        genTicker,
        Dates.formatDate(dt),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  private <T> CompletableFuture<T> runActiveFutures(
      String genTicker, String dt, FuturesResolveOptions options, Backend<T> backend) {
    return recipe("activeFutures", RecipeArgs.futuresResolve(genTicker, dt, options), backend);
  }

  // ── futuresCurve ───────────────────────────────────────────────────────────────────────────────

  /**
   * The futures chain with contract metadata, mid price and annualized carry.
   *
   * @param genTicker the generic futures ticker, for example {@code "CL1 Comdty"}
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> futuresCurve(String genTicker) {
    return runFuturesCurve(genTicker, FuturesCurveOptions.defaults(), Backend.TABLE);
  }

  /**
   * The futures chain with contract metadata, mid price and annualized carry.
   *
   * @param genTicker the generic futures ticker, for example {@code "CL1 Comdty"}
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> futuresCurve(String genTicker, FuturesCurveOptions options) {
    return runFuturesCurve(genTicker, Objects.requireNonNull(options, "options"), Backend.TABLE);
  }

  /**
   * The futures chain with contract metadata, mid price and annualized carry.
   *
   * @param genTicker the generic futures ticker, for example {@code "CL1 Comdty"}
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> futuresCurve(String genTicker, Backend<T> backend) {
    return runFuturesCurve(
        genTicker, FuturesCurveOptions.defaults(), Objects.requireNonNull(backend, "backend"));
  }

  /**
   * The futures chain with contract metadata, mid price and annualized carry.
   *
   * @param genTicker the generic futures ticker, for example {@code "CL1 Comdty"}
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> futuresCurve(
      String genTicker, FuturesCurveOptions options, Backend<T> backend) {
    return runFuturesCurve(
        genTicker,
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  private <T> CompletableFuture<T> runFuturesCurve(
      String genTicker, FuturesCurveOptions options, Backend<T> backend) {
    return recipe("futuresCurve", RecipeArgs.futuresCurve(genTicker, options), backend);
  }

  // ── cdxTicker ──────────────────────────────────────────────────────────────────────────────────

  /**
   * Resolves a generic CDX ticker to the series that applies on a date.
   *
   * @param genTicker the generic CDX ticker, for example {@code "CDX IG CDSI GEN 5Y Corp"}
   * @param dt the reference date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> cdxTicker(String genTicker, String dt) {
    return runCdxTicker(
        genTicker, Dates.formatDate(dt), CdxResolveOptions.defaults(), Backend.TABLE);
  }

  /**
   * Resolves a generic CDX ticker to the series that applies on a date.
   *
   * @param genTicker the generic CDX ticker, for example {@code "CDX IG CDSI GEN 5Y Corp"}
   * @param dt the reference date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> cdxTicker(
      String genTicker, String dt, CdxResolveOptions options) {
    return runCdxTicker(
        genTicker, Dates.formatDate(dt), Objects.requireNonNull(options, "options"), Backend.TABLE);
  }

  /**
   * Resolves a generic CDX ticker to the series that applies on a date.
   *
   * @param genTicker the generic CDX ticker, for example {@code "CDX IG CDSI GEN 5Y Corp"}
   * @param dt the reference date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> cdxTicker(String genTicker, String dt, Backend<T> backend) {
    return runCdxTicker(
        genTicker,
        Dates.formatDate(dt),
        CdxResolveOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Resolves a generic CDX ticker to the series that applies on a date.
   *
   * <p>The series is the highest one whose {@code CDS_FIRST_ACCRUAL_START_DATE} is on or before the
   * date; the {@code Vn} token is its latest version.
   *
   * @param genTicker the generic CDX ticker, for example {@code "CDX IG CDSI GEN 5Y Corp"}
   * @param dt the reference date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> cdxTicker(
      String genTicker, String dt, CdxResolveOptions options, Backend<T> backend) {
    return runCdxTicker(
        genTicker,
        Dates.formatDate(dt),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Resolves a generic CDX ticker to the series that applies on a date.
   *
   * @param genTicker the generic CDX ticker, for example {@code "CDX IG CDSI GEN 5Y Corp"}
   * @param dt the reference date
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> cdxTicker(String genTicker, TemporalAccessor dt) {
    return runCdxTicker(
        genTicker, Dates.formatDate(dt), CdxResolveOptions.defaults(), Backend.TABLE);
  }

  /**
   * Resolves a generic CDX ticker to the series that applies on a date.
   *
   * @param genTicker the generic CDX ticker, for example {@code "CDX IG CDSI GEN 5Y Corp"}
   * @param dt the reference date
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> cdxTicker(
      String genTicker, TemporalAccessor dt, CdxResolveOptions options) {
    return runCdxTicker(
        genTicker, Dates.formatDate(dt), Objects.requireNonNull(options, "options"), Backend.TABLE);
  }

  /**
   * Resolves a generic CDX ticker to the series that applies on a date.
   *
   * @param genTicker the generic CDX ticker, for example {@code "CDX IG CDSI GEN 5Y Corp"}
   * @param dt the reference date
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> cdxTicker(
      String genTicker, TemporalAccessor dt, Backend<T> backend) {
    return runCdxTicker(
        genTicker,
        Dates.formatDate(dt),
        CdxResolveOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Resolves a generic CDX ticker to the series that applies on a date.
   *
   * <p>The series is the highest one whose {@code CDS_FIRST_ACCRUAL_START_DATE} is on or before the
   * date; the {@code Vn} token is its latest version.
   *
   * @param genTicker the generic CDX ticker, for example {@code "CDX IG CDSI GEN 5Y Corp"}
   * @param dt the reference date
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> cdxTicker(
      String genTicker, TemporalAccessor dt, CdxResolveOptions options, Backend<T> backend) {
    return runCdxTicker(
        genTicker,
        Dates.formatDate(dt),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  private <T> CompletableFuture<T> runCdxTicker(
      String genTicker, String dt, CdxResolveOptions options, Backend<T> backend) {
    return recipe("cdxTicker", RecipeArgs.cdxTicker(genTicker, dt, options), backend);
  }

  // ── activeCdx ──────────────────────────────────────────────────────────────────────────────────

  /**
   * Resolves the latest CDX series that had started and traded by a date.
   *
   * @param genTicker the generic CDX ticker, for example {@code "CDX IG CDSI GEN 5Y Corp"}
   * @param dt the reference date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> activeCdx(String genTicker, String dt) {
    return runActiveCdx(
        genTicker, Dates.formatDate(dt), ActiveCdxOptions.defaults(), Backend.TABLE);
  }

  /**
   * Resolves the latest CDX series that had started and traded by a date.
   *
   * @param genTicker the generic CDX ticker, for example {@code "CDX IG CDSI GEN 5Y Corp"}
   * @param dt the reference date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> activeCdx(String genTicker, String dt, ActiveCdxOptions options) {
    return runActiveCdx(
        genTicker, Dates.formatDate(dt), Objects.requireNonNull(options, "options"), Backend.TABLE);
  }

  /**
   * Resolves the latest CDX series that had started and traded by a date.
   *
   * @param genTicker the generic CDX ticker, for example {@code "CDX IG CDSI GEN 5Y Corp"}
   * @param dt the reference date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> activeCdx(String genTicker, String dt, Backend<T> backend) {
    return runActiveCdx(
        genTicker,
        Dates.formatDate(dt),
        ActiveCdxOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Resolves the latest CDX series that had started and traded by a date.
   *
   * <p>Matches {@code cdxTicker} except between a roll and the new series' first print, when the
   * preceding series is still the traded one.
   *
   * @param genTicker the generic CDX ticker, for example {@code "CDX IG CDSI GEN 5Y Corp"}
   * @param dt the reference date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> activeCdx(
      String genTicker, String dt, ActiveCdxOptions options, Backend<T> backend) {
    return runActiveCdx(
        genTicker,
        Dates.formatDate(dt),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Resolves the latest CDX series that had started and traded by a date.
   *
   * @param genTicker the generic CDX ticker, for example {@code "CDX IG CDSI GEN 5Y Corp"}
   * @param dt the reference date
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> activeCdx(String genTicker, TemporalAccessor dt) {
    return runActiveCdx(
        genTicker, Dates.formatDate(dt), ActiveCdxOptions.defaults(), Backend.TABLE);
  }

  /**
   * Resolves the latest CDX series that had started and traded by a date.
   *
   * @param genTicker the generic CDX ticker, for example {@code "CDX IG CDSI GEN 5Y Corp"}
   * @param dt the reference date
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> activeCdx(
      String genTicker, TemporalAccessor dt, ActiveCdxOptions options) {
    return runActiveCdx(
        genTicker, Dates.formatDate(dt), Objects.requireNonNull(options, "options"), Backend.TABLE);
  }

  /**
   * Resolves the latest CDX series that had started and traded by a date.
   *
   * @param genTicker the generic CDX ticker, for example {@code "CDX IG CDSI GEN 5Y Corp"}
   * @param dt the reference date
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> activeCdx(
      String genTicker, TemporalAccessor dt, Backend<T> backend) {
    return runActiveCdx(
        genTicker,
        Dates.formatDate(dt),
        ActiveCdxOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Resolves the latest CDX series that had started and traded by a date.
   *
   * <p>Matches {@code cdxTicker} except between a roll and the new series' first print, when the
   * preceding series is still the traded one.
   *
   * @param genTicker the generic CDX ticker, for example {@code "CDX IG CDSI GEN 5Y Corp"}
   * @param dt the reference date
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> activeCdx(
      String genTicker, TemporalAccessor dt, ActiveCdxOptions options, Backend<T> backend) {
    return runActiveCdx(
        genTicker,
        Dates.formatDate(dt),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  private <T> CompletableFuture<T> runActiveCdx(
      String genTicker, String dt, ActiveCdxOptions options, Backend<T> backend) {
    return recipe("activeCdx", RecipeArgs.activeCdx(genTicker, dt, options), backend);
  }

  // ── dividend ───────────────────────────────────────────────────────────────────────────────────

  /**
   * Dividend or split history (Bloomberg bulk data).
   *
   * @param tickers the securities
   * @param startDate the first ex-date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null}
   *     leaves the start open
   * @param endDate the last ex-date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null}
   *     leaves the end open
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> dividend(
      Collection<String> tickers, String startDate, String endDate) {
    return runDividend(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        DividendOptions.defaults(),
        Backend.TABLE);
  }

  /**
   * Dividend or split history (Bloomberg bulk data).
   *
   * @param tickers the securities
   * @param startDate the first ex-date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null}
   *     leaves the start open
   * @param endDate the last ex-date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null}
   *     leaves the end open
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> dividend(
      Collection<String> tickers, String startDate, String endDate, DividendOptions options) {
    return runDividend(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Backend.TABLE);
  }

  /**
   * Dividend or split history (Bloomberg bulk data).
   *
   * @param tickers the securities
   * @param startDate the first ex-date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null}
   *     leaves the start open
   * @param endDate the last ex-date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null}
   *     leaves the end open
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> dividend(
      Collection<String> tickers, String startDate, String endDate, Backend<T> backend) {
    return runDividend(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        DividendOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Dividend or split history (Bloomberg bulk data).
   *
   * @param tickers the securities
   * @param startDate the first ex-date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null}
   *     leaves the start open
   * @param endDate the last ex-date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null}
   *     leaves the end open
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> dividend(
      Collection<String> tickers,
      String startDate,
      String endDate,
      DividendOptions options,
      Backend<T> backend) {
    return runDividend(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Dividend or split history (Bloomberg bulk data).
   *
   * @param tickers the securities
   * @param startDate the first ex-date; {@code null} leaves the start open
   * @param endDate the last ex-date; {@code null} leaves the end open
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> dividend(
      Collection<String> tickers, TemporalAccessor startDate, TemporalAccessor endDate) {
    return runDividend(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        DividendOptions.defaults(),
        Backend.TABLE);
  }

  /**
   * Dividend or split history (Bloomberg bulk data).
   *
   * @param tickers the securities
   * @param startDate the first ex-date; {@code null} leaves the start open
   * @param endDate the last ex-date; {@code null} leaves the end open
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> dividend(
      Collection<String> tickers,
      TemporalAccessor startDate,
      TemporalAccessor endDate,
      DividendOptions options) {
    return runDividend(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Backend.TABLE);
  }

  /**
   * Dividend or split history (Bloomberg bulk data).
   *
   * @param tickers the securities
   * @param startDate the first ex-date; {@code null} leaves the start open
   * @param endDate the last ex-date; {@code null} leaves the end open
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> dividend(
      Collection<String> tickers,
      TemporalAccessor startDate,
      TemporalAccessor endDate,
      Backend<T> backend) {
    return runDividend(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        DividendOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Dividend or split history (Bloomberg bulk data).
   *
   * @param tickers the securities
   * @param startDate the first ex-date; {@code null} leaves the start open
   * @param endDate the last ex-date; {@code null} leaves the end open
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> dividend(
      Collection<String> tickers,
      TemporalAccessor startDate,
      TemporalAccessor endDate,
      DividendOptions options,
      Backend<T> backend) {
    return runDividend(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Dividend or split history (Bloomberg bulk data).
   *
   * @param ticker the security
   * @param startDate the first ex-date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null}
   *     leaves the start open
   * @param endDate the last ex-date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null}
   *     leaves the end open
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> dividend(String ticker, String startDate, String endDate) {
    return runDividend(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        DividendOptions.defaults(),
        Backend.TABLE);
  }

  /**
   * Dividend or split history (Bloomberg bulk data).
   *
   * @param ticker the security
   * @param startDate the first ex-date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null}
   *     leaves the start open
   * @param endDate the last ex-date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null}
   *     leaves the end open
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> dividend(
      String ticker, String startDate, String endDate, DividendOptions options) {
    return runDividend(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Backend.TABLE);
  }

  /**
   * Dividend or split history (Bloomberg bulk data).
   *
   * @param ticker the security
   * @param startDate the first ex-date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null}
   *     leaves the start open
   * @param endDate the last ex-date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null}
   *     leaves the end open
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> dividend(
      String ticker, String startDate, String endDate, Backend<T> backend) {
    return runDividend(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        DividendOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Dividend or split history (Bloomberg bulk data).
   *
   * @param ticker the security
   * @param startDate the first ex-date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null}
   *     leaves the start open
   * @param endDate the last ex-date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null}
   *     leaves the end open
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> dividend(
      String ticker,
      String startDate,
      String endDate,
      DividendOptions options,
      Backend<T> backend) {
    return runDividend(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Dividend or split history (Bloomberg bulk data).
   *
   * @param ticker the security
   * @param startDate the first ex-date; {@code null} leaves the start open
   * @param endDate the last ex-date; {@code null} leaves the end open
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> dividend(
      String ticker, TemporalAccessor startDate, TemporalAccessor endDate) {
    return runDividend(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        DividendOptions.defaults(),
        Backend.TABLE);
  }

  /**
   * Dividend or split history (Bloomberg bulk data).
   *
   * @param ticker the security
   * @param startDate the first ex-date; {@code null} leaves the start open
   * @param endDate the last ex-date; {@code null} leaves the end open
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> dividend(
      String ticker,
      TemporalAccessor startDate,
      TemporalAccessor endDate,
      DividendOptions options) {
    return runDividend(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Backend.TABLE);
  }

  /**
   * Dividend or split history (Bloomberg bulk data).
   *
   * @param ticker the security
   * @param startDate the first ex-date; {@code null} leaves the start open
   * @param endDate the last ex-date; {@code null} leaves the end open
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> dividend(
      String ticker, TemporalAccessor startDate, TemporalAccessor endDate, Backend<T> backend) {
    return runDividend(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        DividendOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Dividend or split history (Bloomberg bulk data).
   *
   * @param ticker the security
   * @param startDate the first ex-date; {@code null} leaves the start open
   * @param endDate the last ex-date; {@code null} leaves the end open
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> dividend(
      String ticker,
      TemporalAccessor startDate,
      TemporalAccessor endDate,
      DividendOptions options,
      Backend<T> backend) {
    return runDividend(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  private <T> CompletableFuture<T> runDividend(
      List<String> tickers,
      String startDate,
      String endDate,
      DividendOptions options,
      Backend<T> backend) {
    return recipe("dividend", RecipeArgs.dividend(tickers, startDate, endDate, options), backend);
  }

  // ── dividendYield ──────────────────────────────────────────────────────────────────────────────

  /**
   * Trailing realized dividend amount and trailing dividend yield per date.
   *
   * @param tickers the securities
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> dividendYield(
      Collection<String> tickers, String startDate, String endDate) {
    return runDividendYield(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        DividendYieldOptions.defaults(),
        Backend.TABLE);
  }

  /**
   * Trailing realized dividend amount and trailing dividend yield per date.
   *
   * @param tickers the securities
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> dividendYield(
      Collection<String> tickers, String startDate, String endDate, DividendYieldOptions options) {
    return runDividendYield(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Backend.TABLE);
  }

  /**
   * Trailing realized dividend amount and trailing dividend yield per date.
   *
   * @param tickers the securities
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> dividendYield(
      Collection<String> tickers, String startDate, String endDate, Backend<T> backend) {
    return runDividendYield(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        DividendYieldOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Trailing realized dividend amount and trailing dividend yield per date.
   *
   * <p>An end date before the start date fails with {@link BlpValidationException} .
   *
   * @param tickers the securities
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> dividendYield(
      Collection<String> tickers,
      String startDate,
      String endDate,
      DividendYieldOptions options,
      Backend<T> backend) {
    return runDividendYield(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Trailing realized dividend amount and trailing dividend yield per date.
   *
   * @param tickers the securities
   * @param startDate the first date
   * @param endDate the last date
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> dividendYield(
      Collection<String> tickers, TemporalAccessor startDate, TemporalAccessor endDate) {
    return runDividendYield(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        DividendYieldOptions.defaults(),
        Backend.TABLE);
  }

  /**
   * Trailing realized dividend amount and trailing dividend yield per date.
   *
   * @param tickers the securities
   * @param startDate the first date
   * @param endDate the last date
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> dividendYield(
      Collection<String> tickers,
      TemporalAccessor startDate,
      TemporalAccessor endDate,
      DividendYieldOptions options) {
    return runDividendYield(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Backend.TABLE);
  }

  /**
   * Trailing realized dividend amount and trailing dividend yield per date.
   *
   * @param tickers the securities
   * @param startDate the first date
   * @param endDate the last date
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> dividendYield(
      Collection<String> tickers,
      TemporalAccessor startDate,
      TemporalAccessor endDate,
      Backend<T> backend) {
    return runDividendYield(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        DividendYieldOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Trailing realized dividend amount and trailing dividend yield per date.
   *
   * <p>An end date before the start date fails with {@link BlpValidationException} .
   *
   * @param tickers the securities
   * @param startDate the first date
   * @param endDate the last date
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> dividendYield(
      Collection<String> tickers,
      TemporalAccessor startDate,
      TemporalAccessor endDate,
      DividendYieldOptions options,
      Backend<T> backend) {
    return runDividendYield(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Trailing realized dividend amount and trailing dividend yield per date.
   *
   * @param ticker the security
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> dividendYield(String ticker, String startDate, String endDate) {
    return runDividendYield(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        DividendYieldOptions.defaults(),
        Backend.TABLE);
  }

  /**
   * Trailing realized dividend amount and trailing dividend yield per date.
   *
   * @param ticker the security
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> dividendYield(
      String ticker, String startDate, String endDate, DividendYieldOptions options) {
    return runDividendYield(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Backend.TABLE);
  }

  /**
   * Trailing realized dividend amount and trailing dividend yield per date.
   *
   * @param ticker the security
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> dividendYield(
      String ticker, String startDate, String endDate, Backend<T> backend) {
    return runDividendYield(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        DividendYieldOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Trailing realized dividend amount and trailing dividend yield per date.
   *
   * <p>An end date before the start date fails with {@link BlpValidationException} .
   *
   * @param ticker the security
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> dividendYield(
      String ticker,
      String startDate,
      String endDate,
      DividendYieldOptions options,
      Backend<T> backend) {
    return runDividendYield(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Trailing realized dividend amount and trailing dividend yield per date.
   *
   * @param ticker the security
   * @param startDate the first date
   * @param endDate the last date
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> dividendYield(
      String ticker, TemporalAccessor startDate, TemporalAccessor endDate) {
    return runDividendYield(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        DividendYieldOptions.defaults(),
        Backend.TABLE);
  }

  /**
   * Trailing realized dividend amount and trailing dividend yield per date.
   *
   * @param ticker the security
   * @param startDate the first date
   * @param endDate the last date
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> dividendYield(
      String ticker,
      TemporalAccessor startDate,
      TemporalAccessor endDate,
      DividendYieldOptions options) {
    return runDividendYield(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Backend.TABLE);
  }

  /**
   * Trailing realized dividend amount and trailing dividend yield per date.
   *
   * @param ticker the security
   * @param startDate the first date
   * @param endDate the last date
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> dividendYield(
      String ticker, TemporalAccessor startDate, TemporalAccessor endDate, Backend<T> backend) {
    return runDividendYield(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        DividendYieldOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Trailing realized dividend amount and trailing dividend yield per date.
   *
   * <p>An end date before the start date fails with {@link BlpValidationException} .
   *
   * @param ticker the security
   * @param startDate the first date
   * @param endDate the last date
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> dividendYield(
      String ticker,
      TemporalAccessor startDate,
      TemporalAccessor endDate,
      DividendYieldOptions options,
      Backend<T> backend) {
    return runDividendYield(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  private <T> CompletableFuture<T> runDividendYield(
      List<String> tickers,
      String startDate,
      String endDate,
      DividendYieldOptions options,
      Backend<T> backend) {
    return recipe(
        "dividendYield", RecipeArgs.dividendYield(tickers, startDate, endDate, options), backend);
  }

  // ── turnover ───────────────────────────────────────────────────────────────────────────────────

  /**
   * Daily turnover, falling back to volume × VWAP for securities without turnover data.
   *
   * @param tickers the securities
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null}
   *     means 30 days before the end
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null} means
   *     yesterday
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> turnover(
      Collection<String> tickers, String startDate, String endDate) {
    return runTurnover(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        TurnoverOptions.defaults(),
        Backend.TABLE);
  }

  /**
   * Daily turnover, falling back to volume × VWAP for securities without turnover data.
   *
   * @param tickers the securities
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null}
   *     means 30 days before the end
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null} means
   *     yesterday
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> turnover(
      Collection<String> tickers, String startDate, String endDate, TurnoverOptions options) {
    return runTurnover(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Backend.TABLE);
  }

  /**
   * Daily turnover, falling back to volume × VWAP for securities without turnover data.
   *
   * @param tickers the securities
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null}
   *     means 30 days before the end
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null} means
   *     yesterday
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> turnover(
      Collection<String> tickers, String startDate, String endDate, Backend<T> backend) {
    return runTurnover(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        TurnoverOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Daily turnover, falling back to volume × VWAP for securities without turnover data.
   *
   * @param tickers the securities
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null}
   *     means 30 days before the end
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null} means
   *     yesterday
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> turnover(
      Collection<String> tickers,
      String startDate,
      String endDate,
      TurnoverOptions options,
      Backend<T> backend) {
    return runTurnover(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Daily turnover, falling back to volume × VWAP for securities without turnover data.
   *
   * @param tickers the securities
   * @param startDate the first date; {@code null} means 30 days before the end
   * @param endDate the last date; {@code null} means yesterday
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> turnover(
      Collection<String> tickers, TemporalAccessor startDate, TemporalAccessor endDate) {
    return runTurnover(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        TurnoverOptions.defaults(),
        Backend.TABLE);
  }

  /**
   * Daily turnover, falling back to volume × VWAP for securities without turnover data.
   *
   * @param tickers the securities
   * @param startDate the first date; {@code null} means 30 days before the end
   * @param endDate the last date; {@code null} means yesterday
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> turnover(
      Collection<String> tickers,
      TemporalAccessor startDate,
      TemporalAccessor endDate,
      TurnoverOptions options) {
    return runTurnover(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Backend.TABLE);
  }

  /**
   * Daily turnover, falling back to volume × VWAP for securities without turnover data.
   *
   * @param tickers the securities
   * @param startDate the first date; {@code null} means 30 days before the end
   * @param endDate the last date; {@code null} means yesterday
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> turnover(
      Collection<String> tickers,
      TemporalAccessor startDate,
      TemporalAccessor endDate,
      Backend<T> backend) {
    return runTurnover(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        TurnoverOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Daily turnover, falling back to volume × VWAP for securities without turnover data.
   *
   * @param tickers the securities
   * @param startDate the first date; {@code null} means 30 days before the end
   * @param endDate the last date; {@code null} means yesterday
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> turnover(
      Collection<String> tickers,
      TemporalAccessor startDate,
      TemporalAccessor endDate,
      TurnoverOptions options,
      Backend<T> backend) {
    return runTurnover(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Daily turnover, falling back to volume × VWAP for securities without turnover data.
   *
   * @param ticker the security
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null}
   *     means 30 days before the end
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null} means
   *     yesterday
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> turnover(String ticker, String startDate, String endDate) {
    return runTurnover(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        TurnoverOptions.defaults(),
        Backend.TABLE);
  }

  /**
   * Daily turnover, falling back to volume × VWAP for securities without turnover data.
   *
   * @param ticker the security
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null}
   *     means 30 days before the end
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null} means
   *     yesterday
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> turnover(
      String ticker, String startDate, String endDate, TurnoverOptions options) {
    return runTurnover(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Backend.TABLE);
  }

  /**
   * Daily turnover, falling back to volume × VWAP for securities without turnover data.
   *
   * @param ticker the security
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null}
   *     means 30 days before the end
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null} means
   *     yesterday
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> turnover(
      String ticker, String startDate, String endDate, Backend<T> backend) {
    return runTurnover(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        TurnoverOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Daily turnover, falling back to volume × VWAP for securities without turnover data.
   *
   * @param ticker the security
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null}
   *     means 30 days before the end
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text); {@code null} means
   *     yesterday
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> turnover(
      String ticker,
      String startDate,
      String endDate,
      TurnoverOptions options,
      Backend<T> backend) {
    return runTurnover(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Daily turnover, falling back to volume × VWAP for securities without turnover data.
   *
   * @param ticker the security
   * @param startDate the first date; {@code null} means 30 days before the end
   * @param endDate the last date; {@code null} means yesterday
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> turnover(
      String ticker, TemporalAccessor startDate, TemporalAccessor endDate) {
    return runTurnover(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        TurnoverOptions.defaults(),
        Backend.TABLE);
  }

  /**
   * Daily turnover, falling back to volume × VWAP for securities without turnover data.
   *
   * @param ticker the security
   * @param startDate the first date; {@code null} means 30 days before the end
   * @param endDate the last date; {@code null} means yesterday
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> turnover(
      String ticker,
      TemporalAccessor startDate,
      TemporalAccessor endDate,
      TurnoverOptions options) {
    return runTurnover(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Backend.TABLE);
  }

  /**
   * Daily turnover, falling back to volume × VWAP for securities without turnover data.
   *
   * @param ticker the security
   * @param startDate the first date; {@code null} means 30 days before the end
   * @param endDate the last date; {@code null} means yesterday
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> turnover(
      String ticker, TemporalAccessor startDate, TemporalAccessor endDate, Backend<T> backend) {
    return runTurnover(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        TurnoverOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Daily turnover, falling back to volume × VWAP for securities without turnover data.
   *
   * @param ticker the security
   * @param startDate the first date; {@code null} means 30 days before the end
   * @param endDate the last date; {@code null} means yesterday
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> turnover(
      String ticker,
      TemporalAccessor startDate,
      TemporalAccessor endDate,
      TurnoverOptions options,
      Backend<T> backend) {
    return runTurnover(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  private <T> CompletableFuture<T> runTurnover(
      List<String> tickers,
      String startDate,
      String endDate,
      TurnoverOptions options,
      Backend<T> backend) {
    return recipe("turnover", RecipeArgs.turnover(tickers, startDate, endDate, options), backend);
  }

  // ── etfHoldings ────────────────────────────────────────────────────────────────────────────────

  /**
   * ETF constituent holdings (ISIN, weight, position), via BQL.
   *
   * @param etfTicker the ETF ticker ({@code " US Equity"} is appended when it has no space)
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> etfHoldings(String etfTicker) {
    return runEtfHoldings(etfTicker, EtfHoldingsOptions.defaults(), Backend.TABLE);
  }

  /**
   * ETF constituent holdings (ISIN, weight, position), via BQL.
   *
   * @param etfTicker the ETF ticker ({@code " US Equity"} is appended when it has no space)
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> etfHoldings(String etfTicker, EtfHoldingsOptions options) {
    return runEtfHoldings(etfTicker, Objects.requireNonNull(options, "options"), Backend.TABLE);
  }

  /**
   * ETF constituent holdings (ISIN, weight, position), via BQL.
   *
   * @param etfTicker the ETF ticker ({@code " US Equity"} is appended when it has no space)
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> etfHoldings(String etfTicker, Backend<T> backend) {
    return runEtfHoldings(
        etfTicker, EtfHoldingsOptions.defaults(), Objects.requireNonNull(backend, "backend"));
  }

  /**
   * ETF constituent holdings (ISIN, weight, position), via BQL.
   *
   * @param etfTicker the ETF ticker ({@code " US Equity"} is appended when it has no space)
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> etfHoldings(
      String etfTicker, EtfHoldingsOptions options, Backend<T> backend) {
    return runEtfHoldings(
        etfTicker,
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  private <T> CompletableFuture<T> runEtfHoldings(
      String etfTicker, EtfHoldingsOptions options, Backend<T> backend) {
    return recipe("etfHoldings", RecipeArgs.etfHoldings(etfTicker, options), backend);
  }

  // ── volSurface ─────────────────────────────────────────────────────────────────────────────────

  /**
   * A tidy historical implied-volatility surface: one row per security, date and surface point.
   *
   * @param tickers the underlyings
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> volSurface(
      Collection<String> tickers, String startDate, String endDate) {
    return runVolSurface(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        VolSurfaceOptions.defaults(),
        Backend.TABLE);
  }

  /**
   * A tidy historical implied-volatility surface: one row per security, date and surface point.
   *
   * @param tickers the underlyings
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> volSurface(
      Collection<String> tickers, String startDate, String endDate, VolSurfaceOptions options) {
    return runVolSurface(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Backend.TABLE);
  }

  /**
   * A tidy historical implied-volatility surface: one row per security, date and surface point.
   *
   * @param tickers the underlyings
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> volSurface(
      Collection<String> tickers, String startDate, String endDate, Backend<T> backend) {
    return runVolSurface(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        VolSurfaceOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * A tidy historical implied-volatility surface: one row per security, date and surface point.
   *
   * @param tickers the underlyings
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> volSurface(
      Collection<String> tickers,
      String startDate,
      String endDate,
      VolSurfaceOptions options,
      Backend<T> backend) {
    return runVolSurface(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * A tidy historical implied-volatility surface: one row per security, date and surface point.
   *
   * @param tickers the underlyings
   * @param startDate the first date
   * @param endDate the last date
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> volSurface(
      Collection<String> tickers, TemporalAccessor startDate, TemporalAccessor endDate) {
    return runVolSurface(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        VolSurfaceOptions.defaults(),
        Backend.TABLE);
  }

  /**
   * A tidy historical implied-volatility surface: one row per security, date and surface point.
   *
   * @param tickers the underlyings
   * @param startDate the first date
   * @param endDate the last date
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> volSurface(
      Collection<String> tickers,
      TemporalAccessor startDate,
      TemporalAccessor endDate,
      VolSurfaceOptions options) {
    return runVolSurface(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Backend.TABLE);
  }

  /**
   * A tidy historical implied-volatility surface: one row per security, date and surface point.
   *
   * @param tickers the underlyings
   * @param startDate the first date
   * @param endDate the last date
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> volSurface(
      Collection<String> tickers,
      TemporalAccessor startDate,
      TemporalAccessor endDate,
      Backend<T> backend) {
    return runVolSurface(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        VolSurfaceOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * A tidy historical implied-volatility surface: one row per security, date and surface point.
   *
   * @param tickers the underlyings
   * @param startDate the first date
   * @param endDate the last date
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> volSurface(
      Collection<String> tickers,
      TemporalAccessor startDate,
      TemporalAccessor endDate,
      VolSurfaceOptions options,
      Backend<T> backend) {
    return runVolSurface(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * A tidy historical implied-volatility surface: one row per security, date and surface point.
   *
   * @param ticker the underlying
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> volSurface(String ticker, String startDate, String endDate) {
    return runVolSurface(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        VolSurfaceOptions.defaults(),
        Backend.TABLE);
  }

  /**
   * A tidy historical implied-volatility surface: one row per security, date and surface point.
   *
   * @param ticker the underlying
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> volSurface(
      String ticker, String startDate, String endDate, VolSurfaceOptions options) {
    return runVolSurface(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Backend.TABLE);
  }

  /**
   * A tidy historical implied-volatility surface: one row per security, date and surface point.
   *
   * @param ticker the underlying
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> volSurface(
      String ticker, String startDate, String endDate, Backend<T> backend) {
    return runVolSurface(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        VolSurfaceOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * A tidy historical implied-volatility surface: one row per security, date and surface point.
   *
   * @param ticker the underlying
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> volSurface(
      String ticker,
      String startDate,
      String endDate,
      VolSurfaceOptions options,
      Backend<T> backend) {
    return runVolSurface(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * A tidy historical implied-volatility surface: one row per security, date and surface point.
   *
   * @param ticker the underlying
   * @param startDate the first date
   * @param endDate the last date
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> volSurface(
      String ticker, TemporalAccessor startDate, TemporalAccessor endDate) {
    return runVolSurface(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        VolSurfaceOptions.defaults(),
        Backend.TABLE);
  }

  /**
   * A tidy historical implied-volatility surface: one row per security, date and surface point.
   *
   * @param ticker the underlying
   * @param startDate the first date
   * @param endDate the last date
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> volSurface(
      String ticker,
      TemporalAccessor startDate,
      TemporalAccessor endDate,
      VolSurfaceOptions options) {
    return runVolSurface(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Backend.TABLE);
  }

  /**
   * A tidy historical implied-volatility surface: one row per security, date and surface point.
   *
   * @param ticker the underlying
   * @param startDate the first date
   * @param endDate the last date
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> volSurface(
      String ticker, TemporalAccessor startDate, TemporalAccessor endDate, Backend<T> backend) {
    return runVolSurface(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        VolSurfaceOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * A tidy historical implied-volatility surface: one row per security, date and surface point.
   *
   * @param ticker the underlying
   * @param startDate the first date
   * @param endDate the last date
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> volSurface(
      String ticker,
      TemporalAccessor startDate,
      TemporalAccessor endDate,
      VolSurfaceOptions options,
      Backend<T> backend) {
    return runVolSurface(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  private <T> CompletableFuture<T> runVolSurface(
      List<String> tickers,
      String startDate,
      String endDate,
      VolSurfaceOptions options,
      Backend<T> backend) {
    return recipe(
        "volSurface", RecipeArgs.volSurface(tickers, startDate, endDate, options), backend);
  }

  // ── indexMembers ───────────────────────────────────────────────────────────────────────────────

  /**
   * Index constituents, with the member identifier in a {@code member} column.
   *
   * @param index the index ticker, for example {@code "INDU Index"}
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> indexMembers(String index) {
    return runIndexMembers(index, IndexMembersOptions.defaults(), Backend.TABLE);
  }

  /**
   * Index constituents, with the member identifier in a {@code member} column.
   *
   * @param index the index ticker, for example {@code "INDU Index"}
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> indexMembers(String index, IndexMembersOptions options) {
    return runIndexMembers(index, Objects.requireNonNull(options, "options"), Backend.TABLE);
  }

  /**
   * Index constituents, with the member identifier in a {@code member} column.
   *
   * @param index the index ticker, for example {@code "INDU Index"}
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> indexMembers(String index, Backend<T> backend) {
    return runIndexMembers(
        index, IndexMembersOptions.defaults(), Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Index constituents, with the member identifier in a {@code member} column.
   *
   * @param index the index ticker, for example {@code "INDU Index"}
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> indexMembers(
      String index, IndexMembersOptions options, Backend<T> backend) {
    return runIndexMembers(
        index,
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  private <T> CompletableFuture<T> runIndexMembers(
      String index, IndexMembersOptions options, Backend<T> backend) {
    return recipe("indexMembers", RecipeArgs.indexMembers(index, options), backend);
  }

  // ── resolveIsins ───────────────────────────────────────────────────────────────────────────────

  /**
   * Resolves equity ISINs through Bloomberg's {@code /ISIN/} namespace.
   *
   * @param isins equity ISINs
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> resolveIsins(Collection<String> isins) {
    return runResolveIsins(RequestArgs.strings("isins", isins), Backend.TABLE);
  }

  /**
   * Resolves equity ISINs through Bloomberg's {@code /ISIN/} namespace.
   *
   * @param isins equity ISINs
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> resolveIsins(Collection<String> isins, Backend<T> backend) {
    return runResolveIsins(
        RequestArgs.strings("isins", isins), Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Resolves equity ISINs through Bloomberg's {@code /ISIN/} namespace.
   *
   * @param isin the equity ISIN
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> resolveIsins(String isin) {
    return runResolveIsins(RequestArgs.single("isin", isin), Backend.TABLE);
  }

  /**
   * Resolves equity ISINs through Bloomberg's {@code /ISIN/} namespace.
   *
   * @param isin the equity ISIN
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> resolveIsins(String isin, Backend<T> backend) {
    return runResolveIsins(
        RequestArgs.single("isin", isin), Objects.requireNonNull(backend, "backend"));
  }

  private <T> CompletableFuture<T> runResolveIsins(List<String> isins, Backend<T> backend) {
    return recipe("resolveIsins", RecipeArgs.resolveIsins(isins), backend);
  }

  // ── resolveVenues ──────────────────────────────────────────────────────────────────────────────

  /**
   * Resolves each input to its validated primary auction venue.
   *
   * @param securities the securities or ISINs
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> resolveVenues(Collection<String> securities) {
    return runResolveVenues(
        RequestArgs.strings("securities", securities),
        ResolveVenuesOptions.defaults(),
        Backend.TABLE);
  }

  /**
   * Resolves each input to its validated primary auction venue.
   *
   * @param securities the securities or ISINs
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> resolveVenues(
      Collection<String> securities, ResolveVenuesOptions options) {
    return runResolveVenues(
        RequestArgs.strings("securities", securities),
        Objects.requireNonNull(options, "options"),
        Backend.TABLE);
  }

  /**
   * Resolves each input to its validated primary auction venue.
   *
   * @param securities the securities or ISINs
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> resolveVenues(Collection<String> securities, Backend<T> backend) {
    return runResolveVenues(
        RequestArgs.strings("securities", securities),
        ResolveVenuesOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Resolves each input to its validated primary auction venue.
   *
   * <p>One row per input, in input order with duplicates: {@code input_order} , {@code security} ,
   * {@code status} ({@code resolved}, {@code unresolved} , {@code unsupported} , {@code mismatch}
   * ), {@code error} , plus the routing columns.
   *
   * @param securities the securities or ISINs
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> resolveVenues(
      Collection<String> securities, ResolveVenuesOptions options, Backend<T> backend) {
    return runResolveVenues(
        RequestArgs.strings("securities", securities),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Resolves each input to its validated primary auction venue.
   *
   * @param security the security or ISIN
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> resolveVenues(String security) {
    return runResolveVenues(
        RequestArgs.single("security", security), ResolveVenuesOptions.defaults(), Backend.TABLE);
  }

  /**
   * Resolves each input to its validated primary auction venue.
   *
   * @param security the security or ISIN
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> resolveVenues(String security, ResolveVenuesOptions options) {
    return runResolveVenues(
        RequestArgs.single("security", security),
        Objects.requireNonNull(options, "options"),
        Backend.TABLE);
  }

  /**
   * Resolves each input to its validated primary auction venue.
   *
   * @param security the security or ISIN
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> resolveVenues(String security, Backend<T> backend) {
    return runResolveVenues(
        RequestArgs.single("security", security),
        ResolveVenuesOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Resolves each input to its validated primary auction venue.
   *
   * <p>One row per input, in input order with duplicates: {@code input_order} , {@code security} ,
   * {@code status} ({@code resolved}, {@code unresolved} , {@code unsupported} , {@code mismatch}
   * ), {@code error} , plus the routing columns.
   *
   * @param security the security or ISIN
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> resolveVenues(
      String security, ResolveVenuesOptions options, Backend<T> backend) {
    return runResolveVenues(
        RequestArgs.single("security", security),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  private <T> CompletableFuture<T> runResolveVenues(
      List<String> securities, ResolveVenuesOptions options, Backend<T> backend) {
    return recipe("resolveVenues", RecipeArgs.resolveVenues(securities, options), backend);
  }

  // ── auctionSnapshot ────────────────────────────────────────────────────────────────────────────

  /**
   * Auction fields requested only from validated primary venues.
   *
   * @param securities the securities or ISINs
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> auctionSnapshot(Collection<String> securities) {
    return runAuctionSnapshot(
        RequestArgs.strings("securities", securities),
        AuctionSnapshotOptions.defaults(),
        Backend.TABLE);
  }

  /**
   * Auction fields requested only from validated primary venues.
   *
   * @param securities the securities or ISINs
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> auctionSnapshot(
      Collection<String> securities, AuctionSnapshotOptions options) {
    return runAuctionSnapshot(
        RequestArgs.strings("securities", securities),
        Objects.requireNonNull(options, "options"),
        Backend.TABLE);
  }

  /**
   * Auction fields requested only from validated primary venues.
   *
   * @param securities the securities or ISINs
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> auctionSnapshot(
      Collection<String> securities, Backend<T> backend) {
    return runAuctionSnapshot(
        RequestArgs.strings("securities", securities),
        AuctionSnapshotOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Auction fields requested only from validated primary venues.
   *
   * <p>Rows follow {@code resolveVenues} , with {@code venue_topic} and the requested fields typed
   * from field metadata; failed rows keep null data.
   *
   * @param securities the securities or ISINs
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> auctionSnapshot(
      Collection<String> securities, AuctionSnapshotOptions options, Backend<T> backend) {
    return runAuctionSnapshot(
        RequestArgs.strings("securities", securities),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Auction fields requested only from validated primary venues.
   *
   * @param security the security or ISIN
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> auctionSnapshot(String security) {
    return runAuctionSnapshot(
        RequestArgs.single("security", security), AuctionSnapshotOptions.defaults(), Backend.TABLE);
  }

  /**
   * Auction fields requested only from validated primary venues.
   *
   * @param security the security or ISIN
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> auctionSnapshot(String security, AuctionSnapshotOptions options) {
    return runAuctionSnapshot(
        RequestArgs.single("security", security),
        Objects.requireNonNull(options, "options"),
        Backend.TABLE);
  }

  /**
   * Auction fields requested only from validated primary venues.
   *
   * @param security the security or ISIN
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> auctionSnapshot(String security, Backend<T> backend) {
    return runAuctionSnapshot(
        RequestArgs.single("security", security),
        AuctionSnapshotOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Auction fields requested only from validated primary venues.
   *
   * <p>Rows follow {@code resolveVenues} , with {@code venue_topic} and the requested fields typed
   * from field metadata; failed rows keep null data.
   *
   * @param security the security or ISIN
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> auctionSnapshot(
      String security, AuctionSnapshotOptions options, Backend<T> backend) {
    return runAuctionSnapshot(
        RequestArgs.single("security", security),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  private <T> CompletableFuture<T> runAuctionSnapshot(
      List<String> securities, AuctionSnapshotOptions options, Backend<T> backend) {
    return recipe("auctionSnapshot", RecipeArgs.auctionSnapshot(securities, options), backend);
  }

  // ── delayStatus ────────────────────────────────────────────────────────────────────────────────

  /**
   * Reports per security whether this session receives delayed data.
   *
   * @param tickers the securities
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> delayStatus(Collection<String> tickers) {
    return runDelayStatus(
        RequestArgs.strings("tickers", tickers), DelayStatusOptions.defaults(), Backend.TABLE);
  }

  /**
   * Reports per security whether this session receives delayed data.
   *
   * @param tickers the securities
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> delayStatus(
      Collection<String> tickers, DelayStatusOptions options) {
    return runDelayStatus(
        RequestArgs.strings("tickers", tickers),
        Objects.requireNonNull(options, "options"),
        Backend.TABLE);
  }

  /**
   * Reports per security whether this session receives delayed data.
   *
   * @param tickers the securities
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> delayStatus(Collection<String> tickers, Backend<T> backend) {
    return runDelayStatus(
        RequestArgs.strings("tickers", tickers),
        DelayStatusOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Reports per security whether this session receives delayed data.
   *
   * <p>One row per unique security, in input order: the key column, {@code delayed} , {@code
   * delay_minutes} , {@code exchange_delay} , {@code exch_code} , {@code realtime_eid1} –{@code
   * realtime_eid4}, {@code delayed_eid} , {@code realtime_entitled} , {@code delayed_entitled} and
   * {@code error} .
   *
   * @param tickers the securities
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> delayStatus(
      Collection<String> tickers, DelayStatusOptions options, Backend<T> backend) {
    return runDelayStatus(
        RequestArgs.strings("tickers", tickers),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Reports per security whether this session receives delayed data.
   *
   * @param ticker the security
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> delayStatus(String ticker) {
    return runDelayStatus(
        RequestArgs.single("ticker", ticker), DelayStatusOptions.defaults(), Backend.TABLE);
  }

  /**
   * Reports per security whether this session receives delayed data.
   *
   * @param ticker the security
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> delayStatus(String ticker, DelayStatusOptions options) {
    return runDelayStatus(
        RequestArgs.single("ticker", ticker),
        Objects.requireNonNull(options, "options"),
        Backend.TABLE);
  }

  /**
   * Reports per security whether this session receives delayed data.
   *
   * @param ticker the security
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> delayStatus(String ticker, Backend<T> backend) {
    return runDelayStatus(
        RequestArgs.single("ticker", ticker),
        DelayStatusOptions.defaults(),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Reports per security whether this session receives delayed data.
   *
   * <p>One row per unique security, in input order: the key column, {@code delayed} , {@code
   * delay_minutes} , {@code exchange_delay} , {@code exch_code} , {@code realtime_eid1} –{@code
   * realtime_eid4}, {@code delayed_eid} , {@code realtime_entitled} , {@code delayed_entitled} and
   * {@code error} .
   *
   * @param ticker the security
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> delayStatus(
      String ticker, DelayStatusOptions options, Backend<T> backend) {
    return runDelayStatus(
        RequestArgs.single("ticker", ticker),
        Objects.requireNonNull(options, "options"),
        Objects.requireNonNull(backend, "backend"));
  }

  private <T> CompletableFuture<T> runDelayStatus(
      List<String> tickers, DelayStatusOptions options, Backend<T> backend) {
    return recipe("delayStatus", RecipeArgs.delayStatus(tickers, options), backend);
  }

  // ── issuerIsins ────────────────────────────────────────────────────────────────────────────────

  /**
   * Resolves bond ISINs to their issuer's parent equity ticker and equity ISIN.
   *
   * @param bondIsins bond ISINs
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> issuerIsins(Collection<String> bondIsins) {
    return runIssuerIsins(RequestArgs.strings("bondIsins", bondIsins), Backend.TABLE);
  }

  /**
   * Resolves bond ISINs to their issuer's parent equity ticker and equity ISIN.
   *
   * @param bondIsins bond ISINs
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> issuerIsins(Collection<String> bondIsins, Backend<T> backend) {
    return runIssuerIsins(
        RequestArgs.strings("bondIsins", bondIsins), Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Resolves bond ISINs to their issuer's parent equity ticker and equity ISIN.
   *
   * @param bondIsin the bond ISIN
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> issuerIsins(String bondIsin) {
    return runIssuerIsins(RequestArgs.single("bondIsin", bondIsin), Backend.TABLE);
  }

  /**
   * Resolves bond ISINs to their issuer's parent equity ticker and equity ISIN.
   *
   * @param bondIsin the bond ISIN
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> issuerIsins(String bondIsin, Backend<T> backend) {
    return runIssuerIsins(
        RequestArgs.single("bondIsin", bondIsin), Objects.requireNonNull(backend, "backend"));
  }

  private <T> CompletableFuture<T> runIssuerIsins(List<String> bondIsins, Backend<T> backend) {
    return recipe("issuerIsins", RecipeArgs.issuerIsins(bondIsins), backend);
  }

  // ── etfNavRelationships ────────────────────────────────────────────────────────────────────────

  /**
   * The NAV and iNAV tickers each ETF relates to, with their validation status.
   *
   * @param tickers the ETF tickers (trimmed)
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> etfNavRelationships(Collection<String> tickers) {
    return runEtfNavRelationships(RequestArgs.strings("tickers", tickers), Backend.TABLE);
  }

  /**
   * The NAV and iNAV tickers each ETF relates to, with their validation status.
   *
   * <p>One row per input in input order: {@code input_order} , {@code etf_ticker} , the NAV and
   * iNAV tickers, names, market sectors and validation errors.
   *
   * @param tickers the ETF tickers (trimmed)
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> etfNavRelationships(
      Collection<String> tickers, Backend<T> backend) {
    return runEtfNavRelationships(
        RequestArgs.strings("tickers", tickers), Objects.requireNonNull(backend, "backend"));
  }

  /**
   * The NAV and iNAV tickers each ETF relates to, with their validation status.
   *
   * @param ticker the ETF ticker (trimmed)
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> etfNavRelationships(String ticker) {
    return runEtfNavRelationships(RequestArgs.single("ticker", ticker), Backend.TABLE);
  }

  /**
   * The NAV and iNAV tickers each ETF relates to, with their validation status.
   *
   * <p>One row per input in input order: {@code input_order} , {@code etf_ticker} , the NAV and
   * iNAV tickers, names, market sectors and validation errors.
   *
   * @param ticker the ETF ticker (trimmed)
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> etfNavRelationships(String ticker, Backend<T> backend) {
    return runEtfNavRelationships(
        RequestArgs.single("ticker", ticker), Objects.requireNonNull(backend, "backend"));
  }

  private <T> CompletableFuture<T> runEtfNavRelationships(
      List<String> tickers, Backend<T> backend) {
    return recipe("etfNavRelationships", RecipeArgs.etfNav(tickers), backend);
  }

  // ── etfNavSnapshot ─────────────────────────────────────────────────────────────────────────────

  /**
   * Current NAV and iNAV levels per ETF.
   *
   * @param tickers the ETF tickers (trimmed)
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> etfNavSnapshot(Collection<String> tickers) {
    return runEtfNavSnapshot(RequestArgs.strings("tickers", tickers), Backend.TABLE);
  }

  /**
   * Current NAV and iNAV levels per ETF.
   *
   * @param tickers the ETF tickers (trimmed)
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> etfNavSnapshot(Collection<String> tickers, Backend<T> backend) {
    return runEtfNavSnapshot(
        RequestArgs.strings("tickers", tickers), Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Current NAV and iNAV levels per ETF.
   *
   * @param ticker the ETF ticker (trimmed)
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> etfNavSnapshot(String ticker) {
    return runEtfNavSnapshot(RequestArgs.single("ticker", ticker), Backend.TABLE);
  }

  /**
   * Current NAV and iNAV levels per ETF.
   *
   * @param ticker the ETF ticker (trimmed)
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> etfNavSnapshot(String ticker, Backend<T> backend) {
    return runEtfNavSnapshot(
        RequestArgs.single("ticker", ticker), Objects.requireNonNull(backend, "backend"));
  }

  private <T> CompletableFuture<T> runEtfNavSnapshot(List<String> tickers, Backend<T> backend) {
    return recipe("etfNavSnapshot", RecipeArgs.etfNav(tickers), backend);
  }

  // ── etfNavHistory ──────────────────────────────────────────────────────────────────────────────

  /**
   * Daily NAV and iNAV history per ETF.
   *
   * @param tickers the ETF tickers (trimmed)
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> etfNavHistory(
      Collection<String> tickers, String startDate, String endDate) {
    return runEtfNavHistory(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Backend.TABLE);
  }

  /**
   * Daily NAV and iNAV history per ETF.
   *
   * <p>Rows carry {@code nav_source_ticker} and {@code nav_source_field} ; only observed dates
   * appear.
   *
   * @param tickers the ETF tickers (trimmed)
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> etfNavHistory(
      Collection<String> tickers, String startDate, String endDate, Backend<T> backend) {
    return runEtfNavHistory(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Daily NAV and iNAV history per ETF.
   *
   * @param tickers the ETF tickers (trimmed)
   * @param startDate the first date
   * @param endDate the last date
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> etfNavHistory(
      Collection<String> tickers, TemporalAccessor startDate, TemporalAccessor endDate) {
    return runEtfNavHistory(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Backend.TABLE);
  }

  /**
   * Daily NAV and iNAV history per ETF.
   *
   * <p>Rows carry {@code nav_source_ticker} and {@code nav_source_field} ; only observed dates
   * appear.
   *
   * @param tickers the ETF tickers (trimmed)
   * @param startDate the first date
   * @param endDate the last date
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> etfNavHistory(
      Collection<String> tickers,
      TemporalAccessor startDate,
      TemporalAccessor endDate,
      Backend<T> backend) {
    return runEtfNavHistory(
        RequestArgs.strings("tickers", tickers),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Daily NAV and iNAV history per ETF.
   *
   * @param ticker the ETF ticker (trimmed)
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> etfNavHistory(String ticker, String startDate, String endDate) {
    return runEtfNavHistory(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Backend.TABLE);
  }

  /**
   * Daily NAV and iNAV history per ETF.
   *
   * <p>Rows carry {@code nav_source_ticker} and {@code nav_source_field} ; only observed dates
   * appear.
   *
   * @param ticker the ETF ticker (trimmed)
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> etfNavHistory(
      String ticker, String startDate, String endDate, Backend<T> backend) {
    return runEtfNavHistory(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * Daily NAV and iNAV history per ETF.
   *
   * @param ticker the ETF ticker (trimmed)
   * @param startDate the first date
   * @param endDate the last date
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> etfNavHistory(
      String ticker, TemporalAccessor startDate, TemporalAccessor endDate) {
    return runEtfNavHistory(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Backend.TABLE);
  }

  /**
   * Daily NAV and iNAV history per ETF.
   *
   * <p>Rows carry {@code nav_source_ticker} and {@code nav_source_field} ; only observed dates
   * appear.
   *
   * @param ticker the ETF ticker (trimmed)
   * @param startDate the first date
   * @param endDate the last date
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> etfNavHistory(
      String ticker, TemporalAccessor startDate, TemporalAccessor endDate, Backend<T> backend) {
    return runEtfNavHistory(
        RequestArgs.single("ticker", ticker),
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(backend, "backend"));
  }

  private <T> CompletableFuture<T> runEtfNavHistory(
      List<String> tickers, String startDate, String endDate, Backend<T> backend) {
    return recipe("etfNavHistory", RecipeArgs.etfNavHistory(tickers, startDate, endDate), backend);
  }

  // ── currencyConversion ─────────────────────────────────────────────────────────────────────────

  /**
   * {@code PX_LAST} history converted to a target currency (the {@code CRNCY} override).
   *
   * @param ticker the security
   * @param targetCcy the target currency, for example {@code "USD"}
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public CompletableFuture<Table> currencyConversion(
      String ticker, String targetCcy, String startDate, String endDate) {
    return runCurrencyConversion(
        ticker, targetCcy, Dates.formatDate(startDate), Dates.formatDate(endDate), Backend.TABLE);
  }

  /**
   * {@code PX_LAST} history converted to a target currency (the {@code CRNCY} override).
   *
   * @param ticker the security
   * @param targetCcy the target currency, for example {@code "USD"}
   * @param startDate the first date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param endDate the last date ({@code YYYY-MM-DD} or {@code YYYYMMDD} text)
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   * @throws IllegalArgumentException if a date text is ambiguous or not a date
   */
  public <T> CompletableFuture<T> currencyConversion(
      String ticker, String targetCcy, String startDate, String endDate, Backend<T> backend) {
    return runCurrencyConversion(
        ticker,
        targetCcy,
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(backend, "backend"));
  }

  /**
   * {@code PX_LAST} history converted to a target currency (the {@code CRNCY} override).
   *
   * @param ticker the security
   * @param targetCcy the target currency, for example {@code "USD"}
   * @param startDate the first date
   * @param endDate the last date
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if a required argument is {@code null}
   */
  public CompletableFuture<Table> currencyConversion(
      String ticker, String targetCcy, TemporalAccessor startDate, TemporalAccessor endDate) {
    return runCurrencyConversion(
        ticker, targetCcy, Dates.formatDate(startDate), Dates.formatDate(endDate), Backend.TABLE);
  }

  /**
   * {@code PX_LAST} history converted to a target currency (the {@code CRNCY} override).
   *
   * @param ticker the security
   * @param targetCcy the target currency, for example {@code "USD"}
   * @param startDate the first date
   * @param endDate the last date
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if a required argument is {@code null}
   */
  public <T> CompletableFuture<T> currencyConversion(
      String ticker,
      String targetCcy,
      TemporalAccessor startDate,
      TemporalAccessor endDate,
      Backend<T> backend) {
    return runCurrencyConversion(
        ticker,
        targetCcy,
        Dates.formatDate(startDate),
        Dates.formatDate(endDate),
        Objects.requireNonNull(backend, "backend"));
  }

  private <T> CompletableFuture<T> runCurrencyConversion(
      String ticker, String targetCcy, String startDate, String endDate, Backend<T> backend) {
    return recipe(
        "currencyConversion",
        RecipeArgs.currencyConversion(ticker, targetCcy, startDate, endDate),
        backend);
  }

  private <T> CompletableFuture<T> recipe(String method, JsonValue args, Backend<T> backend) {
    return callTable(method, args, backend);
  }
}
