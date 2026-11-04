package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonValue;
import io.github.underloam.xbbg.table.Table;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Typed Bloomberg request wrappers ({@code bdp}, {@code bdh}, {@code bds}, ...), the Java form of
 * the {@code @xbbg/core} {@code Engine} methods of the same names.
 *
 * <p>Each wrapper sends one argument object that the engine plans into a generic request exactly
 * like the JavaScript wrapper builds it, then runs it like {@link #request(RequestSpec, Backend)}:
 * delayed-data warnings (when {@code onDelayed} is {@link DelayedPolicy#WARN}), the ticker
 * key-column deprecation warning, and the requested {@link Backend}. Null arguments fail fast with
 * {@link NullPointerException}; argument combinations the engine rejects and request errors
 * complete the future exceptionally ({@link IllegalArgumentException}, {@link
 * BlpValidationException}, {@link BlpRequestException}, ...).
 */
abstract class EngineRequests extends EngineBase {
  EngineRequests(long handle, KeyColumn keyColumn) {
    super(handle, keyColumn);
  }

  /**
   * Reference data ({@code ReferenceDataRequest}, {@code refdata} extractor): one value per ticker
   * and field.
   *
   * @param tickers the securities
   * @param fields the field mnemonics
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bdp(Collection<String> tickers, Collection<String> fields) {
    return bdp(tickers, fields, BdpOptions.defaults(), Backend.TABLE);
  }

  /**
   * Reference data ({@code ReferenceDataRequest}, {@code refdata} extractor): one value per ticker
   * and field.
   *
   * @param tickers the securities
   * @param fields the field mnemonics
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bdp(
      Collection<String> tickers, Collection<String> fields, BdpOptions options) {
    return bdp(tickers, fields, options, Backend.TABLE);
  }

  /**
   * Reference data ({@code ReferenceDataRequest}, {@code refdata} extractor): one value per ticker
   * and field.
   *
   * @param tickers the securities
   * @param fields the field mnemonics
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bdp(
      Collection<String> tickers, Collection<String> fields, Backend<T> backend) {
    return bdp(tickers, fields, BdpOptions.defaults(), backend);
  }

  /**
   * Reference data ({@code ReferenceDataRequest}, {@code refdata} extractor): one value per ticker
   * and field.
   *
   * @param tickers the securities
   * @param fields the field mnemonics
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bdp(
      Collection<String> tickers,
      Collection<String> fields,
      BdpOptions options,
      Backend<T> backend) {
    JsonValue args = TypedRequests.bdp(tickers, fields, options);
    return callTable(
        "bdp",
        args,
        Objects.requireNonNull(backend, "backend"),
        TypedRequests.warns(options.onDelayed().orElse(null)));
  }

  /**
   * Bulk reference data ({@code ReferenceDataRequest}, {@code bulk} extractor): one row per
   * bulk-field row.
   *
   * @param tickers the securities
   * @param fields the bulk field mnemonics
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bds(Collection<String> tickers, Collection<String> fields) {
    return bds(tickers, fields, BdsOptions.defaults(), Backend.TABLE);
  }

  /**
   * Bulk reference data ({@code ReferenceDataRequest}, {@code bulk} extractor): one row per
   * bulk-field row.
   *
   * @param tickers the securities
   * @param fields the bulk field mnemonics
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bds(
      Collection<String> tickers, Collection<String> fields, BdsOptions options) {
    return bds(tickers, fields, options, Backend.TABLE);
  }

  /**
   * Bulk reference data ({@code ReferenceDataRequest}, {@code bulk} extractor): one row per
   * bulk-field row.
   *
   * @param tickers the securities
   * @param fields the bulk field mnemonics
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bds(
      Collection<String> tickers, Collection<String> fields, Backend<T> backend) {
    return bds(tickers, fields, BdsOptions.defaults(), backend);
  }

  /**
   * Bulk reference data ({@code ReferenceDataRequest}, {@code bulk} extractor): one row per
   * bulk-field row.
   *
   * @param tickers the securities
   * @param fields the bulk field mnemonics
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bds(
      Collection<String> tickers,
      Collection<String> fields,
      BdsOptions options,
      Backend<T> backend) {
    JsonValue args = TypedRequests.bds(tickers, fields, options);
    return callTable("bds", args, Objects.requireNonNull(backend, "backend"), false);
  }

  /**
   * Historical data ({@code HistoricalDataRequest}) between {@link BdhOptions#start()} and {@link
   * BdhOptions#end()}.
   *
   * @param tickers the securities
   * @param fields the field mnemonics
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bdh(Collection<String> tickers, Collection<String> fields) {
    return bdh(tickers, fields, BdhOptions.defaults(), Backend.TABLE);
  }

  /**
   * Historical data ({@code HistoricalDataRequest}) between {@link BdhOptions#start()} and {@link
   * BdhOptions#end()}.
   *
   * @param tickers the securities
   * @param fields the field mnemonics
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bdh(
      Collection<String> tickers, Collection<String> fields, BdhOptions options) {
    return bdh(tickers, fields, options, Backend.TABLE);
  }

  /**
   * Historical data ({@code HistoricalDataRequest}) between {@link BdhOptions#start()} and {@link
   * BdhOptions#end()}.
   *
   * @param tickers the securities
   * @param fields the field mnemonics
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bdh(
      Collection<String> tickers, Collection<String> fields, Backend<T> backend) {
    return bdh(tickers, fields, BdhOptions.defaults(), backend);
  }

  /**
   * Historical data ({@code HistoricalDataRequest}) between {@link BdhOptions#start()} and {@link
   * BdhOptions#end()}.
   *
   * @param tickers the securities
   * @param fields the field mnemonics
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bdh(
      Collection<String> tickers,
      Collection<String> fields,
      BdhOptions options,
      Backend<T> backend) {
    JsonValue args = TypedRequests.bdh(tickers, fields, options);
    return callTable(
        "bdh",
        args,
        Objects.requireNonNull(backend, "backend"),
        TypedRequests.warns(options.onDelayed().orElse(null)));
  }

  /**
   * Intraday bars ({@code IntradayBarRequest}); {@code eventType} defaults to {@code TRADE} and
   * {@code interval} to 1 minute.
   *
   * @param ticker the security
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bdib(String ticker) {
    return bdib(ticker, BdibOptions.defaults(), Backend.TABLE);
  }

  /**
   * Intraday bars ({@code IntradayBarRequest}); {@code eventType} defaults to {@code TRADE} and
   * {@code interval} to 1 minute.
   *
   * @param ticker the security
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bdib(String ticker, BdibOptions options) {
    return bdib(ticker, options, Backend.TABLE);
  }

  /**
   * Intraday bars ({@code IntradayBarRequest}); {@code eventType} defaults to {@code TRADE} and
   * {@code interval} to 1 minute.
   *
   * @param ticker the security
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bdib(String ticker, Backend<T> backend) {
    return bdib(ticker, BdibOptions.defaults(), backend);
  }

  /**
   * Intraday bars ({@code IntradayBarRequest}); {@code eventType} defaults to {@code TRADE} and
   * {@code interval} to 1 minute.
   *
   * @param ticker the security
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bdib(String ticker, BdibOptions options, Backend<T> backend) {
    JsonValue args = TypedRequests.bdib(ticker, options);
    return callTable(
        "bdib",
        args,
        Objects.requireNonNull(backend, "backend"),
        TypedRequests.warns(options.onDelayed().orElse(null)));
  }

  /**
   * Intraday ticks ({@code IntradayTickRequest}); {@code eventTypes} default to {@code [TRADE]}.
   *
   * @param ticker the security
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bdtick(String ticker) {
    return bdtick(ticker, BdtickOptions.defaults(), Backend.TABLE);
  }

  /**
   * Intraday ticks ({@code IntradayTickRequest}); {@code eventTypes} default to {@code [TRADE]}.
   *
   * @param ticker the security
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bdtick(String ticker, BdtickOptions options) {
    return bdtick(ticker, options, Backend.TABLE);
  }

  /**
   * Intraday ticks ({@code IntradayTickRequest}); {@code eventTypes} default to {@code [TRADE]}.
   *
   * @param ticker the security
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bdtick(String ticker, Backend<T> backend) {
    return bdtick(ticker, BdtickOptions.defaults(), backend);
  }

  /**
   * Intraday ticks ({@code IntradayTickRequest}); {@code eventTypes} default to {@code [TRADE]}.
   *
   * @param ticker the security
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bdtick(String ticker, BdtickOptions options, Backend<T> backend) {
    JsonValue args = TypedRequests.bdtick(ticker, options);
    return callTable(
        "bdtick",
        args,
        Objects.requireNonNull(backend, "backend"),
        TypedRequests.warns(options.onDelayed().orElse(null)));
  }

  /**
   * A Bloomberg Query Language query ({@code sendQuery} on {@code //blp/bqlsvc}).
   *
   * @param query the BQL expression
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bql(String query) {
    return bql(query, BqlOptions.defaults(), Backend.TABLE);
  }

  /**
   * A Bloomberg Query Language query ({@code sendQuery} on {@code //blp/bqlsvc}).
   *
   * @param query the BQL expression
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bql(String query, BqlOptions options) {
    return bql(query, options, Backend.TABLE);
  }

  /**
   * A Bloomberg Query Language query ({@code sendQuery} on {@code //blp/bqlsvc}).
   *
   * @param query the BQL expression
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bql(String query, Backend<T> backend) {
    return bql(query, BqlOptions.defaults(), backend);
  }

  /**
   * A Bloomberg Query Language query ({@code sendQuery} on {@code //blp/bqlsvc}).
   *
   * @param query the BQL expression
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bql(String query, BqlOptions options, Backend<T> backend) {
    JsonValue args = TypedRequests.bql(query, options);
    return callTable("bql", args, Objects.requireNonNull(backend, "backend"), false);
  }

  /**
   * An equity screen ({@code BeqsRequest}); {@code screenType} defaults to {@code PRIVATE} and
   * {@code group} to {@code General}.
   *
   * @param screen the screen name
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> beqs(String screen) {
    return beqs(screen, BeqsOptions.defaults(), Backend.TABLE);
  }

  /**
   * An equity screen ({@code BeqsRequest}); {@code screenType} defaults to {@code PRIVATE} and
   * {@code group} to {@code General}.
   *
   * @param screen the screen name
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> beqs(String screen, BeqsOptions options) {
    return beqs(screen, options, Backend.TABLE);
  }

  /**
   * An equity screen ({@code BeqsRequest}); {@code screenType} defaults to {@code PRIVATE} and
   * {@code group} to {@code General}.
   *
   * @param screen the screen name
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> beqs(String screen, Backend<T> backend) {
    return beqs(screen, BeqsOptions.defaults(), backend);
  }

  /**
   * An equity screen ({@code BeqsRequest}); {@code screenType} defaults to {@code PRIVATE} and
   * {@code group} to {@code General}.
   *
   * @param screen the screen name
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> beqs(String screen, BeqsOptions options, Backend<T> backend) {
    JsonValue args = TypedRequests.beqs(screen, options);
    return callTable("beqs", args, Objects.requireNonNull(backend, "backend"), false);
  }

  /**
   * A saved search ({@code ExcelGetGridRequest} on {@code //blp/exrsvc}).
   *
   * @param searchSpec the search domain, for example {@code FI:SRCHEX}
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bsrch(String searchSpec) {
    return bsrch(searchSpec, BsrchOptions.defaults(), Backend.TABLE);
  }

  /**
   * A saved search ({@code ExcelGetGridRequest} on {@code //blp/exrsvc}).
   *
   * @param searchSpec the search domain, for example {@code FI:SRCHEX}
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bsrch(String searchSpec, BsrchOptions options) {
    return bsrch(searchSpec, options, Backend.TABLE);
  }

  /**
   * A saved search ({@code ExcelGetGridRequest} on {@code //blp/exrsvc}).
   *
   * @param searchSpec the search domain, for example {@code FI:SRCHEX}
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bsrch(String searchSpec, Backend<T> backend) {
    return bsrch(searchSpec, BsrchOptions.defaults(), backend);
  }

  /**
   * A saved search ({@code ExcelGetGridRequest} on {@code //blp/exrsvc}).
   *
   * @param searchSpec the search domain, for example {@code FI:SRCHEX}
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bsrch(
      String searchSpec, BsrchOptions options, Backend<T> backend) {
    JsonValue args = TypedRequests.bsrch(searchSpec, options);
    return callTable("bsrch", args, Objects.requireNonNull(backend, "backend"), false);
  }

  /**
   * A technical-analysis study ({@code studyRequest} on {@code //blp/tasvc}) merged over the
   * study's defaults.
   *
   * @param ticker the security
   * @param study the study name or alias, for example {@code "rsi"}, {@code "SMA"} or {@code "Fear
   *     Greed"}
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bta(String ticker, String study) {
    return bta(ticker, study, BtaOptions.defaults(), Backend.TABLE);
  }

  /**
   * A technical-analysis study ({@code studyRequest} on {@code //blp/tasvc}) merged over the
   * study's defaults.
   *
   * @param ticker the security
   * @param study the study name or alias, for example {@code "rsi"}, {@code "SMA"} or {@code "Fear
   *     Greed"}
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bta(String ticker, String study, BtaOptions options) {
    return bta(ticker, study, options, Backend.TABLE);
  }

  /**
   * A technical-analysis study ({@code studyRequest} on {@code //blp/tasvc}) merged over the
   * study's defaults.
   *
   * @param ticker the security
   * @param study the study name or alias, for example {@code "rsi"}, {@code "SMA"} or {@code "Fear
   *     Greed"}
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bta(String ticker, String study, Backend<T> backend) {
    return bta(ticker, study, BtaOptions.defaults(), backend);
  }

  /**
   * A technical-analysis study ({@code studyRequest} on {@code //blp/tasvc}) merged over the
   * study's defaults.
   *
   * @param ticker the security
   * @param study the study name or alias, for example {@code "rsi"}, {@code "SMA"} or {@code "Fear
   *     Greed"}
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bta(
      String ticker, String study, BtaOptions options, Backend<T> backend) {
    JsonValue args = TypedRequests.bta(ticker, study, options);
    return callTable("bta", args, Objects.requireNonNull(backend, "backend"), false);
  }

  /**
   * A technical-analysis study with its own settings ({@code studyRequest} on {@code //blp/tasvc}).
   *
   * @param ticker the security
   * @param study the study and its settings
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bta(String ticker, BtaStudy study) {
    return bta(ticker, study, BtaOptions.defaults(), Backend.TABLE);
  }

  /**
   * A technical-analysis study with its own settings ({@code studyRequest} on {@code //blp/tasvc}).
   *
   * @param ticker the security
   * @param study the study and its settings
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bta(String ticker, BtaStudy study, BtaOptions options) {
    return bta(ticker, study, options, Backend.TABLE);
  }

  /**
   * A technical-analysis study with its own settings ({@code studyRequest} on {@code //blp/tasvc}).
   *
   * @param ticker the security
   * @param study the study and its settings
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bta(String ticker, BtaStudy study, Backend<T> backend) {
    return bta(ticker, study, BtaOptions.defaults(), backend);
  }

  /**
   * A technical-analysis study with its own settings ({@code studyRequest} on {@code //blp/tasvc}).
   *
   * @param ticker the security
   * @param study the study and its settings
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bta(
      String ticker, BtaStudy study, BtaOptions options, Backend<T> backend) {
    JsonValue args = TypedRequests.bta(ticker, study, options);
    return callTable("bta", args, Objects.requireNonNull(backend, "backend"), false);
  }

  /**
   * Field metadata ({@code //blp/apiflds}): a field search when {@link BfldsOptions#searchSpec()}
   * is set, otherwise field info over {@link BfldsOptions#fields()}.
   *
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bflds() {
    return bflds(BfldsOptions.defaults(), Backend.TABLE);
  }

  /**
   * Field metadata ({@code //blp/apiflds}): a field search when {@link BfldsOptions#searchSpec()}
   * is set, otherwise field info over {@link BfldsOptions#fields()}.
   *
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bflds(BfldsOptions options) {
    return bflds(options, Backend.TABLE);
  }

  /**
   * Field metadata ({@code //blp/apiflds}): a field search when {@link BfldsOptions#searchSpec()}
   * is set, otherwise field info over {@link BfldsOptions#fields()}.
   *
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bflds(Backend<T> backend) {
    return bflds(BfldsOptions.defaults(), backend);
  }

  /**
   * Field metadata ({@code //blp/apiflds}): a field search when {@link BfldsOptions#searchSpec()}
   * is set, otherwise field info over {@link BfldsOptions#fields()}.
   *
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bflds(BfldsOptions options, Backend<T> backend) {
    JsonValue args = TypedRequests.bflds(options);
    return callTable("bflds", args, Objects.requireNonNull(backend, "backend"), false);
  }

  /**
   * Field info ({@code FieldInfoRequest}) for the given fields.
   *
   * @param fields the field mnemonics
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> fieldInfo(Collection<String> fields) {
    return fieldInfo(fields, BfldsOptions.defaults(), Backend.TABLE);
  }

  /**
   * Field info ({@code FieldInfoRequest}) for the given fields.
   *
   * @param fields the field mnemonics
   * @param options the options; {@code fields} and {@code searchSpec} must be unset
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> fieldInfo(Collection<String> fields, BfldsOptions options) {
    return fieldInfo(fields, options, Backend.TABLE);
  }

  /**
   * Field info ({@code FieldInfoRequest}) for the given fields.
   *
   * @param fields the field mnemonics
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> fieldInfo(Collection<String> fields, Backend<T> backend) {
    return fieldInfo(fields, BfldsOptions.defaults(), backend);
  }

  /**
   * Field info ({@code FieldInfoRequest}) for the given fields.
   *
   * @param fields the field mnemonics
   * @param options the options; {@code fields} and {@code searchSpec} must be unset
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   * @throws IllegalArgumentException if the options set {@code fields} or {@code searchSpec}
   */
  public <T> CompletableFuture<T> fieldInfo(
      Collection<String> fields, BfldsOptions options, Backend<T> backend) {
    JsonValue args = TypedRequests.fieldInfo(fields, options);
    return callTable("fieldInfo", args, Objects.requireNonNull(backend, "backend"), false);
  }

  /**
   * Field info ({@code FieldInfoRequest}) for the given fields.
   *
   * @param field the field mnemonic
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> fieldInfo(String field) {
    return fieldInfo(RequestArgs.single("field", field), BfldsOptions.defaults(), Backend.TABLE);
  }

  /**
   * Field info ({@code FieldInfoRequest}) for the given fields.
   *
   * @param field the field mnemonic
   * @param options the options; {@link BfldsOptions#fields()} and {@link BfldsOptions#searchSpec()}
   *     must be unset
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> fieldInfo(String field, BfldsOptions options) {
    return fieldInfo(RequestArgs.single("field", field), options, Backend.TABLE);
  }

  /**
   * Field info ({@code FieldInfoRequest}) for the given fields.
   *
   * @param field the field mnemonic
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> fieldInfo(String field, Backend<T> backend) {
    return fieldInfo(RequestArgs.single("field", field), BfldsOptions.defaults(), backend);
  }

  /**
   * Field info ({@code FieldInfoRequest}) for the given fields.
   *
   * @param field the field mnemonic
   * @param options the options; {@link BfldsOptions#fields()} and {@link BfldsOptions#searchSpec()}
   *     must be unset
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   * @throws IllegalArgumentException if the options set a field list or a search spec
   */
  public <T> CompletableFuture<T> fieldInfo(
      String field, BfldsOptions options, Backend<T> backend) {
    return fieldInfo(RequestArgs.single("field", field), options, backend);
  }

  /**
   * A field search ({@code FieldSearchRequest}).
   *
   * @param searchSpec free text to search field names and descriptions for
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> fieldSearch(String searchSpec) {
    return fieldSearch(searchSpec, BfldsOptions.defaults(), Backend.TABLE);
  }

  /**
   * A field search ({@code FieldSearchRequest}).
   *
   * @param searchSpec free text to search field names and descriptions for
   * @param options the options; {@code fields} and {@code searchSpec} must be unset
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> fieldSearch(String searchSpec, BfldsOptions options) {
    return fieldSearch(searchSpec, options, Backend.TABLE);
  }

  /**
   * A field search ({@code FieldSearchRequest}).
   *
   * @param searchSpec free text to search field names and descriptions for
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> fieldSearch(String searchSpec, Backend<T> backend) {
    return fieldSearch(searchSpec, BfldsOptions.defaults(), backend);
  }

  /**
   * A field search ({@code FieldSearchRequest}).
   *
   * @param searchSpec free text to search field names and descriptions for
   * @param options the options; {@code fields} and {@code searchSpec} must be unset
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   * @throws IllegalArgumentException if the options set {@code fields} or {@code searchSpec}
   */
  public <T> CompletableFuture<T> fieldSearch(
      String searchSpec, BfldsOptions options, Backend<T> backend) {
    JsonValue args = TypedRequests.fieldSearch(searchSpec, options);
    return callTable("fieldSearch", args, Objects.requireNonNull(backend, "backend"), false);
  }

  /**
   * An instrument lookup ({@code instrumentListRequest} on {@code //blp/instruments}).
   *
   * @param query the lookup text
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> blkp(String query) {
    return blkp(query, BlkpOptions.defaults(), Backend.TABLE);
  }

  /**
   * An instrument lookup ({@code instrumentListRequest} on {@code //blp/instruments}).
   *
   * @param query the lookup text
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> blkp(String query, BlkpOptions options) {
    return blkp(query, options, Backend.TABLE);
  }

  /**
   * An instrument lookup ({@code instrumentListRequest} on {@code //blp/instruments}).
   *
   * @param query the lookup text
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> blkp(String query, Backend<T> backend) {
    return blkp(query, BlkpOptions.defaults(), backend);
  }

  /**
   * An instrument lookup ({@code instrumentListRequest} on {@code //blp/instruments}).
   *
   * @param query the lookup text
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> blkp(String query, BlkpOptions options, Backend<T> backend) {
    JsonValue args = TypedRequests.blkp(query, options);
    return callTable("blkp", args, Objects.requireNonNull(backend, "backend"), false);
  }

  /**
   * Portfolio data ({@code PortfolioDataRequest}) for a portfolio.
   *
   * @param portfolio the portfolio, for example {@code "U12345-1 Client"}
   * @param fields the portfolio field mnemonics
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bport(String portfolio, Collection<String> fields) {
    return bport(portfolio, fields, RequestOptions.defaults(), Backend.TABLE);
  }

  /**
   * Portfolio data ({@code PortfolioDataRequest}) for a portfolio.
   *
   * @param portfolio the portfolio, for example {@code "U12345-1 Client"}
   * @param fields the portfolio field mnemonics
   * @param options the options; per-security overrides are rejected
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bport(
      String portfolio, Collection<String> fields, RequestOptions options) {
    return bport(portfolio, fields, options, Backend.TABLE);
  }

  /**
   * Portfolio data ({@code PortfolioDataRequest}) for a portfolio.
   *
   * @param portfolio the portfolio, for example {@code "U12345-1 Client"}
   * @param fields the portfolio field mnemonics
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bport(
      String portfolio, Collection<String> fields, Backend<T> backend) {
    return bport(portfolio, fields, RequestOptions.defaults(), backend);
  }

  /**
   * Portfolio data ({@code PortfolioDataRequest}) for a portfolio.
   *
   * @param portfolio the portfolio, for example {@code "U12345-1 Client"}
   * @param fields the portfolio field mnemonics
   * @param options the options; per-security overrides are rejected
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bport(
      String portfolio, Collection<String> fields, RequestOptions options, Backend<T> backend) {
    JsonValue args = TypedRequests.bport(portfolio, fields, options);
    return callTable("bport", args, Objects.requireNonNull(backend, "backend"), false);
  }

  /**
   * Portfolio data ({@code PortfolioDataRequest}) for a portfolio.
   *
   * @param portfolio the portfolio, for example {@code "U12345-1 Client"}
   * @param field the portfolio field mnemonic
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bport(String portfolio, String field) {
    return bport(
        portfolio, RequestArgs.single("field", field), RequestOptions.defaults(), Backend.TABLE);
  }

  /**
   * Portfolio data ({@code PortfolioDataRequest}) for a portfolio.
   *
   * @param portfolio the portfolio, for example {@code "U12345-1 Client"}
   * @param field the portfolio field mnemonic
   * @param options the options; per-security overrides are rejected
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bport(String portfolio, String field, RequestOptions options) {
    return bport(portfolio, RequestArgs.single("field", field), options, Backend.TABLE);
  }

  /**
   * Portfolio data ({@code PortfolioDataRequest}) for a portfolio.
   *
   * @param portfolio the portfolio, for example {@code "U12345-1 Client"}
   * @param field the portfolio field mnemonic
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bport(String portfolio, String field, Backend<T> backend) {
    return bport(portfolio, RequestArgs.single("field", field), RequestOptions.defaults(), backend);
  }

  /**
   * Portfolio data ({@code PortfolioDataRequest}) for a portfolio.
   *
   * @param portfolio the portfolio, for example {@code "U12345-1 Client"}
   * @param field the portfolio field mnemonic
   * @param options the options; per-security overrides are rejected
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bport(
      String portfolio, String field, RequestOptions options, Backend<T> backend) {
    return bport(portfolio, RequestArgs.single("field", field), options, backend);
  }

  /**
   * A curve lookup ({@code curveListRequest} on {@code //blp/instruments}).
   *
   * @param ticker the lookup text
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bcurves(String ticker) {
    return bcurves(ticker, RequestOptions.defaults(), Backend.TABLE);
  }

  /**
   * A curve lookup ({@code curveListRequest} on {@code //blp/instruments}).
   *
   * @param ticker the lookup text
   * @param options the options; overrides are rejected (the operation has none)
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bcurves(String ticker, RequestOptions options) {
    return bcurves(ticker, options, Backend.TABLE);
  }

  /**
   * A curve lookup ({@code curveListRequest} on {@code //blp/instruments}).
   *
   * @param ticker the lookup text
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bcurves(String ticker, Backend<T> backend) {
    return bcurves(ticker, RequestOptions.defaults(), backend);
  }

  /**
   * A curve lookup ({@code curveListRequest} on {@code //blp/instruments}).
   *
   * @param ticker the lookup text
   * @param options the options; overrides are rejected (the operation has none)
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bcurves(
      String ticker, RequestOptions options, Backend<T> backend) {
    JsonValue args = TypedRequests.instrumentList(ticker, options);
    return callTable("bcurves", args, Objects.requireNonNull(backend, "backend"), false);
  }

  /**
   * A government-security lookup ({@code govtListRequest} on {@code //blp/instruments}).
   *
   * @param ticker the lookup text
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bgovts(String ticker) {
    return bgovts(ticker, RequestOptions.defaults(), Backend.TABLE);
  }

  /**
   * A government-security lookup ({@code govtListRequest} on {@code //blp/instruments}).
   *
   * @param ticker the lookup text
   * @param options the options; overrides are rejected (the operation has none)
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> bgovts(String ticker, RequestOptions options) {
    return bgovts(ticker, options, Backend.TABLE);
  }

  /**
   * A government-security lookup ({@code govtListRequest} on {@code //blp/instruments}).
   *
   * @param ticker the lookup text
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bgovts(String ticker, Backend<T> backend) {
    return bgovts(ticker, RequestOptions.defaults(), backend);
  }

  /**
   * A government-security lookup ({@code govtListRequest} on {@code //blp/instruments}).
   *
   * @param ticker the lookup text
   * @param options the options; overrides are rejected (the operation has none)
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> bgovts(
      String ticker, RequestOptions options, Backend<T> backend) {
    JsonValue args = TypedRequests.instrumentList(ticker, options);
    return callTable("bgovts", args, Objects.requireNonNull(backend, "backend"), false);
  }

  /**
   * CDX index information: {@code bdp} of the ticker over {@link CdxFields#INFO}.
   *
   * @param ticker the CDX index ticker
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> acdxInfo(String ticker) {
    return acdxInfo(ticker, BdpOptions.defaults(), Backend.TABLE);
  }

  /**
   * CDX index information: {@code bdp} of the ticker over {@link CdxFields#INFO}.
   *
   * @param ticker the CDX index ticker
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> acdxInfo(String ticker, BdpOptions options) {
    return acdxInfo(ticker, options, Backend.TABLE);
  }

  /**
   * CDX index information: {@code bdp} of the ticker over {@link CdxFields#INFO}.
   *
   * @param ticker the CDX index ticker
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> acdxInfo(String ticker, Backend<T> backend) {
    return acdxInfo(ticker, BdpOptions.defaults(), backend);
  }

  /**
   * CDX index information: {@code bdp} of the ticker over {@link CdxFields#INFO}.
   *
   * @param ticker the CDX index ticker
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> acdxInfo(String ticker, BdpOptions options, Backend<T> backend) {
    JsonValue args = TypedRequests.acdxInfo(ticker, options);
    return callTable(
        "acdxInfo",
        args,
        Objects.requireNonNull(backend, "backend"),
        TypedRequests.warns(options.onDelayed().orElse(null)));
  }

  /**
   * CDX pricing: {@code bdp} of the ticker over {@link CdxFields#PRICING}, with the recovery rate
   * sent as the {@code CDS_RR} override.
   *
   * @param ticker the CDX index ticker
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> acdxPricing(String ticker) {
    return acdxPricing(ticker, CdxOptions.defaults(), Backend.TABLE);
  }

  /**
   * CDX pricing: {@code bdp} of the ticker over {@link CdxFields#PRICING}, with the recovery rate
   * sent as the {@code CDS_RR} override.
   *
   * @param ticker the CDX index ticker
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> acdxPricing(String ticker, CdxOptions options) {
    return acdxPricing(ticker, options, Backend.TABLE);
  }

  /**
   * CDX pricing: {@code bdp} of the ticker over {@link CdxFields#PRICING}, with the recovery rate
   * sent as the {@code CDS_RR} override.
   *
   * @param ticker the CDX index ticker
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> acdxPricing(String ticker, Backend<T> backend) {
    return acdxPricing(ticker, CdxOptions.defaults(), backend);
  }

  /**
   * CDX pricing: {@code bdp} of the ticker over {@link CdxFields#PRICING}, with the recovery rate
   * sent as the {@code CDS_RR} override.
   *
   * @param ticker the CDX index ticker
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> acdxPricing(
      String ticker, CdxOptions options, Backend<T> backend) {
    JsonValue args = TypedRequests.cdx(ticker, options);
    return callTable(
        "acdxPricing",
        args,
        Objects.requireNonNull(backend, "backend"),
        TypedRequests.warns(options.onDelayed().orElse(null)));
  }

  /**
   * CDX risk measures: {@code bdp} of the ticker over {@link CdxFields#RISK}, with the recovery
   * rate sent as the {@code CDS_RR} override.
   *
   * @param ticker the CDX index ticker
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> acdxRisk(String ticker) {
    return acdxRisk(ticker, CdxOptions.defaults(), Backend.TABLE);
  }

  /**
   * CDX risk measures: {@code bdp} of the ticker over {@link CdxFields#RISK}, with the recovery
   * rate sent as the {@code CDS_RR} override.
   *
   * @param ticker the CDX index ticker
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Table> acdxRisk(String ticker, CdxOptions options) {
    return acdxRisk(ticker, options, Backend.TABLE);
  }

  /**
   * CDX risk measures: {@code bdp} of the ticker over {@link CdxFields#RISK}, with the recovery
   * rate sent as the {@code CDS_RR} override.
   *
   * @param ticker the CDX index ticker
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> acdxRisk(String ticker, Backend<T> backend) {
    return acdxRisk(ticker, CdxOptions.defaults(), backend);
  }

  /**
   * CDX risk measures: {@code bdp} of the ticker over {@link CdxFields#RISK}, with the recovery
   * rate sent as the {@code CDS_RR} override.
   *
   * @param ticker the CDX index ticker
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public <T> CompletableFuture<T> acdxRisk(String ticker, CdxOptions options, Backend<T> backend) {
    JsonValue args = TypedRequests.cdx(ticker, options);
    return callTable(
        "acdxRisk",
        args,
        Objects.requireNonNull(backend, "backend"),
        TypedRequests.warns(options.onDelayed().orElse(null)));
  }
}
