package io.github.underloam.xbbg;

import io.github.underloam.xbbg.table.Table;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Typed Bloomberg requests on the configured engine ({@code Blp.bdp}, {@code Blp.bdh}, ...): the
 * {@code @xbbg/core} top-level functions. Tickers and fields take a single {@code String} or a
 * {@code Collection<String>}. Configuration and connection errors complete the future exceptionally
 * before any request is sent.
 *
 * <p>The JavaScript positional forms map to options: {@code abdh(t, f, start, end)} is {@code
 * bdh(t, f, BdhOptions.builder().start(start).end(end).build())}, {@code abds(t, f, overrides)} is
 * {@code bds(t, f, BdsOptions.builder().overrides(overrides).build())}, {@code abdib(t, dt,
 * interval)} is {@code bdib(t, BdibOptions.builder().day(dt).interval(interval).build())} and
 * {@code abdtick(t, start, end)} is {@code bdtick(t, BdtickOptions.builder().start(start)
 * .end(end).build())}.
 */
abstract class BlpRequests extends BlpBase {
  BlpRequests() {}

  /**
   * Reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdp(Collection, Collection, BdpOptions, Backend)}.
   *
   * @param ticker the security
   * @param field the field mnemonic
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bdp(String ticker, String field) {
    return bdp(
        RequestArgs.single("ticker", ticker),
        RequestArgs.single("field", field),
        BdpOptions.defaults(),
        Backend.TABLE);
  }

  /**
   * Reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdp(Collection, Collection, BdpOptions, Backend)}.
   *
   * @param ticker the security
   * @param field the field mnemonic
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bdp(String ticker, String field, BdpOptions options) {
    return bdp(
        RequestArgs.single("ticker", ticker),
        RequestArgs.single("field", field),
        options,
        Backend.TABLE);
  }

  /**
   * Reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdp(Collection, Collection, BdpOptions, Backend)}.
   *
   * @param ticker the security
   * @param field the field mnemonic
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bdp(String ticker, String field, Backend<T> backend) {
    return bdp(
        RequestArgs.single("ticker", ticker),
        RequestArgs.single("field", field),
        BdpOptions.defaults(),
        backend);
  }

  /**
   * Reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdp(Collection, Collection, BdpOptions, Backend)}.
   *
   * @param ticker the security
   * @param field the field mnemonic
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bdp(
      String ticker, String field, BdpOptions options, Backend<T> backend) {
    return bdp(
        RequestArgs.single("ticker", ticker), RequestArgs.single("field", field), options, backend);
  }

  /**
   * Reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdp(Collection, Collection, BdpOptions, Backend)}.
   *
   * @param ticker the security
   * @param fields the field mnemonics
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bdp(String ticker, Collection<String> fields) {
    return bdp(RequestArgs.single("ticker", ticker), fields, BdpOptions.defaults(), Backend.TABLE);
  }

  /**
   * Reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdp(Collection, Collection, BdpOptions, Backend)}.
   *
   * @param ticker the security
   * @param fields the field mnemonics
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bdp(
      String ticker, Collection<String> fields, BdpOptions options) {
    return bdp(RequestArgs.single("ticker", ticker), fields, options, Backend.TABLE);
  }

  /**
   * Reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdp(Collection, Collection, BdpOptions, Backend)}.
   *
   * @param ticker the security
   * @param fields the field mnemonics
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bdp(
      String ticker, Collection<String> fields, Backend<T> backend) {
    return bdp(RequestArgs.single("ticker", ticker), fields, BdpOptions.defaults(), backend);
  }

  /**
   * Reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdp(Collection, Collection, BdpOptions, Backend)}.
   *
   * @param ticker the security
   * @param fields the field mnemonics
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bdp(
      String ticker, Collection<String> fields, BdpOptions options, Backend<T> backend) {
    return bdp(RequestArgs.single("ticker", ticker), fields, options, backend);
  }

  /**
   * Reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdp(Collection, Collection, BdpOptions, Backend)}.
   *
   * @param tickers the securities
   * @param field the field mnemonic
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bdp(Collection<String> tickers, String field) {
    return bdp(tickers, RequestArgs.single("field", field), BdpOptions.defaults(), Backend.TABLE);
  }

  /**
   * Reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdp(Collection, Collection, BdpOptions, Backend)}.
   *
   * @param tickers the securities
   * @param field the field mnemonic
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bdp(
      Collection<String> tickers, String field, BdpOptions options) {
    return bdp(tickers, RequestArgs.single("field", field), options, Backend.TABLE);
  }

  /**
   * Reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdp(Collection, Collection, BdpOptions, Backend)}.
   *
   * @param tickers the securities
   * @param field the field mnemonic
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bdp(
      Collection<String> tickers, String field, Backend<T> backend) {
    return bdp(tickers, RequestArgs.single("field", field), BdpOptions.defaults(), backend);
  }

  /**
   * Reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdp(Collection, Collection, BdpOptions, Backend)}.
   *
   * @param tickers the securities
   * @param field the field mnemonic
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bdp(
      Collection<String> tickers, String field, BdpOptions options, Backend<T> backend) {
    return bdp(tickers, RequestArgs.single("field", field), options, backend);
  }

  /**
   * Reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdp(Collection, Collection, BdpOptions, Backend)}.
   *
   * @param tickers the securities
   * @param fields the field mnemonics
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bdp(
      Collection<String> tickers, Collection<String> fields) {
    return bdp(tickers, fields, BdpOptions.defaults(), Backend.TABLE);
  }

  /**
   * Reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdp(Collection, Collection, BdpOptions, Backend)}.
   *
   * @param tickers the securities
   * @param fields the field mnemonics
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bdp(
      Collection<String> tickers, Collection<String> fields, BdpOptions options) {
    return bdp(tickers, fields, options, Backend.TABLE);
  }

  /**
   * Reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdp(Collection, Collection, BdpOptions, Backend)}.
   *
   * @param tickers the securities
   * @param fields the field mnemonics
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bdp(
      Collection<String> tickers, Collection<String> fields, Backend<T> backend) {
    return bdp(tickers, fields, BdpOptions.defaults(), backend);
  }

  /**
   * Reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdp(Collection, Collection, BdpOptions, Backend)}.
   *
   * @param tickers the securities
   * @param fields the field mnemonics
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bdp(
      Collection<String> tickers,
      Collection<String> fields,
      BdpOptions options,
      Backend<T> backend) {
    final List<String> tickerList = RequestArgs.strings("tickers", tickers);
    final List<String> fieldList = RequestArgs.strings("fields", fields);
    final BdpOptions checked = Objects.requireNonNull(options, "options");
    final Backend<T> form = Objects.requireNonNull(backend, "backend");
    return withEngine(engine -> engine.bdp(tickerList, fieldList, checked, form));
  }

  /**
   * Historical data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdh(Collection, Collection, BdhOptions, Backend)}.
   *
   * @param ticker the security
   * @param field the field mnemonic
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bdh(String ticker, String field) {
    return bdh(
        RequestArgs.single("ticker", ticker),
        RequestArgs.single("field", field),
        BdhOptions.defaults(),
        Backend.TABLE);
  }

  /**
   * Historical data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdh(Collection, Collection, BdhOptions, Backend)}.
   *
   * @param ticker the security
   * @param field the field mnemonic
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bdh(String ticker, String field, BdhOptions options) {
    return bdh(
        RequestArgs.single("ticker", ticker),
        RequestArgs.single("field", field),
        options,
        Backend.TABLE);
  }

  /**
   * Historical data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdh(Collection, Collection, BdhOptions, Backend)}.
   *
   * @param ticker the security
   * @param field the field mnemonic
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bdh(String ticker, String field, Backend<T> backend) {
    return bdh(
        RequestArgs.single("ticker", ticker),
        RequestArgs.single("field", field),
        BdhOptions.defaults(),
        backend);
  }

  /**
   * Historical data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdh(Collection, Collection, BdhOptions, Backend)}.
   *
   * @param ticker the security
   * @param field the field mnemonic
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bdh(
      String ticker, String field, BdhOptions options, Backend<T> backend) {
    return bdh(
        RequestArgs.single("ticker", ticker), RequestArgs.single("field", field), options, backend);
  }

  /**
   * Historical data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdh(Collection, Collection, BdhOptions, Backend)}.
   *
   * @param ticker the security
   * @param fields the field mnemonics
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bdh(String ticker, Collection<String> fields) {
    return bdh(RequestArgs.single("ticker", ticker), fields, BdhOptions.defaults(), Backend.TABLE);
  }

  /**
   * Historical data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdh(Collection, Collection, BdhOptions, Backend)}.
   *
   * @param ticker the security
   * @param fields the field mnemonics
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bdh(
      String ticker, Collection<String> fields, BdhOptions options) {
    return bdh(RequestArgs.single("ticker", ticker), fields, options, Backend.TABLE);
  }

  /**
   * Historical data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdh(Collection, Collection, BdhOptions, Backend)}.
   *
   * @param ticker the security
   * @param fields the field mnemonics
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bdh(
      String ticker, Collection<String> fields, Backend<T> backend) {
    return bdh(RequestArgs.single("ticker", ticker), fields, BdhOptions.defaults(), backend);
  }

  /**
   * Historical data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdh(Collection, Collection, BdhOptions, Backend)}.
   *
   * @param ticker the security
   * @param fields the field mnemonics
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bdh(
      String ticker, Collection<String> fields, BdhOptions options, Backend<T> backend) {
    return bdh(RequestArgs.single("ticker", ticker), fields, options, backend);
  }

  /**
   * Historical data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdh(Collection, Collection, BdhOptions, Backend)}.
   *
   * @param tickers the securities
   * @param field the field mnemonic
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bdh(Collection<String> tickers, String field) {
    return bdh(tickers, RequestArgs.single("field", field), BdhOptions.defaults(), Backend.TABLE);
  }

  /**
   * Historical data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdh(Collection, Collection, BdhOptions, Backend)}.
   *
   * @param tickers the securities
   * @param field the field mnemonic
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bdh(
      Collection<String> tickers, String field, BdhOptions options) {
    return bdh(tickers, RequestArgs.single("field", field), options, Backend.TABLE);
  }

  /**
   * Historical data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdh(Collection, Collection, BdhOptions, Backend)}.
   *
   * @param tickers the securities
   * @param field the field mnemonic
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bdh(
      Collection<String> tickers, String field, Backend<T> backend) {
    return bdh(tickers, RequestArgs.single("field", field), BdhOptions.defaults(), backend);
  }

  /**
   * Historical data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdh(Collection, Collection, BdhOptions, Backend)}.
   *
   * @param tickers the securities
   * @param field the field mnemonic
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bdh(
      Collection<String> tickers, String field, BdhOptions options, Backend<T> backend) {
    return bdh(tickers, RequestArgs.single("field", field), options, backend);
  }

  /**
   * Historical data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdh(Collection, Collection, BdhOptions, Backend)}.
   *
   * @param tickers the securities
   * @param fields the field mnemonics
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bdh(
      Collection<String> tickers, Collection<String> fields) {
    return bdh(tickers, fields, BdhOptions.defaults(), Backend.TABLE);
  }

  /**
   * Historical data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdh(Collection, Collection, BdhOptions, Backend)}.
   *
   * @param tickers the securities
   * @param fields the field mnemonics
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bdh(
      Collection<String> tickers, Collection<String> fields, BdhOptions options) {
    return bdh(tickers, fields, options, Backend.TABLE);
  }

  /**
   * Historical data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdh(Collection, Collection, BdhOptions, Backend)}.
   *
   * @param tickers the securities
   * @param fields the field mnemonics
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bdh(
      Collection<String> tickers, Collection<String> fields, Backend<T> backend) {
    return bdh(tickers, fields, BdhOptions.defaults(), backend);
  }

  /**
   * Historical data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdh(Collection, Collection, BdhOptions, Backend)}.
   *
   * @param tickers the securities
   * @param fields the field mnemonics
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bdh(
      Collection<String> tickers,
      Collection<String> fields,
      BdhOptions options,
      Backend<T> backend) {
    final List<String> tickerList = RequestArgs.strings("tickers", tickers);
    final List<String> fieldList = RequestArgs.strings("fields", fields);
    final BdhOptions checked = Objects.requireNonNull(options, "options");
    final Backend<T> form = Objects.requireNonNull(backend, "backend");
    return withEngine(engine -> engine.bdh(tickerList, fieldList, checked, form));
  }

  /**
   * Bulk reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bds(Collection, Collection, BdsOptions, Backend)}.
   *
   * @param ticker the security
   * @param field the field mnemonic
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bds(String ticker, String field) {
    return bds(
        RequestArgs.single("ticker", ticker),
        RequestArgs.single("field", field),
        BdsOptions.defaults(),
        Backend.TABLE);
  }

  /**
   * Bulk reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bds(Collection, Collection, BdsOptions, Backend)}.
   *
   * @param ticker the security
   * @param field the field mnemonic
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bds(String ticker, String field, BdsOptions options) {
    return bds(
        RequestArgs.single("ticker", ticker),
        RequestArgs.single("field", field),
        options,
        Backend.TABLE);
  }

  /**
   * Bulk reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bds(Collection, Collection, BdsOptions, Backend)}.
   *
   * @param ticker the security
   * @param field the field mnemonic
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bds(String ticker, String field, Backend<T> backend) {
    return bds(
        RequestArgs.single("ticker", ticker),
        RequestArgs.single("field", field),
        BdsOptions.defaults(),
        backend);
  }

  /**
   * Bulk reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bds(Collection, Collection, BdsOptions, Backend)}.
   *
   * @param ticker the security
   * @param field the field mnemonic
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bds(
      String ticker, String field, BdsOptions options, Backend<T> backend) {
    return bds(
        RequestArgs.single("ticker", ticker), RequestArgs.single("field", field), options, backend);
  }

  /**
   * Bulk reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bds(Collection, Collection, BdsOptions, Backend)}.
   *
   * @param ticker the security
   * @param fields the field mnemonics
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bds(String ticker, Collection<String> fields) {
    return bds(RequestArgs.single("ticker", ticker), fields, BdsOptions.defaults(), Backend.TABLE);
  }

  /**
   * Bulk reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bds(Collection, Collection, BdsOptions, Backend)}.
   *
   * @param ticker the security
   * @param fields the field mnemonics
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bds(
      String ticker, Collection<String> fields, BdsOptions options) {
    return bds(RequestArgs.single("ticker", ticker), fields, options, Backend.TABLE);
  }

  /**
   * Bulk reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bds(Collection, Collection, BdsOptions, Backend)}.
   *
   * @param ticker the security
   * @param fields the field mnemonics
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bds(
      String ticker, Collection<String> fields, Backend<T> backend) {
    return bds(RequestArgs.single("ticker", ticker), fields, BdsOptions.defaults(), backend);
  }

  /**
   * Bulk reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bds(Collection, Collection, BdsOptions, Backend)}.
   *
   * @param ticker the security
   * @param fields the field mnemonics
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bds(
      String ticker, Collection<String> fields, BdsOptions options, Backend<T> backend) {
    return bds(RequestArgs.single("ticker", ticker), fields, options, backend);
  }

  /**
   * Bulk reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bds(Collection, Collection, BdsOptions, Backend)}.
   *
   * @param tickers the securities
   * @param field the field mnemonic
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bds(Collection<String> tickers, String field) {
    return bds(tickers, RequestArgs.single("field", field), BdsOptions.defaults(), Backend.TABLE);
  }

  /**
   * Bulk reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bds(Collection, Collection, BdsOptions, Backend)}.
   *
   * @param tickers the securities
   * @param field the field mnemonic
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bds(
      Collection<String> tickers, String field, BdsOptions options) {
    return bds(tickers, RequestArgs.single("field", field), options, Backend.TABLE);
  }

  /**
   * Bulk reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bds(Collection, Collection, BdsOptions, Backend)}.
   *
   * @param tickers the securities
   * @param field the field mnemonic
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bds(
      Collection<String> tickers, String field, Backend<T> backend) {
    return bds(tickers, RequestArgs.single("field", field), BdsOptions.defaults(), backend);
  }

  /**
   * Bulk reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bds(Collection, Collection, BdsOptions, Backend)}.
   *
   * @param tickers the securities
   * @param field the field mnemonic
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bds(
      Collection<String> tickers, String field, BdsOptions options, Backend<T> backend) {
    return bds(tickers, RequestArgs.single("field", field), options, backend);
  }

  /**
   * Bulk reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bds(Collection, Collection, BdsOptions, Backend)}.
   *
   * @param tickers the securities
   * @param fields the field mnemonics
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bds(
      Collection<String> tickers, Collection<String> fields) {
    return bds(tickers, fields, BdsOptions.defaults(), Backend.TABLE);
  }

  /**
   * Bulk reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bds(Collection, Collection, BdsOptions, Backend)}.
   *
   * @param tickers the securities
   * @param fields the field mnemonics
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bds(
      Collection<String> tickers, Collection<String> fields, BdsOptions options) {
    return bds(tickers, fields, options, Backend.TABLE);
  }

  /**
   * Bulk reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bds(Collection, Collection, BdsOptions, Backend)}.
   *
   * @param tickers the securities
   * @param fields the field mnemonics
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bds(
      Collection<String> tickers, Collection<String> fields, Backend<T> backend) {
    return bds(tickers, fields, BdsOptions.defaults(), backend);
  }

  /**
   * Bulk reference data on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bds(Collection, Collection, BdsOptions, Backend)}.
   *
   * @param tickers the securities
   * @param fields the field mnemonics
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bds(
      Collection<String> tickers,
      Collection<String> fields,
      BdsOptions options,
      Backend<T> backend) {
    final List<String> tickerList = RequestArgs.strings("tickers", tickers);
    final List<String> fieldList = RequestArgs.strings("fields", fields);
    final BdsOptions checked = Objects.requireNonNull(options, "options");
    final Backend<T> form = Objects.requireNonNull(backend, "backend");
    return withEngine(engine -> engine.bds(tickerList, fieldList, checked, form));
  }

  /**
   * Intraday bars on the configured engine ({@link Xbbg#engine()}); see {@link Engine#bdib(String,
   * BdibOptions, Backend)}.
   *
   * @param ticker the security
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bdib(String ticker) {
    return bdib(ticker, BdibOptions.defaults(), Backend.TABLE);
  }

  /**
   * Intraday bars on the configured engine ({@link Xbbg#engine()}); see {@link Engine#bdib(String,
   * BdibOptions, Backend)}.
   *
   * @param ticker the security
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bdib(String ticker, BdibOptions options) {
    return bdib(ticker, options, Backend.TABLE);
  }

  /**
   * Intraday bars on the configured engine ({@link Xbbg#engine()}); see {@link Engine#bdib(String,
   * BdibOptions, Backend)}.
   *
   * @param ticker the security
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bdib(String ticker, Backend<T> backend) {
    return bdib(ticker, BdibOptions.defaults(), backend);
  }

  /**
   * Intraday bars on the configured engine ({@link Xbbg#engine()}); see {@link Engine#bdib(String,
   * BdibOptions, Backend)}.
   *
   * @param ticker the security
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bdib(
      String ticker, BdibOptions options, Backend<T> backend) {
    final String security = Objects.requireNonNull(ticker, "ticker");
    final BdibOptions checked = Objects.requireNonNull(options, "options");
    final Backend<T> form = Objects.requireNonNull(backend, "backend");
    return withEngine(engine -> engine.bdib(security, checked, form));
  }

  /**
   * Intraday ticks on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdtick(String, BdtickOptions, Backend)}.
   *
   * @param ticker the security
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bdtick(String ticker) {
    return bdtick(ticker, BdtickOptions.defaults(), Backend.TABLE);
  }

  /**
   * Intraday ticks on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdtick(String, BdtickOptions, Backend)}.
   *
   * @param ticker the security
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> bdtick(String ticker, BdtickOptions options) {
    return bdtick(ticker, options, Backend.TABLE);
  }

  /**
   * Intraday ticks on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdtick(String, BdtickOptions, Backend)}.
   *
   * @param ticker the security
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bdtick(String ticker, Backend<T> backend) {
    return bdtick(ticker, BdtickOptions.defaults(), backend);
  }

  /**
   * Intraday ticks on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#bdtick(String, BdtickOptions, Backend)}.
   *
   * @param ticker the security
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> bdtick(
      String ticker, BdtickOptions options, Backend<T> backend) {
    final String security = Objects.requireNonNull(ticker, "ticker");
    final BdtickOptions checked = Objects.requireNonNull(options, "options");
    final Backend<T> form = Objects.requireNonNull(backend, "backend");
    return withEngine(engine -> engine.bdtick(security, checked, form));
  }
}
