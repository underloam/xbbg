package io.github.underloam.xbbg;

import io.github.underloam.xbbg.table.Table;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Recipes on the configured engine: the {@code @xbbg/core} top-level {@code delayStatus}. Tickers
 * take a single {@code String} or a {@code Collection<String>}. Configuration and connection errors
 * complete the future exceptionally before any request is sent.
 */
abstract class BlpRecipes extends BlpRequests {
  BlpRecipes() {}

  /**
   * Delayed-data status on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#delayStatus(Collection, DelayStatusOptions, Backend)}.
   *
   * @param ticker the security
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> delayStatus(String ticker) {
    return delayStatus(
        RequestArgs.single("tickers", ticker), DelayStatusOptions.defaults(), Backend.TABLE);
  }

  /**
   * Delayed-data status on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#delayStatus(Collection, DelayStatusOptions, Backend)}.
   *
   * @param ticker the security
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> delayStatus(String ticker, DelayStatusOptions options) {
    return delayStatus(RequestArgs.single("tickers", ticker), options, Backend.TABLE);
  }

  /**
   * Delayed-data status on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#delayStatus(Collection, DelayStatusOptions, Backend)}.
   *
   * @param ticker the security
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> delayStatus(String ticker, Backend<T> backend) {
    return delayStatus(
        RequestArgs.single("tickers", ticker), DelayStatusOptions.defaults(), backend);
  }

  /**
   * Delayed-data status on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#delayStatus(Collection, DelayStatusOptions, Backend)}.
   *
   * @param ticker the security
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> delayStatus(
      String ticker, DelayStatusOptions options, Backend<T> backend) {
    return delayStatus(RequestArgs.single("tickers", ticker), options, backend);
  }

  /**
   * Delayed-data status on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#delayStatus(Collection, DelayStatusOptions, Backend)}.
   *
   * @param tickers the securities
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> delayStatus(Collection<String> tickers) {
    return delayStatus(tickers, DelayStatusOptions.defaults(), Backend.TABLE);
  }

  /**
   * Delayed-data status on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#delayStatus(Collection, DelayStatusOptions, Backend)}.
   *
   * @param tickers the securities
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> delayStatus(
      Collection<String> tickers, DelayStatusOptions options) {
    return delayStatus(tickers, options, Backend.TABLE);
  }

  /**
   * Delayed-data status on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#delayStatus(Collection, DelayStatusOptions, Backend)}.
   *
   * @param tickers the securities
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> delayStatus(
      Collection<String> tickers, Backend<T> backend) {
    return delayStatus(tickers, DelayStatusOptions.defaults(), backend);
  }

  /**
   * Delayed-data status on the configured engine ({@link Xbbg#engine()}); see {@link
   * Engine#delayStatus(Collection, DelayStatusOptions, Backend)}.
   *
   * @param tickers the securities
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> delayStatus(
      Collection<String> tickers, DelayStatusOptions options, Backend<T> backend) {
    final List<String> tickerList = RequestArgs.strings("tickers", tickers);
    final DelayStatusOptions checked = Objects.requireNonNull(options, "options");
    final Backend<T> form = Objects.requireNonNull(backend, "backend");
    return withEngine(engine -> engine.delayStatus(tickerList, checked, form));
  }
}
