package io.github.underloam.xbbg.ext;

import io.github.underloam.xbbg.Backend;
import io.github.underloam.xbbg.BdpOptions;
import io.github.underloam.xbbg.CdxFields;
import io.github.underloam.xbbg.CdxOptions;
import io.github.underloam.xbbg.Engine;
import io.github.underloam.xbbg.Xbbg;
import io.github.underloam.xbbg.internal.Futures;
import io.github.underloam.xbbg.table.Table;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * CDX index helpers (JS {@code ext.cdx}): reference-data requests over the generated CDX field
 * bundles ({@link CdxFields}) on the configured engine. The same requests on an explicit engine are
 * {@link Engine#acdxInfo(String, BdpOptions, Backend)}, {@link Engine#acdxPricing(String,
 * CdxOptions, Backend)} and {@link Engine#acdxRisk(String, CdxOptions, Backend)}.
 *
 * <p>Configuration and connection errors complete the returned future exceptionally, like request
 * errors. Cancelling the returned future cancels the request, never the shared connection.
 */
public final class Cdx {
  private Cdx() {}

  /**
   * CDX index information on the configured engine ({@link Xbbg#engine()}): {@code bdp} of the
   * ticker over {@link CdxFields#INFO} (JS {@code ext.cdx.acdx_info}).
   *
   * @param ticker the CDX index ticker
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> acdxInfo(String ticker) {
    return acdxInfo(ticker, BdpOptions.defaults(), Backend.TABLE);
  }

  /**
   * CDX index information on the configured engine ({@link Xbbg#engine()}): {@code bdp} of the
   * ticker over {@link CdxFields#INFO} (JS {@code ext.cdx.acdx_info}).
   *
   * @param ticker the CDX index ticker
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> acdxInfo(String ticker, BdpOptions options) {
    return acdxInfo(ticker, options, Backend.TABLE);
  }

  /**
   * CDX index information on the configured engine ({@link Xbbg#engine()}): {@code bdp} of the
   * ticker over {@link CdxFields#INFO} (JS {@code ext.cdx.acdx_info}).
   *
   * @param ticker the CDX index ticker
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> acdxInfo(String ticker, Backend<T> backend) {
    return acdxInfo(ticker, BdpOptions.defaults(), backend);
  }

  /**
   * CDX index information on the configured engine ({@link Xbbg#engine()}): {@code bdp} of the
   * ticker over {@link CdxFields#INFO} (JS {@code ext.cdx.acdx_info}).
   *
   * @param ticker the CDX index ticker
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> acdxInfo(
      String ticker, BdpOptions options, Backend<T> backend) {
    final String security = Objects.requireNonNull(ticker, "ticker");
    final BdpOptions checked = Objects.requireNonNull(options, "options");
    final Backend<T> form = Objects.requireNonNull(backend, "backend");
    return Futures.compose(Xbbg.engine(), engine -> engine.acdxInfo(security, checked, form));
  }

  /**
   * CDX pricing on the configured engine ({@link Xbbg#engine()}): {@code bdp} of the ticker over
   * {@link CdxFields#PRICING}, with {@link CdxOptions#recoveryRate()} sent as the {@code CDS_RR}
   * override (JS {@code ext.cdx.acdx_pricing}).
   *
   * @param ticker the CDX index ticker
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> acdxPricing(String ticker) {
    return acdxPricing(ticker, CdxOptions.defaults(), Backend.TABLE);
  }

  /**
   * CDX pricing on the configured engine ({@link Xbbg#engine()}): {@code bdp} of the ticker over
   * {@link CdxFields#PRICING}, with {@link CdxOptions#recoveryRate()} sent as the {@code CDS_RR}
   * override (JS {@code ext.cdx.acdx_pricing}).
   *
   * @param ticker the CDX index ticker
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> acdxPricing(String ticker, CdxOptions options) {
    return acdxPricing(ticker, options, Backend.TABLE);
  }

  /**
   * CDX pricing on the configured engine ({@link Xbbg#engine()}): {@code bdp} of the ticker over
   * {@link CdxFields#PRICING}, with {@link CdxOptions#recoveryRate()} sent as the {@code CDS_RR}
   * override (JS {@code ext.cdx.acdx_pricing}).
   *
   * @param ticker the CDX index ticker
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> acdxPricing(String ticker, Backend<T> backend) {
    return acdxPricing(ticker, CdxOptions.defaults(), backend);
  }

  /**
   * CDX pricing on the configured engine ({@link Xbbg#engine()}): {@code bdp} of the ticker over
   * {@link CdxFields#PRICING}, with {@link CdxOptions#recoveryRate()} sent as the {@code CDS_RR}
   * override (JS {@code ext.cdx.acdx_pricing}).
   *
   * @param ticker the CDX index ticker
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> acdxPricing(
      String ticker, CdxOptions options, Backend<T> backend) {
    final String security = Objects.requireNonNull(ticker, "ticker");
    final CdxOptions checked = Objects.requireNonNull(options, "options");
    final Backend<T> form = Objects.requireNonNull(backend, "backend");
    return Futures.compose(Xbbg.engine(), engine -> engine.acdxPricing(security, checked, form));
  }

  /**
   * CDX risk measures on the configured engine ({@link Xbbg#engine()}): {@code bdp} of the ticker
   * over {@link CdxFields#RISK}, with {@link CdxOptions#recoveryRate()} sent as the {@code CDS_RR}
   * override (JS {@code ext.cdx.acdx_risk}).
   *
   * @param ticker the CDX index ticker
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> acdxRisk(String ticker) {
    return acdxRisk(ticker, CdxOptions.defaults(), Backend.TABLE);
  }

  /**
   * CDX risk measures on the configured engine ({@link Xbbg#engine()}): {@code bdp} of the ticker
   * over {@link CdxFields#RISK}, with {@link CdxOptions#recoveryRate()} sent as the {@code CDS_RR}
   * override (JS {@code ext.cdx.acdx_risk}).
   *
   * @param ticker the CDX index ticker
   * @param options the options
   * @return a future completing with the result {@link Table}
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Table> acdxRisk(String ticker, CdxOptions options) {
    return acdxRisk(ticker, options, Backend.TABLE);
  }

  /**
   * CDX risk measures on the configured engine ({@link Xbbg#engine()}): {@code bdp} of the ticker
   * over {@link CdxFields#RISK}, with {@link CdxOptions#recoveryRate()} sent as the {@code CDS_RR}
   * override (JS {@code ext.cdx.acdx_risk}).
   *
   * @param ticker the CDX index ticker
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> acdxRisk(String ticker, Backend<T> backend) {
    return acdxRisk(ticker, CdxOptions.defaults(), backend);
  }

  /**
   * CDX risk measures on the configured engine ({@link Xbbg#engine()}): {@code bdp} of the ticker
   * over {@link CdxFields#RISK}, with {@link CdxOptions#recoveryRate()} sent as the {@code CDS_RR}
   * override (JS {@code ext.cdx.acdx_risk}).
   *
   * @param ticker the CDX index ticker
   * @param options the options
   * @param backend the result form
   * @param <T> the result type
   * @return a future completing with the result in the backend's form
   * @throws NullPointerException if an argument is {@code null}
   */
  public static <T> CompletableFuture<T> acdxRisk(
      String ticker, CdxOptions options, Backend<T> backend) {
    final String security = Objects.requireNonNull(ticker, "ticker");
    final CdxOptions checked = Objects.requireNonNull(options, "options");
    final Backend<T> form = Objects.requireNonNull(backend, "backend");
    return Futures.compose(Xbbg.engine(), engine -> engine.acdxRisk(security, checked, form));
  }
}
