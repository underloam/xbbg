package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.Json;
import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import io.github.underloam.xbbg.internal.JsonValue;
import java.util.List;
import java.util.Objects;

/**
 * JSON argument objects of the recipe engine methods: the JavaScript positional arguments as named
 * fields plus the set options (the native side documents every field and default). Dates arrive
 * already formatted; a {@code null} date is left out, which the native side treats like the
 * JavaScript {@code ''}.
 */
final class RecipeArgs {
  private RecipeArgs() {}

  static JsonValue bqr(String ticker, BqrOptions options) {
    JsonObjectBuilder args = Json.object().put("ticker", required(ticker, "ticker"));
    options.encodeInto(args);
    return args.build();
  }

  static JsonValue yas(List<String> tickers, List<String> fields, YasOptions options) {
    JsonObjectBuilder args = Json.object().put("tickers", tickers).put("fields", fields);
    options.encodeInto(args);
    return args.build();
  }

  static JsonValue preferreds(String equityTicker, PreferredsOptions options) {
    JsonObjectBuilder args =
        Json.object().put("equityTicker", required(equityTicker, "equityTicker"));
    options.encodeInto(args);
    return args.build();
  }

  static JsonValue corporateBonds(String ticker, CorporateBondsOptions options) {
    JsonObjectBuilder args = Json.object().put("ticker", required(ticker, "ticker"));
    options.encodeInto(args);
    return args.build();
  }

  /** {@code futTicker} and {@code activeFutures}. */
  static JsonValue futuresResolve(String genTicker, String dt, FuturesResolveOptions options) {
    JsonObjectBuilder args =
        Json.object().put("genTicker", required(genTicker, "genTicker")).put("dt", dt);
    options.encodeInto(args);
    return args.build();
  }

  static JsonValue futuresCurve(String genTicker, FuturesCurveOptions options) {
    JsonObjectBuilder args = Json.object().put("genTicker", required(genTicker, "genTicker"));
    options.encodeInto(args);
    return args.build();
  }

  static JsonValue cdxTicker(String genTicker, String dt, CdxResolveOptions options) {
    JsonObjectBuilder args =
        Json.object().put("genTicker", required(genTicker, "genTicker")).put("dt", dt);
    options.encodeInto(args);
    return args.build();
  }

  static JsonValue activeCdx(String genTicker, String dt, ActiveCdxOptions options) {
    JsonObjectBuilder args =
        Json.object().put("genTicker", required(genTicker, "genTicker")).put("dt", dt);
    options.encodeInto(args);
    return args.build();
  }

  static JsonValue dividend(
      List<String> tickers, String startDate, String endDate, DividendOptions options) {
    JsonObjectBuilder args = dated("tickers", tickers, startDate, endDate);
    options.encodeInto(args);
    return args.build();
  }

  static JsonValue dividendYield(
      List<String> tickers, String startDate, String endDate, DividendYieldOptions options) {
    JsonObjectBuilder args = dated("tickers", tickers, startDate, endDate);
    options.encodeInto(args);
    return args.build();
  }

  static JsonValue turnover(
      List<String> tickers, String startDate, String endDate, TurnoverOptions options) {
    JsonObjectBuilder args = dated("tickers", tickers, startDate, endDate);
    options.encodeInto(args);
    return args.build();
  }

  static JsonValue etfHoldings(String etfTicker, EtfHoldingsOptions options) {
    JsonObjectBuilder args = Json.object().put("etfTicker", required(etfTicker, "etfTicker"));
    options.encodeInto(args);
    return args.build();
  }

  static JsonValue volSurface(
      List<String> tickers, String startDate, String endDate, VolSurfaceOptions options) {
    JsonObjectBuilder args = dated("tickers", tickers, startDate, endDate);
    options.encodeInto(args);
    return args.build();
  }

  static JsonValue indexMembers(String index, IndexMembersOptions options) {
    JsonObjectBuilder args = Json.object().put("index", required(index, "index"));
    options.encodeInto(args);
    return args.build();
  }

  static JsonValue resolveIsins(List<String> isins) {
    return Json.object().put("isins", isins).build();
  }

  static JsonValue resolveVenues(List<String> securities, ResolveVenuesOptions options) {
    JsonObjectBuilder args = Json.object().put("securities", securities);
    options.encodeInto(args);
    return args.build();
  }

  static JsonValue auctionSnapshot(List<String> securities, AuctionSnapshotOptions options) {
    JsonObjectBuilder args = Json.object().put("securities", securities);
    options.encodeInto(args);
    return args.build();
  }

  static JsonValue delayStatus(List<String> tickers, DelayStatusOptions options) {
    JsonObjectBuilder args = Json.object().put("tickers", tickers);
    options.encodeInto(args);
    return args.build();
  }

  static JsonValue issuerIsins(List<String> bondIsins) {
    return Json.object().put("bondIsins", bondIsins).build();
  }

  /** {@code etfNavRelationships} and {@code etfNavSnapshot}; tickers are trimmed natively. */
  static JsonValue etfNav(List<String> tickers) {
    return Json.object().put("tickers", tickers).build();
  }

  static JsonValue etfNavHistory(List<String> tickers, String startDate, String endDate) {
    return dated("tickers", tickers, startDate, endDate).build();
  }

  static JsonValue currencyConversion(
      String ticker, String targetCcy, String startDate, String endDate) {
    return Json.object()
        .put("ticker", required(ticker, "ticker"))
        .put("targetCcy", required(targetCcy, "targetCcy"))
        .put("startDate", startDate)
        .put("endDate", endDate)
        .build();
  }

  private static JsonObjectBuilder dated(
      String listName, List<String> values, String startDate, String endDate) {
    return Json.object().put(listName, values).put("startDate", startDate).put("endDate", endDate);
  }

  private static String required(String value, String name) {
    return Objects.requireNonNull(value, name);
  }
}
