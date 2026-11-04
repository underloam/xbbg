package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.Json;
import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import io.github.underloam.xbbg.internal.JsonValue;
import java.util.Collection;
import java.util.Objects;

/**
 * Argument objects of the typed request wrappers: one builder per native dispatch name, with the
 * positional JS arguments as named fields followed by the set options. The native planner (Rust
 * {@code xbbg_host::request}) turns each object into the generic request the TS wrapper builds.
 */
final class TypedRequests {
  private TypedRequests() {}

  /** Whether the call must emit delayed-data warnings after the result arrives. */
  static boolean warns(DelayedPolicy policy) {
    return RequestArgs.warnsOnDelayed(policy);
  }

  static JsonValue bdp(Collection<String> tickers, Collection<String> fields, BdpOptions options) {
    JsonObjectBuilder args = lists(tickers, fields);
    Objects.requireNonNull(options, "options").encodeInto(args);
    return args.build();
  }

  static JsonValue bds(Collection<String> tickers, Collection<String> fields, BdsOptions options) {
    JsonObjectBuilder args = lists(tickers, fields);
    Objects.requireNonNull(options, "options").encodeInto(args);
    return args.build();
  }

  static JsonValue bdh(Collection<String> tickers, Collection<String> fields, BdhOptions options) {
    JsonObjectBuilder args = lists(tickers, fields);
    Objects.requireNonNull(options, "options").encodeInto(args);
    return args.build();
  }

  static JsonValue bdib(String ticker, BdibOptions options) {
    JsonObjectBuilder args = text("ticker", ticker);
    Objects.requireNonNull(options, "options").encodeInto(args);
    return args.build();
  }

  static JsonValue bdtick(String ticker, BdtickOptions options) {
    JsonObjectBuilder args = text("ticker", ticker);
    Objects.requireNonNull(options, "options").encodeInto(args);
    return args.build();
  }

  static JsonValue bql(String query, BqlOptions options) {
    JsonObjectBuilder args = text("query", query);
    Objects.requireNonNull(options, "options").encodeInto(args);
    return args.build();
  }

  static JsonValue beqs(String screen, BeqsOptions options) {
    JsonObjectBuilder args = text("screen", screen);
    Objects.requireNonNull(options, "options").encodeInto(args);
    return args.build();
  }

  static JsonValue bsrch(String searchSpec, BsrchOptions options) {
    JsonObjectBuilder args = text("searchSpec", searchSpec);
    Objects.requireNonNull(options, "options").encodeInto(args);
    return args.build();
  }

  static JsonValue bta(String ticker, String study, BtaOptions options) {
    JsonObjectBuilder args =
        text("ticker", ticker).put("study", Objects.requireNonNull(study, "study"));
    Objects.requireNonNull(options, "options").encodeInto(args);
    return args.build();
  }

  static JsonValue bta(String ticker, BtaStudy study, BtaOptions options) {
    JsonObjectBuilder args =
        text("ticker", ticker).put("study", Objects.requireNonNull(study, "study").toJson());
    Objects.requireNonNull(options, "options").encodeInto(args);
    return args.build();
  }

  static JsonValue bflds(BfldsOptions options) {
    JsonObjectBuilder args = Json.object();
    Objects.requireNonNull(options, "options").encodeInto(args);
    return args.build();
  }

  /**
   * {@code fieldInfo}: the fields argument is the only field list; options that also set {@code
   * fields} or {@code searchSpec} are rejected (JS lets them override the argument).
   */
  static JsonValue fieldInfo(Collection<String> fields, BfldsOptions options) {
    Objects.requireNonNull(options, "options");
    if (options.fields().isPresent() || options.searchSpec().isPresent()) {
      throw new IllegalArgumentException(
          "fieldInfo() takes its fields as an argument; BfldsOptions fields and searchSpec must be"
              + " unset");
    }
    JsonObjectBuilder args = Json.object().put("fields", RequestArgs.strings("fields", fields));
    options.encodeInto(args);
    return args.build();
  }

  /**
   * {@code fieldSearch}: the search argument is the only search; options that also set {@code
   * fields} or {@code searchSpec} are rejected (JS lets them override the argument).
   */
  static JsonValue fieldSearch(String searchSpec, BfldsOptions options) {
    Objects.requireNonNull(options, "options");
    if (options.fields().isPresent() || options.searchSpec().isPresent()) {
      throw new IllegalArgumentException(
          "fieldSearch() takes its search as an argument; BfldsOptions fields and searchSpec must"
              + " be unset");
    }
    JsonObjectBuilder args = text("searchSpec", searchSpec);
    options.encodeInto(args);
    return args.build();
  }

  static JsonValue blkp(String query, BlkpOptions options) {
    JsonObjectBuilder args = text("query", query);
    Objects.requireNonNull(options, "options").encodeInto(args);
    return args.build();
  }

  static JsonValue bport(String portfolio, Collection<String> fields, RequestOptions options) {
    JsonObjectBuilder args =
        text("portfolio", portfolio).put("fields", RequestArgs.strings("fields", fields));
    Objects.requireNonNull(options, "options").encodeInto(args);
    return args.build();
  }

  /** {@code bcurves} and {@code bgovts} share one argument shape. */
  static JsonValue instrumentList(String ticker, RequestOptions options) {
    JsonObjectBuilder args = text("ticker", ticker);
    Objects.requireNonNull(options, "options").encodeInto(args);
    return args.build();
  }

  static JsonValue acdxInfo(String ticker, BdpOptions options) {
    JsonObjectBuilder args = text("ticker", ticker);
    Objects.requireNonNull(options, "options").encodeInto(args);
    return args.build();
  }

  /** {@code acdxPricing} and {@code acdxRisk} share one argument shape. */
  static JsonValue cdx(String ticker, CdxOptions options) {
    JsonObjectBuilder args = text("ticker", ticker);
    Objects.requireNonNull(options, "options").encodeInto(args);
    return args.build();
  }

  private static JsonObjectBuilder lists(Collection<String> tickers, Collection<String> fields) {
    return Json.object()
        .put("tickers", RequestArgs.strings("tickers", tickers))
        .put("fields", RequestArgs.strings("fields", fields));
  }

  private static JsonObjectBuilder text(String name, String value) {
    return Json.object().put(name, Objects.requireNonNull(value, name));
  }
}
