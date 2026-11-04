package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.ArrowIpcSchema;
import io.github.underloam.xbbg.internal.EngineAccess;
import io.github.underloam.xbbg.internal.Futures;
import io.github.underloam.xbbg.internal.Json;
import io.github.underloam.xbbg.internal.JsonFields;
import io.github.underloam.xbbg.internal.JsonValue;
import io.github.underloam.xbbg.internal.NativeBridge;
import io.github.underloam.xbbg.internal.NativeCleaner;
import io.github.underloam.xbbg.internal.ResultKind;
import io.github.underloam.xbbg.table.DelayedDataInfo;
import io.github.underloam.xbbg.table.Table;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * The engine core: lifecycle, the generic request pipeline, identity, field metadata and schema
 * introspection, plus the helpers the request, recipe and subscription layers use to reach the
 * native engine.
 */
abstract class EngineBase {
  static {
    EngineAccess.install(
        new EngineAccess.Accessor() {
          @Override
          public <T> CompletableFuture<T> callTable(
              Engine engine,
              String method,
              JsonValue args,
              Backend<T> backend,
              boolean warnDelayed) {
            return engine.callTable(method, args, backend, warnDelayed);
          }

          @Override
          public CompletableFuture<Object> call(
              Engine engine, String method, JsonValue args, ResultKind kind) {
            return engine.call(method, args, kind);
          }

          @Override
          public Object callSync(Engine engine, String method, JsonValue args, ResultKind kind) {
            return engine.callSync(method, args, kind);
          }

          @Override
          public long handle(Engine engine) {
            return engine.handle();
          }
        });
  }

  private final long handle;
  private final KeyColumn keyColumn;
  private final AtomicBoolean closed = new AtomicBoolean();

  /**
   * Releases the handle once this object is unreachable. Release, not close: subscriptions and
   * calls in flight hold the native engine and keep using it after this object is collected.
   */
  private final NativeCleaner.Cleanable release;

  EngineBase(long handle, KeyColumn keyColumn) {
    this.handle = handle;
    this.keyColumn = keyColumn;
    this.release = NativeCleaner.register(this, new Release(handle));
  }

  // ------------------------------------------------------------------ helpers for the layers

  /**
   * Returns the native engine handle.
   *
   * @return the handle
   * @throws IllegalStateException if the engine is closed
   */
  protected final long handle() {
    if (closed.get()) {
      throw new IllegalStateException("engine is closed");
    }
    return handle;
  }

  /**
   * Calls an async engine method. A closed engine gives a failed future.
   *
   * @param method the method name
   * @param args the argument object
   * @param kind the result kind
   * @return the future result; cancelling it cancels the native call
   */
  protected final CompletableFuture<Object> call(String method, JsonValue args, ResultKind kind) {
    long engine;
    try {
      engine = handle();
    } catch (IllegalStateException e) {
      return Futures.failed(e);
    }
    return NativeBridge.engineCall(this, engine, method, args, kind);
  }

  /**
   * Calls a sync engine method on the calling thread.
   *
   * @param method the method name
   * @param args the argument object
   * @param kind the result kind
   * @return the result
   * @throws IllegalStateException if the engine is closed
   */
  protected final Object callSync(String method, JsonValue args, ResultKind kind) {
    return NativeBridge.engineCallSync(handle(), method, args, kind);
  }

  /**
   * Calls a table-producing method and converts the result, emitting the {@code ticker} key-column
   * deprecation warning like {@code request(RequestSpec, Backend)}.
   *
   * @param method the method name
   * @param args the argument object
   * @param backend the result backend
   * @param <T> the result type
   * @return the future result
   */
  protected final <T> CompletableFuture<T> callTable(
      String method, JsonValue args, Backend<T> backend) {
    return callTable(method, args, backend, false);
  }

  /**
   * Calls a table-producing method and converts the result after the warnings {@code
   * request(RequestSpec, Backend)} emits: one {@code BlpDelayedDataWarning} per delayed security
   * when {@code warnDelayed}, then the once-per-JVM {@code ticker} key-column deprecation.
   *
   * @param method the method name
   * @param args the argument object
   * @param backend the result backend
   * @param warnDelayed whether to emit delayed-data warnings ({@code onDelayed = WARN})
   * @param <T> the result type
   * @return the future result; cancelling it cancels the native call
   */
  protected final <T> CompletableFuture<T> callTable(
      String method, JsonValue args, final Backend<T> backend, final boolean warnDelayed) {
    final ResultKind kind = backend.nativeKind();
    return Futures.map(
        call(method, args, kind),
        value -> {
          emitResultWarnings(value, kind, warnDelayed);
          return backend.fromNative(value);
        });
  }

  /**
   * Calls an async method whose result kind is {@code JSON} and decodes the parsed value.
   *
   * @param method the method name
   * @param args the argument object
   * @param decoder converts the parsed JSON ({@code null} for a null result)
   * @param <T> the result type
   * @return the future result
   */
  protected final <T> CompletableFuture<T> callJson(
      final String method, JsonValue args, final Function<Object, T> decoder) {
    return Futures.map(
        call(method, args, ResultKind.JSON), value -> decoder.apply(parseJson(method, value)));
  }

  /**
   * Calls a sync method whose result kind is {@code JSON} and decodes the parsed value.
   *
   * @param method the method name
   * @param args the argument object
   * @param decoder converts the parsed JSON ({@code null} for a null result)
   * @param <T> the result type
   * @return the result
   */
  protected final <T> T callJsonSync(String method, JsonValue args, Function<Object, T> decoder) {
    return decoder.apply(parseJson(method, callSync(method, args, ResultKind.JSON)));
  }

  private static Object parseJson(String method, Object value) {
    return value == null ? null : JsonFields.parse((String) value, method + " result");
  }

  private void emitResultWarnings(Object value, ResultKind kind, boolean warnDelayed) {
    if (kind == ResultKind.TABLE) {
      Table table = (Table) value;
      if (warnDelayed) {
        Optional<Map<String, DelayedDataInfo>> delayed = table.delayed();
        if (delayed.isPresent()) {
          XbbgWarnings.emitDelayedData(delayed.get());
        }
      }
      XbbgWarnings.warnTickerKeyColumn(keyColumn, table.columnNames());
      return;
    }
    if (!warnDelayed && keyColumn != KeyColumn.TICKER) {
      return;
    }
    ArrowIpcSchema schema;
    try {
      schema = ArrowIpcSchema.read((byte[]) value);
    } catch (IllegalArgumentException e) {
      throw new BlpInternalException("native " + kind + " result is not Arrow IPC", e);
    }
    if (warnDelayed) {
      Map<String, DelayedDataInfo> delayed = rawDelayed(schema.metadata().get("xbbg.delayed"));
      if (delayed != null) {
        XbbgWarnings.emitDelayedData(delayed);
      }
    }
    XbbgWarnings.warnTickerKeyColumn(keyColumn, schema.fieldNames());
  }

  /**
   * Parses raw {@code xbbg.delayed} metadata ({@code {security: {delayed, delay_minutes}}}).
   * Malformed metadata gives {@code null}, like the JavaScript package.
   */
  static Map<String, DelayedDataInfo> rawDelayed(String json) {
    if (json == null) {
      return null;
    }
    try {
      Object parsed = Json.parse(json);
      if (!(parsed instanceof Map)) {
        return null;
      }
      Map<String, DelayedDataInfo> result = new LinkedHashMap<String, DelayedDataInfo>();
      for (Map.Entry<?, ?> entry : ((Map<?, ?>) parsed).entrySet()) {
        if (!(entry.getValue() instanceof Map)) {
          return null;
        }
        Map<?, ?> info = (Map<?, ?>) entry.getValue();
        Object delayed = info.get("delayed");
        Object minutes = info.get("delay_minutes");
        if (!(delayed instanceof Boolean) || !info.containsKey("delay_minutes")) {
          return null;
        }
        Integer delayMinutes = null;
        if (minutes instanceof Long) {
          delayMinutes = Integer.valueOf(((Long) minutes).intValue());
        } else if (minutes instanceof Double) {
          delayMinutes = Integer.valueOf(((Double) minutes).intValue());
        } else if (minutes != null) {
          return null;
        }
        result.put(
            String.valueOf(entry.getKey()), new DelayedDataInfo((Boolean) delayed, delayMinutes));
      }
      return result;
    } catch (IllegalArgumentException malformed) {
      return null;
    }
  }

  // ------------------------------------------------------------------ lifecycle

  /**
   * Whether {@code close()} was called.
   *
   * @return {@code true} once closed
   */
  public final boolean isClosed() {
    return closed.get();
  }

  /** Shuts the native engine down (its subscriptions end) and releases it; idempotent. */
  final void shutdownAndRelease() {
    if (closed.compareAndSet(false, true)) {
      try {
        NativeBridge.engineClose(handle);
      } finally {
        // engineClose already dropped the handle, so the release action does nothing native; this
        // deregisters it from the cleaner.
        release.clean();
      }
    }
  }

  /**
   * Returns the name of the column holding each result row's security.
   *
   * @return {@code ticker} (deprecated default) or {@code security}
   */
  public final KeyColumn keyColumn() {
    return keyColumn;
  }

  /**
   * Signals shutdown without waiting: closes admission and signals the engine's sessions so blocked
   * operations terminate. Prefer {@code close()}, which also releases the engine.
   */
  public final void signalShutdown() {
    callSync("signalShutdown", JsonValue.EMPTY_OBJECT, ResultKind.VOID);
  }

  /**
   * Whether any request worker is healthy.
   *
   * @return {@code true} if requests can be served
   */
  public final boolean isAvailable() {
    return (Boolean) callSync("isAvailable", JsonValue.EMPTY_OBJECT, ResultKind.BOOL);
  }

  // ------------------------------------------------------------------ generic requests

  /**
   * Sends a generic request and returns a {@link Table}.
   *
   * @param spec the request
   * @return the future table with its result metadata
   */
  public final CompletableFuture<Table> request(RequestSpec spec) {
    return request(spec, Backend.TABLE);
  }

  /**
   * Sends a generic request. With {@code onDelayed = WARN}, one {@code BlpDelayedDataWarning} is
   * emitted per delayed security before the result is converted.
   *
   * @param spec the request
   * @param backend the result form
   * @param <T> the result type
   * @return the future result
   */
  public final <T> CompletableFuture<T> request(RequestSpec spec, Backend<T> backend) {
    Objects.requireNonNull(spec, "spec");
    Objects.requireNonNull(backend, "backend");
    boolean warnDelayed = spec.onDelayed().orElse(null) == DelayedPolicy.WARN;
    return callTable("request", spec.toJson(), backend, warnDelayed);
  }

  /**
   * Sends a generic request and returns Arrow IPC stream bytes.
   *
   * @param spec the request
   * @return the future IPC stream
   */
  public final CompletableFuture<byte[]> requestRaw(RequestSpec spec) {
    return request(spec, Backend.IPC_STREAM);
  }

  /**
   * Sends a generic request and returns Arrow IPC file bytes.
   *
   * @param spec the request
   * @return the future IPC file
   */
  public final CompletableFuture<byte[]> requestIpcFile(RequestSpec spec) {
    return request(spec, Backend.IPC_FILE);
  }

  // ------------------------------------------------------------------ identity

  /**
   * Returns the seat type of the engine's identity. The first call authorizes (with the configured
   * {@code auth}, otherwise the logged-in Desktop user) and may take seconds.
   *
   * @return the future seat type
   */
  public final CompletableFuture<SeatType> seatType() {
    return Futures.map(
        call("seatType", JsonValue.EMPTY_OBJECT, ResultKind.STRING),
        value -> SeatType.parse((String) value));
  }

  /**
   * Checks the identity's entitlement to entitlement ids (for example from {@code eidData()} of a
   * result requested with {@code returnEids}).
   *
   * @param service the Bloomberg service
   * @param eids the entitlement ids
   * @return the future report
   */
  public final CompletableFuture<EntitlementReport> checkEntitlements(String service, int... eids) {
    List<Integer> list = new ArrayList<Integer>(eids.length);
    for (int eid : eids) {
      list.add(eid);
    }
    return checkEntitlements(service, list);
  }

  /**
   * Checks the identity's entitlement to entitlement ids.
   *
   * @param service the Bloomberg service
   * @param eids the entitlement ids
   * @return the future report
   */
  public final CompletableFuture<EntitlementReport> checkEntitlements(
      String service, Collection<Integer> eids) {
    JsonValue args =
        Json.object()
            .put("service", Objects.requireNonNull(service, "service"))
            .put("eids", new ArrayList<Integer>(eids))
            .build();
    return callJson(
        "checkEntitlements",
        args,
        value -> {
          Map<String, Object> report = JsonFields.object(value, "checkEntitlements result");
          List<Object> failed = JsonFields.list(report, "failedEids");
          List<Integer> eidList = new ArrayList<Integer>(failed.size());
          for (Object eid : failed) {
            eidList.add(Integer.valueOf((int) JsonFields.longOf(eid, "failedEids")));
          }
          return new EntitlementReport(JsonFields.bool(report, "entitled"), eidList);
        });
  }

  /**
   * Whether the engine's identity is authorized for a service.
   *
   * @param service the Bloomberg service
   * @return the future verdict
   */
  public final CompletableFuture<Boolean> identityIsAuthorized(String service) {
    return Futures.map(
        call("identityIsAuthorized", serviceArgs(service), ResultKind.BOOL),
        value -> (Boolean) value);
  }

  // ------------------------------------------------------------------ field metadata

  /**
   * Resolves the Arrow type of fields, querying {@code //blp/apiflds} for fields not yet in the
   * process-wide field cache; unknown fields resolve to {@code string}.
   *
   * @param fields the fields
   * @return the future field to Arrow type name
   */
  public final CompletableFuture<Map<String, String>> resolveFieldTypes(Collection<String> fields) {
    return resolveFieldTypes(fields, null, null);
  }

  /**
   * Resolves the Arrow type of fields, with manual types taking precedence.
   *
   * @param fields the fields
   * @param overrides field to Arrow type name, or {@code null}
   * @return the future field to Arrow type name
   */
  public final CompletableFuture<Map<String, String>> resolveFieldTypes(
      Collection<String> fields, Map<String, String> overrides) {
    return resolveFieldTypes(fields, overrides, null);
  }

  /**
   * Resolves the Arrow type of fields.
   *
   * @param fields the fields
   * @param overrides field to Arrow type name, or {@code null}
   * @param defaultType the type of unknown fields, or {@code null} for {@code string}
   * @return the future field to Arrow type name
   */
  public final CompletableFuture<Map<String, String>> resolveFieldTypes(
      Collection<String> fields, Map<String, String> overrides, String defaultType) {
    JsonValue args =
        Json.object()
            .put("fields", new ArrayList<String>(fields))
            .put("overrides", overrides)
            .put("defaultType", defaultType)
            .build();
    return callJson("resolveFieldTypes", args, value -> stringMap(value, "resolveFieldTypes"));
  }

  /**
   * Returns a field's definition from the field cache, without querying Bloomberg.
   *
   * @param field the field id
   * @return the definition, or empty when not cached
   */
  public final Optional<FieldInfo> getFieldInfo(String field) {
    JsonValue args = Json.object().put("field", Objects.requireNonNull(field, "field")).build();
    return callJsonSync(
        "getFieldInfo",
        args,
        value -> {
          if (value == null) {
            return Optional.<FieldInfo>empty();
          }
          Map<String, Object> info = JsonFields.object(value, "getFieldInfo result");
          return Optional.of(
              new FieldInfo(
                  JsonFields.string(info, "fieldId"),
                  JsonFields.string(info, "arrowType"),
                  JsonFields.string(info, "description"),
                  JsonFields.string(info, "category")));
        });
  }

  /** Clears the process-wide field cache. */
  public final void clearFieldCache() {
    callSync("clearFieldCache", JsonValue.EMPTY_OBJECT, ResultKind.VOID);
  }

  /** Writes the process-wide field cache to its file. */
  public final void saveFieldCache() {
    callSync("saveFieldCache", JsonValue.EMPTY_OBJECT, ResultKind.VOID);
  }

  /**
   * Checks fields with Bloomberg and returns the invalid or refused ones (the reasons are logged).
   *
   * @param fields the fields
   * @return the future invalid fields
   */
  public final CompletableFuture<List<String>> validateFields(Collection<String> fields) {
    JsonValue args = Json.object().put("fields", new ArrayList<String>(fields)).build();
    return callJson(
        "validateFields", args, value -> JsonFields.stringList(value, "validateFields result"));
  }

  /**
   * Whether the engine validates fields before sending requests.
   *
   * @return {@code false} when the validation mode is {@code disabled}
   */
  public final boolean isFieldValidationEnabled() {
    return (Boolean) callSync("isFieldValidationEnabled", JsonValue.EMPTY_OBJECT, ResultKind.BOOL);
  }

  // ------------------------------------------------------------------ schema introspection

  /**
   * Returns a service's schema as JSON text (cached in memory and on disk).
   *
   * @param service the Bloomberg service
   * @return the future schema JSON
   */
  public final CompletableFuture<String> getSchema(String service) {
    return jsonText(call("getSchema", serviceArgs(service), ResultKind.JSON), "getSchema");
  }

  /**
   * Returns one operation's schema as JSON text.
   *
   * @param service the Bloomberg service
   * @param operation the operation name
   * @return the future operation schema JSON
   */
  public final CompletableFuture<String> getOperation(String service, String operation) {
    JsonValue args =
        Json.object()
            .put("service", Objects.requireNonNull(service, "service"))
            .put("operation", Objects.requireNonNull(operation, "operation"))
            .build();
    return jsonText(call("getOperation", args, ResultKind.JSON), "getOperation");
  }

  /**
   * Lists a service's operations.
   *
   * @param service the Bloomberg service
   * @return the future operation names
   */
  public final CompletableFuture<List<String>> listOperations(String service) {
    return callJson(
        "listOperations",
        serviceArgs(service),
        value -> JsonFields.stringList(value, "listOperations result"));
  }

  /**
   * Lists a service's operations (alias of {@code listOperations(String)}).
   *
   * @param service the Bloomberg service
   * @return the future operation names
   */
  public final CompletableFuture<List<String>> bops(String service) {
    return listOperations(service);
  }

  /**
   * Returns a service's schema (alias of {@code getSchema(String)}).
   *
   * @param service the Bloomberg service
   * @return the future schema JSON
   */
  public final CompletableFuture<String> bschema(String service) {
    return getSchema(service);
  }

  /**
   * Returns one operation's schema (alias of {@code getOperation(String, String)}).
   *
   * @param service the Bloomberg service
   * @param operation the operation name
   * @return the future operation schema JSON
   */
  public final CompletableFuture<String> bschema(String service, String operation) {
    return getOperation(service, operation);
  }

  /**
   * Returns a service's schema if it is already in the memory cache.
   *
   * @param service the Bloomberg service
   * @return the schema JSON, or empty
   */
  public final Optional<String> getCachedSchema(String service) {
    Object value = callSync("getCachedSchema", serviceArgs(service), ResultKind.JSON);
    return value == null || "null".equals(value)
        ? Optional.<String>empty()
        : Optional.of((String) value);
  }

  /**
   * Drops a service's schema from the memory and disk caches.
   *
   * @param service the Bloomberg service
   */
  public final void invalidateSchema(String service) {
    callSync("invalidateSchema", serviceArgs(service), ResultKind.VOID);
  }

  /** Drops every cached schema. */
  public final void clearSchemaCache() {
    callSync("clearSchemaCache", JsonValue.EMPTY_OBJECT, ResultKind.VOID);
  }

  /**
   * Lists the services whose schemas are cached.
   *
   * @return the service URIs
   */
  public final List<String> listCachedSchemas() {
    return callJsonSync(
        "listCachedSchemas",
        JsonValue.EMPTY_OBJECT,
        value -> JsonFields.stringList(value, "listCachedSchemas result"));
  }

  /**
   * Returns the allowed values of an enumeration element.
   *
   * @param service the Bloomberg service
   * @param operation the operation name
   * @param element the element name
   * @return the future values, or empty when the element is not an enumeration
   */
  public final CompletableFuture<Optional<List<String>>> getEnumValues(
      String service, String operation, String element) {
    JsonValue args =
        Json.object()
            .put("service", Objects.requireNonNull(service, "service"))
            .put("operation", Objects.requireNonNull(operation, "operation"))
            .put("element", Objects.requireNonNull(element, "element"))
            .build();
    return callJson("getEnumValues", args, value -> optionalStrings(value, "getEnumValues"));
  }

  /**
   * Lists the valid element names of an operation's request.
   *
   * @param service the Bloomberg service
   * @param operation the operation name
   * @return the future element names, or empty when unknown
   */
  public final CompletableFuture<Optional<List<String>>> listValidElements(
      String service, String operation) {
    JsonValue args =
        Json.object()
            .put("service", Objects.requireNonNull(service, "service"))
            .put("operation", Objects.requireNonNull(operation, "operation"))
            .build();
    return callJson(
        "listValidElements", args, value -> optionalStrings(value, "listValidElements"));
  }

  // ------------------------------------------------------------------ subscriptions

  /**
   * Returns the engine's shared subscription feeds.
   *
   * @return the feeds
   */
  public final List<FeedInfo> subscriptionFeeds() {
    return callJsonSync(
        "subscriptionFeeds",
        JsonValue.EMPTY_OBJECT,
        value -> {
          List<Object> items = JsonFields.array(value, "subscriptionFeeds result");
          List<FeedInfo> feeds = new ArrayList<FeedInfo>(items.size());
          for (Object item : items) {
            Map<String, Object> feed = JsonFields.object(item, "subscriptionFeeds item");
            feeds.add(
                new FeedInfo(
                    JsonFields.string(feed, "service"),
                    JsonFields.string(feed, "topic"),
                    JsonFields.strings(feed, "options"),
                    JsonFields.strings(feed, "fields"),
                    JsonFields.longValue(feed, "consumers"),
                    JsonFields.optBool(feed, "delayed"),
                    JsonFields.string(feed, "state"),
                    JsonFields.bool(feed, "isolated"),
                    stringMap(JsonFields.obj(feed, "fieldErrors"), "fieldErrors")));
          }
          return Collections.unmodifiableList(feeds);
        });
  }

  // ------------------------------------------------------------------ shared decoding

  private static JsonValue serviceArgs(String service) {
    return Json.object().put("service", Objects.requireNonNull(service, "service")).build();
  }

  private static CompletableFuture<String> jsonText(
      CompletableFuture<Object> call, final String method) {
    return Futures.map(
        call,
        value -> {
          if (value == null) {
            throw new BlpInternalException("native " + method + " returned no schema");
          }
          return (String) value;
        });
  }

  private static Map<String, String> stringMap(Object value, String what) {
    Map<String, Object> object = JsonFields.object(value, what);
    Map<String, String> result = new LinkedHashMap<String, String>();
    for (Map.Entry<String, Object> entry : object.entrySet()) {
      if (!(entry.getValue() instanceof String)) {
        throw JsonFields.malformed(what + "." + entry.getKey(), "must be a string");
      }
      result.put(entry.getKey(), (String) entry.getValue());
    }
    return Collections.unmodifiableMap(result);
  }

  private static Optional<List<String>> optionalStrings(Object value, String what) {
    return value == null
        ? Optional.<List<String>>empty()
        : Optional.of(JsonFields.stringList(value, what + " result"));
  }

  /**
   * Releases the native engine of a collected {@code Engine} without shutting it down: the native
   * engine lives on while subscriptions or calls in flight hold it. Must not reference the engine.
   */
  private static final class Release implements Runnable {
    private final long handle;

    Release(long handle) {
      this.handle = handle;
    }

    @Override
    public void run() {
      NativeBridge.engineRelease(handle);
    }
  }
}
