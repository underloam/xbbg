package io.github.underloam.xbbg.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.underloam.xbbg.Backend;
import io.github.underloam.xbbg.BlpErrorKind;
import io.github.underloam.xbbg.BlpException;
import io.github.underloam.xbbg.BlpInternalException;
import io.github.underloam.xbbg.BlpLimitException;
import io.github.underloam.xbbg.BlpRequestException;
import io.github.underloam.xbbg.BlpSessionException;
import io.github.underloam.xbbg.BlpSubscriptionDataLossException;
import io.github.underloam.xbbg.BlpTimeoutException;
import io.github.underloam.xbbg.BlpValidationException;
import io.github.underloam.xbbg.BlpapiNotFoundException;
import io.github.underloam.xbbg.Xbbg;
import io.github.underloam.xbbg.table.Column;
import io.github.underloam.xbbg.table.DataType;
import io.github.underloam.xbbg.table.ResultMetadata;
import io.github.underloam.xbbg.table.RowsResult;
import io.github.underloam.xbbg.table.Table;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Runs against a {@code --features test-support} build of {@code xbbg_jni}. */
class NativeBridgeIntegrationTest {
  @BeforeAll
  static void natives() {
    NativeTestSupport.assumeAvailable();
  }

  private static JsonValue args(Object... keysAndValues) {
    JsonObjectBuilder builder = Json.object();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      builder.put((String) keysAndValues[i], keysAndValues[i + 1]);
    }
    return builder.build();
  }

  private static Map<String, Object> expected(JsonValue window) {
    String text =
        (String) NativeTestSupport.invoke("core.sampleTableExpected", window, ResultKind.JSON);
    return JsonFields.object(Json.parse(text), "expected");
  }

  @Test
  void handshakeAndVersions() {
    assertEquals(NativeLibraries.ABI, NativeXbbg.abiVersion());
    assertFalse(Xbbg.nativeVersion().isEmpty());
    assertTrue(Xbbg.sdkVersion().matches("\\d+\\.\\d+\\.\\d+\\.\\d+"), Xbbg.sdkVersion());
    assertTrue(NativeLibraries.isLoaded());
    assertTrue(NativeLibraries.loadedBlpapi().getFileName().toString().contains("blpapi3"));
  }

  @Test
  void tablesMatchTheNativeRenderingForEveryColumn() {
    for (JsonValue window :
        Arrays.asList(
            JsonValue.EMPTY_OBJECT,
            args("offset", 0),
            args("offset", 5, "length", 3),
            args("offset", 11))) {
      Table table = (Table) NativeTestSupport.invoke("core.sampleTable", window, ResultKind.TABLE);
      assertMatches(expected(window), table, window.text());
    }
  }

  @Test
  void asyncTablesMatchToo() throws Exception {
    Table table =
        (Table)
            NativeTestSupport.invokeAsync(
                    "core.sampleTable", JsonValue.EMPTY_OBJECT, ResultKind.TABLE)
                .get(30, TimeUnit.SECONDS);
    assertMatches(expected(JsonValue.EMPTY_OBJECT), table, "async default window");
  }

  @Test
  void tableListsCarryTheWholeBatchAndTheWindow() {
    Table[] tables =
        (Table[])
            NativeTestSupport.invoke(
                "core.sampleTable", JsonValue.EMPTY_OBJECT, ResultKind.TABLE_LIST);
    assertEquals(2, tables.length);
    assertMatches(expected(args("offset", 0)), tables[0], "whole");
    assertMatches(expected(JsonValue.EMPTY_OBJECT), tables[1], "window");
  }

  @Test
  void ipcResultsCarryTheSameSchema() {
    Map<String, Object> expected = expected(JsonValue.EMPTY_OBJECT);
    List<String> names = new ArrayList<String>();
    for (Object column : JsonFields.list(expected, "columns")) {
      names.add(JsonFields.string(JsonFields.object(column, "column"), "name"));
    }
    for (ResultKind kind : Arrays.asList(ResultKind.IPC_STREAM, ResultKind.IPC_FILE)) {
      byte[] ipc =
          (byte[]) NativeTestSupport.invoke("core.sampleTable", JsonValue.EMPTY_OBJECT, kind);
      ArrowIpcSchema schema;
      try {
        schema = ArrowIpcSchema.read(ipc);
      } catch (IllegalArgumentException e) {
        throw new AssertionError(
            kind
                + " bytes ("
                + ipc.length
                + ") start with "
                + Hashes.hex(Arrays.copyOf(ipc, Math.min(48, ipc.length))),
            e);
      }
      assertEquals(names, schema.fieldNames(), kind.name());
      assertEquals(stringMap(JsonFields.obj(expected, "metadata")), schema.metadata(), kind.name());
    }
  }

  @Test
  void rowsBackendMaterializesTheNativeTable() {
    Table table =
        (Table)
            NativeTestSupport.invoke("core.sampleTable", JsonValue.EMPTY_OBJECT, ResultKind.TABLE);
    RowsResult rows = Backends.fromNative(Backend.ROWS, table);
    assertEquals(table.numRows(), rows.size());
    assertEquals(table.resultMetadata(), rows.resultMetadata());
    for (int row = 0; row < table.numRows(); row++) {
      Map<String, Object> fromTable = table.row(row).toMap();
      Map<String, Object> fromRows = rows.get(row);
      assertEquals(fromTable.keySet(), fromRows.keySet());
      for (String key : fromTable.keySet()) {
        assertTrue(java.util.Objects.deepEquals(fromTable.get(key), fromRows.get(key)), key);
      }
    }
  }

  @Test
  void raisedErrorsKeepKindAndContext() throws Exception {
    Map<String, Object> context = new LinkedHashMap<String, Object>();
    context.put("service", "//blp/refdata");
    context.put("operation", "ReferenceDataRequest");
    context.put("requestId", "req-7");
    context.put("bloombergRequestIds", Arrays.asList("bbg-1"));
    context.put("topic", "IBM US Equity");
    context.put("detail", "gap");
    context.put("element", "fields");
    context.put("suggestion", "PX_LAST");
    String[] names = {
      "SESSION",
      "REQUEST",
      "LIMIT",
      "DATALOSS",
      "VALIDATION",
      "TIMEOUT",
      "INTERNAL",
      "ARGUMENT",
      "STATE",
      "CANCELLED",
      "SDK_NOT_FOUND"
    };
    Class<?>[] classes = {
      BlpSessionException.class, BlpRequestException.class, BlpLimitException.class,
      BlpSubscriptionDataLossException.class, BlpValidationException.class,
          BlpTimeoutException.class,
      BlpInternalException.class, IllegalArgumentException.class, IllegalStateException.class,
      CancellationException.class, BlpapiNotFoundException.class
    };
    for (int i = 0; i < names.length; i++) {
      JsonValue raise = args("kind", names[i], "message", "raised " + names[i], "context", context);
      Throwable sync =
          assertThrows(
              Throwable.class,
              () -> NativeTestSupport.invoke("core.raise", raise, ResultKind.VOID));
      assertEquals(classes[i], sync.getClass(), names[i]);
      assertEquals("raised " + names[i], sync.getMessage());
      Throwable asyncError;
      try {
        NativeTestSupport.invokeAsync("core.raise", raise, ResultKind.VOID)
            .get(30, TimeUnit.SECONDS);
        throw new AssertionError(names[i] + " async call succeeded");
      } catch (ExecutionException e) {
        asyncError = e.getCause();
      } catch (CancellationException e) {
        asyncError = e;
      }
      assertEquals(classes[i], asyncError.getClass(), names[i] + " async");
      if (sync instanceof BlpException) {
        BlpException blp = (BlpException) sync;
        assertEquals(BlpErrorKind.valueOf(names[i]), blp.kind());
        if (blp.kind() != BlpErrorKind.SDK_NOT_FOUND) {
          assertEquals("bbg-1", blp.bloombergRequestId().get(), names[i]);
        }
      }
    }
    BlpRequestException request =
        assertThrows(
            BlpRequestException.class,
            () ->
                NativeTestSupport.invoke(
                    "core.raise", args("kind", "REQUEST", "context", context), ResultKind.VOID));
    assertEquals("//blp/refdata", request.service().get());
    assertEquals("ReferenceDataRequest", request.operation().get());
    assertEquals("req-7", request.requestId().get());
    BlpSubscriptionDataLossException dataLoss =
        assertThrows(
            BlpSubscriptionDataLossException.class,
            () ->
                NativeTestSupport.invoke(
                    "core.raise", args("kind", "DATALOSS", "context", context), ResultKind.VOID));
    assertEquals("IBM US Equity", dataLoss.topic().get());
    assertEquals("gap", dataLoss.detail().get());
    BlpValidationException validation =
        assertThrows(
            BlpValidationException.class,
            () ->
                NativeTestSupport.invoke(
                    "core.raise", args("kind", "VALIDATION", "context", context), ResultKind.VOID));
    assertEquals("fields", validation.element().get());
    assertEquals("PX_LAST", validation.suggestion().get());
  }

  @Test
  void panicsBecomeInternalErrors() throws Exception {
    BlpInternalException sync =
        assertThrows(
            BlpInternalException.class,
            () -> NativeTestSupport.invoke("core.panic", args("message", "boom"), ResultKind.VOID));
    assertEquals("native panic: boom", sync.getMessage());
    ExecutionException async =
        assertThrows(
            ExecutionException.class,
            () ->
                NativeTestSupport.invokeAsync(
                        "core.panic", args("message", "late"), ResultKind.VOID)
                    .get(30, TimeUnit.SECONDS));
    assertInstanceOf(BlpInternalException.class, async.getCause());
    assertEquals("native panic: late", async.getCause().getMessage());
  }

  @Test
  void echoReturnsEveryScalarKind() throws Exception {
    assertNull(NativeTestSupport.invoke("core.echo", JsonValue.EMPTY_OBJECT, ResultKind.VOID));
    assertEquals("x", NativeTestSupport.invoke("core.echo", args("value", "x"), ResultKind.STRING));
    assertEquals(true, NativeTestSupport.invoke("core.echo", args("value", true), ResultKind.BOOL));
    assertEquals(
        Long.valueOf(42),
        NativeTestSupport.invoke("core.echo", args("value", 42), ResultKind.HANDLE));
    Object json =
        NativeTestSupport.invoke(
            "core.echo", args("value", JsonValue.parse("{\"a\":[1,\"\u00e9\"]}")), ResultKind.JSON);
    assertEquals(Json.parse("{\"a\":[1,\"\u00e9\"]}"), Json.parse((String) json));
    assertEquals(
        "caf\u00e9 \ud83d\ude00",
        NativeTestSupport.invokeAsync(
                "core.echo", args("value", "caf\u00e9 \ud83d\ude00"), ResultKind.STRING)
            .get(30, TimeUnit.SECONDS));
  }

  @Test
  void argumentProblemsAreIllegalArguments() throws Exception {
    assertThrows(
        IllegalArgumentException.class,
        () -> NativeTestSupport.invoke("core.echo", args("unexpected", 1), ResultKind.VOID));
    ExecutionException async =
        assertThrows(
            ExecutionException.class,
            () ->
                NativeTestSupport.invokeAsync("core.sleep", args("millis", "soon"), ResultKind.VOID)
                    .get(30, TimeUnit.SECONDS));
    assertInstanceOf(IllegalArgumentException.class, async.getCause());
    assertThrows(IllegalArgumentException.class, () -> NativeBridge.setLogLevel("verbose"));
    BlpInternalException unknown =
        assertThrows(
            BlpInternalException.class,
            () -> NativeTestSupport.invoke("core.nope", JsonValue.EMPTY_OBJECT, ResultKind.VOID));
    assertTrue(unknown.getMessage().contains("nope"), unknown.getMessage());
  }

  @Test
  void cancellingASleepStopsTheNativeCall() throws Exception {
    int pending = NativeBridge.pendingCount();
    CompletableFuture<Object> sleep =
        NativeTestSupport.invokeAsync("core.sleep", args("millis", 600_000), ResultKind.VOID);
    assertTrue(waitForCallsInFlight(1));
    assertTrue(sleep.cancel(true));
    assertThrows(CancellationException.class, sleep::join);
    assertTrue(waitForCallsInFlight(0), "the native call ends after cancellation");
    assertEquals(pending, NativeBridge.pendingCount());

    assertNull(
        NativeTestSupport.invokeAsync("core.sleep", args("millis", 10), ResultKind.VOID)
            .get(30, TimeUnit.SECONDS));
  }

  @Test
  void manyConcurrentCallsAllComplete() throws Exception {
    List<CompletableFuture<Object>> calls = new ArrayList<CompletableFuture<Object>>();
    for (int i = 0; i < 500; i++) {
      calls.add(NativeTestSupport.invokeAsync("core.echo", args("value", i), ResultKind.HANDLE));
    }
    for (int i = 0; i < calls.size(); i++) {
      assertEquals(Long.valueOf(i), calls.get(i).get(30, TimeUnit.SECONDS));
    }
  }

  private static boolean waitForCallsInFlight(long count) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      Object value =
          NativeTestSupport.invoke("core.callsInFlight", JsonValue.EMPTY_OBJECT, ResultKind.JSON);
      if (JsonFields.longOf(Json.parse((String) value), "callsInFlight") == count) {
        return true;
      }
      Thread.sleep(10);
    }
    return false;
  }

  private static void assertMatches(Map<String, Object> expected, Table table, String label) {
    assertEquals(JsonFields.longValue(expected, "numRows"), table.numRows(), label);
    List<Object> columns = JsonFields.list(expected, "columns");
    assertEquals(columns.size(), table.numColumns(), label);
    for (int c = 0; c < columns.size(); c++) {
      Map<String, Object> spec = JsonFields.object(columns.get(c), "column");
      Column column = table.column(c);
      String where = label + " column " + column.name();
      assertEquals(JsonFields.string(spec, "name"), column.name(), where);
      assertEquals(JsonFields.longValue(spec, "type"), column.type().code(), where);
      assertEquals(JsonFields.bool(spec, "nullable"), column.nullable(), where);
      assertEquals(JsonFields.optString(spec, "timezone"), column.timezone().orElse(null), where);
      List<Object> values = JsonFields.list(spec, "values");
      assertEquals(values.size(), column.size(), where);
      for (int row = 0; row < values.size(); row++) {
        assertEquals(values.get(row), render(column, row), where + " row " + row);
      }
    }
    Map<String, String> metadata = stringMap(JsonFields.obj(expected, "metadata"));
    assertEquals(metadata, table.metadata(), label);
    assertEquals(
        ResultMetadata.decode(metadata, Json.write(JsonFields.obj(expected, "decodedMetadata"))),
        table.resultMetadata(),
        label);
  }

  private static String render(Column column, int row) {
    if (column.isNull(row)) {
      return null;
    }
    DataType type = column.type();
    switch (type) {
      case BOOL:
        return Boolean.toString(column.getBoolean(row));
      case UINT64:
        return Long.toUnsignedString(column.getLong(row));
      case FLOAT32:
        return String.format("%08x", Float.floatToRawIntBits(column.getFloat(row)));
      case FLOAT64:
        return String.format("%016x", Double.doubleToRawLongBits(column.getDouble(row)));
      case UTF8:
      case LARGE_UTF8:
        return column.getString(row);
      case BINARY:
      case LARGE_BINARY:
        return Hashes.hex(column.getBytes(row));
      default:
        return Long.toString(column.getLong(row));
    }
  }

  private static Map<String, String> stringMap(Map<String, Object> object) {
    Map<String, String> result = new LinkedHashMap<String, String>();
    for (Map.Entry<String, Object> entry : object.entrySet()) {
      result.put(entry.getKey(), (String) entry.getValue());
    }
    return result;
  }
}
