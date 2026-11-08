package io.github.underloam.xbbg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.underloam.xbbg.internal.ArrowIpcSchema;
import io.github.underloam.xbbg.internal.Hashes;
import io.github.underloam.xbbg.internal.JsonValue;
import io.github.underloam.xbbg.internal.NativeLibraries;
import io.github.underloam.xbbg.internal.NativeTestSupport;
import io.github.underloam.xbbg.internal.Platform;
import io.github.underloam.xbbg.internal.ResultKind;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

/** Engine-level behaviour against a {@code --features test-support} build of the natives. */
@Isolated
class NativeCoreIntegrationTest {
  @BeforeAll
  static void natives() {
    NativeTestSupport.assumeAvailable();
  }

  @Test
  void unreachableHostsFailWithASessionError() {
    BlpSessionException error =
        assertThrows(BlpSessionException.class, () -> Engine.open(XbbgTest.UNREACHABLE));
    assertTrue(error.getMessage().contains("Session start failed"), error.getMessage());
    assertEquals(BlpErrorKind.SESSION, error.kind());

    CompletableFuture<Engine> async = Engine.connect(XbbgTest.UNREACHABLE);
    assertThrows(BlpSessionException.class, () -> Xbbg.await(async));
  }

  @Test
  void invalidConfigurationIsRejectedNatively() {
    EngineConfig zfpWithHost = EngineConfig.builder().host("h").zfpRemote("8194").build();
    RuntimeException error = assertThrows(RuntimeException.class, () -> Engine.open(zfpWithHost));
    assertTrue(
        error instanceof BlpValidationException || error instanceof IllegalArgumentException,
        error.toString());
    assertTrue(error.getMessage().contains("zfpRemote"), error.getMessage());
  }

  @Test
  void logLevelRoundTrips() {
    LogLevel original = Xbbg.getLogLevel();
    try {
      for (LogLevel level : LogLevel.values()) {
        Xbbg.setLogLevel(level);
        assertEquals(level, Xbbg.getLogLevel());
      }
    } finally {
      Xbbg.setLogLevel(original);
    }
  }

  @Test
  void runtimeSettingsAreFrozenOnceLoaded() {
    assertThrows(IllegalStateException.class, () -> Xbbg.setSdkPath(Paths.get("blpapi")));
    assertThrows(IllegalStateException.class, () -> Xbbg.setSdkSource(null));
  }

  @Test
  void bundledNativesAreExtractedOnceAndVerified() throws IOException {
    Path extracted = NativeLibraries.extractBundled("xbbg_jni");
    Properties properties = new Properties();
    try (InputStream in =
        NativeLibraries.class.getResourceAsStream(
            "/io/github/underloam/xbbg/native/"
                + Platform.current().id()
                + "/natives.properties")) {
      if (in != null) {
        properties.load(in);
      }
    }
    String expected = properties.getProperty(extracted.getFileName() + ".sha256");
    if (expected != null) {
      assertEquals(expected, Hashes.sha256Hex(extracted));
    }
    assertTrue(
        extracted.startsWith(NativeLibraries.cacheRoot().resolve("jvm")), extracted.toString());
    assertTrue(
        extracted
            .getParent()
            .getParent()
            .getFileName()
            .toString()
            .startsWith(Xbbg.version() + "-"));
    long modified = Files.getLastModifiedTime(extracted).toMillis();
    assertEquals(extracted, NativeLibraries.extractBundled("xbbg_jni"));
    assertEquals(modified, Files.getLastModifiedTime(extracted).toMillis(), "never rewritten");
    assertThrows(
        UnsatisfiedLinkError.class, () -> NativeLibraries.extractBundled("no_such_library"));
  }

  @Test
  void ipcResultsFeedTheTickerDeprecationCheck() {
    final List<XbbgWarning> warnings = new ArrayList<XbbgWarning>();
    Consumer<XbbgWarning> listener =
        new Consumer<XbbgWarning>() {
          @Override
          public void accept(XbbgWarning warning) {
            warnings.add(warning);
          }
        };
    XbbgWarnings.addListener(listener);
    XbbgWarnings.resetTickerKeyColumnWarning();
    try {
      byte[] ipc =
          (byte[])
              NativeTestSupport.invoke(
                  "core.sampleTable", JsonValue.EMPTY_OBJECT, ResultKind.IPC_STREAM);
      XbbgWarnings.warnTickerKeyColumn(KeyColumn.TICKER, ArrowIpcSchema.read(ipc).fieldNames());
    } finally {
      XbbgWarnings.removeListener(listener);
      XbbgWarnings.resetTickerKeyColumnWarning();
    }
    assertEquals(1, warnings.size());
    assertEquals(XbbgWarnings.TICKER_KEY_COLUMN_CODE, warnings.get(0).code());
  }
}
