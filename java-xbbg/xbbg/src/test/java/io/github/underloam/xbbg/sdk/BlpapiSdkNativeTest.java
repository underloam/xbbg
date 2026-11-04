package io.github.underloam.xbbg.sdk;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.underloam.xbbg.BlpException;
import io.github.underloam.xbbg.BlpInternalException;
import io.github.underloam.xbbg.BlpapiNotFoundException;
import io.github.underloam.xbbg.internal.Futures;
import io.github.underloam.xbbg.internal.Platform;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Round trips through {@code xbbg_sdk_jni} with archives generated on the fly; no network. */
class BlpapiSdkNativeTest {
  private static final byte[] LIB = "not really blpapi".getBytes(StandardCharsets.US_ASCII);

  @BeforeAll
  static void requireProvisioner() {
    String resource =
        "/io/github/underloam/xbbg/native/"
            + Platform.current().id()
            + "/"
            + System.mapLibraryName("xbbg_sdk_jni");
    boolean present = BlpapiSdkNativeTest.class.getResource(resource) != null;
    if (Boolean.getBoolean("xbbg.requireNatives")) {
      assertTrue(present, "the jar has no " + resource);
    }
    Assumptions.assumeTrue(present, "xbbg_sdk_jni is not staged for this platform");
  }

  private static Throwable failure(ProvisionOptions options) {
    CompletionException error =
        assertThrows(CompletionException.class, () -> BlpapiSdk.provision(options).join());
    return Futures.unwrap(error);
  }

  private static ProvisionOptions.Builder offline(BlpapiSource source, Path cache) {
    return ProvisionOptions.builder().source(source).cacheDir(cache).offline(true);
  }

  @Test
  void localIndexInstallsVerifiesCachesAndLists(@TempDir Path dir) throws Exception {
    String[] host = SdkFixtures.hostWheel();
    Path wheel = SdkFixtures.wheel(dir.resolve("files"), "3.26.9.1", host[0], host[1], LIB, false);
    SdkFixtures.wheel(dir.resolve("files"), "3.24.6", host[0], host[1], LIB, false);
    Path index =
        SdkFixtures.simpleIndex(
            dir, wheel, dir.resolve("files").resolve("blpapi-3.24.6-" + host[0] + ".whl"));
    Path cache = dir.resolve("cache");
    ProvisionOptions options = offline(BlpapiSource.pypi(index.toString()), cache).build();

    ProvisionResult first = BlpapiSdk.provision(options).join();
    assertEquals("3.26.9.1", first.version());
    assertEquals(Platform.current().id(), first.platform());
    assertFalse(first.cacheHit());
    assertArrayEquals(LIB, Files.readAllBytes(first.libraryPath()));
    assertEquals(SdkFixtures.sha256Hex(LIB), first.libSha256());
    assertEquals(SdkFixtures.sha256Hex(Files.readAllBytes(wheel)), first.artifactSha256());
    assertTrue(first.libraryPath().startsWith(cache.toAbsolutePath()));
    assertEquals(1, first.licensePaths().size());
    assertEquals(
        BlpapiSource.pypi(index.toString()).kind(), BlpapiSource.parse(first.source()).kind());

    ProvisionResult again = BlpapiSdk.provision(options).join();
    assertTrue(again.cacheHit());
    assertEquals(first.libraryPath(), again.libraryPath());

    List<ProvisionResult> installed = BlpapiSdk.installed(cache);
    assertEquals(1, installed.size());
    assertEquals(again, installed.get(0));
    assertTrue(
        BlpapiSdk.installed(cache, "macos-aarch64").isEmpty()
            || Platform.current().id().equals("macos-aarch64"));

    Throwable old =
        failure(offline(BlpapiSource.pypi(index.toString()), cache).version("3.24.6").build());
    assertInstanceOf(IllegalArgumentException.class, old);
    assertTrue(old.getMessage().contains("3.24.10"), old.getMessage());
  }

  @Test
  void generatedCondaPackageInstallsAsAnArtifact(@TempDir Path dir) throws Exception {
    String[] host = SdkFixtures.hostConda();
    Path conda = SdkFixtures.conda(dir, "3.26.9.1", host[0], host[1], LIB);
    ProvisionResult result =
        BlpapiSdk.provision(
                offline(BlpapiSource.artifact(conda.toString()), dir.resolve("cache")).build())
            .join();
    assertEquals("3.26.9.1", result.version());
    assertArrayEquals(LIB, Files.readAllBytes(result.libraryPath()));
    assertEquals("artifact:" + conda.toAbsolutePath(), result.source());
  }

  @Test
  void failuresMapToTheContractExceptions(@TempDir Path dir) throws Exception {
    String[] host = SdkFixtures.hostWheel();
    Path cache = dir.resolve("cache");
    Path tampered = SdkFixtures.wheel(dir.resolve("bad"), "3.26.9.1", host[0], host[1], LIB, true);
    Throwable integrity =
        failure(offline(BlpapiSource.artifact(tampered.toString()), cache).build());
    assertInstanceOf(BlpInternalException.class, integrity);
    assertEquals("SDK_INTEGRITY", ((BlpException) integrity).code());

    Path good = SdkFixtures.wheel(dir.resolve("good"), "3.26.9.1", host[0], host[1], LIB, false);
    Throwable hash =
        failure(offline(BlpapiSource.artifact(good.toString(), repeat('0', 64)), cache).build());
    assertEquals("SDK_INTEGRITY", ((BlpException) hash).code());

    String other =
        Platform.current().id().equals("linux-x86_64") ? "windows-x86_64" : "linux-x86_64";
    Throwable platform =
        failure(offline(BlpapiSource.artifact(good.toString()), cache).platform(other).build());
    assertInstanceOf(IllegalArgumentException.class, platform);

    Throwable missing =
        failure(offline(BlpapiSource.pypi(BlpapiSource.BLOOMBERG_PYPI_INDEX), cache).build());
    assertInstanceOf(BlpapiNotFoundException.class, missing);
    assertEquals("SDK_OFFLINE", ((BlpException) missing).code());

    Throwable remote =
        failure(
            ProvisionOptions.builder()
                .source(
                    BlpapiSource.artifact(
                        "https://example.invalid/blpapi-3.26.9.1-" + host[0] + ".whl"))
                .cacheDir(cache)
                .build());
    assertInstanceOf(IllegalArgumentException.class, remote);
    assertTrue(remote.getMessage().contains("#sha256="), remote.getMessage());
  }

  @Test
  void commandLineProvisionsAndLists(@TempDir Path dir) throws Exception {
    String[] host = SdkFixtures.hostWheel();
    Path wheel = SdkFixtures.wheel(dir, "3.26.9.1", host[0], host[1], LIB, false);
    String cache = dir.resolve("cache").toString();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    int code =
        BlpapiSdk.run(
            new String[] {"--source", wheel.toString(), "--cache-dir", cache, "--offline"},
            name -> null,
            SdkFixtures.utf8(out),
            SdkFixtures.utf8(err));
    assertEquals(0, code, new String(err.toByteArray(), StandardCharsets.UTF_8));
    String printed = new String(out.toByteArray(), StandardCharsets.UTF_8);
    assertTrue(printed.contains("version          3.26.9.1"), printed);
    assertTrue(printed.contains("cache hit        false"), printed);

    // Without --source the spec comes from XBBG_SDK_SOURCE.
    out.reset();
    assertEquals(
        0,
        BlpapiSdk.run(
            new String[] {"--cache-dir", cache, "--offline"},
            name -> "XBBG_SDK_SOURCE".equals(name) ? wheel.toString() : null,
            SdkFixtures.utf8(out),
            SdkFixtures.utf8(err)),
        new String(err.toByteArray(), StandardCharsets.UTF_8));
    assertTrue(
        new String(out.toByteArray(), StandardCharsets.UTF_8).contains("cache hit        true"));

    out.reset();
    assertEquals(
        0,
        BlpapiSdk.run(
            new String[] {"--installed", "--cache-dir", cache},
            name -> null,
            SdkFixtures.utf8(out),
            SdkFixtures.utf8(err)));
    assertTrue(
        new String(out.toByteArray(), StandardCharsets.UTF_8).contains("cache hit        true"));

    assertEquals(
        1,
        BlpapiSdk.run(
            new String[] {
              "--source",
              "pypi:" + BlpapiSource.BLOOMBERG_PYPI_INDEX,
              "--cache-dir",
              dir.resolve("empty").toString(),
              "--offline"
            },
            name -> null,
            SdkFixtures.utf8(out),
            SdkFixtures.utf8(err)));
    assertTrue(new String(err.toByteArray(), StandardCharsets.UTF_8).contains("[SDK_OFFLINE]"));
  }

  private static String repeat(char c, int n) {
    StringBuilder text = new StringBuilder(n);
    for (int i = 0; i < n; i++) {
      text.append(c);
    }
    return text.toString();
  }
}
