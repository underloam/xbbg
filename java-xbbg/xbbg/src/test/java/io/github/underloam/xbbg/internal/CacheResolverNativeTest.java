package io.github.underloam.xbbg.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The provisioned-cache step of {@link BlpapiLocator}, resolved by the native resolver in {@code
 * xbbg_sdk_jni}: selection, ordering and rejection of tampered installs, against a temporary cache
 * of synthetic libraries with real {@code provenance.json} files.
 */
class CacheResolverNativeTest {
  private static final String LIB_NAME = "libblpapi3_64.so";
  private static final String PLATFORM = "linux-x86_64";

  @TempDir Path temp;

  @BeforeAll
  static void requireProvisioner() {
    String resource =
        "/io/github/underloam/xbbg/native/"
            + Platform.current().id()
            + "/"
            + System.mapLibraryName("xbbg_sdk_jni");
    boolean present = CacheResolverNativeTest.class.getResource(resource) != null;
    if (Boolean.getBoolean(NativeTestSupport.REQUIRE_PROPERTY)) {
      assertTrue(present, "the jar has no " + resource);
    }
    Assumptions.assumeTrue(present, "xbbg_sdk_jni is not staged for this platform");
  }

  private BlpapiLocator locator() {
    Map<String, String> properties = new HashMap<String, String>();
    properties.put("user.dir", temp.toString());
    properties.put("user.home", temp.resolve("home").toString());
    return new BlpapiLocator(
        Platform.LINUX_X86_64,
        lookup(new HashMap<String, String>()),
        lookup(properties),
        null,
        temp.resolve("cache"));
  }

  private static Function<String, String> lookup(final Map<String, String> values) {
    return new Function<String, String>() {
      @Override
      public String apply(String name) {
        return values.get(name);
      }
    };
  }

  private Path install(String version, String name) {
    return temp.resolve("cache").resolve("sdk").resolve(version).resolve(PLATFORM).resolve(name);
  }

  /** Writes a synthetic library and a matching {@code provenance.json}; returns the library. */
  private Path provision(Path install, String content, long provenanceMillis) throws IOException {
    Path lib = install.resolve("lib").resolve(LIB_NAME);
    Files.createDirectories(lib.getParent());
    Files.write(lib, content.getBytes(StandardCharsets.UTF_8));
    writeProvenance(install, "lib/" + LIB_NAME, Hashes.sha256Hex(lib), provenanceMillis);
    return lib.toAbsolutePath();
  }

  private void writeProvenance(Path install, String lib, String libSha, long millis)
      throws IOException {
    Map<String, Object> provenance = new LinkedHashMap<String, Object>();
    provenance.put("version", install.getParent().getParent().getFileName().toString());
    provenance.put("platform", PLATFORM);
    provenance.put("source", "pypi:https://example.invalid/simple");
    provenance.put("artifactUrl", "https://example.invalid/blpapi.whl");
    provenance.put("artifactSha256", libSha);
    provenance.put("lib", lib);
    provenance.put("libSha256", libSha);
    provenance.put("licenses", Arrays.asList("licenses/LICENSE"));
    Files.createDirectories(install);
    Path file = install.resolve("provenance.json");
    Files.write(file, Json.write(provenance).getBytes(StandardCharsets.UTF_8));
    Files.setLastModifiedTime(file, FileTime.fromMillis(millis));
  }

  @Test
  void findsTheNewestValidInstall() throws IOException {
    long now = System.currentTimeMillis();
    provision(install("3.24.10.1", "pypi-aaaaaaaaaaaa"), "old", now);
    Path newest = provision(install("3.25.1.1", "conda-bbbbbbbbbbbb"), "newest", now);
    provision(install("3.9.0.0", "pypi-cccccccccccc"), "numeric order, not text order", now);
    // Another platform's install and an incomplete one are never chosen.
    Path other =
        temp.resolve("cache")
            .resolve("sdk")
            .resolve("9.0.0.0")
            .resolve("macos-aarch64")
            .resolve("pypi-dddddddddddd");
    Files.createDirectories(other);
    Files.createDirectories(install("9.9.9.9", "pypi-incomplete"));

    assertEquals(newest, locator().findLocal());
  }

  @Test
  void installsOfOneVersionPreferTheNewestProvenance() throws IOException {
    long now = System.currentTimeMillis();
    provision(install("3.25.1.1", "pypi-aaaaaaaaaaaa"), "older provenance", now - 60_000L);
    Path newer = provision(install("3.25.1.1", "conda-bbbbbbbbbbbb"), "newer provenance", now);
    assertEquals(newer, locator().findLocal());
  }

  @Test
  void aTamperedLibraryIsSkippedAndReported() throws IOException {
    long now = System.currentTimeMillis();
    Path good = provision(install("3.24.10.1", "pypi-aaaaaaaaaaaa"), "intact runtime", now);
    Path tamperedInstall = install("3.25.1.1", "conda-bbbbbbbbbbbb");
    Path tampered = provision(tamperedInstall, "tampered runtime", now);
    BlpapiLocator locator = locator();
    assertEquals(tampered, locator.findLocal());

    byte[] bytes = Files.readAllBytes(tampered);
    bytes[0] ^= 1;
    Files.write(tampered, bytes);

    assertEquals(good, locator.findLocal());
    Files.write(good, new byte[] {1});
    assertNull(locator.findLocal());
    String message = locator.notFound().getMessage();
    assertTrue(message.contains("skipped " + tamperedInstall.toAbsolutePath()), message);
    assertTrue(message.contains("sha256"), message);
  }

  @Test
  void provenanceThatEscapesTheInstallIsRejected() throws IOException {
    Path install = install("3.25.1.1", "pypi-x");
    Files.createDirectories(temp.resolve("cache").resolve("sdk"));
    Files.write(
        temp.resolve("cache").resolve("sdk").resolve(LIB_NAME),
        "outside".getBytes(StandardCharsets.UTF_8));
    writeProvenance(
        install,
        "../../../" + LIB_NAME,
        Hashes.sha256Hex("outside".getBytes(StandardCharsets.UTF_8)),
        System.currentTimeMillis());
    BlpapiLocator locator = locator();
    assertNull(locator.findLocal());
    String message = locator.notFound().getMessage();
    assertTrue(message.contains("skipped " + install.toAbsolutePath()), message);
  }

  /** The Java bootstrap rules (needed before any native library loads) must not drift from Rust. */
  @Test
  void bootstrapCacheRootAgreesWithTheNativeDefault() {
    Path nativeRoot = NativeSdk.defaultCacheRoot();
    Assumptions.assumeTrue(nativeRoot != null, "the native resolver found no home directory");
    String property = System.getProperty("xbbg.cache.dir");
    System.clearProperty("xbbg.cache.dir");
    try {
      assertEquals(nativeRoot.toAbsolutePath().normalize(), NativeLibraries.cacheRoot());
    } finally {
      if (property != null) {
        System.setProperty("xbbg.cache.dir", property);
      }
    }
  }
}
