package io.github.underloam.xbbg.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The Java bootstrap rules for the cache root (needed before any native library loads) must not
 * drift from the native default, because Python, Node and Java share one runtime cache.
 */
class CacheRootNativeTest {
  @BeforeAll
  static void requireProvisioner() {
    String resource =
        "/io/github/underloam/xbbg/native/"
            + Platform.current().id()
            + "/"
            + System.mapLibraryName("xbbg_sdk_jni");
    boolean present = CacheRootNativeTest.class.getResource(resource) != null;
    if (Boolean.getBoolean(NativeTestSupport.REQUIRE_PROPERTY)) {
      assertTrue(present, "the jar has no " + resource);
    }
    Assumptions.assumeTrue(present, "xbbg_sdk_jni is not staged for this platform");
  }

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
