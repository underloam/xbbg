package io.github.underloam.xbbg.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class RuntimeSupportTest {
  @Test
  void detectsSupportedPlatforms() {
    assertEquals(Platform.WINDOWS_X86_64, Platform.detect("Windows 11", "amd64"));
    assertEquals(Platform.LINUX_X86_64, Platform.detect("Linux", "x86_64"));
    assertEquals(Platform.MACOS_AARCH64, Platform.detect("Mac OS X", "aarch64"));
    assertEquals(Platform.MACOS_AARCH64, Platform.detect("Mac OS X", "arm64"));
    UnsatisfiedLinkError error =
        assertThrows(UnsatisfiedLinkError.class, () -> Platform.detect("Mac OS X", "x86_64"));
    assertEquals(
        "xbbg has no native libraries for platform macos-x86_64 (os.name=Mac OS X, os.arch=x86_64);"
            + " supported platforms: windows-x86_64, linux-x86_64, macos-aarch64",
        error.getMessage());
    assertThrows(UnsatisfiedLinkError.class, () -> Platform.detect("FreeBSD", "amd64"));
    assertEquals("blpapi3_64.dll", Platform.WINDOWS_X86_64.blpapiLibraryNames().get(0));
    assertEquals("libblpapi3_64.so", Platform.MACOS_AARCH64.blpapiLibraryNames().get(0));
  }

  @Test
  void cacheRootFollowsTheSharedRules() {
    String userHome = Paths.get("user-home").toAbsolutePath().toString();
    String home = Paths.get("home").toAbsolutePath().toString();
    assertEquals(
        Paths.get("prop").toAbsolutePath(),
        NativeLibraries.cacheRoot("prop", "env", "linux", null, null, null, home, userHome));
    assertEquals(
        Paths.get("env").toAbsolutePath(),
        NativeLibraries.cacheRoot(" ", "env", "linux", null, null, null, home, userHome));
    assertEquals(
        Paths.get("local", "xbbg"),
        NativeLibraries.cacheRoot(null, null, "windows", "local", "profile", null, home, userHome));
    assertEquals(
        Paths.get("profile", "AppData", "Local", "xbbg"),
        NativeLibraries.cacheRoot(null, null, "windows", null, "profile", null, home, userHome));
    String xdg = Paths.get("xdg").toAbsolutePath().toString();
    assertEquals(
        Paths.get(xdg, "xbbg"),
        NativeLibraries.cacheRoot(null, null, "linux", null, null, xdg, home, userHome));
  }

  /** The cases of the Rust {@code xbbg_sdk} cache-root tests, plus the JVM home fallback. */
  @Test
  void cacheRootMatchesRust() {
    String userHome = Paths.get("user-home").toAbsolutePath().toString();
    String home = Paths.get("home").toAbsolutePath().toString();
    // explicit_cache_dir_wins
    assertEquals(
        Paths.get("custom", "cache").toAbsolutePath(),
        NativeLibraries.cacheRoot(
            null, Paths.get("custom", "cache").toString(), "linux", "x", null, null, "y", null));
    // An empty variable is unset, as in Rust.
    assertEquals(
        Paths.get(home, ".cache", "xbbg"),
        NativeLibraries.cacheRoot(null, "", "linux", null, null, "", home, userHome));
    // linux_uses_xdg_cache_home_when_absolute; HOME comes before the JVM's user.home
    assertEquals(
        Paths.get(home, ".cache", "xbbg"),
        NativeLibraries.cacheRoot(null, null, "linux", null, null, "relative", home, userHome));
    // macos_uses_library_caches
    assertEquals(
        Paths.get(home, "Library", "Caches", "xbbg"),
        NativeLibraries.cacheRoot(null, null, "macos", null, null, xdgIgnored(), home, userHome));
    // Where Rust has no home directory (HOME unset or empty), Java uses user.home.
    assertEquals(
        Paths.get(userHome, ".cache", "xbbg"),
        NativeLibraries.cacheRoot(null, null, "linux", null, null, null, "", userHome));
    assertEquals(
        Paths.get(userHome, "Library", "Caches", "xbbg"),
        NativeLibraries.cacheRoot(null, null, "macos", null, null, null, null, userHome));
    // windows_prefers_local_app_data_then_user_profile; HOME plays no part on Windows
    assertEquals(
        Paths.get(userHome, "AppData", "Local", "xbbg"),
        NativeLibraries.cacheRoot(null, null, "windows", "", "", null, home, userHome));
  }

  private static String xdgIgnored() {
    return Paths.get("xdg").toAbsolutePath().toString();
  }

  @Test
  void redactsUrlUserInfoOnly() {
    assertEquals("conda-forge", Text.redactUserInfo("conda-forge"));
    assertEquals("https://h/a@b", Text.redactUserInfo("https://h/a@b"));
    assertEquals("http://***@proxy:8080", Text.redactUserInfo("http://u:p@w@proxy:8080"));
    assertEquals(
        "a https://***@h1/x?q=1 b socks5h://***@h2#f",
        Text.redactUserInfo("a https://u:p@h1/x?q=1 b socks5h://v@h2#f"));
    assertEquals("file:///tmp/a@b", Text.redactUserInfo("file:///tmp/a@b"));
  }

  @Test
  void requestStringsMatchJavaScriptNumberText() {
    assertEquals("40", Text.toRequestString(40.0));
    assertEquals("-3", Text.toRequestString(-3.0));
    assertEquals("0", Text.toRequestString(-0.0));
    assertEquals("0.1", Text.toRequestString(0.1));
    assertEquals("0.000001", Text.toRequestString(1e-6));
    assertEquals("1e-7", Text.toRequestString(1e-7));
    assertEquals("1.5e-7", Text.toRequestString(1.5e-7));
    assertEquals("123456789012345680000", Text.toRequestString(1.2345678901234568e20));
    assertEquals("1e+21", Text.toRequestString(1e21));
    assertEquals("NaN", Text.toRequestString(Double.NaN));
    assertEquals("-Infinity", Text.toRequestString(Double.NEGATIVE_INFINITY));
    assertEquals("0.1", Text.toRequestString(0.1f));
    assertEquals("12", Text.toRequestString(12));
    assertEquals("12.50", Text.toRequestString(new java.math.BigDecimal("12.50")));
    assertEquals("true", Text.toRequestString(true));
  }

  @Test
  void cleanerRunsTheActionOnceForExplicitAndCollectedOwners() throws Exception {
    final AtomicInteger runs = new AtomicInteger();
    NativeCleaner.Cleanable cleanable =
        NativeCleaner.register(
            new Object(),
            new Runnable() {
              @Override
              public void run() {
                runs.incrementAndGet();
              }
            });
    cleanable.clean();
    cleanable.clean();
    assertEquals(1, runs.get());

    final CountDownLatch collected = new CountDownLatch(1);
    registerGarbage(collected);
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
    while (collected.getCount() > 0 && System.nanoTime() < deadline) {
      System.gc();
      collected.await(50, TimeUnit.MILLISECONDS);
    }
    assertEquals(0, collected.getCount(), "the action runs after the owner is collected");
  }

  private static void registerGarbage(final CountDownLatch collected) {
    NativeCleaner.register(
        new byte[1024],
        new Runnable() {
          @Override
          public void run() {
            collected.countDown();
          }
        });
  }

  @Test
  void cleanerRejectsNulls() {
    assertThrows(NullPointerException.class, () -> NativeCleaner.register(null, () -> {}));
    assertThrows(NullPointerException.class, () -> NativeCleaner.register(new Object(), null));
  }

  @Test
  void versionResourceIsStamped() {
    assertTrue(Versions.version().matches("\\d+\\.\\d+\\.\\d+.*"), Versions.version());
    assertEquals(Versions.version(), Versions.versionOrUnknown());
  }
}
