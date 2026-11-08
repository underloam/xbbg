package consumer;

import io.github.underloam.xbbg.Xbbg;
import io.github.underloam.xbbg.sdk.BlpapiSdk;
import io.github.underloam.xbbg.sdk.BlpapiSource;
import io.github.underloam.xbbg.sdk.ProvisionOptions;
import io.github.underloam.xbbg.sdk.ProvisionResult;
import java.nio.file.Paths;

/**
 * Smoke test for a built {@code io.github.underloam:xbbg} jar, shared by the Maven and Gradle
 * consumer projects next to this directory and run on Java 8 through 25.
 *
 * <p>It resolves the jar like any application would, extracts and loads the bundled natives, and
 * reports the versions. System properties:
 *
 * <ul>
 *   <li>{@code consumer.blpapiArtifact}: a local Bloomberg wheel or conda package; the Bloomberg
 *       runtime is provisioned from it (through {@code xbbg_sdk_jni}) into {@code xbbg.cache.dir}
 *       and used for loading. Without it, the normal runtime discovery applies.
 *   <li>{@code consumer.expectVersion}, {@code consumer.expectNativeVersion}: fail unless the jar
 *       and the native library report exactly these versions.
 * </ul>
 */
public final class Main {
  private Main() {}

  /**
   * Runs the smoke test; any failure ends the JVM with a non-zero status.
   *
   * @param args ignored
   */
  public static void main(String[] args) {
    System.out.println("java.version=" + System.getProperty("java.version"));
    String version = Xbbg.version();
    System.out.println("xbbg.version=" + version);

    String artifact = setting("consumer.blpapiArtifact");
    if (artifact != null) {
      String cacheDir = setting("xbbg.cache.dir");
      if (cacheDir == null) {
        throw new IllegalStateException("consumer.blpapiArtifact needs xbbg.cache.dir");
      }
      ProvisionResult provisioned =
          BlpapiSdk.provision(
                  ProvisionOptions.builder()
                      .source(BlpapiSource.artifact(artifact))
                      .cacheDir(Paths.get(cacheDir))
                      .build())
              .join();
      System.out.println(
          "blpapi.provisioned="
              + provisioned.libraryPath()
              + " version="
              + provisioned.version()
              + " cacheHit="
              + provisioned.cacheHit());
      Xbbg.setSdkPath(provisioned.libraryPath());
    }

    String nativeVersion = Xbbg.nativeVersion();
    System.out.println("xbbg.nativeVersion=" + nativeVersion);
    System.out.println("blpapi.runtime=" + Xbbg.sdkVersion());
    expect("consumer.expectVersion", version);
    expect("consumer.expectNativeVersion", nativeVersion);
    System.out.println("consumer smoke OK");
  }

  private static String setting(String name) {
    String value = System.getProperty(name);
    return value == null || value.trim().isEmpty() ? null : value.trim();
  }

  private static void expect(String name, String actual) {
    String expected = setting(name);
    if (expected != null && !expected.equals(actual)) {
      throw new IllegalStateException(name + ": expected " + expected + ", got " + actual);
    }
  }
}
