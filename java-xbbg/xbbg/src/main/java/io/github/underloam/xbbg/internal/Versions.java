package io.github.underloam.xbbg.internal;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * The library version, read from the build-time resource {@code
 * io/github/underloam/xbbg/xbbg.properties}.
 *
 * <p>Internal API: not covered by compatibility guarantees.
 */
public final class Versions {
  private static final String RESOURCE = "/io/github/underloam/xbbg/xbbg.properties";
  private static final String VERSION = load();

  private Versions() {}

  /**
   * Returns the library version.
   *
   * @return the version stamped at build time
   * @throws IllegalStateException if the build did not stamp the version resource
   */
  public static String version() {
    if (VERSION == null) {
      throw new IllegalStateException(
          "xbbg version is unavailable: " + RESOURCE + " is missing or was not filtered at build");
    }
    return VERSION;
  }

  /**
   * Returns the library version, or {@code unknown} when the build did not stamp it.
   *
   * @return the version or {@code unknown}
   */
  public static String versionOrUnknown() {
    return VERSION == null ? "unknown" : VERSION;
  }

  private static String load() {
    InputStream in = Versions.class.getResourceAsStream(RESOURCE);
    if (in == null) {
      return null;
    }
    try {
      Properties properties = new Properties();
      properties.load(in);
      String version = properties.getProperty("version");
      if (version == null || version.trim().isEmpty() || version.contains("${")) {
        return null;
      }
      return version.trim();
    } catch (IOException e) {
      return null;
    } finally {
      try {
        in.close();
      } catch (IOException ignored) {
        // Closing a resource stream cannot lose data.
      }
    }
  }
}
