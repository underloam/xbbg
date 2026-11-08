package io.github.underloam.xbbg.internal;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * The platforms xbbg ships native libraries for.
 *
 * <p>Internal API: not covered by compatibility guarantees.
 */
public enum Platform {
  /** Windows on x86-64. */
  WINDOWS_X86_64("windows-x86_64"),
  /** Linux on x86-64. */
  LINUX_X86_64("linux-x86_64"),
  /** macOS on Apple silicon (Bloomberg ships {@code .so} files on macOS too). */
  MACOS_AARCH64("macos-aarch64");

  private static final List<String> WINDOWS_LIBRARIES = Collections.singletonList("blpapi3_64.dll");
  private static final List<String> LINUX_LIBRARIES =
      Collections.unmodifiableList(Arrays.asList("libblpapi3_64.so", "libblpapi3.so"));
  private static final List<String> MACOS_LIBRARIES =
      Collections.unmodifiableList(
          Arrays.asList("libblpapi3_64.so", "libblpapi3.so", "libblpapi3.dylib"));

  private final String id;

  Platform(String id) {
    this.id = id;
  }

  /**
   * Returns the platform id used in resource and cache paths.
   *
   * @return for example {@code windows-x86_64}
   */
  public String id() {
    return id;
  }

  /**
   * Returns the Bloomberg runtime library file names accepted on this platform, in preference
   * order.
   *
   * @return the file names
   */
  public List<String> blpapiLibraryNames() {
    switch (this) {
      case WINDOWS_X86_64:
        return WINDOWS_LIBRARIES;
      case LINUX_X86_64:
        return LINUX_LIBRARIES;
      case MACOS_AARCH64:
        return MACOS_LIBRARIES;
    }
    throw new AssertionError(this);
  }

  /**
   * Whether this is a Windows platform.
   *
   * @return {@code true} on Windows
   */
  public boolean isWindows() {
    return this == WINDOWS_X86_64;
  }

  /**
   * Returns the platform of the running JVM.
   *
   * @return the platform
   * @throws UnsatisfiedLinkError if the platform is not supported
   */
  public static Platform current() {
    return detect(System.getProperty("os.name", ""), System.getProperty("os.arch", ""));
  }

  /**
   * Maps {@code os.name} and {@code os.arch} values to a platform.
   *
   * @param osName the {@code os.name} value
   * @param osArch the {@code os.arch} value
   * @return the platform
   * @throws UnsatisfiedLinkError if the platform is not supported
   */
  public static Platform detect(String osName, String osArch) {
    String os = osName.toLowerCase(Locale.ROOT);
    String arch = osArch.toLowerCase(Locale.ROOT);
    String normalizedOs =
        os.startsWith("windows")
            ? "windows"
            : os.startsWith("linux")
                ? "linux"
                : os.startsWith("mac") || os.startsWith("darwin") ? "macos" : os;
    String normalizedArch =
        arch.equals("amd64") || arch.equals("x86_64") || arch.equals("x86-64")
            ? "x86_64"
            : arch.equals("aarch64") || arch.equals("arm64") ? "aarch64" : arch;
    String id = normalizedOs + "-" + normalizedArch;
    for (Platform platform : values()) {
      if (platform.id.equals(id)) {
        return platform;
      }
    }
    throw new UnsatisfiedLinkError(
        "xbbg has no native libraries for platform "
            + id
            + " (os.name="
            + osName
            + ", os.arch="
            + osArch
            + "); supported platforms: windows-x86_64, linux-x86_64, macos-aarch64");
  }
}
