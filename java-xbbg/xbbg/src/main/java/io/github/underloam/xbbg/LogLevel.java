package io.github.underloam.xbbg;

/**
 * Level of xbbg's own native log (written to stderr). Bloomberg's C SDK logging is configured
 * separately with {@link EngineConfig.Builder#sdkLogLevel(SdkLogLevel)}.
 */
public enum LogLevel {
  /** Everything. */
  TRACE("trace"),
  /** Debugging detail and above. */
  DEBUG("debug"),
  /** Informational messages and above. */
  INFO("info"),
  /** Warnings and errors (the default). */
  WARN("warn"),
  /** Errors only. */
  ERROR("error");

  private final String wireValue;

  LogLevel(String wireValue) {
    this.wireValue = wireValue;
  }

  /**
   * Returns the level name the native library uses.
   *
   * @return {@code trace}, {@code debug}, {@code info}, {@code warn} or {@code error}
   */
  public String wireValue() {
    return wireValue;
  }

  /**
   * Parses a level the way the native library does: {@code trace}, {@code debug}, {@code info},
   * {@code warn} or {@code warning}, {@code error}, or {@code 0} to {@code 4}, ignoring ASCII case.
   *
   * @param value the level text
   * @return the level
   * @throws IllegalArgumentException if the text names no level
   * @throws NullPointerException if {@code value} is null
   */
  public static LogLevel parse(String value) {
    if (value == null) {
      throw new NullPointerException("value");
    }
    StringBuilder lower = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      lower.append(c >= 'A' && c <= 'Z' ? (char) (c + ('a' - 'A')) : c);
    }
    switch (lower.toString()) {
      case "trace":
      case "0":
        return TRACE;
      case "debug":
      case "1":
        return DEBUG;
      case "info":
      case "2":
        return INFO;
      case "warn":
      case "warning":
      case "3":
        return WARN;
      case "error":
      case "4":
        return ERROR;
      default:
        throw new IllegalArgumentException(
            "Invalid log level '" + value + "'. Expected: trace, debug, info, warn, error");
    }
  }

  @Override
  public String toString() {
    return wireValue;
  }
}
