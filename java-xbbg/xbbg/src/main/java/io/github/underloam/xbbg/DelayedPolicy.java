package io.github.underloam.xbbg;

import java.util.Locale;

/**
 * What to do when Bloomberg delivers delayed rather than real-time data ({@code onDelayed}).
 *
 * <p>Used by the request options ({@code bdp}, {@code bdh}, {@code bdib}, {@code bdtick}), by
 * {@link RequestSpec} and by subscription stream options. Leaving the policy unset skips the
 * delayed-data check entirely.
 */
public enum DelayedPolicy {
  /** Record the delayed status and emit one {@code BlpDelayedDataWarning} per delayed security. */
  WARN("warn"),
  /** Fail the request (or reject the subscription topic) when the data is delayed. */
  RAISE("raise"),
  /** Record the delayed status only. */
  IGNORE("ignore");

  private final String wireValue;

  DelayedPolicy(String wireValue) {
    this.wireValue = wireValue;
  }

  /**
   * Returns the value sent to the engine.
   *
   * @return {@code "warn"}, {@code "raise"} or {@code "ignore"}
   */
  public String wireValue() {
    return wireValue;
  }

  /**
   * Parses a policy name, ignoring case and surrounding whitespace (the engine's rule).
   *
   * @param value {@code warn}, {@code raise} or {@code ignore}
   * @return the policy
   * @throws IllegalArgumentException if the value names no policy
   * @throws NullPointerException if {@code value} is null
   */
  public static DelayedPolicy parse(String value) {
    String normalized = value.trim().toLowerCase(Locale.ROOT);
    for (DelayedPolicy policy : values()) {
      if (policy.wireValue.equals(normalized)) {
        return policy;
      }
    }
    throw new IllegalArgumentException("onDelayed must be warn, raise, or ignore");
  }

  @Override
  public String toString() {
    return wireValue;
  }
}
