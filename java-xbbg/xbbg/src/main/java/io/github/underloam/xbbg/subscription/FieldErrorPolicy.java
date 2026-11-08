package io.github.underloam.xbbg.subscription;

import java.util.Locale;

/**
 * What a subscription does with fields Bloomberg rejects ({@code onFieldError}).
 *
 * <p>The default is {@link #WARN}: rejected fields are recorded in {@link
 * Subscription#fieldErrors()} and reported once as a {@code BlpFieldWarning}.
 */
public enum FieldErrorPolicy {
  /** Record the rejection and emit one {@code BlpFieldWarning}. */
  WARN("warn"),
  /** Fail only the topics that requested a rejected field explicitly. */
  RAISE("raise"),
  /** Record the rejection only. */
  IGNORE("ignore");

  private final String wireValue;

  FieldErrorPolicy(String wireValue) {
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
  public static FieldErrorPolicy parse(String value) {
    String normalized = value.trim().toLowerCase(Locale.ROOT);
    for (FieldErrorPolicy policy : values()) {
      if (policy.wireValue.equals(normalized)) {
        return policy;
      }
    }
    throw new IllegalArgumentException("onFieldError must be warn, raise, or ignore");
  }

  @Override
  public String toString() {
    return wireValue;
  }
}
