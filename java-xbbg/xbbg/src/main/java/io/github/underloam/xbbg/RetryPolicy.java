package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.Json;
import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import io.github.underloam.xbbg.internal.JsonValue;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;

/**
 * Retries of transient request failures ({@link EngineConfig.Builder#retryPolicy}). Omitted fields
 * keep the engine defaults: no retries, 1000 ms initial delay, backoff factor 2.0, 30000 ms maximum
 * delay.
 */
public final class RetryPolicy {
  private final Integer maxRetries;
  private final Long initialDelayMs;
  private final Double backoffFactor;
  private final Long maxDelayMs;

  private RetryPolicy(Builder builder) {
    this.maxRetries = builder.maxRetries;
    this.initialDelayMs = builder.initialDelayMs;
    this.backoffFactor = builder.backoffFactor;
    this.maxDelayMs = builder.maxDelayMs;
  }

  /**
   * Starts a policy.
   *
   * @return a builder with every field omitted
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the maximum number of retries.
   *
   * @return the value, or empty for the default (0)
   */
  public OptionalInt maxRetries() {
    return maxRetries == null ? OptionalInt.empty() : OptionalInt.of(maxRetries);
  }

  /**
   * Returns the first retry delay.
   *
   * @return milliseconds, or empty for the default (1000)
   */
  public OptionalLong initialDelayMs() {
    return initialDelayMs == null ? OptionalLong.empty() : OptionalLong.of(initialDelayMs);
  }

  /**
   * Returns the delay multiplier between retries.
   *
   * @return the factor, or empty for the default (2.0)
   */
  public OptionalDouble backoffFactor() {
    return backoffFactor == null ? OptionalDouble.empty() : OptionalDouble.of(backoffFactor);
  }

  /**
   * Returns the longest delay between retries.
   *
   * @return milliseconds, or empty for the default (30000)
   */
  public OptionalLong maxDelayMs() {
    return maxDelayMs == null ? OptionalLong.empty() : OptionalLong.of(maxDelayMs);
  }

  JsonValue toJson() {
    JsonObjectBuilder json = Json.object();
    json.put("maxRetries", maxRetries);
    json.put("initialDelayMs", initialDelayMs);
    json.put("backoffFactor", backoffFactor);
    json.put("maxDelayMs", maxDelayMs);
    return json.build();
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof RetryPolicy)) {
      return false;
    }
    RetryPolicy that = (RetryPolicy) other;
    return Objects.equals(maxRetries, that.maxRetries)
        && Objects.equals(initialDelayMs, that.initialDelayMs)
        && Objects.equals(backoffFactor, that.backoffFactor)
        && Objects.equals(maxDelayMs, that.maxDelayMs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(maxRetries, initialDelayMs, backoffFactor, maxDelayMs);
  }

  @Override
  public String toString() {
    return "RetryPolicy" + toJson();
  }

  /** Builds a {@link RetryPolicy}. */
  public static final class Builder {
    private Integer maxRetries;
    private Long initialDelayMs;
    private Double backoffFactor;
    private Long maxDelayMs;

    private Builder() {}

    /**
     * Sets the maximum number of retries.
     *
     * @param value the count, not negative
     * @return this builder
     */
    public Builder maxRetries(int value) {
      this.maxRetries = EngineConfig.checkUnsigned("retryPolicy.maxRetries", value);
      return this;
    }

    /**
     * Sets the first retry delay; the engine rejects negative values.
     *
     * @param value milliseconds
     * @return this builder
     */
    public Builder initialDelayMs(long value) {
      this.initialDelayMs = value;
      return this;
    }

    /**
     * Sets the delay multiplier between retries.
     *
     * @param value the factor; must be finite
     * @return this builder
     */
    public Builder backoffFactor(double value) {
      this.backoffFactor = EngineConfig.checkFinite("retryPolicy.backoffFactor", value);
      return this;
    }

    /**
     * Sets the longest delay between retries; the engine rejects negative values.
     *
     * @param value milliseconds
     * @return this builder
     */
    public Builder maxDelayMs(long value) {
      this.maxDelayMs = value;
      return this;
    }

    /**
     * Builds the policy.
     *
     * @return the policy
     */
    public RetryPolicy build() {
      return new RetryPolicy(this);
    }
  }
}
