package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for the {@code futTicker} and {@code activeFutures} recipes (TS {@code
 * FuturesResolveOptions}).
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent. Used by {@link Engine#futTicker(String, String, FuturesResolveOptions)} and {@link
 * Engine#activeFutures(String, String, FuturesResolveOptions)}.
 */
public final class FuturesResolveOptions {
  private static final FuturesResolveOptions DEFAULTS = builder().build();

  private final String freq;

  private FuturesResolveOptions(Builder builder) {
    this.freq = builder.freq;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static FuturesResolveOptions defaults() {
    return DEFAULTS;
  }

  /**
   * Returns a builder with nothing set.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns a builder initialised with these options.
   *
   * @return a new builder
   */
  public Builder toBuilder() {
    return new Builder(this);
  }

  /**
   * Returns {@code freq}: the contract cycle, {@code "Q"}/{@code "QE"} for quarterly; anything
   * else, or unset, is monthly.
   *
   * @return the value, or empty when unset
   */
  public Optional<String> freq() {
    return Optional.ofNullable(freq);
  }

  /** Writes the set options as recipe argument fields. */
  void encodeInto(JsonObjectBuilder args) {
    args.put("freq", freq);
  }

  @Override
  public boolean equals(Object object) {
    return this == object
        || (object instanceof FuturesResolveOptions
            && Objects.equals(freq, ((FuturesResolveOptions) object).freq));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(freq);
  }

  @Override
  public String toString() {
    return RequestArgs.describe("FuturesResolveOptions", "freq", freq);
  }

  /** Builds {@link FuturesResolveOptions}; not thread-safe. */
  public static final class Builder {
    private String freq;

    private Builder() {}

    private Builder(FuturesResolveOptions options) {
      this.freq = options.freq;
    }

    /**
     * Sets {@code freq}: {@code "Q"} or {@code "QE"} for quarterly contracts.
     *
     * @param freq the frequency, or {@code null} to unset
     * @return this builder
     */
    public Builder freq(String freq) {
      this.freq = freq;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return immutable options
     */
    public FuturesResolveOptions build() {
      return new FuturesResolveOptions(this);
    }
  }
}
