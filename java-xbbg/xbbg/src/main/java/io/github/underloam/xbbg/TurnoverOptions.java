package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for the {@code turnover} recipe (TS {@code TurnoverOptions}).
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent, so the recipe default applies. Used by {@link Engine#turnover(java.util.Collection, String,
 * String, TurnoverOptions)}.
 */
public final class TurnoverOptions {
  private static final TurnoverOptions DEFAULTS = builder().build();

  private final String ccy;
  private final Double factor;

  private TurnoverOptions(Builder builder) {
    this.ccy = builder.ccy;
    this.factor = builder.factor;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static TurnoverOptions defaults() {
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
   * Returns {@code ccy}: the currency to convert turnover into; unset keeps local currency.
   *
   * @return the value, or empty when unset
   */
  public Optional<String> ccy() {
    return Optional.ofNullable(ccy);
  }

  /**
   * Returns {@code factor}: the divisor applied after conversion; unset means 1.
   *
   * @return the value, or empty when unset
   */
  public Optional<Double> factor() {
    return Optional.ofNullable(factor);
  }

  /** Writes the set options as recipe argument fields. */
  void encodeInto(JsonObjectBuilder args) {
    args.put("ccy", ccy);
    args.put("factor", factor);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof TurnoverOptions)) {
      return false;
    }
    TurnoverOptions other = (TurnoverOptions) object;
    return Objects.equals(ccy, other.ccy) && Objects.equals(factor, other.factor);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ccy, factor);
  }

  @Override
  public String toString() {
    return RequestArgs.describe("TurnoverOptions", "ccy", ccy, "factor", factor);
  }

  /** Builds {@link TurnoverOptions}; not thread-safe. */
  public static final class Builder {
    private String ccy;
    private Double factor;

    private Builder() {}

    private Builder(TurnoverOptions options) {
      this.ccy = options.ccy;
      this.factor = options.factor;
    }

    /**
     * Sets {@code ccy}: the target currency, for example {@code "USD"}.
     *
     * @param ccy the currency, or {@code null} to unset
     * @return this builder
     */
    public Builder ccy(String ccy) {
      this.ccy = ccy;
      return this;
    }

    /**
     * Sets {@code factor}: the divisor applied after conversion, for example {@code 1e6}.
     *
     * @param factor the factor, or {@code null} to unset
     * @return this builder
     */
    public Builder factor(Double factor) {
      this.factor = factor;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return immutable options
     */
    public TurnoverOptions build() {
      return new TurnoverOptions(this);
    }
  }
}
