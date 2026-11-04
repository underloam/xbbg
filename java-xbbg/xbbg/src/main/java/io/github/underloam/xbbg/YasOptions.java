package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for the {@code yas} yield and spread analysis recipe (TS {@code YasOptions}). Each set
 * value becomes a YAS override.
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent. Used by {@link Engine#yas(java.util.Collection, java.util.Collection, YasOptions)}.
 */
public final class YasOptions {
  private static final YasOptions DEFAULTS = builder().build();

  private final String settleDt;
  private final Integer yieldType;
  private final Double spread;
  private final Double yieldVal;
  private final Double price;
  private final String benchmark;

  private YasOptions(Builder builder) {
    this.settleDt = builder.settleDt;
    this.yieldType = builder.yieldType;
    this.spread = builder.spread;
    this.yieldVal = builder.yieldVal;
    this.price = builder.price;
    this.benchmark = builder.benchmark;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static YasOptions defaults() {
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
   * Returns {@code settleDt}: the settlement date ({@code YAS_SETTLE_DT}), as {@code YYYYMMDD}.
   *
   * @return the value, or empty when unset
   */
  public Optional<String> settleDt() {
    return Optional.ofNullable(settleDt);
  }

  /**
   * Returns {@code yieldType}: the {@code YAS_YLD_FLAG} code (1 = YTM, 2 = YTC, 3 = YTR, 4 = YTP, 5
   * = YTW, 6 = YTWR, 7 = EYTW, 8 = EYTWR, 9 = YTAL).
   *
   * @return the value, or empty when unset
   */
  public Optional<Integer> yieldType() {
    return Optional.ofNullable(yieldType);
  }

  /**
   * Returns {@code spread}: the yield spread override ({@code YAS_YLD_SPREAD}).
   *
   * @return the value, or empty when unset
   */
  public Optional<Double> spread() {
    return Optional.ofNullable(spread);
  }

  /**
   * Returns {@code yieldVal}: the yield override ({@code YAS_BOND_YLD}).
   *
   * @return the value, or empty when unset
   */
  public Optional<Double> yieldVal() {
    return Optional.ofNullable(yieldVal);
  }

  /**
   * Returns {@code price}: the price override ({@code YAS_BOND_PX}).
   *
   * @return the value, or empty when unset
   */
  public Optional<Double> price() {
    return Optional.ofNullable(price);
  }

  /**
   * Returns {@code benchmark}: the benchmark bond for spreads ({@code YAS_BNCHMRK_BOND}).
   *
   * @return the value, or empty when unset
   */
  public Optional<String> benchmark() {
    return Optional.ofNullable(benchmark);
  }

  /** Writes the set options as recipe argument fields. */
  void encodeInto(JsonObjectBuilder args) {
    args.put("settleDt", settleDt);
    args.put("yieldType", yieldType);
    args.put("spread", spread);
    args.put("yieldVal", yieldVal);
    args.put("price", price);
    args.put("benchmark", benchmark);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof YasOptions)) {
      return false;
    }
    YasOptions other = (YasOptions) object;
    return Objects.equals(settleDt, other.settleDt)
        && Objects.equals(yieldType, other.yieldType)
        && Objects.equals(spread, other.spread)
        && Objects.equals(yieldVal, other.yieldVal)
        && Objects.equals(price, other.price)
        && Objects.equals(benchmark, other.benchmark);
  }

  @Override
  public int hashCode() {
    return Objects.hash(settleDt, yieldType, spread, yieldVal, price, benchmark);
  }

  @Override
  public String toString() {
    return RequestArgs.describe(
        "YasOptions",
        "settleDt",
        settleDt,
        "yieldType",
        yieldType,
        "spread",
        spread,
        "yieldVal",
        yieldVal,
        "price",
        price,
        "benchmark",
        benchmark);
  }

  /** Builds {@link YasOptions}; not thread-safe. */
  public static final class Builder {
    private String settleDt;
    private Integer yieldType;
    private Double spread;
    private Double yieldVal;
    private Double price;
    private String benchmark;

    private Builder() {}

    private Builder(YasOptions options) {
      this.settleDt = options.settleDt;
      this.yieldType = options.yieldType;
      this.spread = options.spread;
      this.yieldVal = options.yieldVal;
      this.price = options.price;
      this.benchmark = options.benchmark;
    }

    /**
     * Sets {@code settleDt} from text, formatted with {@link Dates#formatDate(String)}; blank text
     * unsets it.
     *
     * @param settleDt ISO 8601 or Bloomberg date text, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the text is ambiguous or not a date
     */
    public Builder settleDt(String settleDt) {
      this.settleDt = settleDt == null ? null : Dates.formatDate(settleDt);
      return this;
    }

    /**
     * Sets {@code settleDt} from a {@code java.time} value, formatted with {@link
     * Dates#formatDate(TemporalAccessor)}.
     *
     * @param settleDt the date, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the value carries no date
     */
    public Builder settleDt(TemporalAccessor settleDt) {
      this.settleDt = settleDt == null ? null : Dates.formatDate(settleDt);
      return this;
    }

    /**
     * Sets {@code yieldType}: the {@code YAS_YLD_FLAG} code, 1–9 (the engine rejects other codes
     * with a {@link BlpValidationException}).
     *
     * @param yieldType the code, or {@code null} to unset
     * @return this builder
     */
    public Builder yieldType(Integer yieldType) {
      this.yieldType = yieldType;
      return this;
    }

    /**
     * Sets {@code spread}: the yield spread override.
     *
     * @param spread the spread, or {@code null} to unset
     * @return this builder
     */
    public Builder spread(Double spread) {
      this.spread = spread;
      return this;
    }

    /**
     * Sets {@code yieldVal}: the yield override.
     *
     * @param yieldVal the yield, or {@code null} to unset
     * @return this builder
     */
    public Builder yieldVal(Double yieldVal) {
      this.yieldVal = yieldVal;
      return this;
    }

    /**
     * Sets {@code price}: the price override.
     *
     * @param price the price, or {@code null} to unset
     * @return this builder
     */
    public Builder price(Double price) {
      this.price = price;
      return this;
    }

    /**
     * Sets {@code benchmark}: the benchmark bond for spread calculations.
     *
     * @param benchmark the benchmark security, or {@code null} to unset
     * @return this builder
     */
    public Builder benchmark(String benchmark) {
      this.benchmark = benchmark;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return immutable options
     */
    public YasOptions build() {
      return new YasOptions(this);
    }
  }
}
