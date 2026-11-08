package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for the {@code futuresCurve} recipe (TS {@code FuturesCurveOptions}).
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent, so the recipe default applies. Used by {@link Engine#futuresCurve(String,
 * FuturesCurveOptions)}.
 */
public final class FuturesCurveOptions {
  private static final FuturesCurveOptions DEFAULTS = builder().build();

  private final String asof;
  private final String chainField;
  private final List<String> fields;
  private final Integer maxContracts;

  private FuturesCurveOptions(Builder builder) {
    this.asof = builder.asof;
    this.chainField = builder.chainField;
    this.fields = builder.fields;
    this.maxContracts = builder.maxContracts;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static FuturesCurveOptions defaults() {
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
   * Returns {@code asof}: the chain date ({@code CHAIN_DATE}), as {@code YYYYMMDD}; unset means
   * today's chain.
   *
   * @return the value, or empty when unset
   */
  public Optional<String> asof() {
    return Optional.ofNullable(asof);
  }

  /**
   * Returns {@code chainField}: the bulk chain field; unset means {@code
   * FUT_CHAIN_LAST_TRADE_DATES}.
   *
   * @return the value, or empty when unset
   */
  public Optional<String> chainField() {
    return Optional.ofNullable(chainField);
  }

  /**
   * Returns {@code fields}: contract fields; {@code PX_BID}, {@code PX_ASK} and {@code
   * LAST_TRADEABLE_DT} are always added.
   *
   * @return the value, or empty when unset
   */
  public Optional<List<String>> fields() {
    return Optional.ofNullable(fields);
  }

  /**
   * Returns {@code maxContracts}: the most contracts to keep from the chain; unset keeps all.
   *
   * @return the value, or empty when unset
   */
  public Optional<Integer> maxContracts() {
    return Optional.ofNullable(maxContracts);
  }

  /** Writes the set options as recipe argument fields. */
  void encodeInto(JsonObjectBuilder args) {
    args.put("asof", asof);
    args.put("chainField", chainField);
    args.put("fields", fields);
    args.put("maxContracts", maxContracts);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof FuturesCurveOptions)) {
      return false;
    }
    FuturesCurveOptions other = (FuturesCurveOptions) object;
    return Objects.equals(asof, other.asof)
        && Objects.equals(chainField, other.chainField)
        && Objects.equals(fields, other.fields)
        && Objects.equals(maxContracts, other.maxContracts);
  }

  @Override
  public int hashCode() {
    return Objects.hash(asof, chainField, fields, maxContracts);
  }

  @Override
  public String toString() {
    return RequestArgs.describe(
        "FuturesCurveOptions",
        "asof",
        asof,
        "chainField",
        chainField,
        "fields",
        fields,
        "maxContracts",
        maxContracts);
  }

  /** Builds {@link FuturesCurveOptions}; not thread-safe. */
  public static final class Builder {
    private String asof;
    private String chainField;
    private List<String> fields;
    private Integer maxContracts;

    private Builder() {}

    private Builder(FuturesCurveOptions options) {
      this.asof = options.asof;
      this.chainField = options.chainField;
      this.fields = options.fields;
      this.maxContracts = options.maxContracts;
    }

    /**
     * Sets {@code asof} from text, formatted with {@link Dates#formatDate(String)}; blank text
     * unsets it.
     *
     * @param asof ISO 8601 or Bloomberg date text, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the text is ambiguous or not a date
     */
    public Builder asof(String asof) {
      this.asof = asof == null ? null : Dates.formatDate(asof);
      return this;
    }

    /**
     * Sets {@code asof} from a {@code java.time} value, formatted with {@link
     * Dates#formatDate(TemporalAccessor)}.
     *
     * @param asof the date, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the value carries no date
     */
    public Builder asof(TemporalAccessor asof) {
      this.asof = asof == null ? null : Dates.formatDate(asof);
      return this;
    }

    /**
     * Sets {@code chainField}: the bulk chain field.
     *
     * @param chainField the field, or {@code null} to unset
     * @return this builder
     */
    public Builder chainField(String chainField) {
      this.chainField = chainField;
      return this;
    }

    /**
     * Sets {@code fields}: contract fields to fetch.
     *
     * @param fields the fields, or {@code null} to unset
     * @return this builder
     * @throws NullPointerException if an item is null
     */
    public Builder fields(Collection<String> fields) {
      this.fields = fields == null ? null : RequestArgs.strings("fields", fields);
      return this;
    }

    /**
     * Sets {@code maxContracts}: the most contracts to keep.
     *
     * @param maxContracts the limit, or {@code null} to unset
     * @return this builder
     */
    public Builder maxContracts(Integer maxContracts) {
      this.maxContracts = maxContracts;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return immutable options
     */
    public FuturesCurveOptions build() {
      return new FuturesCurveOptions(this);
    }
  }
}
