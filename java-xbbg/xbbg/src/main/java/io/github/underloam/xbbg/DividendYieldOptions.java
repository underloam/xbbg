package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for the {@code dividendYield} recipe (TS {@code DividendYieldOptions}).
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent, so the recipe default applies. Used by {@link Engine#dividendYield(java.util.Collection,
 * String, String, DividendYieldOptions)}.
 */
public final class DividendYieldOptions {
  private static final DividendYieldOptions DEFAULTS = builder().build();

  private final List<String> dividendTypes;
  private final Integer windowDays;

  private DividendYieldOptions(Builder builder) {
    this.dividendTypes = builder.dividendTypes;
    this.windowDays = builder.windowDays;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static DividendYieldOptions defaults() {
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
   * Returns {@code dividendTypes}: the Bloomberg dividend types to count; unset counts every type.
   *
   * @return the value, or empty when unset
   */
  public Optional<List<String>> dividendTypes() {
    return Optional.ofNullable(dividendTypes);
  }

  /**
   * Returns {@code windowDays}: the trailing window in days; unset means 365 (at least 1).
   *
   * @return the value, or empty when unset
   */
  public Optional<Integer> windowDays() {
    return Optional.ofNullable(windowDays);
  }

  /** Writes the set options as recipe argument fields. */
  void encodeInto(JsonObjectBuilder args) {
    args.put("dividendTypes", dividendTypes);
    args.put("windowDays", windowDays);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof DividendYieldOptions)) {
      return false;
    }
    DividendYieldOptions other = (DividendYieldOptions) object;
    return Objects.equals(dividendTypes, other.dividendTypes)
        && Objects.equals(windowDays, other.windowDays);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dividendTypes, windowDays);
  }

  @Override
  public String toString() {
    return RequestArgs.describe(
        "DividendYieldOptions", "dividendTypes", dividendTypes, "windowDays", windowDays);
  }

  /** Builds {@link DividendYieldOptions}; not thread-safe. */
  public static final class Builder {
    private List<String> dividendTypes;
    private Integer windowDays;

    private Builder() {}

    private Builder(DividendYieldOptions options) {
      this.dividendTypes = options.dividendTypes;
      this.windowDays = options.windowDays;
    }

    /**
     * Sets {@code dividendTypes}: the dividend types to count, for example {@code "Regular Cash"}.
     *
     * @param dividendTypes the types, or {@code null} to unset
     * @return this builder
     * @throws NullPointerException if an item is null
     */
    public Builder dividendTypes(Collection<String> dividendTypes) {
      this.dividendTypes =
          dividendTypes == null ? null : RequestArgs.strings("dividendTypes", dividendTypes);
      return this;
    }

    /**
     * Sets {@code windowDays}: the trailing window in days.
     *
     * @param windowDays the window, or {@code null} to unset
     * @return this builder
     */
    public Builder windowDays(Integer windowDays) {
      this.windowDays = windowDays;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return immutable options
     */
    public DividendYieldOptions build() {
      return new DividendYieldOptions(this);
    }
  }
}
