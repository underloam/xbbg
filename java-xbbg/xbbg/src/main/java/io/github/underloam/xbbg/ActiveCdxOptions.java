package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for the {@code activeCdx} recipe (TS {@code ActiveCdxOptions}): the {@link
 * CdxResolveOptions} plus a minimum activity window.
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent, so the recipe default applies. Used by {@link Engine#activeCdx(String, String,
 * ActiveCdxOptions)}.
 */
public final class ActiveCdxOptions {
  private static final ActiveCdxOptions DEFAULTS = builder().build();

  private final Integer lookbackDays;
  private final Boolean versionless;

  private ActiveCdxOptions(Builder builder) {
    this.lookbackDays = builder.lookbackDays;
    this.versionless = builder.versionless;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static ActiveCdxOptions defaults() {
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
   * Returns {@code lookbackDays}: the minimum trading-activity window; the window always reaches
   * back to the resolved series' first accrual date.
   *
   * @return the value, or empty when unset
   */
  public Optional<Integer> lookbackDays() {
    return Optional.ofNullable(lookbackDays);
  }

  /**
   * Returns {@code versionless}: return the legacy alias without the {@code Vn} token; unset means
   * {@code false}.
   *
   * @return the value, or empty when unset
   */
  public Optional<Boolean> versionless() {
    return Optional.ofNullable(versionless);
  }

  /** Writes the set options as recipe argument fields. */
  void encodeInto(JsonObjectBuilder args) {
    args.put("lookbackDays", lookbackDays);
    args.put("versionless", versionless);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof ActiveCdxOptions)) {
      return false;
    }
    ActiveCdxOptions other = (ActiveCdxOptions) object;
    return Objects.equals(lookbackDays, other.lookbackDays)
        && Objects.equals(versionless, other.versionless);
  }

  @Override
  public int hashCode() {
    return Objects.hash(lookbackDays, versionless);
  }

  @Override
  public String toString() {
    return RequestArgs.describe(
        "ActiveCdxOptions", "lookbackDays", lookbackDays, "versionless", versionless);
  }

  /** Builds {@link ActiveCdxOptions}; not thread-safe. */
  public static final class Builder {
    private Integer lookbackDays;
    private Boolean versionless;

    private Builder() {}

    private Builder(ActiveCdxOptions options) {
      this.lookbackDays = options.lookbackDays;
      this.versionless = options.versionless;
    }

    /**
     * Sets {@code lookbackDays}: the minimum activity window in days.
     *
     * @param lookbackDays the window, or {@code null} to unset
     * @return this builder
     */
    public Builder lookbackDays(Integer lookbackDays) {
      this.lookbackDays = lookbackDays;
      return this;
    }

    /**
     * Sets {@code versionless}: whether to drop the {@code Vn} token from the resolved ticker.
     *
     * @param versionless the flag, or {@code null} to unset ({@code false})
     * @return this builder
     */
    public Builder versionless(Boolean versionless) {
      this.versionless = versionless;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return immutable options
     */
    public ActiveCdxOptions build() {
      return new ActiveCdxOptions(this);
    }
  }
}
