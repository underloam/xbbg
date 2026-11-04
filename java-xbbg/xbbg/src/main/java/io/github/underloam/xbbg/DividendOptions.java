package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for the {@code dividend} recipe (TS {@code DividendOptions}).
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent, so the recipe default applies. Used by {@link Engine#dividend(java.util.Collection, String,
 * String, DividendOptions)}.
 */
public final class DividendOptions {
  private static final DividendOptions DEFAULTS = builder().build();

  private final String dvdType;

  private DividendOptions(Builder builder) {
    this.dvdType = builder.dvdType;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static DividendOptions defaults() {
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
   * Returns {@code dvdType}: a dividend type alias ({@code all}, {@code dvd}, {@code split}, …; see
   * {@link io.github.underloam.xbbg.ext.Ext#getDvdTypes()}) or a Bloomberg bulk field; unset means
   * {@code DVD_Hist_All}.
   *
   * @return the value, or empty when unset
   */
  public Optional<String> dvdType() {
    return Optional.ofNullable(dvdType);
  }

  /** Writes the set options as recipe argument fields. */
  void encodeInto(JsonObjectBuilder args) {
    args.put("dvdType", dvdType);
  }

  @Override
  public boolean equals(Object object) {
    return this == object
        || (object instanceof DividendOptions
            && Objects.equals(dvdType, ((DividendOptions) object).dvdType));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(dvdType);
  }

  @Override
  public String toString() {
    return RequestArgs.describe("DividendOptions", "dvdType", dvdType);
  }

  /** Builds {@link DividendOptions}; not thread-safe. */
  public static final class Builder {
    private String dvdType;

    private Builder() {}

    private Builder(DividendOptions options) {
      this.dvdType = options.dvdType;
    }

    /**
     * Sets {@code dvdType}: a dividend type alias or Bloomberg bulk field.
     *
     * @param dvdType the type, or {@code null} to unset
     * @return this builder
     */
    public Builder dvdType(String dvdType) {
      this.dvdType = dvdType;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return immutable options
     */
    public DividendOptions build() {
      return new DividendOptions(this);
    }
  }
}
