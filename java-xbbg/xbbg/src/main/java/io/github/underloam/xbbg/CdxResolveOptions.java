package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for the {@code cdxTicker} recipe (TS {@code CdxResolveOptions}).
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent, so the recipe default applies. Used by {@link Engine#cdxTicker(String, String,
 * CdxResolveOptions)}.
 */
public final class CdxResolveOptions {
  private static final CdxResolveOptions DEFAULTS = builder().build();

  private final Boolean versionless;

  private CdxResolveOptions(Builder builder) {
    this.versionless = builder.versionless;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static CdxResolveOptions defaults() {
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
    args.put("versionless", versionless);
  }

  @Override
  public boolean equals(Object object) {
    return this == object
        || (object instanceof CdxResolveOptions
            && Objects.equals(versionless, ((CdxResolveOptions) object).versionless));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(versionless);
  }

  @Override
  public String toString() {
    return RequestArgs.describe("CdxResolveOptions", "versionless", versionless);
  }

  /** Builds {@link CdxResolveOptions}; not thread-safe. */
  public static final class Builder {
    private Boolean versionless;

    private Builder() {}

    private Builder(CdxResolveOptions options) {
      this.versionless = options.versionless;
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
    public CdxResolveOptions build() {
      return new CdxResolveOptions(this);
    }
  }
}
