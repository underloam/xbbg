package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for the {@code delayStatus} recipe (TS {@code DelayStatusOptions}).
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent, so the recipe default applies. Used by {@link Engine#delayStatus(java.util.Collection,
 * DelayStatusOptions)} and {@link Blp#delayStatus(java.util.Collection, DelayStatusOptions)}.
 */
public final class DelayStatusOptions {
  private static final DelayStatusOptions DEFAULTS = builder().build();

  private final Boolean checkIdentity;

  private DelayStatusOptions(Builder builder) {
    this.checkIdentity = builder.checkIdentity;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static DelayStatusOptions defaults() {
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
   * Returns {@code checkIdentity}: also check the identity's entitlement to each feed EID (needs an
   * authorized identity); unset means {@code false}.
   *
   * @return the value, or empty when unset
   */
  public Optional<Boolean> checkIdentity() {
    return Optional.ofNullable(checkIdentity);
  }

  /** Writes the set options as recipe argument fields. */
  void encodeInto(JsonObjectBuilder args) {
    args.put("checkIdentity", checkIdentity);
  }

  @Override
  public boolean equals(Object object) {
    return this == object
        || (object instanceof DelayStatusOptions
            && Objects.equals(checkIdentity, ((DelayStatusOptions) object).checkIdentity));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(checkIdentity);
  }

  @Override
  public String toString() {
    return RequestArgs.describe("DelayStatusOptions", "checkIdentity", checkIdentity);
  }

  /** Builds {@link DelayStatusOptions}; not thread-safe. */
  public static final class Builder {
    private Boolean checkIdentity;

    private Builder() {}

    private Builder(DelayStatusOptions options) {
      this.checkIdentity = options.checkIdentity;
    }

    /**
     * Sets {@code checkIdentity}: whether to check feed EID entitlements for the identity.
     *
     * @param checkIdentity the flag, or {@code null} to unset ({@code false})
     * @return this builder
     */
    public Builder checkIdentity(Boolean checkIdentity) {
      this.checkIdentity = checkIdentity;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return immutable options
     */
    public DelayStatusOptions build() {
      return new DelayStatusOptions(this);
    }
  }
}
