package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for the {@code resolveVenues} recipe (TS {@code ResolveVenuesOptions}).
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent. Used by {@link Engine#resolveVenues(java.util.Collection, ResolveVenuesOptions)}.
 */
public final class ResolveVenuesOptions {
  private static final ResolveVenuesOptions DEFAULTS = builder().build();

  private final Map<String, String> pcsOverrides;

  private ResolveVenuesOptions(Builder builder) {
    this.pcsOverrides = builder.pcsOverrides;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static ResolveVenuesOptions defaults() {
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
   * Returns {@code pcsOverrides}: exchange name → pricing source, overriding the built-in routing
   * of preferred-share venues.
   *
   * @return the value, or empty when unset
   */
  public Optional<Map<String, String>> pcsOverrides() {
    return Optional.ofNullable(pcsOverrides);
  }

  /** Writes the set options as recipe argument fields. */
  void encodeInto(JsonObjectBuilder args) {
    args.put("pcsOverrides", pcsOverrides);
  }

  @Override
  public boolean equals(Object object) {
    return this == object
        || (object instanceof ResolveVenuesOptions
            && Objects.equals(pcsOverrides, ((ResolveVenuesOptions) object).pcsOverrides));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(pcsOverrides);
  }

  @Override
  public String toString() {
    return RequestArgs.describe("ResolveVenuesOptions", "pcsOverrides", pcsOverrides);
  }

  /** Copies a pricing-source override map, rejecting null keys and values. */
  static Map<String, String> copyPcsOverrides(Map<String, String> pcsOverrides) {
    Map<String, String> copy = new LinkedHashMap<String, String>();
    for (Map.Entry<String, String> entry : pcsOverrides.entrySet()) {
      copy.put(
          Objects.requireNonNull(entry.getKey(), "pcsOverrides key"),
          Objects.requireNonNull(entry.getValue(), "pcsOverrides value"));
    }
    return Collections.unmodifiableMap(copy);
  }

  /** Builds {@link ResolveVenuesOptions}; not thread-safe. */
  public static final class Builder {
    private Map<String, String> pcsOverrides;

    private Builder() {}

    private Builder(ResolveVenuesOptions options) {
      this.pcsOverrides = options.pcsOverrides;
    }

    /**
     * Sets {@code pcsOverrides}: exchange name → pricing source.
     *
     * @param pcsOverrides the overrides, or {@code null} to unset
     * @return this builder
     * @throws NullPointerException if a key or value is null
     */
    public Builder pcsOverrides(Map<String, String> pcsOverrides) {
      this.pcsOverrides = pcsOverrides == null ? null : copyPcsOverrides(pcsOverrides);
      return this;
    }

    /**
     * Builds the options.
     *
     * @return immutable options
     */
    public ResolveVenuesOptions build() {
      return new ResolveVenuesOptions(this);
    }
  }
}
