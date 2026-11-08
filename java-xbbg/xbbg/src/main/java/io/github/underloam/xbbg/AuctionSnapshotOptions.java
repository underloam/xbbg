package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for the {@code auctionSnapshot} recipe (TS {@code AuctionSnapshotOptions}).
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent, so the recipe default applies. Used by {@link Engine#auctionSnapshot(java.util.Collection,
 * AuctionSnapshotOptions)}.
 */
public final class AuctionSnapshotOptions {
  private static final AuctionSnapshotOptions DEFAULTS = builder().build();

  private final List<String> fields;
  private final Map<String, String> pcsOverrides;

  private AuctionSnapshotOptions(Builder builder) {
    this.fields = builder.fields;
    this.pcsOverrides = builder.pcsOverrides;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static AuctionSnapshotOptions defaults() {
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
   * Returns {@code fields}: the auction fields to fetch; unset or empty means {@link
   * io.github.underloam.xbbg.ext.AuctionFields#defaults()}.
   *
   * @return the value, or empty when unset
   */
  public Optional<List<String>> fields() {
    return Optional.ofNullable(fields);
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
    args.put("fields", fields);
    args.put("pcsOverrides", pcsOverrides);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof AuctionSnapshotOptions)) {
      return false;
    }
    AuctionSnapshotOptions other = (AuctionSnapshotOptions) object;
    return Objects.equals(fields, other.fields) && Objects.equals(pcsOverrides, other.pcsOverrides);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fields, pcsOverrides);
  }

  @Override
  public String toString() {
    return RequestArgs.describe(
        "AuctionSnapshotOptions", "fields", fields, "pcsOverrides", pcsOverrides);
  }

  /** Builds {@link AuctionSnapshotOptions}; not thread-safe. */
  public static final class Builder {
    private List<String> fields;
    private Map<String, String> pcsOverrides;

    private Builder() {}

    private Builder(AuctionSnapshotOptions options) {
      this.fields = options.fields;
      this.pcsOverrides = options.pcsOverrides;
    }

    /**
     * Sets {@code fields}: the auction fields to fetch.
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
     * Sets {@code pcsOverrides}: exchange name → pricing source.
     *
     * @param pcsOverrides the overrides, or {@code null} to unset
     * @return this builder
     * @throws NullPointerException if a key or value is null
     */
    public Builder pcsOverrides(Map<String, String> pcsOverrides) {
      this.pcsOverrides =
          pcsOverrides == null ? null : ResolveVenuesOptions.copyPcsOverrides(pcsOverrides);
      return this;
    }

    /**
     * Builds the options.
     *
     * @return immutable options
     */
    public AuctionSnapshotOptions build() {
      return new AuctionSnapshotOptions(this);
    }
  }
}
