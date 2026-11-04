package io.github.underloam.xbbg.subscription;

import io.github.underloam.xbbg.DelayedPolicy;
import io.github.underloam.xbbg.OverflowPolicy;
import io.github.underloam.xbbg.SubscriptionDeliveryMode;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Options of {@code subscribeAuction} and {@code streamAuction} (the JavaScript {@code
 * AuctionStreamOptions}): every {@link StreamOptions} member except {@code aliases}, which are
 * derived from the input securities, plus per-exchange pricing-source overrides.
 *
 * <p>{@code fields} defaults to the auction {@code default} field group and {@code zeroAsNull} to
 * the requested zero-price fields.
 */
public final class AuctionStreamOptions {
  private static final AuctionStreamOptions DEFAULTS = new Builder().build();

  private final StreamOptions stream;
  private final Map<String, String> pcsOverrides;

  private AuctionStreamOptions(StreamOptions stream, Map<String, String> pcsOverrides) {
    this.stream = stream;
    this.pcsOverrides = pcsOverrides;
  }

  /**
   * Returns options with nothing set.
   *
   * @return the default options
   */
  public static AuctionStreamOptions defaults() {
    return DEFAULTS;
  }

  /**
   * Starts a builder with nothing set.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the stream options (never with aliases).
   *
   * @return the stream options
   */
  public StreamOptions stream() {
    return stream;
  }

  /**
   * Returns the per-exchange pricing-source (PCS) overrides used to route venues.
   *
   * @return the overrides, or empty when unset
   */
  public Optional<Map<String, String>> pcsOverrides() {
    return Optional.ofNullable(pcsOverrides);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof AuctionStreamOptions)) {
      return false;
    }
    AuctionStreamOptions that = (AuctionStreamOptions) other;
    return stream.equals(that.stream) && Objects.equals(pcsOverrides, that.pcsOverrides);
  }

  @Override
  public int hashCode() {
    return 31 * stream.hashCode() + Objects.hashCode(pcsOverrides);
  }

  @Override
  public String toString() {
    return "AuctionStreamOptions{stream=" + stream + ", pcsOverrides=" + pcsOverrides + "}";
  }

  /** Builds {@link AuctionStreamOptions}; setters mirror {@link StreamOptions.Builder}. */
  public static final class Builder {
    private final StreamOptions.Builder stream = StreamOptions.builder();
    private Map<String, String> pcsOverrides;

    private Builder() {}

    /**
     * Sets the delivery mode.
     *
     * @param delivery the mode
     * @return this builder
     */
    public Builder delivery(SubscriptionDeliveryMode delivery) {
      stream.delivery(delivery);
      return this;
    }

    /**
     * Sets Bloomberg subscription options.
     *
     * @param options the options
     * @return this builder
     */
    public Builder options(Collection<String> options) {
      stream.options(options);
      return this;
    }

    /**
     * Requests Bloomberg upstream conflation.
     *
     * @param conflate whether to conflate
     * @return this builder
     */
    public Builder conflate(boolean conflate) {
      stream.conflate(conflate);
      return this;
    }

    /**
     * Sets the maximum number of already-queued updates per read.
     *
     * @param flushThreshold the threshold
     * @return this builder
     */
    public Builder flushThreshold(int flushThreshold) {
      stream.flushThreshold(flushThreshold);
      return this;
    }

    /**
     * Sets the overflow policy.
     *
     * @param overflowPolicy the policy
     * @return this builder
     */
    public Builder overflowPolicy(OverflowPolicy overflowPolicy) {
      stream.overflowPolicy(overflowPolicy);
      return this;
    }

    /**
     * Sets the queue capacity.
     *
     * @param streamCapacity the capacity
     * @return this builder
     */
    public Builder streamCapacity(int streamCapacity) {
      stream.streamCapacity(streamCapacity);
      return this;
    }

    /**
     * Exposes every top-level scalar field of the shared feed.
     *
     * @param allFields whether to expose all fields
     * @return this builder
     */
    public Builder allFields(boolean allFields) {
      stream.allFields(allFields);
      return this;
    }

    /**
     * Sets the requested auction fields (empty or unset: the {@code default} group).
     *
     * @param fields the fields
     * @return this builder
     */
    public Builder fields(Collection<String> fields) {
      stream.fields(fields);
      return this;
    }

    /**
     * Sets the delayed-data policy.
     *
     * @param onDelayed the policy
     * @return this builder
     */
    public Builder onDelayed(DelayedPolicy onDelayed) {
      stream.onDelayed(onDelayed);
      return this;
    }

    /**
     * Gives the subscription its own feed and session.
     *
     * @param isolated whether to isolate
     * @return this builder
     */
    public Builder isolated(boolean isolated) {
      stream.isolated(isolated);
      return this;
    }

    /**
     * Chooses whether rows are delivered (rejected by {@code streamAuction}).
     *
     * @param rows whether to deliver rows
     * @return this builder
     */
    public Builder rows(boolean rows) {
      stream.rows(rows);
      return this;
    }

    /**
     * Sets the rejected-field policy.
     *
     * @param onFieldError the policy
     * @return this builder
     */
    public Builder onFieldError(FieldErrorPolicy onFieldError) {
      stream.onFieldError(onFieldError);
      return this;
    }

    /**
     * Sets the fields whose numeric zero becomes null (unset: the requested zero-price fields).
     *
     * @param zeroAsNull the fields
     * @return this builder
     */
    public Builder zeroAsNull(Collection<String> zeroAsNull) {
      stream.zeroAsNull(zeroAsNull);
      return this;
    }

    /**
     * Sets per-exchange pricing-source overrides used to route venues.
     *
     * @param pcsOverrides exchange to pricing source, copied in iteration order
     * @return this builder
     */
    public Builder pcsOverrides(Map<String, String> pcsOverrides) {
      this.pcsOverrides =
          pcsOverrides == null
              ? null
              : Collections.unmodifiableMap(new LinkedHashMap<String, String>(pcsOverrides));
      return this;
    }

    /**
     * Builds the options.
     *
     * @return immutable options
     */
    public AuctionStreamOptions build() {
      return new AuctionStreamOptions(stream.build(), pcsOverrides);
    }
  }
}
