package io.github.underloam.xbbg.subscription;

import io.github.underloam.xbbg.BlpValidationException;
import io.github.underloam.xbbg.DelayedPolicy;
import io.github.underloam.xbbg.OverflowPolicy;
import io.github.underloam.xbbg.SubscriptionDeliveryMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Options of a subscription (the JavaScript {@code StreamOptions}). Every option is optional; an
 * unset option keeps the engine default. Build instances with {@link #builder()}.
 *
 * <p>Checks that need several options together (for example {@code delivery=state} with {@code
 * rows=false}) run when the subscription is created, with the same messages as JavaScript.
 */
public final class StreamOptions {
  private static final StreamOptions DEFAULTS = new Builder().build();

  private final SubscriptionDeliveryMode delivery;
  private final List<String> options;
  private final Boolean conflate;
  private final Integer flushThreshold;
  private final OverflowPolicy overflowPolicy;
  private final Integer streamCapacity;
  private final Boolean allFields;
  private final List<String> fields;
  private final Map<String, String> aliases;
  private final DelayedPolicy onDelayed;
  private final Boolean isolated;
  private final Boolean rows;
  private final FieldErrorPolicy onFieldError;
  private final List<String> zeroAsNull;

  private StreamOptions(Builder builder) {
    this.delivery = builder.delivery;
    this.options = builder.options;
    this.conflate = builder.conflate;
    this.flushThreshold = builder.flushThreshold;
    this.overflowPolicy = builder.overflowPolicy;
    this.streamCapacity = builder.streamCapacity;
    this.allFields = builder.allFields;
    this.fields = builder.fields;
    this.aliases = builder.aliases;
    this.onDelayed = builder.onDelayed;
    this.isolated = builder.isolated;
    this.rows = builder.rows;
    this.onFieldError = builder.onFieldError;
    this.zeroAsNull = builder.zeroAsNull;
  }

  /**
   * Returns options with nothing set.
   *
   * @return the default options
   */
  public static StreamOptions defaults() {
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
   * Starts a builder with these options.
   *
   * @return a new builder holding a copy of these options
   */
  public Builder toBuilder() {
    Builder builder = new Builder();
    builder.delivery = delivery;
    builder.options = options;
    builder.conflate = conflate;
    builder.flushThreshold = flushThreshold;
    builder.overflowPolicy = overflowPolicy;
    builder.streamCapacity = streamCapacity;
    builder.allFields = allFields;
    builder.fields = fields;
    builder.aliases = aliases;
    builder.onDelayed = onDelayed;
    builder.isolated = isolated;
    builder.rows = rows;
    builder.onFieldError = onFieldError;
    builder.zeroAsNull = zeroAsNull;
    return builder;
  }

  /**
   * Returns the delivery mode: sparse deltas (default) or conflated current state.
   *
   * @return the mode, or empty when unset
   */
  public Optional<SubscriptionDeliveryMode> delivery() {
    return Optional.ofNullable(delivery);
  }

  /**
   * Returns the Bloomberg subscription options such as {@code interval=5}. They are part of the
   * shared feed's identity.
   *
   * @return the options, or empty when unset
   */
  public Optional<List<String>> options() {
    return Optional.ofNullable(options);
  }

  /**
   * Returns whether Bloomberg upstream conflation is requested ({@code //blp/mktdata} only).
   *
   * @return the flag, or empty when unset (no conflation)
   */
  public Optional<Boolean> conflate() {
    return Optional.ofNullable(conflate);
  }

  /**
   * Returns the maximum number of already-queued updates (dirty topics in state mode) per read.
   * Reads never wait for this many to arrive.
   *
   * @return the threshold, or empty for the engine default (16)
   */
  public OptionalInt flushThreshold() {
    return flushThreshold == null ? OptionalInt.empty() : OptionalInt.of(flushThreshold);
  }

  /**
   * Returns what happens when this consumer's queue is full.
   *
   * @return the policy, or empty for the engine default ({@code drop_newest})
   */
  public Optional<OverflowPolicy> overflowPolicy() {
    return Optional.ofNullable(overflowPolicy);
  }

  /**
   * Returns this consumer's queue capacity.
   *
   * @return the capacity, or empty for the engine default (256)
   */
  public OptionalInt streamCapacity() {
    return streamCapacity == null ? OptionalInt.empty() : OptionalInt.of(streamCapacity);
  }

  /**
   * Returns whether every top-level scalar field of the shared feed is exposed.
   *
   * @return the flag, or empty when unset (only the requested fields)
   */
  public Optional<Boolean> allFields() {
    return Optional.ofNullable(allFields);
  }

  /**
   * Returns the fields read by {@code mktbar}, {@code depth}, {@code chains}, {@code
   * subscribeAuction} and {@code subscribeEtfInav}. Ignored by {@code subscribe}, {@code stream}
   * and {@code vwap}, which take their fields positionally.
   *
   * @return the fields, or empty when unset
   */
  public Optional<List<String>> fields() {
    return Optional.ofNullable(fields);
  }

  /**
   * Returns the map from upstream topic to the consumer label used in ticks, {@code tickers},
   * status keys and {@code remove}.
   *
   * @return the aliases, or empty when unset
   */
  public Optional<Map<String, String>> aliases() {
    return Optional.ofNullable(aliases);
  }

  /**
   * Returns the delayed-data policy.
   *
   * @return the policy, or empty for {@code warn}
   */
  public Optional<DelayedPolicy> onDelayed() {
    return Optional.ofNullable(onDelayed);
  }

  /**
   * Returns whether the subscription gets its own feed and session.
   *
   * @return the flag, or empty when unset (shared {@code //blp/mktdata} feeds)
   */
  public Optional<Boolean> isolated() {
    return Optional.ofNullable(isolated);
  }

  /**
   * Returns whether rows are delivered; {@code false} makes an image-only board read through {@link
   * Subscription#latest()}.
   *
   * @return the flag, or empty when unset (rows delivered)
   */
  public Optional<Boolean> rows() {
    return Optional.ofNullable(rows);
  }

  /**
   * Returns the rejected-field policy.
   *
   * @return the policy, or empty for {@code warn}
   */
  public Optional<FieldErrorPolicy> onFieldError() {
    return Optional.ofNullable(onFieldError);
  }

  /**
   * Returns the fields whose numeric zero becomes null in {@code latest()} and state deliveries.
   *
   * @return the fields, or empty when unset
   */
  public Optional<List<String>> zeroAsNull() {
    return Optional.ofNullable(zeroAsNull);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof StreamOptions)) {
      return false;
    }
    StreamOptions that = (StreamOptions) other;
    return delivery == that.delivery
        && Objects.equals(options, that.options)
        && Objects.equals(conflate, that.conflate)
        && Objects.equals(flushThreshold, that.flushThreshold)
        && overflowPolicy == that.overflowPolicy
        && Objects.equals(streamCapacity, that.streamCapacity)
        && Objects.equals(allFields, that.allFields)
        && Objects.equals(fields, that.fields)
        && Objects.equals(aliases, that.aliases)
        && onDelayed == that.onDelayed
        && Objects.equals(isolated, that.isolated)
        && Objects.equals(rows, that.rows)
        && onFieldError == that.onFieldError
        && Objects.equals(zeroAsNull, that.zeroAsNull);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        delivery,
        options,
        conflate,
        flushThreshold,
        overflowPolicy,
        streamCapacity,
        allFields,
        fields,
        aliases,
        onDelayed,
        isolated,
        rows,
        onFieldError,
        zeroAsNull);
  }

  @Override
  public String toString() {
    StringBuilder out = new StringBuilder("StreamOptions{");
    append(out, "delivery", delivery);
    append(out, "options", options);
    append(out, "conflate", conflate);
    append(out, "flushThreshold", flushThreshold);
    append(out, "overflowPolicy", overflowPolicy);
    append(out, "streamCapacity", streamCapacity);
    append(out, "allFields", allFields);
    append(out, "fields", fields);
    append(out, "aliases", aliases);
    append(out, "onDelayed", onDelayed);
    append(out, "isolated", isolated);
    append(out, "rows", rows);
    append(out, "onFieldError", onFieldError);
    append(out, "zeroAsNull", zeroAsNull);
    return out.append('}').toString();
  }

  static void append(StringBuilder out, String name, Object value) {
    if (value == null) {
      return;
    }
    if (out.charAt(out.length() - 1) != '{') {
      out.append(", ");
    }
    out.append(name).append('=').append(value);
  }

  static List<String> copyList(Collection<String> values) {
    return values == null ? null : Collections.unmodifiableList(new ArrayList<String>(values));
  }

  static Map<String, String> copyAliases(Map<String, String> aliases) {
    if (aliases == null) {
      return null;
    }
    Map<String, String> copy = new LinkedHashMap<String, String>();
    for (Map.Entry<String, String> entry : aliases.entrySet()) {
      if (entry.getKey() == null || entry.getValue() == null) {
        throw new BlpValidationException(
            "aliases must map topic strings to label strings", "aliases");
      }
      copy.put(entry.getKey(), entry.getValue());
    }
    return Collections.unmodifiableMap(copy);
  }

  static List<String> copyZeroAsNull(Collection<String> fields) {
    if (fields == null) {
      return null;
    }
    for (String field : fields) {
      if (field == null) {
        throw new BlpValidationException(
            "zeroAsNull must be an array of field names", "zeroAsNull");
      }
    }
    return copyList(fields);
  }

  /** Builds {@link StreamOptions}. Passing {@code null} to an object setter unsets the option. */
  public static final class Builder {
    private SubscriptionDeliveryMode delivery;
    private List<String> options;
    private Boolean conflate;
    private Integer flushThreshold;
    private OverflowPolicy overflowPolicy;
    private Integer streamCapacity;
    private Boolean allFields;
    private List<String> fields;
    private Map<String, String> aliases;
    private DelayedPolicy onDelayed;
    private Boolean isolated;
    private Boolean rows;
    private FieldErrorPolicy onFieldError;
    private List<String> zeroAsNull;

    private Builder() {}

    /**
     * Sets the delivery mode. {@code state} cannot be combined with {@code rows=false}, an overflow
     * policy or a stream capacity.
     *
     * @param delivery the mode
     * @return this builder
     */
    public Builder delivery(SubscriptionDeliveryMode delivery) {
      this.delivery = delivery;
      return this;
    }

    /**
     * Sets Bloomberg subscription options. Named entry points trim each option, strip leading
     * {@code &} and drop empty ones; {@code subscribeWithOptions} sends them unchanged.
     *
     * @param options the options, such as {@code interval=5}
     * @return this builder
     */
    public Builder options(Collection<String> options) {
      this.options = copyList(options);
      return this;
    }

    /**
     * Requests Bloomberg upstream conflation ({@code //blp/mktdata} only, not with an {@code
     * interval} option).
     *
     * @param conflate whether to conflate
     * @return this builder
     */
    public Builder conflate(boolean conflate) {
      this.conflate = conflate;
      return this;
    }

    /**
     * Sets the maximum number of already-queued updates per read (must be positive).
     *
     * @param flushThreshold the threshold
     * @return this builder
     */
    public Builder flushThreshold(int flushThreshold) {
      this.flushThreshold = flushThreshold;
      return this;
    }

    /**
     * Sets the overflow policy of this consumer's queue.
     *
     * @param overflowPolicy the policy
     * @return this builder
     */
    public Builder overflowPolicy(OverflowPolicy overflowPolicy) {
      this.overflowPolicy = overflowPolicy;
      return this;
    }

    /**
     * Sets this consumer's queue capacity (must be positive).
     *
     * @param streamCapacity the capacity
     * @return this builder
     */
    public Builder streamCapacity(int streamCapacity) {
      this.streamCapacity = streamCapacity;
      return this;
    }

    /**
     * Exposes every top-level scalar field of the shared feed.
     *
     * @param allFields whether to expose all fields
     * @return this builder
     */
    public Builder allFields(boolean allFields) {
      this.allFields = allFields;
      return this;
    }

    /**
     * Sets the fields for {@code mktbar}, {@code depth}, {@code chains}, {@code subscribeAuction}
     * and {@code subscribeEtfInav}.
     *
     * @param fields the fields
     * @return this builder
     */
    public Builder fields(Collection<String> fields) {
      this.fields = copyList(fields);
      return this;
    }

    /**
     * Sets the upstream-topic to consumer-label aliases.
     *
     * @param aliases the aliases, copied in iteration order
     * @return this builder
     * @throws BlpValidationException if a key or value is null
     */
    public Builder aliases(Map<String, String> aliases) {
      this.aliases = copyAliases(aliases);
      return this;
    }

    /**
     * Sets the delayed-data policy.
     *
     * @param onDelayed the policy
     * @return this builder
     */
    public Builder onDelayed(DelayedPolicy onDelayed) {
      this.onDelayed = onDelayed;
      return this;
    }

    /**
     * Gives the subscription its own feed and session.
     *
     * @param isolated whether to isolate
     * @return this builder
     */
    public Builder isolated(boolean isolated) {
      this.isolated = isolated;
      return this;
    }

    /**
     * Chooses whether rows are delivered ({@code false}: image-only, read with {@code latest()}).
     *
     * @param rows whether to deliver rows
     * @return this builder
     */
    public Builder rows(boolean rows) {
      this.rows = rows;
      return this;
    }

    /**
     * Sets the rejected-field policy.
     *
     * @param onFieldError the policy
     * @return this builder
     */
    public Builder onFieldError(FieldErrorPolicy onFieldError) {
      this.onFieldError = onFieldError;
      return this;
    }

    /**
     * Sets the fields whose numeric zero becomes null in {@code latest()} and state deliveries.
     *
     * @param zeroAsNull the fields
     * @return this builder
     * @throws BlpValidationException if a field is null
     */
    public Builder zeroAsNull(Collection<String> zeroAsNull) {
      this.zeroAsNull = copyZeroAsNull(zeroAsNull);
      return this;
    }

    /**
     * Builds the options.
     *
     * @return immutable options
     */
    public StreamOptions build() {
      return new StreamOptions(this);
    }
  }
}
