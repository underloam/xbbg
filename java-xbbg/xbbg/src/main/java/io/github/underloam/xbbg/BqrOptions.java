package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for the {@code bqr} dealer-quote recipe (TS {@code BqrOptions}).
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent, so the recipe default applies. Used by {@link Engine#bqr(String, BqrOptions)}.
 */
public final class BqrOptions {
  private static final BqrOptions DEFAULTS = builder().build();

  private final String startDatetime;
  private final String endDatetime;
  private final List<String> eventTypes;
  private final Boolean includeBrokerCodes;

  private BqrOptions(Builder builder) {
    this.startDatetime = builder.startDatetime;
    this.endDatetime = builder.endDatetime;
    this.eventTypes = builder.eventTypes;
    this.includeBrokerCodes = builder.includeBrokerCodes;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static BqrOptions defaults() {
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
   * Returns {@code startDatetime}: the window start; unset means one hour before the end.
   *
   * @return the value, or empty when unset
   */
  public Optional<String> startDatetime() {
    return Optional.ofNullable(startDatetime);
  }

  /**
   * Returns {@code endDatetime}: the window end; unset means now (a UTC instant).
   *
   * @return the value, or empty when unset
   */
  public Optional<String> endDatetime() {
    return Optional.ofNullable(endDatetime);
  }

  /**
   * Returns {@code eventTypes}: the tick event types; unset means {@code BID} and {@code ASK}.
   *
   * @return the value, or empty when unset
   */
  public Optional<List<String>> eventTypes() {
    return Optional.ofNullable(eventTypes);
  }

  /**
   * Returns {@code includeBrokerCodes}: request broker codes and fail on non-empty quotes without
   * them; unset means {@code true}.
   *
   * @return the value, or empty when unset
   */
  public Optional<Boolean> includeBrokerCodes() {
    return Optional.ofNullable(includeBrokerCodes);
  }

  /** Writes the set options as recipe argument fields. */
  void encodeInto(JsonObjectBuilder args) {
    args.put("startDatetime", startDatetime);
    args.put("endDatetime", endDatetime);
    args.put("eventTypes", eventTypes);
    args.put("includeBrokerCodes", includeBrokerCodes);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof BqrOptions)) {
      return false;
    }
    BqrOptions other = (BqrOptions) object;
    return Objects.equals(startDatetime, other.startDatetime)
        && Objects.equals(endDatetime, other.endDatetime)
        && Objects.equals(eventTypes, other.eventTypes)
        && Objects.equals(includeBrokerCodes, other.includeBrokerCodes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(startDatetime, endDatetime, eventTypes, includeBrokerCodes);
  }

  @Override
  public String toString() {
    return RequestArgs.describe(
        "BqrOptions",
        "startDatetime",
        startDatetime,
        "endDatetime",
        endDatetime,
        "eventTypes",
        eventTypes,
        "includeBrokerCodes",
        includeBrokerCodes);
  }

  /** Builds {@link BqrOptions}; not thread-safe. */
  public static final class Builder {
    private String startDatetime;
    private String endDatetime;
    private List<String> eventTypes;
    private Boolean includeBrokerCodes;

    private Builder() {}

    private Builder(BqrOptions options) {
      this.startDatetime = options.startDatetime;
      this.endDatetime = options.endDatetime;
      this.eventTypes = options.eventTypes;
      this.includeBrokerCodes = options.includeBrokerCodes;
    }

    /**
     * Sets {@code startDatetime} from text, formatted with {@link Dates#formatDateTime(String)}
     * (naive text stays naive and is read as UTC); blank text unsets it.
     *
     * @param startDatetime ISO 8601 or Bloomberg date text, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the text is ambiguous
     */
    public Builder startDatetime(String startDatetime) {
      this.startDatetime = startDatetime == null ? null : Dates.formatDateTime(startDatetime);
      return this;
    }

    /**
     * Sets {@code startDatetime} from a {@code java.time} value, formatted with {@link
     * Dates#formatDateTime(TemporalAccessor)}.
     *
     * @param startDatetime the datetime, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the value carries no date
     */
    public Builder startDatetime(TemporalAccessor startDatetime) {
      this.startDatetime = startDatetime == null ? null : Dates.formatDateTime(startDatetime);
      return this;
    }

    /**
     * Sets {@code endDatetime} from text, formatted with {@link Dates#formatDateTime(String)};
     * blank text unsets it.
     *
     * @param endDatetime ISO 8601 or Bloomberg date text, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the text is ambiguous
     */
    public Builder endDatetime(String endDatetime) {
      this.endDatetime = endDatetime == null ? null : Dates.formatDateTime(endDatetime);
      return this;
    }

    /**
     * Sets {@code endDatetime} from a {@code java.time} value, formatted with {@link
     * Dates#formatDateTime(TemporalAccessor)}.
     *
     * @param endDatetime the datetime, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the value carries no date
     */
    public Builder endDatetime(TemporalAccessor endDatetime) {
      this.endDatetime = endDatetime == null ? null : Dates.formatDateTime(endDatetime);
      return this;
    }

    /**
     * Sets {@code eventTypes}: the tick event types, for example {@code TRADE}, {@code BID}, {@code
     * ASK}, {@code BID_BEST}.
     *
     * @param eventTypes the event types, or {@code null} to unset
     * @return this builder
     * @throws NullPointerException if an item is null
     */
    public Builder eventTypes(Collection<String> eventTypes) {
      this.eventTypes = eventTypes == null ? null : RequestArgs.strings("eventTypes", eventTypes);
      return this;
    }

    /**
     * Sets {@code includeBrokerCodes}: whether to request broker codes and require them on
     * non-empty results.
     *
     * @param includeBrokerCodes the flag, or {@code null} to unset ({@code true})
     * @return this builder
     */
    public Builder includeBrokerCodes(Boolean includeBrokerCodes) {
      this.includeBrokerCodes = includeBrokerCodes;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return immutable options
     */
    public BqrOptions build() {
      return new BqrOptions(this);
    }
  }
}
