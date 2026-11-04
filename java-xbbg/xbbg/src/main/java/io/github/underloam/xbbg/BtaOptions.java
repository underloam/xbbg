package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for {@code bta} technical-analysis studies (TS {@code BtaOptions}). {@code kwargs}
 * entries named {@code startDate}, {@code start_date}, {@code endDate}, {@code end_date}, {@code
 * periodicitySelection}, {@code periodicity} and {@code interval} configure the data range (and
 * beat the typed options); string date entries are formatted like {@link Dates#formatDate(String)};
 * every other entry is sent as a raw request element.
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent, so the engine default applies. Used by {@link Engine#bta(String, String, BtaOptions)}.
 */
public final class BtaOptions {
  private static final BtaOptions DEFAULTS = builder().build();

  private final Map<String, Object> studyParams;
  private final Map<String, Object> kwargs;
  private final String startDate;
  private final String endDate;
  private final String periodicity;
  private final Number interval;
  private final Format format;

  private BtaOptions(Builder builder) {
    this.studyParams = builder.studyParams;
    this.kwargs = RequestArgs.formatDateKwargs(builder.kwargs);
    this.startDate = builder.startDate;
    this.endDate = builder.endDate;
    this.periodicity = builder.periodicity;
    this.interval = builder.interval;
    this.format = builder.format;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static BtaOptions defaults() {
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
   * Returns {@code studyParams}: study attributes merged over the study's defaults; a {@code null}
   * value removes a default, and {@code length} is an alias of {@code period}.
   *
   * @return the value, or empty when unset
   */
  public Optional<Map<String, Object>> studyParams() {
    return Optional.ofNullable(studyParams);
  }

  /**
   * Returns {@code kwargs}: data-range settings and raw request elements (see the class
   * description).
   *
   * @return the value, or empty when unset
   */
  public Optional<Map<String, Object>> kwargs() {
    return Optional.ofNullable(kwargs);
  }

  /**
   * Returns {@code startDate}: the first date as {@code YYYYMMDD}.
   *
   * @return the value, or empty when unset
   */
  public Optional<String> startDate() {
    return Optional.ofNullable(startDate);
  }

  /**
   * Returns {@code endDate}: the last date as {@code YYYYMMDD}.
   *
   * @return the value, or empty when unset
   */
  public Optional<String> endDate() {
    return Optional.ofNullable(endDate);
  }

  /**
   * Returns {@code periodicity}: the data periodicity, upper-cased; {@code INTRADAY} selects an
   * intraday range; unset means {@code DAILY}.
   *
   * @return the value, or empty when unset
   */
  public Optional<String> periodicity() {
    return Optional.ofNullable(periodicity);
  }

  /**
   * Returns {@code interval}: the intraday bar size in minutes (used only with {@code INTRADAY}
   * periodicity).
   *
   * @return the value, or empty when unset
   */
  public Optional<Number> interval() {
    return Optional.ofNullable(interval);
  }

  /**
   * Returns {@code format}: the result layout; unset uses the engine default ({@link Format#LONG}).
   *
   * @return the value, or empty when unset
   */
  public Optional<Format> format() {
    return Optional.ofNullable(format);
  }

  /** Writes the set options as request argument fields. */
  void encodeInto(JsonObjectBuilder args) {
    if (studyParams != null) {
      args.put("studyParams", RequestArgs.paramsJson(studyParams));
    }
    args.put("kwargs", kwargs);
    args.put("startDate", startDate);
    args.put("endDate", endDate);
    args.put("periodicity", periodicity);
    args.put("interval", interval);
    if (format != null) {
      args.put("format", format.wireValue());
    }
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof BtaOptions)) {
      return false;
    }
    BtaOptions other = (BtaOptions) object;
    return RequestArgs.orderedEquals(studyParams, other.studyParams)
        && RequestArgs.orderedEquals(kwargs, other.kwargs)
        && Objects.equals(startDate, other.startDate)
        && Objects.equals(endDate, other.endDate)
        && Objects.equals(periodicity, other.periodicity)
        && Objects.equals(interval, other.interval)
        && Objects.equals(format, other.format);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        RequestArgs.orderedHash(studyParams),
        RequestArgs.orderedHash(kwargs),
        startDate,
        endDate,
        periodicity,
        interval,
        format);
  }

  @Override
  public String toString() {
    return RequestArgs.describe(
        "BtaOptions",
        "studyParams",
        studyParams,
        "kwargs",
        kwargs,
        "startDate",
        startDate,
        "endDate",
        endDate,
        "periodicity",
        periodicity,
        "interval",
        interval,
        "format",
        format);
  }

  /** Builds {@link BtaOptions}; not thread-safe. */
  public static final class Builder {
    private Map<String, Object> studyParams;
    private Map<String, Object> kwargs;
    private String startDate;
    private String endDate;
    private String periodicity;
    private Number interval;
    private Format format;

    private Builder() {}

    private Builder(BtaOptions options) {
      this.studyParams = options.studyParams;
      this.kwargs = options.kwargs;
      this.startDate = options.startDate;
      this.endDate = options.endDate;
      this.periodicity = options.periodicity;
      this.interval = options.interval;
      this.format = options.format;
    }

    /**
     * Replaces {@code studyParams}: study attributes merged over the study's defaults; a {@code
     * null} value removes a default, and {@code length} is an alias of {@code period}.
     *
     * <p>Keys keep the map's iteration order; use a {@link java.util.LinkedHashMap} to control it.
     *
     * @param studyParams the entries, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if a value is not a string, number, boolean or {@code null}
     */
    public Builder studyParams(Map<String, ?> studyParams) {
      this.studyParams =
          studyParams == null ? null : RequestArgs.scalars("studyParams", studyParams, true);
      return this;
    }

    /**
     * Adds or replaces one {@code studyParams} entry, keeping the position of an existing key.
     *
     * @param key the entry name
     * @param value a string, number or boolean, or {@code null} to remove the default
     * @return this builder
     * @throws IllegalArgumentException if the value is not a string, number, boolean or {@code
     *     null}
     */
    public Builder studyParam(String key, Object value) {
      this.studyParams = RequestArgs.withScalar("studyParams", this.studyParams, key, value, true);
      return this;
    }

    /**
     * Replaces {@code kwargs}: data-range settings and raw request elements (see the class
     * description).
     *
     * <p>Keys keep the map's iteration order; use a {@link java.util.LinkedHashMap} to control it.
     *
     * @param kwargs the entries, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if a value is not a string, number or boolean
     */
    public Builder kwargs(Map<String, ?> kwargs) {
      this.kwargs = kwargs == null ? null : RequestArgs.scalars("kwargs", kwargs, false);
      return this;
    }

    /**
     * Adds or replaces one {@code kwargs} entry, keeping the position of an existing key.
     *
     * @param key the entry name
     * @param value a string, number or boolean
     * @return this builder
     * @throws IllegalArgumentException if the value is not a string, number or boolean
     */
    public Builder kwarg(String key, Object value) {
      this.kwargs = RequestArgs.withScalar("kwargs", this.kwargs, key, value, false);
      return this;
    }

    /**
     * Sets {@code startDate} from text: the first date as {@code YYYYMMDD}. The text is formatted
     * with {@link Dates#formatDate(String)}; blank text unsets it.
     *
     * @param startDate ISO 8601 or Bloomberg date text, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the text is ambiguous or not a date
     */
    public Builder startDate(String startDate) {
      this.startDate = startDate == null ? null : Dates.formatDate(startDate);
      return this;
    }

    /**
     * Sets {@code startDate} from a {@code java.time} value: the first date as {@code YYYYMMDD}.
     * The value is formatted with {@link Dates#formatDate(TemporalAccessor)}.
     *
     * @param startDate the date, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the value carries no date
     */
    public Builder startDate(TemporalAccessor startDate) {
      this.startDate = startDate == null ? null : Dates.formatDate(startDate);
      return this;
    }

    /**
     * Sets {@code endDate} from text: the last date as {@code YYYYMMDD}. The text is formatted with
     * {@link Dates#formatDate(String)}; blank text unsets it.
     *
     * @param endDate ISO 8601 or Bloomberg date text, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the text is ambiguous or not a date
     */
    public Builder endDate(String endDate) {
      this.endDate = endDate == null ? null : Dates.formatDate(endDate);
      return this;
    }

    /**
     * Sets {@code endDate} from a {@code java.time} value: the last date as {@code YYYYMMDD}. The
     * value is formatted with {@link Dates#formatDate(TemporalAccessor)}.
     *
     * @param endDate the date, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the value carries no date
     */
    public Builder endDate(TemporalAccessor endDate) {
      this.endDate = endDate == null ? null : Dates.formatDate(endDate);
      return this;
    }

    /**
     * Sets {@code periodicity}: the data periodicity, upper-cased; {@code INTRADAY} selects an
     * intraday range; unset means {@code DAILY}.
     *
     * @param periodicity the value, or {@code null} to unset
     * @return this builder
     */
    public Builder periodicity(String periodicity) {
      this.periodicity = periodicity;
      return this;
    }

    /**
     * Sets {@code interval}: the intraday bar size in minutes (used only with {@code INTRADAY}
     * periodicity).
     *
     * @param interval the value, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the value is not finite
     */
    public Builder interval(Number interval) {
      this.interval =
          interval == null ? null : (Number) RequestArgs.scalar("interval", interval, false);
      return this;
    }

    /**
     * Sets the format: the result layout; unset uses the engine default ({@link Format#LONG}).
     *
     * @param format the format, or {@code null} to unset
     * @return this builder
     */
    public Builder format(Format format) {
      this.format = format;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return immutable options
     */
    public BtaOptions build() {
      return new BtaOptions(this);
    }
  }
}
