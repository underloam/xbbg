package io.github.underloam.xbbg.ext;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A runtime exchange override patch for {@link Ext#setExchangeOverride(String, ExchangeOverride)}.
 *
 * <p>Every member is optional, but a patch must set at least one. Setting a patch again merges it:
 * set members replace earlier values, and giving any session window replaces all six windows.
 */
public final class ExchangeOverride {
  private final String timezone;
  private final String mic;
  private final String exchCode;
  private final TimeRange day;
  private final TimeRange allday;
  private final TimeRange pre;
  private final TimeRange post;
  private final TimeRange am;
  private final TimeRange pm;

  private ExchangeOverride(Builder builder) {
    this.timezone = builder.timezone;
    this.mic = builder.mic;
    this.exchCode = builder.exchCode;
    this.day = builder.day;
    this.allday = builder.allday;
    this.pre = builder.pre;
    this.post = builder.post;
    this.am = builder.am;
    this.pm = builder.pm;
  }

  /**
   * Starts an empty patch.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the IANA timezone.
   *
   * @return the timezone, or empty
   */
  public Optional<String> timezone() {
    return Optional.ofNullable(timezone);
  }

  /**
   * Returns the market identifier code.
   *
   * @return the MIC, or empty
   */
  public Optional<String> mic() {
    return Optional.ofNullable(mic);
  }

  /**
   * Returns the Bloomberg exchange code.
   *
   * @return the exchange code, or empty
   */
  public Optional<String> exchCode() {
    return Optional.ofNullable(exchCode);
  }

  /**
   * Returns the regular session.
   *
   * @return the window, or empty
   */
  public Optional<TimeRange> day() {
    return Optional.ofNullable(day);
  }

  /**
   * Returns the full-day span.
   *
   * @return the window, or empty
   */
  public Optional<TimeRange> allday() {
    return Optional.ofNullable(allday);
  }

  /**
   * Returns the pre-market session.
   *
   * @return the window, or empty
   */
  public Optional<TimeRange> pre() {
    return Optional.ofNullable(pre);
  }

  /**
   * Returns the post-market session.
   *
   * @return the window, or empty
   */
  public Optional<TimeRange> post() {
    return Optional.ofNullable(post);
  }

  /**
   * Returns the morning session.
   *
   * @return the window, or empty
   */
  public Optional<TimeRange> am() {
    return Optional.ofNullable(am);
  }

  /**
   * Returns the afternoon session.
   *
   * @return the window, or empty
   */
  public Optional<TimeRange> pm() {
    return Optional.ofNullable(pm);
  }

  Map<String, Object> toJson() {
    Map<String, Object> object = new LinkedHashMap<String, Object>();
    object.put("timezone", timezone);
    object.put("mic", mic);
    object.put("exchCode", exchCode);
    object.put("day", day == null ? null : day.toJson());
    object.put("allday", allday == null ? null : allday.toJson());
    object.put("pre", pre == null ? null : pre.toJson());
    object.put("post", post == null ? null : post.toJson());
    object.put("am", am == null ? null : am.toJson());
    object.put("pm", pm == null ? null : pm.toJson());
    return object;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ExchangeOverride)) {
      return false;
    }
    ExchangeOverride that = (ExchangeOverride) other;
    return Objects.equals(timezone, that.timezone)
        && Objects.equals(mic, that.mic)
        && Objects.equals(exchCode, that.exchCode)
        && Objects.equals(day, that.day)
        && Objects.equals(allday, that.allday)
        && Objects.equals(pre, that.pre)
        && Objects.equals(post, that.post)
        && Objects.equals(am, that.am)
        && Objects.equals(pm, that.pm);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timezone, mic, exchCode, day, allday, pre, post, am, pm);
  }

  @Override
  public String toString() {
    return "ExchangeOverride{timezone="
        + timezone
        + ", mic="
        + mic
        + ", exchCode="
        + exchCode
        + ", day="
        + day
        + ", allday="
        + allday
        + ", pre="
        + pre
        + ", post="
        + post
        + ", am="
        + am
        + ", pm="
        + pm
        + "}";
  }

  /** Builds an {@link ExchangeOverride}; every setter accepts {@code null} to unset a member. */
  public static final class Builder {
    private String timezone;
    private String mic;
    private String exchCode;
    private TimeRange day;
    private TimeRange allday;
    private TimeRange pre;
    private TimeRange post;
    private TimeRange am;
    private TimeRange pm;

    private Builder() {}

    /**
     * Sets the IANA timezone.
     *
     * @param timezone the timezone, for example {@code "Europe/London"}
     * @return this builder
     */
    public Builder timezone(String timezone) {
      this.timezone = timezone;
      return this;
    }

    /**
     * Sets the market identifier code.
     *
     * @param mic the MIC, for example {@code "XLON"}
     * @return this builder
     */
    public Builder mic(String mic) {
      this.mic = mic;
      return this;
    }

    /**
     * Sets the Bloomberg exchange code.
     *
     * @param exchCode the exchange code, for example {@code "LN"}
     * @return this builder
     */
    public Builder exchCode(String exchCode) {
      this.exchCode = exchCode;
      return this;
    }

    /**
     * Sets the regular session.
     *
     * @param day the {@code "HH:MM"} window
     * @return this builder
     */
    public Builder day(TimeRange day) {
      this.day = day;
      return this;
    }

    /**
     * Sets the full-day span.
     *
     * @param allday the {@code "HH:MM"} window
     * @return this builder
     */
    public Builder allday(TimeRange allday) {
      this.allday = allday;
      return this;
    }

    /**
     * Sets the pre-market session.
     *
     * @param pre the {@code "HH:MM"} window
     * @return this builder
     */
    public Builder pre(TimeRange pre) {
      this.pre = pre;
      return this;
    }

    /**
     * Sets the post-market session.
     *
     * @param post the {@code "HH:MM"} window
     * @return this builder
     */
    public Builder post(TimeRange post) {
      this.post = post;
      return this;
    }

    /**
     * Sets the morning session.
     *
     * @param am the {@code "HH:MM"} window
     * @return this builder
     */
    public Builder am(TimeRange am) {
      this.am = am;
      return this;
    }

    /**
     * Sets the afternoon session.
     *
     * @param pm the {@code "HH:MM"} window
     * @return this builder
     */
    public Builder pm(TimeRange pm) {
      this.pm = pm;
      return this;
    }

    /**
     * Builds the patch.
     *
     * @return the immutable patch
     */
    public ExchangeOverride build() {
      return new ExchangeOverride(this);
    }
  }
}
