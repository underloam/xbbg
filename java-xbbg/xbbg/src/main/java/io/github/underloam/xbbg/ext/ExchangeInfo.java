package io.github.underloam.xbbg.ext;

import io.github.underloam.xbbg.internal.JsonFields;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;

/**
 * Exchange metadata for a ticker, as materialized from a runtime override by {@link
 * Ext#getExchangeOverride(String)} and {@link Ext#listExchangeOverrides()}. Fields the override
 * does not set keep the fallback values ({@code UTC}, no sessions).
 */
public final class ExchangeInfo {
  private final String ticker;
  private final String mic;
  private final String exchCode;
  private final String timezone;
  private final Double utcOffset;
  private final String source;
  private final SessionWindows sessions;

  /**
   * Creates a value.
   *
   * @param ticker the ticker
   * @param mic the market identifier code, or {@code null}
   * @param exchCode the Bloomberg exchange code, or {@code null}
   * @param timezone the IANA timezone
   * @param utcOffset the UTC offset in hours, or {@code null}
   * @param source where the metadata came from ({@code "override"}, {@code "cache"}, …)
   * @param sessions the session windows
   * @throws NullPointerException if {@code ticker}, {@code timezone}, {@code source} or {@code
   *     sessions} is null
   */
  public ExchangeInfo(
      String ticker,
      String mic,
      String exchCode,
      String timezone,
      Double utcOffset,
      String source,
      SessionWindows sessions) {
    this.ticker = Objects.requireNonNull(ticker, "ticker");
    this.mic = mic;
    this.exchCode = exchCode;
    this.timezone = Objects.requireNonNull(timezone, "timezone");
    this.utcOffset = utcOffset;
    this.source = Objects.requireNonNull(source, "source");
    this.sessions = Objects.requireNonNull(sessions, "sessions");
  }

  /**
   * Returns the ticker.
   *
   * @return the ticker
   */
  public String ticker() {
    return ticker;
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
   * Returns the IANA timezone.
   *
   * @return the timezone, {@code "UTC"} unless set
   */
  public String timezone() {
    return timezone;
  }

  /**
   * Returns the UTC offset.
   *
   * @return the offset in hours, or empty
   */
  public OptionalDouble utcOffset() {
    return utcOffset == null ? OptionalDouble.empty() : OptionalDouble.of(utcOffset.doubleValue());
  }

  /**
   * Returns where the metadata came from.
   *
   * @return {@code "override"} for runtime overrides
   */
  public String source() {
    return source;
  }

  /**
   * Returns all session windows.
   *
   * @return the sessions
   */
  public SessionWindows sessions() {
    return sessions;
  }

  /**
   * Returns the regular session.
   *
   * @return the window, or empty
   */
  public Optional<TimeRange> day() {
    return sessions.day();
  }

  /**
   * Returns the full span from the pre-market start to the post-market end.
   *
   * @return the window, or empty
   */
  public Optional<TimeRange> allday() {
    return sessions.allday();
  }

  /**
   * Returns the pre-market session.
   *
   * @return the window, or empty
   */
  public Optional<TimeRange> pre() {
    return sessions.pre();
  }

  /**
   * Returns the post-market session.
   *
   * @return the window, or empty
   */
  public Optional<TimeRange> post() {
    return sessions.post();
  }

  /**
   * Returns the morning session.
   *
   * @return the window, or empty
   */
  public Optional<TimeRange> am() {
    return sessions.am();
  }

  /**
   * Returns the afternoon session.
   *
   * @return the window, or empty
   */
  public Optional<TimeRange> pm() {
    return sessions.pm();
  }

  static ExchangeInfo fromJson(Map<String, Object> object) {
    return new ExchangeInfo(
        JsonFields.string(object, "ticker"),
        JsonFields.optString(object, "mic"),
        JsonFields.optString(object, "exchCode"),
        JsonFields.string(object, "timezone"),
        JsonFields.optDouble(object, "utcOffset"),
        JsonFields.string(object, "source"),
        SessionWindows.fromJson(object));
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ExchangeInfo)) {
      return false;
    }
    ExchangeInfo that = (ExchangeInfo) other;
    return ticker.equals(that.ticker)
        && Objects.equals(mic, that.mic)
        && Objects.equals(exchCode, that.exchCode)
        && timezone.equals(that.timezone)
        && Objects.equals(utcOffset, that.utcOffset)
        && source.equals(that.source)
        && sessions.equals(that.sessions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ticker, mic, exchCode, timezone, utcOffset, source, sessions);
  }

  @Override
  public String toString() {
    return "ExchangeInfo{ticker="
        + ticker
        + ", mic="
        + mic
        + ", exchCode="
        + exchCode
        + ", timezone="
        + timezone
        + ", utcOffset="
        + utcOffset
        + ", source="
        + source
        + ", sessions="
        + sessions
        + "}";
  }
}
