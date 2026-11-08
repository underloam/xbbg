package io.github.underloam.xbbg.ext;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Trading session windows as {@code "HH:MM"} ranges in exchange-local time, from {@link
 * Ext#deriveSessions(String, String, String, String)}. A window the market rule does not define is
 * empty.
 */
public final class SessionWindows {
  private final TimeRange day;
  private final TimeRange allday;
  private final TimeRange pre;
  private final TimeRange post;
  private final TimeRange am;
  private final TimeRange pm;

  /**
   * Creates a value; each window may be {@code null}.
   *
   * @param day the regular session
   * @param allday the full span from the pre-market start to the post-market end
   * @param pre the pre-market session
   * @param post the post-market session
   * @param am the morning session of a market with a lunch break
   * @param pm the afternoon session of a market with a lunch break
   */
  public SessionWindows(
      TimeRange day, TimeRange allday, TimeRange pre, TimeRange post, TimeRange am, TimeRange pm) {
    this.day = day;
    this.allday = allday;
    this.pre = pre;
    this.post = post;
    this.am = am;
    this.pm = pm;
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
   * Returns the full span from the pre-market start to the post-market end.
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
   * Returns the morning session of a market with a lunch break.
   *
   * @return the window, or empty
   */
  public Optional<TimeRange> am() {
    return Optional.ofNullable(am);
  }

  /**
   * Returns the afternoon session of a market with a lunch break.
   *
   * @return the window, or empty
   */
  public Optional<TimeRange> pm() {
    return Optional.ofNullable(pm);
  }

  static SessionWindows fromJson(Map<String, Object> object) {
    return new SessionWindows(
        TimeRange.optional(object, "day"),
        TimeRange.optional(object, "allday"),
        TimeRange.optional(object, "pre"),
        TimeRange.optional(object, "post"),
        TimeRange.optional(object, "am"),
        TimeRange.optional(object, "pm"));
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof SessionWindows)) {
      return false;
    }
    SessionWindows that = (SessionWindows) other;
    return Objects.equals(day, that.day)
        && Objects.equals(allday, that.allday)
        && Objects.equals(pre, that.pre)
        && Objects.equals(post, that.post)
        && Objects.equals(am, that.am)
        && Objects.equals(pm, that.pm);
  }

  @Override
  public int hashCode() {
    return Objects.hash(day, allday, pre, post, am, pm);
  }

  @Override
  public String toString() {
    return "SessionWindows{day="
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
}
