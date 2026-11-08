package io.github.underloam.xbbg.ext;

import io.github.underloam.xbbg.internal.JsonFields;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A {@code start}/{@code end} pair of time or datetime strings, as used by session windows ({@code
 * "HH:MM"}) and default ranges ({@code "YYYY-MM-DD"} or {@code "YYYY-MM-DDTHH:MM:SS"}).
 */
public final class TimeRange {
  private final String start;
  private final String end;

  /**
   * Creates a range.
   *
   * @param start the start
   * @param end the end
   * @throws NullPointerException if either value is null
   */
  public TimeRange(String start, String end) {
    this.start = Objects.requireNonNull(start, "start");
    this.end = Objects.requireNonNull(end, "end");
  }

  /**
   * Returns the start.
   *
   * @return the start text
   */
  public String start() {
    return start;
  }

  /**
   * Returns the end.
   *
   * @return the end text
   */
  public String end() {
    return end;
  }

  static TimeRange fromJson(Map<String, Object> object) {
    return new TimeRange(JsonFields.string(object, "start"), JsonFields.string(object, "end"));
  }

  static TimeRange optional(Map<String, Object> object, String key) {
    Map<String, Object> range = JsonFields.optObj(object, key);
    return range == null ? null : fromJson(range);
  }

  Map<String, Object> toJson() {
    Map<String, Object> object = new LinkedHashMap<String, Object>();
    object.put("start", start);
    object.put("end", end);
    return object;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof TimeRange)) {
      return false;
    }
    TimeRange that = (TimeRange) other;
    return start.equals(that.start) && end.equals(that.end);
  }

  @Override
  public int hashCode() {
    return 31 * start.hashCode() + end.hashCode();
  }

  @Override
  public String toString() {
    return "TimeRange{start=" + start + ", end=" + end + "}";
  }
}
