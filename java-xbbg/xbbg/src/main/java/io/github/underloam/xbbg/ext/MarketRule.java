package io.github.underloam.xbbg.ext;

import io.github.underloam.xbbg.internal.JsonFields;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;

/**
 * How a market's sessions derive from its regular trading hours, from {@link
 * Ext#getMarketRule(String, String)}.
 */
public final class MarketRule {
  private final int preMinutes;
  private final int postMinutes;
  private final Integer lunchStartMin;
  private final Integer lunchEndMin;
  private final boolean isContinuous;

  /**
   * Creates a value.
   *
   * @param preMinutes minutes of pre-market trading before the open
   * @param postMinutes minutes of post-market trading after the close
   * @param lunchStartMin minutes after the open when the lunch break starts, or {@code null}
   * @param lunchEndMin minutes after the open when the lunch break ends, or {@code null}
   * @param isContinuous whether the market trades around the clock
   */
  public MarketRule(
      int preMinutes,
      int postMinutes,
      Integer lunchStartMin,
      Integer lunchEndMin,
      boolean isContinuous) {
    this.preMinutes = preMinutes;
    this.postMinutes = postMinutes;
    this.lunchStartMin = lunchStartMin;
    this.lunchEndMin = lunchEndMin;
    this.isContinuous = isContinuous;
  }

  /**
   * Returns the pre-market length.
   *
   * @return minutes before the open
   */
  public int preMinutes() {
    return preMinutes;
  }

  /**
   * Returns the post-market length.
   *
   * @return minutes after the close
   */
  public int postMinutes() {
    return postMinutes;
  }

  /**
   * Returns when the lunch break starts.
   *
   * @return minutes after the open, or empty without a lunch break
   */
  public OptionalInt lunchStartMin() {
    return lunchStartMin == null ? OptionalInt.empty() : OptionalInt.of(lunchStartMin.intValue());
  }

  /**
   * Returns when the lunch break ends.
   *
   * @return minutes after the open, or empty without a lunch break
   */
  public OptionalInt lunchEndMin() {
    return lunchEndMin == null ? OptionalInt.empty() : OptionalInt.of(lunchEndMin.intValue());
  }

  /**
   * Whether the market trades around the clock.
   *
   * @return {@code true} for a continuous market
   */
  public boolean isContinuous() {
    return isContinuous;
  }

  static MarketRule fromJson(Map<String, Object> object) {
    Long lunchStart = JsonFields.optLong(object, "lunchStartMin");
    Long lunchEnd = JsonFields.optLong(object, "lunchEndMin");
    return new MarketRule(
        JsonFields.intValue(object, "preMinutes"),
        JsonFields.intValue(object, "postMinutes"),
        lunchStart == null ? null : Integer.valueOf(lunchStart.intValue()),
        lunchEnd == null ? null : Integer.valueOf(lunchEnd.intValue()),
        JsonFields.bool(object, "isContinuous"));
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof MarketRule)) {
      return false;
    }
    MarketRule that = (MarketRule) other;
    return preMinutes == that.preMinutes
        && postMinutes == that.postMinutes
        && isContinuous == that.isContinuous
        && Objects.equals(lunchStartMin, that.lunchStartMin)
        && Objects.equals(lunchEndMin, that.lunchEndMin);
  }

  @Override
  public int hashCode() {
    return Objects.hash(preMinutes, postMinutes, lunchStartMin, lunchEndMin, isContinuous);
  }

  @Override
  public String toString() {
    return "MarketRule{preMinutes="
        + preMinutes
        + ", postMinutes="
        + postMinutes
        + ", lunchStartMin="
        + lunchStartMin
        + ", lunchEndMin="
        + lunchEndMin
        + ", isContinuous="
        + isContinuous
        + "}";
  }
}
