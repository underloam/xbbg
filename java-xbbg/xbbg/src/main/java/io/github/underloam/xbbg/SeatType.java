package io.github.underloam.xbbg;

/** The Bloomberg seat type of the engine's identity ({@link Engine#seatType()}). */
public enum SeatType {
  /** A Bloomberg Professional Service seat. */
  BPS,
  /** Not a BPS seat. */
  NONBPS,
  /** The identity has no valid seat. */
  INVALID;

  /**
   * Returns the value Bloomberg reports.
   *
   * @return {@code BPS}, {@code NONBPS} or {@code INVALID}
   */
  public String wireValue() {
    return name();
  }

  /**
   * Parses a seat type exactly as the engine reports it.
   *
   * @param value {@code BPS}, {@code NONBPS} or {@code INVALID}
   * @return the seat type
   * @throws IllegalArgumentException if the value names no seat type
   */
  public static SeatType parse(String value) {
    for (SeatType type : values()) {
      if (type.name().equals(value)) {
        return type;
      }
    }
    throw new IllegalArgumentException(
        "unknown seat type '" + value + "': expected BPS, NONBPS, or INVALID");
  }
}
