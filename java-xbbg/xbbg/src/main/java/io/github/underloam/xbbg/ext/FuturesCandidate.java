package io.github.underloam.xbbg.ext;

import io.github.underloam.xbbg.internal.JsonFields;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A candidate futures contract and its contract month, from {@link
 * Ext#generateFuturesCandidates(String, int, int, int)}.
 */
public final class FuturesCandidate {
  private final String ticker;
  private final int year;
  private final int month;

  /**
   * Creates a value.
   *
   * @param ticker the contract ticker ({@code "ESH24 Index"})
   * @param year the contract year
   * @param month the contract month, 1–12
   * @throws NullPointerException if {@code ticker} is null
   */
  public FuturesCandidate(String ticker, int year, int month) {
    this.ticker = Objects.requireNonNull(ticker, "ticker");
    this.year = year;
    this.month = month;
  }

  /**
   * Returns the contract ticker.
   *
   * @return the ticker
   */
  public String ticker() {
    return ticker;
  }

  /**
   * Returns the contract year.
   *
   * @return the year
   */
  public int year() {
    return year;
  }

  /**
   * Returns the contract month.
   *
   * @return the month, 1–12
   */
  public int month() {
    return month;
  }

  static FuturesCandidate fromJson(Map<String, Object> object) {
    return new FuturesCandidate(
        JsonFields.string(object, "ticker"),
        JsonFields.intValue(object, "year"),
        JsonFields.intValue(object, "month"));
  }

  Map<String, Object> toJson() {
    Map<String, Object> object = new LinkedHashMap<String, Object>();
    object.put("ticker", ticker);
    object.put("year", year);
    object.put("month", month);
    return object;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof FuturesCandidate)) {
      return false;
    }
    FuturesCandidate that = (FuturesCandidate) other;
    return year == that.year && month == that.month && ticker.equals(that.ticker);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ticker, year, month);
  }

  @Override
  public String toString() {
    return "FuturesCandidate{ticker=" + ticker + ", year=" + year + ", month=" + month + "}";
  }
}
