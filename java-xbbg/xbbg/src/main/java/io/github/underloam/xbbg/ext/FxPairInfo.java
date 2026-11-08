package io.github.underloam.xbbg.ext;

import io.github.underloam.xbbg.internal.JsonFields;
import java.util.Map;
import java.util.Objects;

/**
 * The FX ticker and scale for a currency conversion, from {@link Ext#buildFxPair(String, String)}.
 */
public final class FxPairInfo {
  private final String fxPair;
  private final double factor;
  private final String fromCcy;
  private final String toCcy;

  /**
   * Creates a value.
   *
   * @param fxPair the Bloomberg FX ticker ({@code "USDGBP Curncy"})
   * @param factor the minor-unit factor ({@code 100} for pence or cents, otherwise {@code 1})
   * @param fromCcy the upper-cased source currency
   * @param toCcy the upper-cased target currency
   * @throws NullPointerException if a string value is null
   */
  public FxPairInfo(String fxPair, double factor, String fromCcy, String toCcy) {
    this.fxPair = Objects.requireNonNull(fxPair, "fxPair");
    this.factor = factor;
    this.fromCcy = Objects.requireNonNull(fromCcy, "fromCcy");
    this.toCcy = Objects.requireNonNull(toCcy, "toCcy");
  }

  /**
   * Returns the Bloomberg FX ticker (target then source currency).
   *
   * @return the FX pair ticker
   */
  public String fxPair() {
    return fxPair;
  }

  /**
   * Returns the minor-unit factor.
   *
   * @return {@code 100} when the source currency is quoted in pence or cents, otherwise {@code 1}
   */
  public double factor() {
    return factor;
  }

  /**
   * Returns the source currency.
   *
   * @return the upper-cased source currency
   */
  public String fromCcy() {
    return fromCcy;
  }

  /**
   * Returns the target currency.
   *
   * @return the upper-cased target currency
   */
  public String toCcy() {
    return toCcy;
  }

  static FxPairInfo fromJson(Map<String, Object> object) {
    return new FxPairInfo(
        JsonFields.string(object, "fxPair"),
        JsonFields.doubleValue(object, "factor"),
        JsonFields.string(object, "fromCcy"),
        JsonFields.string(object, "toCcy"));
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof FxPairInfo)) {
      return false;
    }
    FxPairInfo that = (FxPairInfo) other;
    return Double.compare(factor, that.factor) == 0
        && fxPair.equals(that.fxPair)
        && fromCcy.equals(that.fromCcy)
        && toCcy.equals(that.toCcy);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fxPair, factor, fromCcy, toCcy);
  }

  @Override
  public String toString() {
    return "FxPairInfo{fxPair="
        + fxPair
        + ", factor="
        + factor
        + ", fromCcy="
        + fromCcy
        + ", toCcy="
        + toCcy
        + "}";
  }
}
