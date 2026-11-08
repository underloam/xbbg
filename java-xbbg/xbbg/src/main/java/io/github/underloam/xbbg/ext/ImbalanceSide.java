package io.github.underloam.xbbg.ext;

import io.github.underloam.xbbg.internal.JsonFields;

/**
 * Direction of an auction order imbalance, from {@link AuctionFields#imbalanceSide(String)}.
 * Undisclosed and unknown codes have no side.
 */
public enum ImbalanceSide {
  /** More buy than sell interest ({@code BUY}, {@code MBUY}, {@code RBUY}). */
  BUY("buy"),
  /** More sell than buy interest ({@code SELL}, {@code MSEL}, {@code RSEL}). */
  SELL("sell"),
  /** No imbalance ({@code NOIM}, {@code NIMB}). */
  NONE("none");

  private final String wireValue;

  ImbalanceSide(String wireValue) {
    this.wireValue = wireValue;
  }

  /**
   * Returns the JavaScript value of the side.
   *
   * @return {@code "buy"}, {@code "sell"} or {@code "none"}
   */
  public String wireValue() {
    return wireValue;
  }

  static ImbalanceSide fromWire(String value) {
    for (ImbalanceSide side : values()) {
      if (side.wireValue.equals(value)) {
        return side;
      }
    }
    throw JsonFields.malformed("imbalanceSide", "is not buy, sell or none: " + value);
  }

  @Override
  public String toString() {
    return wireValue;
  }
}
