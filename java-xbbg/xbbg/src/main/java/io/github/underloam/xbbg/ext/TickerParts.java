package io.github.underloam.xbbg.ext;

import io.github.underloam.xbbg.internal.JsonFields;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Components of a generic Bloomberg ticker, from {@link Ext#parseTicker(String)}. */
public final class TickerParts {
  private final String prefix;
  private final int index;
  private final String asset;
  private final String exchange;

  /**
   * Creates a value.
   *
   * @param prefix the root symbol ({@code "ES"} for {@code "ES1 Index"})
   * @param index the generic position digit ({@code 1} for {@code "ES1 Index"})
   * @param asset the yellow key ({@code "Index"}, {@code "Equity"}, …)
   * @param exchange the exchange of an equity ticker, or {@code null}
   * @throws NullPointerException if {@code prefix} or {@code asset} is null
   */
  public TickerParts(String prefix, int index, String asset, String exchange) {
    this.prefix = Objects.requireNonNull(prefix, "prefix");
    this.index = index;
    this.asset = Objects.requireNonNull(asset, "asset");
    this.exchange = exchange;
  }

  /**
   * Returns the root symbol.
   *
   * @return the prefix
   */
  public String prefix() {
    return prefix;
  }

  /**
   * Returns the generic position digit.
   *
   * @return the 1-based contract position
   */
  public int index() {
    return index;
  }

  /**
   * Returns the yellow key.
   *
   * @return the asset class
   */
  public String asset() {
    return asset;
  }

  /**
   * Returns the exchange of an equity ticker ({@code "US"} for {@code "SPY1 US Equity"}).
   *
   * @return the exchange, or empty for other asset classes
   */
  public Optional<String> exchange() {
    return Optional.ofNullable(exchange);
  }

  static TickerParts fromJson(Map<String, Object> object) {
    return new TickerParts(
        JsonFields.string(object, "prefix"),
        JsonFields.intValue(object, "index"),
        JsonFields.string(object, "asset"),
        JsonFields.optString(object, "exchange"));
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof TickerParts)) {
      return false;
    }
    TickerParts that = (TickerParts) other;
    return index == that.index
        && prefix.equals(that.prefix)
        && asset.equals(that.asset)
        && Objects.equals(exchange, that.exchange);
  }

  @Override
  public int hashCode() {
    return Objects.hash(prefix, index, asset, exchange);
  }

  @Override
  public String toString() {
    return "TickerParts{prefix="
        + prefix
        + ", index="
        + index
        + ", asset="
        + asset
        + ", exchange="
        + exchange
        + "}";
  }
}
