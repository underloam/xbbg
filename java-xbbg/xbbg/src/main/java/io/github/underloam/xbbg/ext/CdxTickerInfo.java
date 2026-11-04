package io.github.underloam.xbbg.ext;

import io.github.underloam.xbbg.internal.JsonFields;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

/** Components of a CDX ticker, from {@link Ext#parseCdxTicker(String)}. */
public final class CdxTickerInfo {
  private final String index;
  private final String series;
  private final Long version;
  private final String tenor;
  private final String asset;
  private final boolean isGeneric;
  private final Long seriesNum;

  /**
   * Creates a value.
   *
   * @param index the index name ({@code "CDX IG CDSI"})
   * @param series {@code "GEN"} or {@code "S<n>"}
   * @param version the explicit {@code V<n>} version, or {@code null}
   * @param tenor the tenor ({@code "5Y"})
   * @param asset the yellow key ({@code "Corp"})
   * @param isGeneric whether the ticker is generic ({@code GEN})
   * @param seriesNum the series number of a specific ticker, or {@code null}
   * @throws NullPointerException if a string value is null
   */
  public CdxTickerInfo(
      String index,
      String series,
      Long version,
      String tenor,
      String asset,
      boolean isGeneric,
      Long seriesNum) {
    this.index = Objects.requireNonNull(index, "index");
    this.series = Objects.requireNonNull(series, "series");
    this.version = version;
    this.tenor = Objects.requireNonNull(tenor, "tenor");
    this.asset = Objects.requireNonNull(asset, "asset");
    this.isGeneric = isGeneric;
    this.seriesNum = seriesNum;
  }

  /**
   * Returns the index name.
   *
   * @return the index, for example {@code "CDX IG CDSI"}
   */
  public String index() {
    return index;
  }

  /**
   * Returns the series token.
   *
   * @return {@code "GEN"} or {@code "S<n>"}
   */
  public String series() {
    return series;
  }

  /**
   * Returns the explicit version token of the ticker.
   *
   * @return the version, or empty for a versionless ticker
   */
  public OptionalLong version() {
    return version == null ? OptionalLong.empty() : OptionalLong.of(version.longValue());
  }

  /**
   * Returns the tenor.
   *
   * @return the tenor, for example {@code "5Y"}
   */
  public String tenor() {
    return tenor;
  }

  /**
   * Returns the yellow key.
   *
   * @return the asset class, for example {@code "Corp"}
   */
  public String asset() {
    return asset;
  }

  /**
   * Whether the ticker is generic.
   *
   * @return {@code true} for a {@code GEN} ticker
   */
  public boolean isGeneric() {
    return isGeneric;
  }

  /**
   * Returns the series number.
   *
   * @return the series number, or empty for a generic ticker
   */
  public OptionalLong seriesNum() {
    return seriesNum == null ? OptionalLong.empty() : OptionalLong.of(seriesNum.longValue());
  }

  static CdxTickerInfo fromJson(Map<String, Object> object) {
    return new CdxTickerInfo(
        JsonFields.string(object, "index"),
        JsonFields.string(object, "series"),
        JsonFields.optLong(object, "version"),
        JsonFields.string(object, "tenor"),
        JsonFields.string(object, "asset"),
        JsonFields.bool(object, "isGeneric"),
        JsonFields.optLong(object, "seriesNum"));
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof CdxTickerInfo)) {
      return false;
    }
    CdxTickerInfo that = (CdxTickerInfo) other;
    return isGeneric == that.isGeneric
        && index.equals(that.index)
        && series.equals(that.series)
        && Objects.equals(version, that.version)
        && tenor.equals(that.tenor)
        && asset.equals(that.asset)
        && Objects.equals(seriesNum, that.seriesNum);
  }

  @Override
  public int hashCode() {
    return Objects.hash(index, series, version, tenor, asset, isGeneric, seriesNum);
  }

  @Override
  public String toString() {
    return "CdxTickerInfo{index="
        + index
        + ", series="
        + series
        + ", version="
        + version
        + ", tenor="
        + tenor
        + ", asset="
        + asset
        + ", isGeneric="
        + isGeneric
        + ", seriesNum="
        + seriesNum
        + "}";
  }
}
