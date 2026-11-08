package io.github.underloam.xbbg;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Surface coordinates of one {@code volSurface} field (TS {@code VolFieldSpec}). An unset part is
 * inferred from the Bloomberg field name.
 *
 * <p>Immutable; build with {@link #builder()}. Used by {@link
 * VolSurfaceOptions.Builder#fieldSpecs(Map)}.
 */
public final class VolFieldSpec {
  private final String metric;
  private final String tenor;
  private final String pointType;
  private final Double point;

  private VolFieldSpec(Builder builder) {
    this.metric = builder.metric;
    this.tenor = builder.tenor;
    this.pointType = builder.pointType;
    this.point = builder.point;
  }

  /**
   * Returns a builder with nothing set.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns {@code metric}, for example {@code implied_volatility}, {@code rate} or {@code value}.
   *
   * @return the value, or empty when inferred
   */
  public Optional<String> metric() {
    return Optional.ofNullable(metric);
  }

  /**
   * Returns {@code tenor}, for example {@code 1M} or {@code 30D}.
   *
   * @return the value, or empty when inferred
   */
  public Optional<String> tenor() {
    return Optional.ofNullable(tenor);
  }

  /**
   * Returns {@code pointType}, for example {@code moneyness}, {@code delta_put} or {@code
   * delta_call}.
   *
   * @return the value, or empty when inferred
   */
  public Optional<String> pointType() {
    return Optional.ofNullable(pointType);
  }

  /**
   * Returns {@code point}, for example {@code 100} (moneyness percent) or {@code -25} (put delta).
   *
   * @return the value, or empty when inferred
   */
  public Optional<Double> point() {
    return Optional.ofNullable(point);
  }

  Map<String, Object> toJson() {
    Map<String, Object> object = new LinkedHashMap<String, Object>();
    object.put("metric", metric);
    object.put("tenor", tenor);
    object.put("pointType", pointType);
    object.put("point", point);
    return object;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof VolFieldSpec)) {
      return false;
    }
    VolFieldSpec other = (VolFieldSpec) object;
    return Objects.equals(metric, other.metric)
        && Objects.equals(tenor, other.tenor)
        && Objects.equals(pointType, other.pointType)
        && Objects.equals(point, other.point);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metric, tenor, pointType, point);
  }

  @Override
  public String toString() {
    return RequestArgs.describe(
        "VolFieldSpec", "metric", metric, "tenor", tenor, "pointType", pointType, "point", point);
  }

  /** Builds {@link VolFieldSpec}; not thread-safe. */
  public static final class Builder {
    private String metric;
    private String tenor;
    private String pointType;
    private Double point;

    private Builder() {}

    /**
     * Sets {@code metric}.
     *
     * @param metric the metric, or {@code null} to infer it
     * @return this builder
     */
    public Builder metric(String metric) {
      this.metric = metric;
      return this;
    }

    /**
     * Sets {@code tenor}.
     *
     * @param tenor the tenor, or {@code null} to infer it
     * @return this builder
     */
    public Builder tenor(String tenor) {
      this.tenor = tenor;
      return this;
    }

    /**
     * Sets {@code pointType}.
     *
     * @param pointType the point type, or {@code null} to infer it
     * @return this builder
     */
    public Builder pointType(String pointType) {
      this.pointType = pointType;
      return this;
    }

    /**
     * Sets {@code point}.
     *
     * @param point the point, or {@code null} to infer it
     * @return this builder
     */
    public Builder point(Double point) {
      this.point = point;
      return this;
    }

    /**
     * Builds the spec.
     *
     * @return an immutable spec
     */
    public VolFieldSpec build() {
      return new VolFieldSpec(this);
    }
  }
}
