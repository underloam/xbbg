package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.Json;
import io.github.underloam.xbbg.internal.JsonArrayBuilder;
import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import io.github.underloam.xbbg.internal.JsonValue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for the {@code volSurface} recipe (TS {@code VolSurfaceOptions}).
 *
 * <p>The surface fields are the preset fields followed by the custom fields (first occurrence of a
 * field wins). Unset presets mean {@link VolSurfacePreset#MONEYNESS_30D}; {@link
 * Builder#noPreset()} selects none (JavaScript turns {@code preset: null} into the default
 * instead). Custom fields are either plain Bloomberg fields ({@link Builder#fields(Collection)}) or
 * fields with explicit coordinates ({@link Builder#fieldSpecs(Map)}).
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent, so the recipe default applies. Used by {@link Engine#volSurface(Collection, String, String,
 * VolSurfaceOptions)}.
 */
public final class VolSurfaceOptions {
  private static final VolSurfaceOptions DEFAULTS = builder().build();

  private final List<VolSurfacePreset> presets;
  private final List<String> fields;
  private final Map<String, VolFieldSpec> fieldSpecs;
  private final Boolean asDecimal;
  private final Boolean includeDerived;
  private final Double riskFreeRate;
  private final String dividendYieldField;

  private VolSurfaceOptions(Builder builder) {
    this.presets = builder.presets;
    this.fields = builder.fields;
    this.fieldSpecs = builder.fieldSpecs;
    this.asDecimal = builder.asDecimal;
    this.includeDerived = builder.includeDerived;
    this.riskFreeRate = builder.riskFreeRate;
    this.dividendYieldField = builder.dividendYieldField;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static VolSurfaceOptions defaults() {
    return DEFAULTS;
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
   * Returns a builder initialised with these options.
   *
   * @return a new builder
   */
  public Builder toBuilder() {
    return new Builder(this);
  }

  /**
   * Returns the presets ({@code preset}); an empty list selects no preset.
   *
   * @return the value, or empty when unset ({@link VolSurfacePreset#MONEYNESS_30D})
   */
  public Optional<List<VolSurfacePreset>> presets() {
    return Optional.ofNullable(presets);
  }

  /**
   * Returns the plain custom fields ({@code fields} as an array).
   *
   * @return the value, or empty when unset or when {@link #fieldSpecs()} is set
   */
  public Optional<List<String>> fields() {
    return Optional.ofNullable(fields);
  }

  /**
   * Returns the custom fields with coordinates ({@code fields} as an object); a {@code null} spec
   * means the coordinates are inferred from the field name.
   *
   * @return the value, or empty when unset or when {@link #fields()} is set
   */
  public Optional<Map<String, VolFieldSpec>> fieldSpecs() {
    return Optional.ofNullable(fieldSpecs);
  }

  /**
   * Returns {@code asDecimal}: divide percent-quoted values by 100; unset means {@code true}.
   *
   * @return the value, or empty when unset
   */
  public Optional<Boolean> asDecimal() {
    return Optional.ofNullable(asDecimal);
  }

  /**
   * Returns {@code includeDerived}: add spot, rate and dividend-yield rows; unset means {@code
   * false}.
   *
   * @return the value, or empty when unset
   */
  public Optional<Boolean> includeDerived() {
    return Optional.ofNullable(includeDerived);
  }

  /**
   * Returns {@code riskFreeRate}: the rate for derived rows; values above 1 in magnitude are
   * percent.
   *
   * @return the value, or empty when unset
   */
  public Optional<Double> riskFreeRate() {
    return Optional.ofNullable(riskFreeRate);
  }

  /**
   * Returns {@code dividendYieldField}: the dividend yield field for derived rows; unset means
   * {@code EQY_DVD_YLD_12M}.
   *
   * @return the value, or empty when unset
   */
  public Optional<String> dividendYieldField() {
    return Optional.ofNullable(dividendYieldField);
  }

  /** Writes the set options as recipe argument fields. */
  void encodeInto(JsonObjectBuilder args) {
    if (presets != null) {
      JsonArrayBuilder wire = Json.array();
      for (VolSurfacePreset preset : presets) {
        wire.add(preset.wireValue());
      }
      args.put("preset", wire);
    }
    if (fields != null) {
      args.put("fields", fields);
    } else if (fieldSpecs != null) {
      JsonObjectBuilder specs = Json.object();
      for (Map.Entry<String, VolFieldSpec> entry : fieldSpecs.entrySet()) {
        VolFieldSpec spec = entry.getValue();
        specs.put(entry.getKey(), spec == null ? JsonValue.NULL : spec.toJson());
      }
      args.put("fields", specs);
    }
    args.put("asDecimal", asDecimal);
    args.put("includeDerived", includeDerived);
    args.put("riskFreeRate", riskFreeRate);
    args.put("dividendYieldField", dividendYieldField);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof VolSurfaceOptions)) {
      return false;
    }
    VolSurfaceOptions other = (VolSurfaceOptions) object;
    return Objects.equals(presets, other.presets)
        && Objects.equals(fields, other.fields)
        && orderedEquals(fieldSpecs, other.fieldSpecs)
        && Objects.equals(asDecimal, other.asDecimal)
        && Objects.equals(includeDerived, other.includeDerived)
        && Objects.equals(riskFreeRate, other.riskFreeRate)
        && Objects.equals(dividendYieldField, other.dividendYieldField);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        presets,
        fields,
        fieldSpecs == null
            ? null
            : new ArrayList<Map.Entry<String, VolFieldSpec>>(fieldSpecs.entrySet()),
        asDecimal,
        includeDerived,
        riskFreeRate,
        dividendYieldField);
  }

  @Override
  public String toString() {
    return RequestArgs.describe(
        "VolSurfaceOptions",
        "presets",
        presets,
        "fields",
        fields,
        "fieldSpecs",
        fieldSpecs,
        "asDecimal",
        asDecimal,
        "includeDerived",
        includeDerived,
        "riskFreeRate",
        riskFreeRate,
        "dividendYieldField",
        dividendYieldField);
  }

  private static boolean orderedEquals(
      Map<String, VolFieldSpec> left, Map<String, VolFieldSpec> right) {
    if (left == null || right == null) {
      return left == null && right == null;
    }
    return new ArrayList<Map.Entry<String, VolFieldSpec>>(left.entrySet())
        .equals(new ArrayList<Map.Entry<String, VolFieldSpec>>(right.entrySet()));
  }

  /** Builds {@link VolSurfaceOptions}; not thread-safe. */
  public static final class Builder {
    private List<VolSurfacePreset> presets;
    private List<String> fields;
    private Map<String, VolFieldSpec> fieldSpecs;
    private Boolean asDecimal;
    private Boolean includeDerived;
    private Double riskFreeRate;
    private String dividendYieldField;

    private Builder() {}

    private Builder(VolSurfaceOptions options) {
      this.presets = options.presets;
      this.fields = options.fields;
      this.fieldSpecs = options.fieldSpecs;
      this.asDecimal = options.asDecimal;
      this.includeDerived = options.includeDerived;
      this.riskFreeRate = options.riskFreeRate;
      this.dividendYieldField = options.dividendYieldField;
    }

    /**
     * Selects one preset.
     *
     * @param preset the preset, or {@code null} to unset (the default preset)
     * @return this builder
     */
    public Builder preset(VolSurfacePreset preset) {
      this.presets = preset == null ? null : Collections.singletonList(preset);
      return this;
    }

    /**
     * Selects presets, in order; an empty collection selects none.
     *
     * @param presets the presets, or {@code null} to unset (the default preset)
     * @return this builder
     * @throws NullPointerException if an item is null
     */
    public Builder presets(Collection<VolSurfacePreset> presets) {
      if (presets == null) {
        this.presets = null;
        return this;
      }
      List<VolSurfacePreset> copy = new ArrayList<VolSurfacePreset>(presets.size());
      for (VolSurfacePreset preset : presets) {
        copy.add(Objects.requireNonNull(preset, "presets must not contain null"));
      }
      this.presets = Collections.unmodifiableList(copy);
      return this;
    }

    /**
     * Selects no preset, so only the custom fields form the surface (without custom fields the
     * recipe still falls back to {@link VolSurfacePreset#MONEYNESS_30D}).
     *
     * @return this builder
     */
    public Builder noPreset() {
      this.presets = Collections.emptyList();
      return this;
    }

    /**
     * Sets plain custom fields whose coordinates are inferred from their names; replaces any {@link
     * #fieldSpecs(Map)}.
     *
     * @param fields Bloomberg fields, or {@code null} to unset
     * @return this builder
     * @throws NullPointerException if an item is null
     */
    public Builder fields(Collection<String> fields) {
      this.fields = fields == null ? null : RequestArgs.strings("fields", fields);
      if (fields != null) {
        this.fieldSpecs = null;
      }
      return this;
    }

    /**
     * Sets custom fields with coordinates, in the map's iteration order (use a {@link
     * LinkedHashMap}); a {@code null} spec infers the field's coordinates. Replaces any {@link
     * #fields(Collection)}.
     *
     * @param fieldSpecs field → spec, or {@code null} to unset
     * @return this builder
     * @throws NullPointerException if a key is null
     */
    public Builder fieldSpecs(Map<String, VolFieldSpec> fieldSpecs) {
      if (fieldSpecs == null) {
        this.fieldSpecs = null;
        return this;
      }
      Map<String, VolFieldSpec> copy = new LinkedHashMap<String, VolFieldSpec>();
      for (Map.Entry<String, VolFieldSpec> entry : fieldSpecs.entrySet()) {
        copy.put(Objects.requireNonNull(entry.getKey(), "fieldSpecs key"), entry.getValue());
      }
      this.fieldSpecs = Collections.unmodifiableMap(copy);
      this.fields = null;
      return this;
    }

    /**
     * Sets {@code asDecimal}: whether to divide percent-quoted values by 100.
     *
     * @param asDecimal the flag, or {@code null} to unset ({@code true})
     * @return this builder
     */
    public Builder asDecimal(Boolean asDecimal) {
      this.asDecimal = asDecimal;
      return this;
    }

    /**
     * Sets {@code includeDerived}: whether to add spot, rate and dividend-yield rows.
     *
     * @param includeDerived the flag, or {@code null} to unset ({@code false})
     * @return this builder
     */
    public Builder includeDerived(Boolean includeDerived) {
      this.includeDerived = includeDerived;
      return this;
    }

    /**
     * Sets {@code riskFreeRate}: the risk-free rate for derived rows.
     *
     * @param riskFreeRate the rate (decimal, or percent when above 1 in magnitude), or {@code null}
     *     to unset
     * @return this builder
     */
    public Builder riskFreeRate(Double riskFreeRate) {
      this.riskFreeRate = riskFreeRate;
      return this;
    }

    /**
     * Sets {@code dividendYieldField}: the dividend yield field for derived rows.
     *
     * @param dividendYieldField the field, or {@code null} to unset
     * @return this builder
     */
    public Builder dividendYieldField(String dividendYieldField) {
      this.dividendYieldField = dividendYieldField;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return immutable options
     */
    public VolSurfaceOptions build() {
      return new VolSurfaceOptions(this);
    }
  }
}
