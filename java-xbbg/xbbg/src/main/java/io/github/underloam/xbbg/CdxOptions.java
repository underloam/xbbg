package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for the CDX pricing and risk helpers (TS {@code CdxOptions}): the {@code bdp} options
 * plus a recovery rate.
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent, so the engine default applies. Used by {@link Engine#acdxPricing(String, CdxOptions)}.
 */
public final class CdxOptions {
  private static final CdxOptions DEFAULTS = builder().build();

  private final Double recoveryRate;
  private final Overrides overrides;
  private final Map<String, Object> kwargs;
  private final Format format;
  private final Boolean includeSecurityErrors;
  private final Boolean returnEids;
  private final DelayedPolicy onDelayed;
  private final Boolean validateFields;

  private CdxOptions(Builder builder) {
    this.recoveryRate = builder.recoveryRate;
    this.overrides = builder.overrides;
    this.kwargs = builder.kwargs;
    this.format = builder.format;
    this.includeSecurityErrors = builder.includeSecurityErrors;
    this.returnEids = builder.returnEids;
    this.onDelayed = builder.onDelayed;
    this.validateFields = builder.validateFields;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static CdxOptions defaults() {
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
   * Returns {@code recoveryRate}: the recovery rate in percent (40 means 40%); sent as the {@code
   * CDS_RR} override, beating an explicit one.
   *
   * @return the value, or empty when unset
   */
  public Optional<Double> recoveryRate() {
    return Optional.ofNullable(recoveryRate);
  }

  /**
   * Returns {@code overrides}: Bloomberg field overrides, built with {@link Overrides}.
   *
   * @return the value, or empty when unset
   */
  public Optional<Overrides> overrides() {
    return Optional.ofNullable(overrides);
  }

  /**
   * Returns {@code kwargs}: extra request parameters (strings, numbers or booleans) the engine
   * routes into request elements or overrides by schema; numbers are sent as JavaScript {@code
   * String(value)} renders them.
   *
   * @return the value, or empty when unset
   */
  public Optional<Map<String, Object>> kwargs() {
    return Optional.ofNullable(kwargs);
  }

  /**
   * Returns {@code format}: the result layout; unset uses the engine default ({@link Format#LONG}).
   *
   * @return the value, or empty when unset
   */
  public Optional<Format> format() {
    return Optional.ofNullable(format);
  }

  /**
   * Returns {@code includeSecurityErrors}: whether to include rows for securities Bloomberg
   * rejected; unset means {@code false}.
   *
   * @return the value, or empty when unset
   */
  public Optional<Boolean> includeSecurityErrors() {
    return Optional.ofNullable(includeSecurityErrors);
  }

  /**
   * Returns {@code returnEids}: whether to request entitlement ids; they surface in the result
   * metadata.
   *
   * @return the value, or empty when unset
   */
  public Optional<Boolean> returnEids() {
    return Optional.ofNullable(returnEids);
  }

  /**
   * Returns {@code onDelayed}: the delayed-data check; unset skips it.
   *
   * @return the value, or empty when unset
   */
  public Optional<DelayedPolicy> onDelayed() {
    return Optional.ofNullable(onDelayed);
  }

  /**
   * Returns {@code validateFields}: per-request field validation: {@code true} forces strict
   * validation, {@code false} disables it, unset follows the engine's validation mode.
   *
   * @return the value, or empty when unset
   */
  public Optional<Boolean> validateFields() {
    return Optional.ofNullable(validateFields);
  }

  /** Writes the set options as request argument fields. */
  void encodeInto(JsonObjectBuilder args) {
    args.put("recoveryRate", recoveryRate);
    if (overrides != null) {
      overrides.encodeRequestParts(args);
    }
    args.put("kwargs", kwargs);
    if (format != null) {
      args.put("format", format.wireValue());
    }
    args.put("includeSecurityErrors", includeSecurityErrors);
    args.put("returnEids", returnEids);
    if (onDelayed != null) {
      args.put("onDelayed", onDelayed.wireValue());
    }
    args.put("validateFields", validateFields);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof CdxOptions)) {
      return false;
    }
    CdxOptions other = (CdxOptions) object;
    return Objects.equals(recoveryRate, other.recoveryRate)
        && Objects.equals(overrides, other.overrides)
        && RequestArgs.orderedEquals(kwargs, other.kwargs)
        && Objects.equals(format, other.format)
        && Objects.equals(includeSecurityErrors, other.includeSecurityErrors)
        && Objects.equals(returnEids, other.returnEids)
        && Objects.equals(onDelayed, other.onDelayed)
        && Objects.equals(validateFields, other.validateFields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        recoveryRate,
        overrides,
        RequestArgs.orderedHash(kwargs),
        format,
        includeSecurityErrors,
        returnEids,
        onDelayed,
        validateFields);
  }

  @Override
  public String toString() {
    return RequestArgs.describe(
        "CdxOptions",
        "recoveryRate",
        recoveryRate,
        "overrides",
        overrides,
        "kwargs",
        kwargs,
        "format",
        format,
        "includeSecurityErrors",
        includeSecurityErrors,
        "returnEids",
        returnEids,
        "onDelayed",
        onDelayed,
        "validateFields",
        validateFields);
  }

  /** Builds {@link CdxOptions}; not thread-safe. */
  public static final class Builder {
    private Double recoveryRate;
    private Overrides overrides;
    private Map<String, Object> kwargs;
    private Format format;
    private Boolean includeSecurityErrors;
    private Boolean returnEids;
    private DelayedPolicy onDelayed;
    private Boolean validateFields;

    private Builder() {}

    private Builder(CdxOptions options) {
      this.recoveryRate = options.recoveryRate;
      this.overrides = options.overrides;
      this.kwargs = options.kwargs;
      this.format = options.format;
      this.includeSecurityErrors = options.includeSecurityErrors;
      this.returnEids = options.returnEids;
      this.onDelayed = options.onDelayed;
      this.validateFields = options.validateFields;
    }

    /**
     * Sets {@code recoveryRate}: the recovery rate in percent (40 means 40%); sent as the {@code
     * CDS_RR} override, beating an explicit one.
     *
     * @param recoveryRate the value, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the value is not finite
     */
    public Builder recoveryRate(Double recoveryRate) {
      if (recoveryRate != null && (Double.isNaN(recoveryRate) || Double.isInfinite(recoveryRate))) {
        throw new IllegalArgumentException(
            "recoveryRate must be a finite number, got " + recoveryRate);
      }
      this.recoveryRate = recoveryRate;
      return this;
    }

    /**
     * Sets the overrides: Bloomberg field overrides, built with {@link Overrides}.
     *
     * @param overrides the overrides, or {@code null} to unset
     * @return this builder
     */
    public Builder overrides(Overrides overrides) {
      this.overrides = overrides;
      return this;
    }

    /**
     * Replaces {@code kwargs}: extra request parameters (strings, numbers or booleans) the engine
     * routes into request elements or overrides by schema; numbers are sent as JavaScript {@code
     * String(value)} renders them.
     *
     * <p>Keys keep the map's iteration order; use a {@link java.util.LinkedHashMap} to control it.
     *
     * @param kwargs the entries, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if a value is not a string, number or boolean
     */
    public Builder kwargs(Map<String, ?> kwargs) {
      this.kwargs = kwargs == null ? null : RequestArgs.scalars("kwargs", kwargs, false);
      return this;
    }

    /**
     * Adds or replaces one {@code kwargs} entry, keeping the position of an existing key.
     *
     * @param key the entry name
     * @param value a string, number or boolean
     * @return this builder
     * @throws IllegalArgumentException if the value is not a string, number or boolean
     */
    public Builder kwarg(String key, Object value) {
      this.kwargs = RequestArgs.withScalar("kwargs", this.kwargs, key, value, false);
      return this;
    }

    /**
     * Sets the format: the result layout; unset uses the engine default ({@link Format#LONG}).
     *
     * @param format the format, or {@code null} to unset
     * @return this builder
     */
    public Builder format(Format format) {
      this.format = format;
      return this;
    }

    /**
     * Sets {@code includeSecurityErrors}: whether to include rows for securities Bloomberg
     * rejected; unset means {@code false}.
     *
     * @param includeSecurityErrors the flag, or {@code null} to unset
     * @return this builder
     */
    public Builder includeSecurityErrors(Boolean includeSecurityErrors) {
      this.includeSecurityErrors = includeSecurityErrors;
      return this;
    }

    /**
     * Sets {@code returnEids}: whether to request entitlement ids; they surface in the result
     * metadata.
     *
     * @param returnEids the flag, or {@code null} to unset
     * @return this builder
     */
    public Builder returnEids(Boolean returnEids) {
      this.returnEids = returnEids;
      return this;
    }

    /**
     * Sets {@code onDelayed}: the delayed-data check; unset skips it.
     *
     * @param onDelayed the policy, or {@code null} to unset
     * @return this builder
     */
    public Builder onDelayed(DelayedPolicy onDelayed) {
      this.onDelayed = onDelayed;
      return this;
    }

    /**
     * Sets {@code validateFields}: per-request field validation: {@code true} forces strict
     * validation, {@code false} disables it, unset follows the engine's validation mode.
     *
     * @param validateFields the flag, or {@code null} to unset
     * @return this builder
     */
    public Builder validateFields(Boolean validateFields) {
      this.validateFields = validateFields;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return immutable options
     */
    public CdxOptions build() {
      return new CdxOptions(this);
    }
  }
}
