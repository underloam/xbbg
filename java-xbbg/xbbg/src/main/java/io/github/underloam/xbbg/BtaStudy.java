package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.Json;
import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import io.github.underloam.xbbg.internal.JsonValue;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A technical-analysis study with its own settings, for {@code bta} (TS {@code RawStudy}).
 *
 * <p>The study name is resolved like a plain study name: lower-cased, {@code -} and spaces become
 * {@code _}, then known aliases map to their Bloomberg attributes element ({@code "SMA"} becomes
 * {@code smavgStudyAttributes}); other names get a {@code StudyAttributes} suffix. Study settings
 * beat {@link BtaOptions#studyParams()}, which beat the study's defaults; {@code calcInterval} and
 * {@code interval} beat the matching {@link BtaOptions} values but not {@code kwargs} entries.
 *
 * <p>Immutable; build with {@link #builder(String)} or {@link #of(String)}.
 */
public final class BtaStudy {
  private final String studyType;
  private final Map<String, Object> settings;

  private BtaStudy(Builder builder) {
    this.studyType = builder.studyType;
    this.settings =
        Collections.unmodifiableMap(new LinkedHashMap<String, Object>(builder.settings));
  }

  /**
   * Returns a study with no settings of its own.
   *
   * @param studyType the study name or alias, for example {@code "rsi"} or {@code "Fear Greed"}
   * @return the study
   */
  public static BtaStudy of(String studyType) {
    return builder(studyType).build();
  }

  /**
   * Returns a builder for a study.
   *
   * @param studyType the study name or alias, for example {@code "rsi"} or {@code "Fear Greed"}
   * @return a new builder
   */
  public static Builder builder(String studyType) {
    return new Builder(studyType);
  }

  /**
   * Returns the study name or alias.
   *
   * @return the study type
   */
  public String studyType() {
    return studyType;
  }

  /**
   * Returns the periodicity this study asks for ({@code calcInterval}).
   *
   * @return the periodicity, or empty when unset
   */
  public Optional<String> calcInterval() {
    return Optional.ofNullable((String) settings.get("calcInterval"));
  }

  /**
   * Returns the intraday bar size this study asks for ({@code interval}).
   *
   * @return the bar size, or empty when unset
   */
  public Optional<Number> interval() {
    return Optional.ofNullable((Number) settings.get("interval"));
  }

  /**
   * Returns every setting in the order it was set: {@code calcInterval}, {@code interval} and the
   * study attributes ({@code null} marks an attribute whose default is removed).
   *
   * @return an unmodifiable map of the settings
   */
  public Map<String, Object> settings() {
    return settings;
  }

  /** The study object sent as the {@code bta} {@code study} argument. */
  JsonValue toJson() {
    JsonObjectBuilder object = Json.object().put("studyType", studyType);
    for (Map.Entry<String, Object> entry : settings.entrySet()) {
      Object value = entry.getValue();
      object.put(entry.getKey(), value == null ? JsonValue.NULL : value);
    }
    return object.build();
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof BtaStudy)) {
      return false;
    }
    BtaStudy other = (BtaStudy) object;
    return studyType.equals(other.studyType) && RequestArgs.orderedEquals(settings, other.settings);
  }

  @Override
  public int hashCode() {
    return Objects.hash(studyType, RequestArgs.orderedHash(settings));
  }

  @Override
  public String toString() {
    return RequestArgs.describe("BtaStudy", "studyType", studyType, "settings", settings);
  }

  /** Builds {@link BtaStudy}; not thread-safe. */
  public static final class Builder {
    private final String studyType;
    private final LinkedHashMap<String, Object> settings = new LinkedHashMap<String, Object>();

    private Builder(String studyType) {
      this.studyType = Objects.requireNonNull(studyType, "studyType");
    }

    /**
     * Sets the periodicity ({@code DAILY}, {@code WEEKLY}, {@code INTRADAY}, ...), upper-cased when
     * sent; {@code INTRADAY} selects an intraday data range.
     *
     * @param calcInterval the periodicity, or {@code null} to unset
     * @return this builder
     */
    public Builder calcInterval(String calcInterval) {
      return setting("calcInterval", calcInterval);
    }

    /**
     * Sets the intraday bar size in minutes (used only with {@code INTRADAY} periodicity).
     *
     * @param interval the bar size, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the value is not finite
     */
    public Builder interval(Number interval) {
      return setting(
          "interval", interval == null ? null : RequestArgs.scalar("interval", interval, false));
    }

    /**
     * Sets the study period (the {@code period} attribute).
     *
     * @param period the period, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the value is not finite
     */
    public Builder period(Number period) {
      return param("period", period);
    }

    /**
     * Sets one study attribute, for example {@code param("priceSourceClose", "PX_OPEN")}. A {@code
     * null} value removes the study's default for that attribute; {@code length} is an alias of
     * {@code period} that applies when {@code period} is not set.
     *
     * @param name the attribute name
     * @param value a string, number, boolean or {@code null}
     * @return this builder
     * @throws IllegalArgumentException if the name is {@code studyType}, {@code study}, {@code
     *     calcInterval} or {@code interval} (set by the builder itself), or the value is not a
     *     string, number, boolean or {@code null}
     */
    public Builder param(String name, Object value) {
      Objects.requireNonNull(name, "name");
      if ("studyType".equals(name) || "study".equals(name)) {
        throw new IllegalArgumentException(
            "the study type is fixed by BtaStudy.builder(studyType)");
      }
      if ("calcInterval".equals(name) || "interval".equals(name)) {
        throw new IllegalArgumentException("use BtaStudy.Builder." + name + "(...) to set " + name);
      }
      settings.put(name, RequestArgs.scalar(name, value, true));
      return this;
    }

    /**
     * Sets several study attributes in the map's iteration order (see {@link #param(String,
     * Object)}).
     *
     * @param params the attributes
     * @return this builder
     * @throws IllegalArgumentException if a name or value is rejected by {@link #param(String,
     *     Object)}
     */
    public Builder params(Map<String, ?> params) {
      for (Map.Entry<String, ?> entry : params.entrySet()) {
        param(entry.getKey(), entry.getValue());
      }
      return this;
    }

    private Builder setting(String name, Object value) {
      if (value == null) {
        settings.remove(name);
      } else {
        settings.put(name, value);
      }
      return this;
    }

    /**
     * Builds the study.
     *
     * @return an immutable study
     */
    public BtaStudy build() {
      return new BtaStudy(this);
    }
  }
}
