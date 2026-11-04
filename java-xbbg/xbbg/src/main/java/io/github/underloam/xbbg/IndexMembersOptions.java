package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for the {@code indexMembers} recipe (TS {@code IndexMembersOptions}).
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent, so the recipe default applies. Used by {@link Engine#indexMembers(String,
 * IndexMembersOptions)}.
 */
public final class IndexMembersOptions {
  private static final IndexMembersOptions DEFAULTS = builder().build();

  private final IndexMemberField field;
  private final String asof;

  private IndexMembersOptions(Builder builder) {
    this.field = builder.field;
    this.asof = builder.asof;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static IndexMembersOptions defaults() {
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
   * Returns {@code field}: the constituent bulk field; unset means {@link
   * IndexMemberField#INDX_MWEIGHT}.
   *
   * @return the value, or empty when unset
   */
  public Optional<IndexMemberField> field() {
    return Optional.ofNullable(field);
  }

  /**
   * Returns {@code asof}: the membership date ({@code END_DATE_OVERRIDE}), as {@code YYYYMMDD};
   * unset means current membership.
   *
   * @return the value, or empty when unset
   */
  public Optional<String> asof() {
    return Optional.ofNullable(asof);
  }

  /** Writes the set options as recipe argument fields. */
  void encodeInto(JsonObjectBuilder args) {
    if (field != null) {
      args.put("field", field.wireValue());
    }
    args.put("asof", asof);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof IndexMembersOptions)) {
      return false;
    }
    IndexMembersOptions other = (IndexMembersOptions) object;
    return Objects.equals(field, other.field) && Objects.equals(asof, other.asof);
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, asof);
  }

  @Override
  public String toString() {
    return RequestArgs.describe("IndexMembersOptions", "field", field, "asof", asof);
  }

  /** Builds {@link IndexMembersOptions}; not thread-safe. */
  public static final class Builder {
    private IndexMemberField field;
    private String asof;

    private Builder() {}

    private Builder(IndexMembersOptions options) {
      this.field = options.field;
      this.asof = options.asof;
    }

    /**
     * Sets {@code field}: the constituent bulk field.
     *
     * @param field the field, or {@code null} to unset
     * @return this builder
     */
    public Builder field(IndexMemberField field) {
      this.field = field;
      return this;
    }

    /**
     * Sets {@code asof} from text, formatted with {@link Dates#formatDate(String)}; blank text
     * unsets it.
     *
     * @param asof ISO 8601 or Bloomberg date text, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the text is ambiguous or not a date
     */
    public Builder asof(String asof) {
      this.asof = asof == null ? null : Dates.formatDate(asof);
      return this;
    }

    /**
     * Sets {@code asof} from a {@code java.time} value, formatted with {@link
     * Dates#formatDate(TemporalAccessor)}.
     *
     * @param asof the date, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the value carries no date
     */
    public Builder asof(TemporalAccessor asof) {
      this.asof = asof == null ? null : Dates.formatDate(asof);
      return this;
    }

    /**
     * Builds the options.
     *
     * @return immutable options
     */
    public IndexMembersOptions build() {
      return new IndexMembersOptions(this);
    }
  }
}
