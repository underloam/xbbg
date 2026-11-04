package io.github.underloam.xbbg;

import java.util.Objects;

/** A cached field definition ({@link Engine#getFieldInfo(String)}). */
public final class FieldInfo {
  private final String fieldId;
  private final String arrowType;
  private final String description;
  private final String category;

  /**
   * Creates a value.
   *
   * @param fieldId the Bloomberg field id
   * @param arrowType the Arrow type the field resolves to
   * @param description the field description
   * @param category the field category
   */
  public FieldInfo(String fieldId, String arrowType, String description, String category) {
    this.fieldId = Objects.requireNonNull(fieldId, "fieldId");
    this.arrowType = Objects.requireNonNull(arrowType, "arrowType");
    this.description = Objects.requireNonNull(description, "description");
    this.category = Objects.requireNonNull(category, "category");
  }

  /**
   * Returns the Bloomberg field id.
   *
   * @return the field id
   */
  public String fieldId() {
    return fieldId;
  }

  /**
   * Returns the Arrow type name the field resolves to (for example {@code float64}).
   *
   * @return the type name
   */
  public String arrowType() {
    return arrowType;
  }

  /**
   * Returns the field description.
   *
   * @return the description
   */
  public String description() {
    return description;
  }

  /**
   * Returns the field category.
   *
   * @return the category
   */
  public String category() {
    return category;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof FieldInfo)) {
      return false;
    }
    FieldInfo that = (FieldInfo) other;
    return fieldId.equals(that.fieldId)
        && arrowType.equals(that.arrowType)
        && description.equals(that.description)
        && category.equals(that.category);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldId, arrowType, description, category);
  }

  @Override
  public String toString() {
    return "FieldInfo{fieldId="
        + fieldId
        + ", arrowType="
        + arrowType
        + ", description="
        + description
        + ", category="
        + category
        + "}";
  }
}
