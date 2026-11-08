package io.github.underloam.xbbg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Overrides that apply to one security only (see {@link Overrides#forSecurity}). */
public final class SecurityOverride {
  private final String security;
  private final List<StringPair> overrides;

  /**
   * Creates a value.
   *
   * @param security the security the overrides apply to
   * @param overrides the overrides, in order
   */
  public SecurityOverride(String security, List<StringPair> overrides) {
    this.security = Objects.requireNonNull(security, "security");
    this.overrides = Collections.unmodifiableList(new ArrayList<StringPair>(overrides));
  }

  /**
   * Returns the security.
   *
   * @return the security
   */
  public String security() {
    return security;
  }

  /**
   * Returns the overrides for the security.
   *
   * @return an unmodifiable list, in insertion order
   */
  public List<StringPair> overrides() {
    return overrides;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof SecurityOverride)) {
      return false;
    }
    SecurityOverride that = (SecurityOverride) other;
    return security.equals(that.security) && overrides.equals(that.overrides);
  }

  @Override
  public int hashCode() {
    return 31 * security.hashCode() + overrides.hashCode();
  }

  @Override
  public String toString() {
    return security + overrides;
  }
}
