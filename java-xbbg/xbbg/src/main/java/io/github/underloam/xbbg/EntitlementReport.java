package io.github.underloam.xbbg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** The result of {@link Engine#checkEntitlements(String, int...)}. */
public final class EntitlementReport {
  private final boolean entitled;
  private final List<Integer> failedEids;

  /**
   * Creates a report.
   *
   * @param entitled whether the identity is entitled to every EID
   * @param failedEids the EIDs the identity is not entitled to
   */
  public EntitlementReport(boolean entitled, List<Integer> failedEids) {
    this.entitled = entitled;
    this.failedEids = Collections.unmodifiableList(new ArrayList<Integer>(failedEids));
  }

  /**
   * Whether the identity is entitled to every requested EID.
   *
   * @return the entitlement verdict
   */
  public boolean entitled() {
    return entitled;
  }

  /**
   * Returns the EIDs the identity is not entitled to.
   *
   * @return an unmodifiable list
   */
  public List<Integer> failedEids() {
    return failedEids;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof EntitlementReport)) {
      return false;
    }
    EntitlementReport that = (EntitlementReport) other;
    return entitled == that.entitled && failedEids.equals(that.failedEids);
  }

  @Override
  public int hashCode() {
    return 31 * Boolean.valueOf(entitled).hashCode() + failedEids.hashCode();
  }

  @Override
  public String toString() {
    return "EntitlementReport{entitled=" + entitled + ", failedEids=" + failedEids + "}";
  }
}
