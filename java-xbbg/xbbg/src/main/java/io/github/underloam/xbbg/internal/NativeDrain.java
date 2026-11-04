package io.github.underloam.xbbg.internal;

import java.util.Collections;
import java.util.List;

/**
 * A frozen delivery drain as the native library publishes it: every delivery with one receipt for
 * the whole drain, or a retryable conversion failure. A non-zero receipt must be settled with
 * {@code commitDrain} or {@code rollbackDrain}; rolling back keeps the drain for a retry.
 *
 * <p>Internal API: not covered by compatibility guarantees.
 */
public final class NativeDrain {
  private final List<NativeDelivery> deliveries;
  private final long receipt;
  private final Throwable conversionFailure;

  private NativeDrain(List<NativeDelivery> deliveries, long receipt, Throwable conversionFailure) {
    this.deliveries = Collections.unmodifiableList(deliveries);
    this.receipt = receipt;
    this.conversionFailure = conversionFailure;
  }

  /**
   * A converted drain.
   *
   * @param deliveries the deliveries, in order
   * @param receipt the drain receipt, or 0 when there is nothing to settle
   * @return the drain
   */
  public static NativeDrain of(List<NativeDelivery> deliveries, long receipt) {
    return new NativeDrain(deliveries, receipt, null);
  }

  /**
   * A conversion failure; the whole drain was rolled back.
   *
   * @param cause the failure
   * @return the drain
   */
  public static NativeDrain conversionFailure(Throwable cause) {
    return new NativeDrain(Collections.<NativeDelivery>emptyList(), 0, cause);
  }

  /**
   * Returns the drained deliveries.
   *
   * @return the deliveries, possibly empty
   */
  public List<NativeDelivery> deliveries() {
    return deliveries;
  }

  /**
   * Returns the drain receipt.
   *
   * @return the receipt, or 0 when there is nothing to settle
   */
  public long receipt() {
    return receipt;
  }

  /**
   * Returns the retryable conversion failure.
   *
   * @return the failure, or {@code null}
   */
  public Throwable conversionFailure() {
    return conversionFailure;
  }
}
