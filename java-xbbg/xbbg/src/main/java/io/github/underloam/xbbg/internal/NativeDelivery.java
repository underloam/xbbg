package io.github.underloam.xbbg.internal;

import io.github.underloam.xbbg.SubscriptionDeliveryMode;

/**
 * One marked delivery as the native library publishes it: data in the requested native
 * representation with an unsettled receipt, a terminal error receipt, or a retryable conversion
 * failure. Every receipt must be settled with {@code commitDelivery} or {@code rollbackDelivery};
 * until then the native source stays restorable. Inside a drain, deliveries carry no receipt.
 *
 * <p>Internal API: not covered by compatibility guarantees.
 */
public final class NativeDelivery {
  private final SubscriptionDeliveryMode kind;
  private final Object data;
  private final boolean hasGap;
  private final long droppedUpdates;
  private final long receipt;
  private final boolean error;
  private final Throwable conversionFailure;

  private NativeDelivery(
      SubscriptionDeliveryMode kind,
      Object data,
      boolean hasGap,
      long droppedUpdates,
      long receipt,
      boolean error,
      Throwable conversionFailure) {
    this.kind = kind;
    this.data = data;
    this.hasGap = hasGap;
    this.droppedUpdates = droppedUpdates;
    this.receipt = receipt;
    this.error = error;
    this.conversionFailure = conversionFailure;
  }

  /**
   * A data delivery.
   *
   * @param kind the delivery kind
   * @param data a {@code Table} or IPC bytes
   * @param hasGap whether updates were dropped before it
   * @param droppedUpdates the dropped count (unsigned)
   * @param receipt the receipt to settle, or 0 inside a drain
   * @return the delivery
   */
  public static NativeDelivery data(
      SubscriptionDeliveryMode kind,
      Object data,
      boolean hasGap,
      long droppedUpdates,
      long receipt) {
    return new NativeDelivery(kind, data, hasGap, droppedUpdates, receipt, false, null);
  }

  /**
   * A terminal stream error; committing its receipt throws the error once.
   *
   * @param receipt the receipt to settle
   * @return the delivery
   */
  public static NativeDelivery error(long receipt) {
    return new NativeDelivery(null, null, false, 0, receipt, true, null);
  }

  /**
   * A conversion failure; the native source was rolled back and can be read again.
   *
   * @param cause the failure
   * @return the delivery
   */
  public static NativeDelivery conversionFailure(Throwable cause) {
    return new NativeDelivery(null, null, false, 0, 0, false, cause);
  }

  /**
   * Returns the delivery kind of a data delivery.
   *
   * @return the kind, or {@code null} for errors and failures
   */
  public SubscriptionDeliveryMode kind() {
    return kind;
  }

  /**
   * Returns the native data of a data delivery.
   *
   * @return a {@code Table} or {@code byte[]}, or {@code null}
   */
  public Object data() {
    return data;
  }

  /**
   * Whether updates were dropped before this delivery.
   *
   * @return the flag
   */
  public boolean hasGap() {
    return hasGap;
  }

  /**
   * Returns the dropped count when {@link #hasGap()}.
   *
   * @return the count as an unsigned 64-bit value
   */
  public long droppedUpdates() {
    return droppedUpdates;
  }

  /**
   * Returns the receipt to settle.
   *
   * @return the receipt, or 0 when there is none
   */
  public long receipt() {
    return receipt;
  }

  /**
   * Whether this is a terminal error receipt.
   *
   * @return the flag
   */
  public boolean isError() {
    return error;
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
