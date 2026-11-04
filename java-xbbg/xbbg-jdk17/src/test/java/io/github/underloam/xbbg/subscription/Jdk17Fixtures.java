package io.github.underloam.xbbg.subscription;

import io.github.underloam.xbbg.SubscriptionDeliveryMode;
import io.github.underloam.xbbg.internal.TickBatch;

/** Builds subscription values whose constructors are package-private, without natives. */
public final class Jdk17Fixtures {
  private Jdk17Fixtures() {}

  /**
   * Builds a delivery.
   *
   * @param kind deltas or state
   * @param data the data
   * @param gap the gap before, or {@code null}
   * @param <T> the data type
   * @return the delivery
   */
  public static <T> SubscriptionDelivery<T> delivery(
      SubscriptionDeliveryMode kind, T data, SubscriptionGap gap) {
    return new SubscriptionDelivery<T>(kind, data, gap);
  }

  /**
   * Builds a gap.
   *
   * @param droppedUpdates the dropped update count
   * @return the gap
   */
  public static SubscriptionGap gap(long droppedUpdates) {
    return new SubscriptionGap(SubscriptionGap.LOCAL_OVERFLOW, droppedUpdates);
  }

  /**
   * Builds a one-row tick of topic {@code IBM US Equity} whose layout has {@code fields} with
   * {@code kinds}, carrying the positions {@code present} (field indexes) with the given value
   * kinds and payload bits.
   *
   * @param fields the layout field names
   * @param kinds the layout kinds ({@code TickBatch.KIND_*})
   * @param present the field index of each carried value
   * @param valueKinds the value kind of each carried value ({@code TickBatch.VALUE_*})
   * @param bits the payload of each carried value
   * @param strings string payloads referenced by {@code VALUE_STR} bits
   * @return the tick
   */
  public static Tick tick(
      String[] fields,
      byte[] kinds,
      char[] present,
      byte[] valueKinds,
      long[] bits,
      String[] strings) {
    TickBatch batch =
        new TickBatch(
            1,
            fields,
            kinds,
            new String[] {"IBM US Equity"},
            new long[] {1_700_000_000_000_000L},
            new int[] {0, present.length},
            present,
            valueKinds,
            bits,
            strings);
    batch.validate();
    return new Tick(batch, new TickLayout(1, fields, kinds), 0);
  }
}
