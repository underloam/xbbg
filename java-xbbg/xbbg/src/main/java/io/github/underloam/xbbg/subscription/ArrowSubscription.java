package io.github.underloam.xbbg.subscription;

import io.github.underloam.xbbg.subscription.SubscriptionCoordinator.ReadMode;
import io.github.underloam.xbbg.table.Table;

/**
 * Arrow view of a {@link Subscription} ({@link Subscription#arrow()}): one {@link Table} per native
 * batch of sparse deltas, at most {@code flushThreshold} rows each and never spanning a layout
 * change.
 *
 * <p>Columns are {@code timestamp}, {@code topic}, the data fields, then a binary {@code
 * __xbbg_present} column whose bit {@code i} (least significant first) marks schema field {@code i
 * + 2} as present; a present null is a clear. Read each table against its own schema: layouts grow
 * and fields can be promoted to other types. The schema metadata key {@code
 * xbbg.subscription_presence} documents the encoding.
 */
public final class ArrowSubscription extends SubscriptionView<Table> {
  ArrowSubscription(SubscriptionCoordinator coordinator) {
    super(
        new ViewIterator<Table, Table>(
            coordinator,
            coordinator.inner::nextArrow,
            coordinator.inner::unsubscribeArrow,
            new Projection<Table, Table>(Projection.Kind.ONE) {
              @Override
              Table one(Table batch) {
                return batch;
              }
            },
            ReadMode.ARROW));
  }
}
