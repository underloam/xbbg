package io.github.underloam.xbbg.subscription;

import java.util.List;
import java.util.Optional;

/**
 * How a view turns native batches into values (the JavaScript {@code SubscriptionBatchProjection}):
 * {@link Kind#MANY} values per batch (scalar ticks), {@link Kind#ONE} value per batch (Arrow
 * tables), or {@link Kind#PUBLICATION} batches that are committed or rolled back at the final
 * publication point (marked deliveries).
 *
 * @param <B> the native batch type
 * @param <V> the value type
 */
abstract class Projection<B, V> {
  enum Kind {
    MANY,
    ONE,
    PUBLICATION
  }

  final Kind kind;

  Projection(Kind kind) {
    this.kind = kind;
  }

  /** {@link Kind#MANY}: every value of one batch, possibly none. */
  List<V> many(B batch) {
    throw new UnsupportedOperationException();
  }

  /** {@link Kind#ONE}: the batch's value. */
  V one(B batch) {
    throw new UnsupportedOperationException();
  }

  /** {@link Kind#PUBLICATION}: whether the batch is a terminal error receipt. */
  boolean hasError(B batch) {
    throw new UnsupportedOperationException();
  }

  /**
   * {@link Kind#PUBLICATION}: commits the batch; empty when close won. A terminal error receipt
   * throws its error.
   */
  Optional<V> publish(B batch) {
    throw new UnsupportedOperationException();
  }

  /** {@link Kind#PUBLICATION}: returns the batch to the native source. */
  void rollback(B batch) {
    throw new UnsupportedOperationException();
  }
}
