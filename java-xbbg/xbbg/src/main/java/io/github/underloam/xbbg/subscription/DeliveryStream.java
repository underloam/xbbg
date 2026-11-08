package io.github.underloam.xbbg.subscription;

import io.github.underloam.xbbg.Backend;
import io.github.underloam.xbbg.internal.Backends;
import io.github.underloam.xbbg.internal.NativeDelivery;
import io.github.underloam.xbbg.internal.NativeDrain;
import io.github.underloam.xbbg.internal.NativeSubscription;
import io.github.underloam.xbbg.internal.ResultKind;
import io.github.underloam.xbbg.subscription.SubscriptionCoordinator.ReadMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Marked deliveries of a {@link Subscription} ({@link Subscription#deliveries(Backend)}): each
 * {@link SubscriptionDelivery} carries its data in the backend's representation and the continuity
 * gap before it. Required for {@code delivery=state} and {@code drop_oldest} subscriptions.
 *
 * <p>Publication is transactional: a delivery is acknowledged only when it reaches the caller. A
 * backend conversion failure rolls the data, gap and any terminal error back, keeps the
 * subscription open and fails the read with the original error; another backend can then read the
 * same delivery. A close that wins the race returns an in-flight delivery to the draining owner.
 * Delivery views with different backends share the {@code delivery} read format.
 *
 * @param <T> the data representation
 */
public final class DeliveryStream<T> extends SubscriptionView<SubscriptionDelivery<T>> {
  DeliveryStream(SubscriptionCoordinator coordinator, Backend<T> backend) {
    super(reader(coordinator, backend));
  }

  /** A native delivery batch: a receipt to publish, a terminal error, or a drained value. */
  private static final class Batch<T> {
    final long receipt;
    final boolean error;
    final SubscriptionDelivery<T> value;

    Batch(long receipt, boolean error, SubscriptionDelivery<T> value) {
      this.receipt = receipt;
      this.error = error;
      this.value = value;
    }

    boolean drained() {
      return receipt == 0 && !error;
    }
  }

  private static <T> SubscriptionDelivery<T> project(
      SubscriptionCoordinator coordinator,
      final Backend<T> backend,
      final NativeDelivery delivery) {
    T data = coordinator.insideRead(() -> Backends.fromNative(backend, delivery.data()));
    SubscriptionGap gap =
        delivery.hasGap()
            ? new SubscriptionGap(SubscriptionGap.LOCAL_OVERFLOW, delivery.droppedUpdates())
            : null;
    return new SubscriptionDelivery<T>(delivery.kind(), data, gap);
  }

  private static <T> ViewIterator<Batch<T>, SubscriptionDelivery<T>> reader(
      final SubscriptionCoordinator coordinator, final Backend<T> backend) {
    final NativeSubscription inner = coordinator.inner;
    final ResultKind format = Backends.nativeKind(backend);
    return new ViewIterator<Batch<T>, SubscriptionDelivery<T>>(
        coordinator,
        () ->
            inner.nextDelivery(format).thenApply(delivery -> read(coordinator, backend, delivery)),
        drain ->
            inner
                .unsubscribeDeliveries(drain, format)
                .thenApply(nativeDrain -> drained(coordinator, backend, nativeDrain)),
        new Projection<Batch<T>, SubscriptionDelivery<T>>(Projection.Kind.PUBLICATION) {
          @Override
          boolean hasError(Batch<T> batch) {
            return batch.error;
          }

          @Override
          Optional<SubscriptionDelivery<T>> publish(Batch<T> batch) {
            if (batch.drained()) {
              return Optional.of(batch.value);
            }
            if (batch.error) {
              // Throws the terminal error once; false means close won.
              if (!inner.commitDelivery(batch.receipt)) {
                return Optional.empty();
              }
              throw new IllegalStateException(
                  "native terminal receipt committed without publishing its error");
            }
            return inner.commitDelivery(batch.receipt)
                ? Optional.of(batch.value)
                : Optional.<SubscriptionDelivery<T>>empty();
          }

          @Override
          void rollback(Batch<T> batch) {
            if (!batch.drained()) {
              inner.rollbackDelivery(batch.receipt);
            }
          }
        },
        ReadMode.DELIVERY);
  }

  private static <T> Batch<T> read(
      SubscriptionCoordinator coordinator, Backend<T> backend, NativeDelivery delivery) {
    NativeSubscription inner = coordinator.inner;
    if (delivery == null) {
      return null;
    }
    if (delivery.conversionFailure() != null) {
      throw new ProjectionError(delivery.conversionFailure());
    }
    if (delivery.isError()) {
      return new Batch<T>(delivery.receipt(), true, null);
    }
    SubscriptionDelivery<T> value;
    try {
      value = project(coordinator, backend, delivery);
    } catch (RuntimeException | Error failure) {
      inner.rollbackDelivery(delivery.receipt());
      throw new ProjectionError(failure);
    }
    return new Batch<T>(delivery.receipt(), false, value);
  }

  private static <T> List<Batch<T>> drained(
      SubscriptionCoordinator coordinator, Backend<T> backend, NativeDrain drain) {
    NativeSubscription inner = coordinator.inner;
    if (drain.conversionFailure() != null) {
      throw new ProjectionError(drain.conversionFailure());
    }
    List<Batch<T>> values = new ArrayList<Batch<T>>(drain.deliveries().size());
    try {
      for (NativeDelivery delivery : drain.deliveries()) {
        values.add(new Batch<T>(0, false, project(coordinator, backend, delivery)));
      }
    } catch (RuntimeException | Error failure) {
      if (drain.receipt() != 0) {
        inner.rollbackDrain(drain.receipt());
      }
      throw new ProjectionError(failure);
    }
    if (drain.receipt() != 0) {
      inner.commitDrain(drain.receipt());
    }
    return values;
  }
}
