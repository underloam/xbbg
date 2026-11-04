package io.github.underloam.xbbg.subscription;

import static io.github.underloam.xbbg.subscription.FakeNativeSubscription.ticks;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.underloam.xbbg.Backend;
import io.github.underloam.xbbg.BlpValidationException;
import io.github.underloam.xbbg.SubscriptionDeliveryMode;
import io.github.underloam.xbbg.internal.NativeDelivery;
import io.github.underloam.xbbg.internal.NativeDrain;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Read serialization, read-format claims and the shared close, over a scripted native side. */
class SubscriptionCoordinatorTest {
  private static final Duration SHORT = Duration.ofMillis(20);

  private static <T> T done(CompletableFuture<T> future) throws Exception {
    return future.get(5, TimeUnit.SECONDS);
  }

  private static Throwable failure(CompletableFuture<?> future) {
    ExecutionException failed =
        assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
    return failed.getCause();
  }

  private static List<Double> bids(List<Tick> ticks) {
    List<Double> values = new ArrayList<Double>();
    for (Tick tick : ticks) {
      values.add(tick.getDouble("BID").getAsDouble());
    }
    return values;
  }

  private static double bid(Optional<Tick> tick) {
    return tick.get().getDouble("BID").getAsDouble();
  }

  @Test
  void theFirstReadClaimsTheFormatWithoutStealingData() throws Exception {
    FakeNativeSubscription fake = new FakeNativeSubscription();
    Subscription subscription = new Subscription(fake);
    CompletableFuture<Optional<Tick>> first = subscription.nextAsync();
    fake.tickReads.get(0).complete(ticks(true, "A", 1.0));
    assertEquals(1.0, bid(done(first)));

    Throwable arrow = failure(subscription.arrow().nextAsync());
    assertInstanceOf(IllegalStateException.class, arrow);
    assertEquals(
        "subscription is already being read as scalar; cannot also read as arrow",
        arrow.getMessage());
    Throwable delivery = failure(subscription.deliveries(Backend.IPC_STREAM).nextAsync());
    assertEquals(
        "subscription is already being read as scalar; cannot also read as delivery",
        delivery.getMessage());
    assertTrue(fake.arrowReads.isEmpty());
    assertTrue(fake.deliveryReads.isEmpty());
    assertTrue(fake.closes.isEmpty());

    CompletableFuture<Optional<Tick>> second = subscription.nextAsync();
    fake.tickReads.get(1).complete(ticks(false, "A", 2.0));
    assertEquals(2.0, bid(done(second)));
  }

  @Test
  void aMismatchedDrainClosesTheSubscriptionThenFails() throws Exception {
    FakeNativeSubscription fake = new FakeNativeSubscription();
    Subscription subscription = new Subscription(fake);
    CompletableFuture<Optional<Tick>> first = subscription.nextAsync();
    fake.tickReads.get(0).complete(ticks(true, "A", 1.0));
    done(first);

    assertInstanceOf(IllegalStateException.class, failure(subscription.arrow().unsubscribe(true)));
    assertEquals(Collections.singletonList("arrow:false"), fake.closes);
    assertEquals(Optional.empty(), done(subscription.nextAsync()));
  }

  @Test
  void deliveryOnlySubscriptionsGuardPlainReadsAndDrains() throws Exception {
    FakeNativeSubscription fake = new FakeNativeSubscription(true, true);
    fake.deliveryDrain = NativeDrain.of(new ArrayList<NativeDelivery>(), 5);
    Subscription subscription = new Subscription(fake);

    Throwable scalar = failure(subscription.nextAsync());
    assertInstanceOf(BlpValidationException.class, scalar);
    assertEquals(Optional.of("delivery"), ((BlpValidationException) scalar).element());
    assertInstanceOf(BlpValidationException.class, failure(subscription.arrow().nextAsync()));
    assertTrue(fake.tickReads.isEmpty());

    // A plain drain drains through the marked API first, so no SDK failure is discarded.
    assertInstanceOf(BlpValidationException.class, failure(subscription.unsubscribe(true)));
    assertEquals(Collections.singletonList("deliveries:true"), fake.closes);
    assertEquals(Collections.singletonList(5L), fake.commits);
  }

  @Test
  void imageOnlySubscriptionsRejectEveryRowRead() {
    FakeNativeSubscription fake = new FakeNativeSubscription(false, false);
    Subscription subscription = new Subscription(fake);
    Throwable error = failure(subscription.nextAsync());
    assertInstanceOf(BlpValidationException.class, error);
    assertEquals(Optional.of("rows"), ((BlpValidationException) error).element());
    assertInstanceOf(
        BlpValidationException.class,
        failure(subscription.deliveries(Backend.IPC_STREAM).nextAsync()));
    assertTrue(fake.tickReads.isEmpty() && fake.deliveryReads.isEmpty());
  }

  @Test
  void readsAreSerializedAndShareOneNativeBatch() throws Exception {
    FakeNativeSubscription fake = new FakeNativeSubscription();
    Subscription subscription = new Subscription(fake);
    CompletableFuture<Optional<Tick>> first = subscription.nextAsync();
    CompletableFuture<Optional<Tick>> second = subscription.nextAsync();
    CompletableFuture<Optional<Tick>> third = subscription.nextAsync();
    assertEquals(1, fake.tickReads.size());
    assertFalse(first.isDone() || second.isDone() || third.isDone());

    fake.tickReads.get(0).complete(ticks(true, "A", 1.0, 2.0));
    assertEquals(1.0, bid(done(first)));
    assertEquals(2.0, bid(done(second)));
    assertFalse(third.isDone());
    assertEquals(2, fake.tickReads.size());

    fake.tickReads.get(1).complete(ticks(false, "A", 3.0));
    assertEquals(3.0, bid(done(third)));
  }

  @Test
  void deliveryViewsWithDifferentBackendsShareSerializedReads() throws Exception {
    FakeNativeSubscription fake = new FakeNativeSubscription();
    Subscription subscription = new Subscription(fake);
    CompletableFuture<Optional<SubscriptionDelivery<byte[]>>> stream =
        subscription.deliveries(Backend.IPC_STREAM).nextAsync();
    CompletableFuture<Optional<SubscriptionDelivery<byte[]>>> file =
        subscription.deliveries(Backend.IPC_FILE).nextAsync();
    assertEquals(1, fake.deliveryReads.size());

    fake.deliveryReads
        .get(0)
        .complete(
            NativeDelivery.data(SubscriptionDeliveryMode.DELTAS, new byte[] {1}, false, 0, 1));
    assertArrayEquals(new byte[] {1}, done(stream).get().data());
    assertEquals(2, fake.deliveryReads.size());
    fake.deliveryReads
        .get(1)
        .complete(NativeDelivery.data(SubscriptionDeliveryMode.DELTAS, new byte[] {2}, true, 3, 2));
    SubscriptionDelivery<byte[]> delivery = done(file).get();
    assertArrayEquals(new byte[] {2}, delivery.data());
    assertEquals(3, delivery.gapBefore().get().droppedUpdates());
    assertEquals(Arrays.asList(1L, 2L), fake.commits);
  }

  @Test
  void aCloseIsSharedByEveryView() throws Exception {
    FakeNativeSubscription fake = new FakeNativeSubscription();
    Subscription subscription = new Subscription(fake);
    ArrowSubscription arrow = subscription.arrow();

    assertEquals(Collections.emptyList(), done(subscription.unsubscribe()));
    assertEquals(Collections.singletonList("ticks:false"), fake.closes);
    assertEquals(1, fake.signalled.get());

    assertEquals(Optional.empty(), done(arrow.nextAsync()));
    assertEquals(Collections.emptyList(), done(arrow.unsubscribe(true)));
    assertEquals(Optional.empty(), done(subscription.deliveries(Backend.IPC_STREAM).nextAsync()));
    assertEquals(Collections.singletonList("ticks:false"), fake.closes);
    assertTrue(fake.arrowReads.isEmpty() && fake.deliveryReads.isEmpty());
    assertEquals(1, fake.signalled.get());
  }

  @Test
  void drainsReturnBufferedValuesBeforeTheNativeDrain() throws Exception {
    FakeNativeSubscription fake = new FakeNativeSubscription();
    Subscription subscription = new Subscription(fake);
    CompletableFuture<Optional<Tick>> first = subscription.nextAsync();
    fake.tickReads.get(0).complete(ticks(true, "A", 1.0, 2.0, 3.0));
    assertEquals(1.0, bid(done(first)));

    fake.tickDrain = Collections.singletonList(ticks(false, "A", 4.0));
    assertEquals(Arrays.asList(2.0, 3.0, 4.0), bids(done(subscription.unsubscribe(true))));
    assertEquals(Collections.singletonList("ticks:true"), fake.closes);
  }

  @Test
  void drainsPlaceAnInFlightBatchBeforeTheNativeDrain() throws Exception {
    FakeNativeSubscription fake = new FakeNativeSubscription();
    Subscription subscription = new Subscription(fake);
    CompletableFuture<Optional<Tick>> read = subscription.nextAsync();
    fake.tickDrain = Collections.singletonList(ticks(false, "A", 3.0));
    CompletableFuture<List<Tick>> close = subscription.unsubscribe(true);
    assertFalse(close.isDone());

    fake.tickReads.get(0).complete(ticks(true, "A", 1.0, 2.0));
    assertEquals(Optional.empty(), done(read));
    assertEquals(Arrays.asList(1.0, 2.0, 3.0), bids(done(close)));
  }

  @Test
  void aCloseWaitsForTheReadInFlightAndDiscardsItsData() throws Exception {
    FakeNativeSubscription fake = new FakeNativeSubscription();
    Subscription subscription = new Subscription(fake);
    CompletableFuture<Optional<Tick>> read = subscription.nextAsync();
    CompletableFuture<List<Tick>> close = subscription.unsubscribe(false);
    assertEquals(Collections.singletonList("ticks:false"), fake.closes);
    assertEquals(1, fake.signalled.get());
    assertFalse(close.isDone());
    assertFalse(read.isDone());

    fake.tickReads.get(0).complete(ticks(true, "A", 1.0));
    assertEquals(Optional.empty(), done(read));
    assertEquals(Collections.emptyList(), done(close));
    assertEquals(Collections.emptyList(), done(subscription.unsubscribe(true)));
    assertEquals(Collections.singletonList("ticks:false"), fake.closes);
  }

  @Test
  void aCloseThatWinsRollsAnInFlightDeliveryBack() throws Exception {
    FakeNativeSubscription fake = new FakeNativeSubscription();
    Subscription subscription = new Subscription(fake);
    DeliveryStream<byte[]> deliveries = subscription.deliveries(Backend.IPC_STREAM);
    CompletableFuture<Optional<SubscriptionDelivery<byte[]>>> read = deliveries.nextAsync();
    CompletableFuture<List<SubscriptionDelivery<byte[]>>> close = deliveries.unsubscribe(false);

    fake.deliveryReads
        .get(0)
        .complete(
            NativeDelivery.data(SubscriptionDeliveryMode.DELTAS, new byte[] {1}, false, 0, 9));
    assertEquals(Optional.empty(), done(read));
    assertEquals(Collections.emptyList(), done(close));
    assertEquals(Collections.singletonList(9L), fake.rollbacks);
    assertTrue(fake.commits.isEmpty());
    assertEquals(Collections.singletonList("deliveries:false"), fake.closes);
  }

  @Test
  void aFailedBackendConversionRollsBackForAnotherBackend() throws Exception {
    FakeNativeSubscription fake = new FakeNativeSubscription();
    Subscription subscription = new Subscription(fake);
    final IllegalStateException boom = new IllegalStateException("synthetic projector failure");
    Backend<byte[]> failing =
        Backend.<byte[], byte[]>map(
            Backend.IPC_STREAM,
            "failing",
            bytes -> {
              throw boom;
            });

    CompletableFuture<Optional<SubscriptionDelivery<byte[]>>> failed =
        subscription.deliveries(failing).nextAsync();
    fake.deliveryReads
        .get(0)
        .complete(
            NativeDelivery.data(SubscriptionDeliveryMode.DELTAS, new byte[] {7}, true, 2, 11));
    assertSame(boom, failure(failed));
    assertEquals(Collections.singletonList(11L), fake.rollbacks);
    assertTrue(fake.commits.isEmpty());
    assertTrue(fake.closes.isEmpty());

    CompletableFuture<Optional<SubscriptionDelivery<byte[]>>> retried =
        subscription.deliveries(Backend.IPC_STREAM).nextAsync();
    fake.deliveryReads
        .get(1)
        .complete(
            NativeDelivery.data(SubscriptionDeliveryMode.DELTAS, new byte[] {7}, true, 2, 12));
    SubscriptionDelivery<byte[]> delivery = done(retried).get();
    assertArrayEquals(new byte[] {7}, delivery.data());
    assertEquals(2, delivery.gapBefore().get().droppedUpdates());
    assertEquals(Collections.singletonList(12L), fake.commits);
  }

  @Test
  void cancellingAReadClosesTheSubscriptionWithoutCancellingTheNativeRead() throws Exception {
    FakeNativeSubscription fake = new FakeNativeSubscription();
    Subscription subscription = new Subscription(fake);
    CompletableFuture<Optional<Tick>> read = subscription.nextAsync();
    assertTrue(read.cancel(true));

    assertEquals(Collections.singletonList("ticks:false"), fake.closes);
    assertEquals(1, fake.signalled.get());
    assertFalse(fake.tickReads.get(0).isCancelled());
    assertEquals(Optional.empty(), done(subscription.nextAsync()));

    CompletableFuture<List<Tick>> close = subscription.unsubscribe(false);
    fake.tickReads.get(0).complete(null);
    assertEquals(Collections.emptyList(), done(close));
    assertEquals(1, fake.tickReads.size());
  }

  @Test
  void aTimedOutReadStaysPendingForTheNextCall() throws Exception {
    FakeNativeSubscription fake = new FakeNativeSubscription();
    Subscription subscription = new Subscription(fake);
    assertThrows(TimeoutException.class, () -> subscription.next(SHORT));
    assertThrows(TimeoutException.class, () -> subscription.next(SHORT));
    assertEquals(1, fake.tickReads.size());

    fake.tickReads.get(0).complete(ticks(true, "A", 1.0));
    assertEquals(1.0, bid(subscription.next(Duration.ofSeconds(5))));
    assertEquals(1, fake.tickReads.size());
    assertTrue(fake.closes.isEmpty());
  }

  @Test
  void anInterruptedWaitKeepsTheReadAndTheInterruptFlag() throws Exception {
    FakeNativeSubscription fake = new FakeNativeSubscription();
    Subscription subscription = new Subscription(fake);
    Thread.currentThread().interrupt();
    CancellationException cancelled =
        assertThrows(CancellationException.class, () -> subscription.next(Duration.ofSeconds(5)));
    assertTrue(Thread.interrupted());
    assertInstanceOf(InterruptedException.class, cancelled.getCause());
    assertTrue(fake.closes.isEmpty());

    fake.tickReads.get(0).complete(ticks(true, "A", 1.0));
    assertEquals(1.0, subscription.iterator().next().getDouble("BID").getAsDouble());
    assertEquals(1, fake.tickReads.size());
  }

  @Test
  void theEndOfTheStreamClosesEveryView() throws Exception {
    FakeNativeSubscription fake = new FakeNativeSubscription();
    Subscription subscription = new Subscription(fake);
    CompletableFuture<Optional<Tick>> read = subscription.nextAsync();
    fake.tickReads.get(0).complete(null);
    assertEquals(Optional.empty(), done(read));
    assertFalse(subscription.iterator().hasNext());
    assertEquals(Collections.emptyList(), done(subscription.unsubscribe(true)));
    assertTrue(fake.closes.isEmpty());
  }

  @Test
  void aReadFailureClosesTheSubscriptionAndIsReportedOnce() throws Exception {
    FakeNativeSubscription fake = new FakeNativeSubscription();
    Subscription subscription = new Subscription(fake);
    CompletableFuture<Optional<Tick>> read = subscription.nextAsync();
    IllegalStateException lost = new IllegalStateException("data loss");
    fake.tickReads.get(0).completeExceptionally(lost);
    assertSame(lost, failure(read));
    assertEquals(Collections.singletonList("ticks:false"), fake.closes);
    assertEquals(Optional.empty(), done(subscription.nextAsync()));
  }

  @Test
  @Timeout(10)
  void blockingCallsInsideABackendConversionDoNotWaitForThemselves() throws Exception {
    FakeNativeSubscription fake = new FakeNativeSubscription();
    final Subscription subscription = new Subscription(fake);
    Backend<byte[]> blockingRead =
        Backend.<byte[], byte[]>map(
            Backend.IPC_STREAM,
            "blockingRead",
            bytes -> {
              assertFalse(subscription.iterator().hasNext(), "hasNext must throw here");
              return bytes;
            });
    CompletableFuture<Optional<SubscriptionDelivery<byte[]>>> read =
        subscription.deliveries(blockingRead).nextAsync();
    fake.deliveryReads
        .get(0)
        .complete(
            NativeDelivery.data(SubscriptionDeliveryMode.DELTAS, new byte[] {1}, false, 0, 1));
    Throwable error = failure(read);
    assertInstanceOf(IllegalStateException.class, error);
    assertTrue(error.getMessage().startsWith("a blocking read inside"), error.getMessage());
    assertEquals(Collections.singletonList(1L), fake.rollbacks);
    assertTrue(fake.closes.isEmpty());

    // close() inside the conversion starts the close; waiting would deadlock on this read.
    Backend<byte[]> closing =
        Backend.<byte[], byte[]>map(
            Backend.IPC_STREAM,
            "closing",
            bytes -> {
              subscription.close();
              return bytes;
            });
    CompletableFuture<Optional<SubscriptionDelivery<byte[]>>> closed =
        subscription.deliveries(closing).nextAsync();
    fake.deliveryReads
        .get(1)
        .complete(
            NativeDelivery.data(SubscriptionDeliveryMode.DELTAS, new byte[] {2}, false, 0, 2));
    assertEquals(Optional.empty(), done(closed));
    assertEquals(Collections.singletonList("ticks:false"), fake.closes);
    assertEquals(Arrays.asList(1L, 2L), fake.rollbacks);
    assertTrue(fake.commits.isEmpty());
    assertEquals(Collections.emptyList(), done(subscription.unsubscribe()));

    // Outside a conversion close() waits as usual.
    subscription.close();
  }

  /**
   * Completes a read from another thread, as the native bridge does; a daemon thread, so a
   * regression that blocks it cannot keep the test JVM alive.
   */
  private static void completeAsync(FakeNativeSubscription fake, int read, double bid) {
    Thread completer =
        new Thread(
            () -> fake.tickReads.get(read).complete(ticks(true, "A", bid)), "fake-native-read");
    completer.setDaemon(true);
    completer.start();
  }

  @Test
  @Timeout(10)
  void closingFromAReadCallbackDoesNotWaitForItself() throws Exception {
    FakeNativeSubscription fake = new FakeNativeSubscription();
    final Subscription subscription = new Subscription(fake);
    CompletableFuture<Void> callback =
        subscription.nextAsync().thenAccept(tick -> subscription.close());
    completeAsync(fake, 0, 1.0);
    // Before the fix the callback waited for the read that was running it, forever.
    done(callback);
    assertEquals(Collections.singletonList("ticks:false"), fake.closes);
    assertEquals(Optional.empty(), done(subscription.nextAsync()));
    assertEquals(Collections.emptyList(), done(subscription.unsubscribe()));
  }

  @Test
  @Timeout(10)
  void blockingReadsInsideAReadCallbackFailInsteadOfWaiting() throws Exception {
    FakeNativeSubscription fake = new FakeNativeSubscription();
    final Subscription subscription = new Subscription(fake);
    CompletableFuture<List<Throwable>> callback =
        subscription
            .nextAsync()
            .thenApply(tick -> {
              List<Throwable> caught = new ArrayList<Throwable>();
              try {
                for (Tick next : subscription) {
                  caught.add(new AssertionError("iterated " + next));
                }
              } catch (IllegalStateException expected) {
                caught.add(expected);
              }
              try {
                subscription.next(Duration.ofSeconds(5));
              } catch (IllegalStateException | TimeoutException expected) {
                caught.add(expected);
              }
              return caught;
            });
    completeAsync(fake, 0, 1.0);
    List<Throwable> caught = done(callback);
    assertEquals(2, caught.size(), caught.toString());
    for (Throwable error : caught) {
      assertInstanceOf(IllegalStateException.class, error);
      assertTrue(error.getMessage().contains("read callback"), error.getMessage());
    }
    // Outside the callback, reads work again; the next read starts once the first one settled.
    CompletableFuture<Optional<Tick>> later = subscription.nextAsync();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (fake.tickReads.size() < 2 && System.nanoTime() < deadline) {
      Thread.sleep(1);
    }
    fake.tickReads.get(1).complete(ticks(false, "A", 2.0));
    assertEquals(2.0, bid(done(later)));
    assertTrue(fake.closes.isEmpty());
  }

  @Test
  @Timeout(10)
  void negativeAndHugeTimeoutsNeitherHangNorOverflow() throws Exception {
    FakeNativeSubscription fake = new FakeNativeSubscription();
    Subscription subscription = new Subscription(fake);
    // A deadline that already passed times out at once and leaves the read in flight.
    assertThrows(TimeoutException.class, () -> subscription.next(Duration.ofSeconds(-5)));
    assertEquals(1, fake.tickReads.size());
    fake.tickReads.get(0).complete(ticks(true, "A", 1.0));
    assertEquals(1.0, bid(subscription.next(Duration.ofSeconds(Long.MAX_VALUE))));
    assertEquals(1, fake.tickReads.size(), "the carried read was reused");
    assertEquals(0, SubscriptionView.timeoutNanos(Duration.ofNanos(-1)));
    assertEquals(7, SubscriptionView.timeoutNanos(Duration.ofNanos(7)));
    assertEquals(Long.MAX_VALUE, SubscriptionView.timeoutNanos(Duration.ofDays(400 * 366L)));
    assertThrows(NullPointerException.class, () -> subscription.next(null));
  }

  @Test
  @Timeout(20)
  void readsLeftByTimedOutCallsAreHandedOutInStartOrder() throws Exception {
    FakeNativeSubscription fake = new FakeNativeSubscription();
    final Subscription subscription = new Subscription(fake);
    // Read 1 starts first with the longer timeout; read 2 starts second and times out first.
    CompletableFuture<Throwable> first =
        CompletableFuture.supplyAsync(() -> {
          try {
            return new AssertionError(
                "read 1 returned " + subscription.next(SHORT.multipliedBy(25)));
          } catch (TimeoutException expected) {
            return expected;
          }
        });
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (fake.tickReads.isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(1);
    }
    assertEquals(1, fake.tickReads.size(), "read 1 started");
    assertThrows(TimeoutException.class, () -> subscription.next(SHORT));
    assertInstanceOf(TimeoutException.class, done(first));

    fake.tickReads.get(0).complete(ticks(true, "A", 1.0));
    fake.tickReads.get(1).complete(ticks(false, "A", 2.0));
    assertEquals(1.0, bid(done(subscription.nextAsync())));
    assertEquals(2.0, bid(done(subscription.nextAsync())));
    assertEquals(2, fake.tickReads.size(), "no value was lost to a third read");
  }
}
