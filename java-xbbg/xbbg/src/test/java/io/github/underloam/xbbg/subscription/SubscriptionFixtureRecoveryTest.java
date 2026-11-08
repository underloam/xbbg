package io.github.underloam.xbbg.subscription;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.underloam.xbbg.Backend;
import io.github.underloam.xbbg.BlpSubscriptionDataLossException;
import io.github.underloam.xbbg.BlpValidationException;
import io.github.underloam.xbbg.SubscriptionDeliveryMode;
import io.github.underloam.xbbg.internal.NativeDelivery;
import io.github.underloam.xbbg.internal.NativeDrain;
import io.github.underloam.xbbg.internal.NativeSubscription;
import io.github.underloam.xbbg.internal.NativeTestSupport;
import io.github.underloam.xbbg.internal.ResultKind;
import io.github.underloam.xbbg.internal.SubscriptionBridge;
import io.github.underloam.xbbg.table.Table;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * State recovery, frozen drains, publication-pause races and fixture input validation against the
 * synthetic source (napi-internals Appendix B §5.6). Runs only with {@code
 * XBBG_TEST_SUBSCRIPTIONS=1}.
 */
class SubscriptionFixtureRecoveryTest {
  private static final String TOPIC = "SYNTHETIC Equity";
  private static final String FIRST = "FIRST SYNTHETIC Equity";
  private static final String SECOND = "SECOND SYNTHETIC Equity";
  private static final String INITPAINT =
      ",\"MKTDATA_EVENT_TYPE\":\"SUMMARY\",\"MKTDATA_EVENT_SUBTYPE\":\"INITPAINT\"}";
  private static final Duration WAIT = Duration.ofSeconds(10);

  @BeforeAll
  static void requireFixture() {
    Assumptions.assumeTrue(
        "1".equals(System.getenv("XBBG_TEST_SUBSCRIPTIONS")),
        "set XBBG_TEST_SUBSCRIPTIONS=1 to run the subscription fixture tests");
    NativeTestSupport.assumeAvailable();
  }

  private static List<String> list(String... values) {
    return Arrays.asList(values);
  }

  private static Map<String, Object> options(Object... namesAndValues) {
    Map<String, Object> options = new LinkedHashMap<String, Object>();
    for (int i = 0; i < namesAndValues.length; i += 2) {
      options.put((String) namesAndValues[i], namesAndValues[i + 1]);
    }
    return options;
  }

  private static Map<String, String> aliases() {
    Map<String, String> aliases = new LinkedHashMap<String, String>();
    aliases.put(FIRST, "first");
    aliases.put(SECOND, "second");
    return aliases;
  }

  private static Throwable failure(CompletableFuture<?> future) {
    ExecutionException failed =
        assertThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
    return failed.getCause();
  }

  private static <T> T done(CompletableFuture<T> future) throws Exception {
    return future.get(10, TimeUnit.SECONDS);
  }

  /** A table backend whose conversion fails at one zero-based call. */
  private static Backend<Table> failingAt(final int failAt, final String message) {
    final AtomicInteger calls = new AtomicInteger();
    return Backend.<Table, Table>map(
        Backend.TABLE,
        "failing",
        table -> {
          if (calls.getAndIncrement() == failAt) {
            throw new IllegalStateException(message);
          }
          return table;
        });
  }

  private static boolean valid(SubscriptionDelivery<Table> delivery) {
    return delivery.data().column("valid").getBoolean(0);
  }

  private static Double bid(SubscriptionDelivery<Table> delivery) {
    Table data = delivery.data();
    return data.column("BID").isNull(0) ? null : data.column("BID").getDouble(0);
  }

  @Test
  void stateDeliveriesConflateSparseUpdatesAndClearsIntoValidRows() throws Exception {
    try (SubscriptionFixture source = SubscriptionFixture.create(1, "drop_oldest");
        Subscription subscription =
            source.subscribe(
                list(TOPIC),
                list("BID", "ASK"),
                options("delivery", "state", "flushThreshold", 2L))) {
      source.publishJson(TOPIC, "{\"BID\":1.25,\"ASK\":2.25}");
      source.publishJson(TOPIC, "{\"BID\":3.75}");
      source.publishJson(TOPIC, "{\"ASK\":null}");
      assertEquals(SubscriptionDeliveryMode.STATE, subscription.delivery());
      assertInstanceOf(BlpValidationException.class, failure(subscription.nextAsync()));

      SubscriptionDelivery<Table> delivery = subscription.deliveries().next(WAIT).get();
      assertEquals(SubscriptionDeliveryMode.STATE, delivery.kind());
      assertFalse(delivery.gapBefore().isPresent());
      Table data = delivery.data();
      assertEquals(1, data.numRows());
      assertEquals(TOPIC, data.column("topic").getString(0));
      assertEquals(3.75, data.column("BID").getDouble(0));
      assertTrue(data.column("ASK").isNull(0));
      assertTrue(data.column("valid").getBoolean(0));
    }
  }

  @Test
  void dataLossEndsDeltasButInvalidatesAndRepaintsStateRows() throws Exception {
    try (SubscriptionFixture source = SubscriptionFixture.create(1, "drop_oldest");
        Subscription raw = source.subscribe(list(TOPIC), list("BID"), options());
        Subscription state =
            source.subscribe(list(TOPIC), list("BID"), options("delivery", "state"))) {
      source.publishJson(TOPIC, "{\"BID\":7}");
      assertTrue(raw.deliveries().next(WAIT).isPresent());
      assertTrue(valid(state.deliveries().next(WAIT).get()));

      source.dataLoss(TOPIC);
      assertInstanceOf(
          BlpSubscriptionDataLossException.class, failure(raw.deliveries().nextAsync()));
      assertEquals(Optional.empty(), raw.deliveries().next(WAIT));
      SubscriptionDelivery<Table> invalid = state.deliveries().next(WAIT).get();
      assertEquals(SubscriptionDeliveryMode.STATE, invalid.kind());
      assertFalse(valid(invalid));
      assertNull(bid(invalid));

      source.settle();
      source.publishJson(TOPIC, "{\"BID\":13.25" + INITPAINT);
      SubscriptionDelivery<Table> repaint = state.deliveries().next(WAIT).get();
      assertTrue(valid(repaint));
      assertEquals(13.25, bid(repaint));
      assertFalse(repaint.data().column("live").getBoolean(0));

      source.publishJson(TOPIC, "{\"BID\":14.25}");
      SubscriptionDelivery<Table> live = state.deliveries().next(WAIT).get();
      assertTrue(valid(live));
      assertTrue(live.data().column("live").getBoolean(0));
      assertEquals(14.25, bid(live));
    }
  }

  @Test
  void drainsKeepThePendingInvalidationAndTheRecoveredStateAcrossARetry() throws Exception {
    for (boolean retry : new boolean[] {false, true}) {
      try (SubscriptionFixture source = SubscriptionFixture.create(1, "drop_oldest");
          Subscription subscription =
              source.subscribe(
                  list(TOPIC), list("BID"), options("delivery", "state", "flushThreshold", 1L))) {
        source.publishJson(TOPIC, "{\"BID\":1.25}");
        assertTrue(subscription.deliveries().next(WAIT).isPresent());
        source.dataLoss(TOPIC);
        source.settle();
        source.publishJson(TOPIC, "{\"BID\":2.5" + INITPAINT);
        source.publishJson(TOPIC, "{\"BID\":3.75}");

        if (retry) {
          Backend<Table> failing = failingAt(1, "synthetic recovered-state drain failure");
          Throwable error = failure(subscription.deliveries(failing).unsubscribe(true));
          assertEquals("synthetic recovered-state drain failure", error.getMessage());
          assertFalse(subscription.isActive());
        }
        List<SubscriptionDelivery<Table>> drained =
            done(subscription.deliveries().unsubscribe(true));
        assertEquals(2, drained.size(), "retry=" + retry);
        assertFalse(valid(drained.get(0)));
        assertNull(bid(drained.get(0)));
        assertTrue(valid(drained.get(1)));
        assertEquals(3.75, bid(drained.get(1)));
        assertFalse(drained.get(0).gapBefore().isPresent());
        assertFalse(drained.get(1).gapBefore().isPresent());
      }
    }
  }

  @Test
  void unsubscribeFreezesTheFinalStateOfEveryAlias() throws Exception {
    try (SubscriptionFixture source = SubscriptionFixture.create(1, "drop_oldest");
        Subscription subscription =
            source.subscribe(
                list(FIRST, SECOND),
                list("BID"),
                options("delivery", "state", "flushThreshold", 1L, "aliases", aliases()))) {
      source.publishJson(FIRST, "{\"BID\":1.25}");
      source.publishJson(SECOND, "{\"BID\":2.5}");

      List<SubscriptionDelivery<Table>> drained = done(subscription.deliveries().unsubscribe(true));
      assertEquals(2, drained.size());
      List<String> topics = new ArrayList<String>();
      for (SubscriptionDelivery<Table> delivery : drained) {
        assertEquals(SubscriptionDeliveryMode.STATE, delivery.kind());
        assertFalse(delivery.gapBefore().isPresent());
        assertTrue(valid(delivery));
        topics.add(delivery.data().column("topic").getString(0));
      }
      assertEquals(list("first", "second"), topics);
      assertEquals(1.25, bid(drained.get(0)));
      assertEquals(2.5, bid(drained.get(1)));
      assertFalse(subscription.isActive());
    }
  }

  @Test
  void aFailedRawDrainConversionIsRetriedWhole() throws Exception {
    for (int failAt : new int[] {0, 1}) {
      try (SubscriptionFixture source = SubscriptionFixture.create(2, "drop_oldest");
          Subscription subscription =
              source.subscribe(list(TOPIC), list("BID"), options("flushThreshold", 1L))) {
        for (double value : new double[] {1.25, 2.5, 3.75, 4.5}) {
          source.publishJson(TOPIC, "{\"BID\":" + value + "}");
        }
        Backend<Table> failing = failingAt(failAt, "synthetic raw drain conversion failure");
        Throwable error = failure(subscription.deliveries(failing).unsubscribe(true));
        assertEquals("synthetic raw drain conversion failure", error.getMessage());
        assertFalse(subscription.isActive());
        assertEquals(Optional.empty(), subscription.deliveries(failing).next(WAIT));

        List<SubscriptionDelivery<Table>> drained =
            done(subscription.deliveries().unsubscribe(true));
        assertEquals(2, drained.size(), "failAt=" + failAt);
        assertEquals(3.75, bid(drained.get(0)));
        assertEquals(4.5, bid(drained.get(1)));
        assertEquals(2, drained.get(0).gapBefore().get().droppedUpdates());
        assertFalse(drained.get(1).gapBefore().isPresent());
      }
    }
  }

  @Test
  void aFailedStateDrainConversionIsRetriedWithTheFrozenSnapshots() throws Exception {
    try (SubscriptionFixture source = SubscriptionFixture.create(1, "drop_oldest");
        Subscription subscription =
            source.subscribe(
                list(FIRST, SECOND),
                list("BID"),
                options("delivery", "state", "flushThreshold", 1L, "aliases", aliases()))) {
      source.publishJson(FIRST, "{\"BID\":1.25}");
      source.publishJson(SECOND, "{\"BID\":2.5}");
      Backend<Table> failing = failingAt(1, "synthetic state drain conversion failure");
      Throwable error = failure(subscription.deliveries(failing).unsubscribe(true));
      assertEquals("synthetic state drain conversion failure", error.getMessage());
      assertFalse(subscription.isActive());

      List<SubscriptionDelivery<Table>> drained = done(subscription.deliveries().unsubscribe(true));
      assertEquals(2, drained.size());
      assertEquals("first", drained.get(0).data().column("topic").getString(0));
      assertEquals(1.25, bid(drained.get(0)));
      assertTrue(valid(drained.get(0)));
      assertEquals("second", drained.get(1).data().column("topic").getString(0));
      assertEquals(2.5, bid(drained.get(1)));
      assertTrue(valid(drained.get(1)));
    }
  }

  @Test
  void signalClosingDuringAPausedPublicationKeepsTheDeliveryForTheDrain() throws Exception {
    try (SubscriptionFixture source = SubscriptionFixture.create(1, "drop_oldest")) {
      NativeSubscription subscription = source.subscribeNative(list(TOPIC), list("BID"), options());
      try {
        for (double value : new double[] {1.25, 2.5, 3.75}) {
          source.publishJson(TOPIC, "{\"BID\":" + value + "}");
        }
        SubscriptionFixture.PublicationPause pause = source.pauseNextDelivery(subscription);
        CompletableFuture<NativeDelivery> pending = subscription.nextDelivery(ResultKind.TABLE);
        pause.ready();
        subscription.signalClosing();
        pause.release();
        assertNull(done(pending));

        NativeDrain drain = done(subscription.unsubscribeDeliveries(true, ResultKind.TABLE));
        assertEquals(1, drain.deliveries().size());
        NativeDelivery delivery = drain.deliveries().get(0);
        assertEquals(3.75, ((Table) delivery.data()).column("BID").getDouble(0));
        assertTrue(delivery.hasGap());
        assertEquals(2, delivery.droppedUpdates());
        assertTrue(subscription.commitDrain(drain.receipt()));
        assertFalse(subscription.isActive());
      } finally {
        subscription.release();
      }
    }
  }

  @Test
  void cancellingAPausedReadSkipsTheBackendAndClosesEveryView() throws Exception {
    try (SubscriptionFixture source = SubscriptionFixture.create(1, "drop_oldest");
        Subscription subscription = source.subscribe(list(TOPIC), list("BID"), options())) {
      source.publishJson(TOPIC, "{\"BID\":9.5}");
      final AtomicInteger projects = new AtomicInteger();
      Backend<Table> counting =
          Backend.<Table, Table>map(
              Backend.TABLE,
              "counting",
              table -> {
                projects.incrementAndGet();
                return table;
              });
      DeliveryStream<Table> view = subscription.deliveries(counting);
      SubscriptionFixture.PublicationPause pause = source.pauseNextDelivery(subscription);
      CompletableFuture<Optional<SubscriptionDelivery<Table>>> pending = view.nextAsync();
      pause.ready();
      assertTrue(pending.cancel(true));
      pause.release();

      assertEquals(new ArrayList<SubscriptionDelivery<Table>>(), done(view.unsubscribe()));
      assertTrue(pending.isCancelled());
      assertEquals(0, projects.get());
      assertFalse(subscription.isActive());
      assertEquals(Optional.empty(), view.next(WAIT));
      assertEquals(Optional.empty(), subscription.next(WAIT));
      assertEquals(Optional.empty(), subscription.deliveries().next(WAIT));
    }
  }

  @Test
  void invalidFieldKeysAreRejectedWithoutPoisoningLaterUpdates() throws Exception {
    try (SubscriptionFixture source = SubscriptionFixture.create(4, "drop_newest");
        Subscription subscription = source.subscribe(list(TOPIC), list("BID"), options())) {
      for (String control : new String[] {"\\u0000", "\\u0001"}) {
        BlpValidationException rejected =
            assertThrows(
                BlpValidationException.class,
                () -> source.publishJson(TOPIC, "{\"BAD" + control + "KEY\":1}"));
        assertEquals(
            "Configuration error: synthetic field names contain XML 1.0-forbidden characters",
            rejected.getMessage());
      }
      source.publishJson(TOPIC, "{\"BID\":2.5}");

      DeliveryStream<Table> deliveries = subscription.deliveries();
      SubscriptionDelivery<Table> delivery = deliveries.next(WAIT).get();
      assertEquals(2.5, bid(delivery));
      assertFalse(delivery.gapBefore().isPresent());
      assertThrows(TimeoutException.class, () -> deliveries.next(Duration.ofMillis(200)));
    }
  }

  @Test
  void aCloseStartedInsideTheBackendHandsTheDeliveryToTheDrainingOwner() throws Exception {
    for (final boolean throwAfterClose : new boolean[] {false, true}) {
      try (SubscriptionFixture source = SubscriptionFixture.create(1, "drop_oldest");
          Subscription subscription = source.subscribe(list(TOPIC), list("BID"), options())) {
        for (double value : new double[] {1.25, 2.5, 3.75}) {
          source.publishJson(TOPIC, "{\"BID\":" + value + "}");
        }
        final AtomicReference<CompletableFuture<List<SubscriptionDelivery<Table>>>> closing =
            new AtomicReference<CompletableFuture<List<SubscriptionDelivery<Table>>>>();
        Backend<Table> reentrant =
            Backend.<Table, Table>map(
                Backend.TABLE,
                "reentrant",
                table -> {
                  closing.set(subscription.deliveries().unsubscribe(true));
                  if (throwAfterClose) {
                    throw new IllegalStateException(
                        "expected close handoff, not a late terminal read error");
                  }
                  return table;
                });

        assertEquals(Optional.empty(), subscription.deliveries(reentrant).next(WAIT));
        List<SubscriptionDelivery<Table>> drained = done(closing.get());
        assertEquals(1, drained.size(), "throwAfterClose=" + throwAfterClose);
        assertEquals(2, drained.get(0).gapBefore().get().droppedUpdates());
        assertEquals(3.75, bid(drained.get(0)));
        assertFalse(subscription.isActive());
      }
    }
  }

  @Test
  void aBlockingCloseInsideTheBackendStartsTheCloseWithoutDeadlock() throws Exception {
    try (SubscriptionFixture source = SubscriptionFixture.create(1, "drop_oldest")) {
      // Not a try-with-resources resource: the conversion below closes it explicitly.
      Subscription subscription = source.subscribe(list(TOPIC), list("BID"), options());
      try {
        source.publishJson(TOPIC, "{\"BID\":9.5}");
        final AtomicInteger conversions = new AtomicInteger();
        Backend<Table> closing =
            Backend.<Table, Table>map(
                Backend.TABLE,
                "closing",
                table -> {
                  conversions.incrementAndGet();
                  subscription.close();
                  return table;
                });

        // A close that waited for the read running its own conversion would time out here.
        assertEquals(Optional.empty(), subscription.deliveries(closing).next(WAIT));
        assertEquals(1, conversions.get());
        assertEquals(
            new ArrayList<SubscriptionDelivery<Table>>(),
            done(subscription.deliveries().unsubscribe(true)));
        assertFalse(subscription.isActive());
        assertEquals(Optional.empty(), subscription.next(WAIT));
      } finally {
        subscription.close();
      }
    }
  }

  @Test
  void anSdkTerminalFailureWinsOverThePlainDrainGuard() throws Exception {
    for (String mode : new String[] {"scalar", "arrow", "nativeScalar", "nativeArrow"}) {
      try (SubscriptionFixture source = SubscriptionFixture.create(4, "drop_oldest")) {
        NativeSubscription nativeSubscription =
            source.subscribeNative(list(TOPIC), list("BID"), options());
        try {
          source.publishJson(TOPIC, "{\"BID\":1.25}");
          source.dataLoss(TOPIC);
          assertTrue(nativeSubscription.requiresDeliveries());
          Subscription subscription = SubscriptionBridge.subscription(nativeSubscription);
          CompletableFuture<?> drain;
          if ("scalar".equals(mode)) {
            drain = subscription.unsubscribe(true);
          } else if ("arrow".equals(mode)) {
            drain = subscription.arrow().unsubscribe(true);
          } else if ("nativeScalar".equals(mode)) {
            drain = nativeSubscription.unsubscribeTicks(true);
          } else {
            drain = nativeSubscription.unsubscribeArrow(true);
          }
          Throwable error = failure(drain);
          assertInstanceOf(BlpSubscriptionDataLossException.class, error, mode);
          assertFalse(nativeSubscription.isActive(), mode);
        } finally {
          nativeSubscription.release();
        }
      }
    }
  }

  @Test
  void aDrainTerminalFailureIsPublishedBeforeAnyConversionAndConsumedOnce() throws Exception {
    try (SubscriptionFixture source = SubscriptionFixture.create(4, "drop_oldest")) {
      NativeSubscription nativeSubscription =
          source.subscribeNative(list(TOPIC), list("BID"), options());
      try {
        source.publishJson(TOPIC, "{\"BID\":1.25}");
        source.dataLoss(TOPIC);
        final AtomicInteger conversions = new AtomicInteger();
        Backend<Table> hiding =
            Backend.<Table, Table>map(
                Backend.TABLE,
                "hiding",
                table -> {
                  conversions.incrementAndGet();
                  throw new IllegalStateException("must not hide SDK failure");
                });
        DeliveryStream<Table> view =
            SubscriptionBridge.subscription(nativeSubscription).deliveries(hiding);

        assertInstanceOf(BlpSubscriptionDataLossException.class, failure(view.unsubscribe(true)));
        assertEquals(0, conversions.get());
        assertEquals(Optional.empty(), view.next(WAIT));
        NativeDrain again = done(nativeSubscription.unsubscribeDeliveries(true, ResultKind.TABLE));
        assertTrue(again.deliveries().isEmpty());
        assertEquals(0, again.receipt());
      } finally {
        nativeSubscription.release();
      }
    }
  }
}
