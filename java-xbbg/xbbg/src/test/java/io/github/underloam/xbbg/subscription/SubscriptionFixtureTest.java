package io.github.underloam.xbbg.subscription;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.underloam.xbbg.Backend;
import io.github.underloam.xbbg.BlpInternalException;
import io.github.underloam.xbbg.BlpSubscriptionDataLossException;
import io.github.underloam.xbbg.BlpValidationException;
import io.github.underloam.xbbg.SubscriptionDeliveryMode;
import io.github.underloam.xbbg.internal.NativeTestSupport;
import io.github.underloam.xbbg.table.Table;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Subscription behaviour against the synthetic source of the test-support natives. Runs only with
 * {@code XBBG_TEST_SUBSCRIPTIONS=1}.
 */
class SubscriptionFixtureTest {
  private static final String TOPIC = "SYNTHETIC Equity";
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

  private static Throwable failure(CompletableFuture<?> future) throws Exception {
    ExecutionException failed =
        assertThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
    return failed.getCause();
  }

  @Test
  void scalarTicksReportPresentClearedAndAbsentFields() throws Exception {
    try (SubscriptionFixture source = SubscriptionFixture.create();
        Subscription subscription =
            source.subscribe(list(TOPIC), list("BID", "FLAG", "NAME"), options())) {
      source.publishJson(TOPIC, "{\"BID\":4.5,\"FLAG\":true,\"NAME\":\"first\"}");
      source.publishJson(TOPIC, "{\"BID\":null,\"NAME\":\"second\"}");
      Tick first = subscription.next(WAIT).get();
      Tick second = subscription.next(WAIT).get();

      assertEquals(TOPIC, first.topic());
      assertEquals(4.5, first.getDouble("BID").getAsDouble());
      assertEquals(true, first.get("FLAG"));
      assertEquals("first", first.getString("NAME").get());
      assertTrue(first.timestampUs() > 0);

      assertTrue(second.has("BID"));
      assertTrue(second.isNull("BID"));
      assertFalse(second.has("FLAG"));
      assertFalse(second.getDouble("FLAG").isPresent());
      assertEquals("second", second.get(subscription.field("NAME")));
    }
  }

  @Test
  void drainingUnsubscribeReturnsUnreadTicksAndEndsTheStream() throws Exception {
    try (SubscriptionFixture source = SubscriptionFixture.create()) {
      Subscription subscription = source.subscribe(list(TOPIC), list("BID"), options());
      source.publishJson(TOPIC, "{\"BID\":1.25}");
      source.publishJson(TOPIC, "{\"BID\":2.5}");
      source.settle();

      List<Tick> drained = subscription.unsubscribe(true).get(10, TimeUnit.SECONDS);
      assertEquals(2, drained.size());
      assertEquals(2.5, drained.get(1).getDouble("BID").getAsDouble());
      assertEquals(Optional.empty(), subscription.next(WAIT));
      assertFalse(subscription.isActive());
      assertEquals(Collections.emptyList(), subscription.unsubscribe(true).get());
    }
  }

  @Test
  void theFirstReadFixesTheReadFormat() throws Exception {
    try (SubscriptionFixture source = SubscriptionFixture.create();
        Subscription subscription = source.subscribe(list(TOPIC), list("BID"), options())) {
      source.publishJson(TOPIC, "{\"BID\":1.25}");
      assertTrue(subscription.next(WAIT).isPresent());

      Throwable error = failure(subscription.arrow().nextAsync());
      assertInstanceOf(IllegalStateException.class, error);
      assertEquals(
          "subscription is already being read as scalar; cannot also read as arrow",
          error.getMessage());
    }
  }

  @Test
  void arrowTablesCarryThePresenceBitmap() throws Exception {
    try (SubscriptionFixture source = SubscriptionFixture.create();
        Subscription subscription = source.subscribe(list(TOPIC), list("BID", "ASK"), options())) {
      source.publishJson(TOPIC, "{\"BID\":5.5}");
      Table table = subscription.arrow().next(WAIT).get();

      assertEquals(1, table.numRows());
      assertEquals("timestamp", table.columnNames().get(0));
      assertEquals("topic", table.columnNames().get(1));
      assertTrue(table.columnNames().contains("__xbbg_present"));
      assertEquals(5.5, table.column("BID").getDouble(0));
      assertEquals(TOPIC, table.column("topic").getString(0));
    }
  }

  @Test
  void imageOnlySubscriptionsRejectRowReadsButServeLatest() throws Exception {
    try (SubscriptionFixture source = SubscriptionFixture.create();
        Subscription subscription =
            source.subscribe(list(TOPIC), list("BID"), options("rows", false))) {
      Throwable error = failure(subscription.nextAsync());
      assertInstanceOf(BlpValidationException.class, error);
      assertEquals(Optional.of("rows"), ((BlpValidationException) error).element());

      source.publishJson(TOPIC, "{\"BID\":7.5}");
      source.settle();
      Table latest = subscription.latest();
      assertEquals(1, latest.numRows());
      assertEquals(TOPIC, latest.column("topic").getString(0));
      assertEquals(7.5, latest.column("BID").getDouble(0));
    }
  }

  @Test
  void stateSubscriptionsRequireDeliveries() throws Exception {
    try (SubscriptionFixture source = SubscriptionFixture.create();
        Subscription subscription =
            source.subscribe(list(TOPIC), list("BID"), options("delivery", "state"))) {
      assertEquals(SubscriptionDeliveryMode.STATE, subscription.delivery());
      Throwable error = failure(subscription.nextAsync());
      assertInstanceOf(BlpValidationException.class, error);
      assertEquals(Optional.of("delivery"), ((BlpValidationException) error).element());

      source.publishJson(TOPIC, "{\"BID\":1.25}");
      SubscriptionDelivery<Table> delivery = subscription.deliveries().next(WAIT).get();
      assertEquals(SubscriptionDeliveryMode.STATE, delivery.kind());
      assertNotNull(delivery.data());
    }
  }

  @Test
  void dropOldestOverflowReportsTheGapOnTheNextDelivery() throws Exception {
    try (SubscriptionFixture source = SubscriptionFixture.create();
        Subscription subscription =
            source.subscribe(
                list(TOPIC),
                list("BID"),
                options("overflowPolicy", "drop_oldest", "streamCapacity", 1L))) {
      for (double value : new double[] {1.25, 2.5, 3.75}) {
        source.publishJson(TOPIC, "{\"BID\":" + value + "}");
      }
      source.settle();
      SubscriptionDelivery<Table> delivery = subscription.deliveries().next(WAIT).get();

      SubscriptionGap gap = delivery.gapBefore().get();
      assertEquals(SubscriptionGap.LOCAL_OVERFLOW, gap.cause());
      assertTrue(gap.droppedUpdates() > 0);
      Table data = delivery.data();
      assertEquals(3.75, data.column("BID").getDouble(data.numRows() - 1));
    }
  }

  @Test
  void dataLossFailsTheStreamAfterTheDataBeforeIt() throws Exception {
    try (SubscriptionFixture source = SubscriptionFixture.create();
        Subscription subscription = source.subscribe(list(TOPIC), list("BID"), options())) {
      source.publishJson(TOPIC, "{\"BID\":1.25}");
      source.settle();
      source.dataLoss(TOPIC);
      source.settle();

      assertEquals(1.25, subscription.next(WAIT).get().getDouble("BID").getAsDouble());
      assertNotNull(failure(subscription.nextAsync()));
      assertEquals(Optional.empty(), subscription.next(WAIT));
    }
  }

  @Test
  void aCloseThatWinsReturnsThePausedDeliveryToTheDrain() throws Exception {
    try (SubscriptionFixture source = SubscriptionFixture.create()) {
      Subscription subscription = source.subscribe(list(TOPIC), list("BID"), options());
      DeliveryStream<Table> deliveries = subscription.deliveries();
      source.publishJson(TOPIC, "{\"BID\":1.25}");
      source.settle();
      SubscriptionFixture.PublicationPause pause = source.pauseNextDelivery(subscription);
      CompletableFuture<Optional<SubscriptionDelivery<Table>>> read = deliveries.nextAsync();
      pause.ready();

      CompletableFuture<List<SubscriptionDelivery<Table>>> close = deliveries.unsubscribe(true);
      pause.release();

      assertEquals(Optional.empty(), read.get(10, TimeUnit.SECONDS));
      List<SubscriptionDelivery<Table>> drained = close.get(10, TimeUnit.SECONDS);
      assertEquals(1, drained.size());
      assertEquals(1.25, drained.get(0).data().column("BID").getDouble(0));
    }
  }

  @Test
  void cancellingAPendingReadClosesTheSubscription() throws Exception {
    try (SubscriptionFixture source = SubscriptionFixture.create()) {
      Subscription subscription = source.subscribe(list(TOPIC), list("BID"), options());
      CompletableFuture<Optional<Tick>> read = subscription.nextAsync();
      assertTrue(read.cancel(true));

      assertEquals(
          Collections.emptyList(), subscription.unsubscribe(true).get(10, TimeUnit.SECONDS));
      assertEquals(Optional.empty(), subscription.next(WAIT));
      assertFalse(subscription.isActive());
    }
  }

  @Test
  void membershipChangesUpdateTickersAndFields() throws Exception {
    try (SubscriptionFixture source = SubscriptionFixture.create();
        Subscription subscription = source.subscribe(list(TOPIC), list("BID"), options())) {
      subscription.addFields(list("ASK")).get(10, TimeUnit.SECONDS);
      assertEquals(list("BID", "ASK"), subscription.fields());

      subscription.add(list("OTHER Equity")).get(10, TimeUnit.SECONDS);
      assertTrue(subscription.tickers().contains("OTHER Equity"));
      subscription.remove(list("OTHER Equity")).get(10, TimeUnit.SECONDS);
      assertEquals(list(TOPIC), subscription.tickers());

      SubscriptionStatus status = subscription.status();
      assertTrue(status.topicStates().containsKey(TOPIC));
      assertEquals(status.topicStates(), subscription.topicStates());
    }
  }

  @Test
  void strictConsumersReadScalarAndArrowByDefault() throws Exception {
    try (SubscriptionFixture source = SubscriptionFixture.create(4, "drop_newest")) {
      Subscription scalar =
          source.subscribe(list("SCALAR SYNTHETIC Equity"), list("BID"), options());
      source.publishJson("SCALAR SYNTHETIC Equity", "{\"BID\":4.5}");
      assertEquals(4.5, scalar.next(WAIT).get().getDouble("BID").getAsDouble());
      assertEquals(Collections.emptyList(), scalar.unsubscribe(false).get(10, TimeUnit.SECONDS));

      Subscription arrow = source.subscribe(list("ARROW SYNTHETIC Equity"), list("BID"), options());
      source.publishJson("ARROW SYNTHETIC Equity", "{\"BID\":5.5}");
      assertEquals(5.5, arrow.arrow().next(WAIT).get().column("BID").getDouble(0));
      assertEquals(
          Collections.emptyList(), arrow.arrow().unsubscribe(false).get(10, TimeUnit.SECONDS));
    }
  }

  @Test
  void strictOverflowClosesWithDataLossAfterTheQueuedData() throws Exception {
    for (boolean arrowReads : new boolean[] {false, true}) {
      try (SubscriptionFixture source = SubscriptionFixture.create(1, "drop_newest");
          Subscription subscription = source.subscribe(list(TOPIC), list("BID"), options())) {
        source.publishJson(TOPIC, "{\"BID\":1.25}");
        source.publishJson(TOPIC, "{\"BID\":2.5}");
        source.settle();

        Throwable error;
        if (arrowReads) {
          ArrowSubscription arrow = subscription.arrow();
          Table table = arrow.next(WAIT).get();
          assertEquals(1, table.numRows());
          assertEquals(1.25, table.column("BID").getDouble(0));
          error = failure(arrow.nextAsync());
          assertEquals(Optional.empty(), arrow.next(WAIT));
        } else {
          assertEquals(1.25, subscription.next(WAIT).get().getDouble("BID").getAsDouble());
          error = failure(subscription.nextAsync());
          assertEquals(Optional.empty(), subscription.next(WAIT));
        }
        assertInstanceOf(BlpSubscriptionDataLossException.class, error);
        assertFalse(subscription.isActive());
        assertTrue(error.getMessage().contains(TOPIC), error.getMessage());
      }
    }
  }

  @Test
  void eachBatchIsReadAgainstItsOwnLayout() throws Exception {
    try (SubscriptionFixture source = SubscriptionFixture.create();
        Subscription subscription =
            source.subscribe(list(TOPIC), list("NUMBER", "LABEL"), options())) {
      source.publishJson(TOPIC, "{\"NUMBER\":1}");
      source.publishJson(TOPIC, "{\"NUMBER\":2,\"LABEL\":\"grown\"}");
      source.publishJson(TOPIC, "{\"NUMBER\":\"changed kind\"}");
      Tick first = subscription.next(WAIT).get();
      Tick second = subscription.next(WAIT).get();
      Tick promoted = subscription.next(WAIT).get();

      assertFalse(first.has("LABEL"));
      assertEquals("grown", second.get("LABEL"));
      assertEquals(2.0, second.getDouble("NUMBER").getAsDouble());
      assertEquals("changed kind", promoted.get("NUMBER"));
      assertEquals(Optional.of("str"), promoted.fieldKind("NUMBER"));
      assertNotEquals(first.layoutVersion(), promoted.layoutVersion());
      // Earlier ticks keep their own layout after the promotion.
      assertEquals(Optional.of("f64"), first.fieldKind("NUMBER"));
      assertEquals(1.0, first.get("NUMBER"));
    }

    try (SubscriptionFixture source = SubscriptionFixture.create();
        Subscription subscription = source.subscribe(list(TOPIC), list("NUMBER"), options())) {
      source.publishJson(TOPIC, "{\"NUMBER\":1}");
      source.publishJson(TOPIC, "{\"NUMBER\":\"changed kind\"}");
      Table before = subscription.arrow().next(WAIT).get();
      Table after = subscription.arrow().next(WAIT).get();
      assertEquals(1, before.numRows());
      assertEquals(1.0, before.column("NUMBER").getDouble(0));
      assertEquals(1, after.numRows());
      assertEquals("changed kind", after.column("NUMBER").getString(0));
    }
  }

  @Test
  void aFailedBackendConversionRestoresTheDeliveryForAnotherBackend() throws Exception {
    try (SubscriptionFixture source = SubscriptionFixture.create(1, "drop_oldest");
        Subscription subscription = source.subscribe(list(TOPIC), list("BID"), options())) {
      for (double value : new double[] {1.25, 2.5, 3.75}) {
        source.publishJson(TOPIC, "{\"BID\":" + value + "}");
      }
      source.settle();
      final IllegalStateException boom = new IllegalStateException("synthetic projector failure");
      Backend<Table> failing =
          Backend.<Table, Table>map(
              Backend.TABLE,
              "failing",
              table -> {
                throw boom;
              });
      assertSame(boom, failure(subscription.deliveries(failing).nextAsync()));
      assertTrue(subscription.isActive());

      SubscriptionDelivery<Table> recovered = subscription.deliveries().next(WAIT).get();
      assertEquals(3.75, recovered.data().column("BID").getDouble(0));
      assertEquals(2, recovered.gapBefore().get().droppedUpdates());
      assertEquals(SubscriptionGap.LOCAL_OVERFLOW, recovered.gapBefore().get().cause());
    }
  }

  @Test
  void sessionTerminationFailsAPendingReadWithAnInternalError() throws Exception {
    try (SubscriptionFixture source = SubscriptionFixture.create();
        Subscription subscription = source.subscribe(list(TOPIC), list("BID"), options())) {
      CompletableFuture<Optional<Tick>> read = subscription.nextAsync();
      source.terminate();

      assertInstanceOf(BlpInternalException.class, failure(read));
      assertEquals(Optional.empty(), subscription.next(WAIT));
      assertFalse(subscription.isActive());
      assertEquals(SessionState.TERMINATED, subscription.sessionStatus().state());
    }
  }
}
