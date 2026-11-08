package io.github.underloam.xbbg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.underloam.xbbg.subscription.SessionState;
import io.github.underloam.xbbg.subscription.StreamOptions;
import io.github.underloam.xbbg.subscription.Subscription;
import io.github.underloam.xbbg.subscription.Tick;
import io.github.underloam.xbbg.table.Table;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Subscriptions against a running Bloomberg Terminal (localhost:8194). Excluded by default; run
 * with the {@code live} group.
 */
@Tag("live")
class SubscriptionsLiveTest {
  private static final List<String> IBM = Collections.singletonList("IBM US Equity");
  private static final List<String> LAST_PRICE = Collections.singletonList("LAST_PRICE");
  private static Engine engine;

  @BeforeAll
  static void connect() {
    engine = Engine.open();
  }

  @AfterAll
  static void close() {
    Xbbg.resetConfiguration();
    if (engine != null) {
      engine.close();
    }
  }

  @Test
  void subscribeDeliversTheInitialPaint() throws Exception {
    try (Subscription subscription = Xbbg.await(engine.subscribe(IBM, LAST_PRICE))) {
      Optional<Tick> tick = subscription.next(Duration.ofSeconds(60));
      assertTrue(tick.isPresent());
      assertEquals("IBM US Equity", tick.get().topic());
      assertEquals(SessionState.UP, subscription.sessionStatus().state());
      assertEquals(IBM, subscription.tickers());
    }
  }

  @Test
  void imageOnlyStreamsServeLatest() throws Exception {
    StreamOptions options = StreamOptions.builder().rows(false).build();
    try (Subscription subscription = Xbbg.await(Blp.subscribe(IBM, LAST_PRICE, options))) {
      Table latest = subscription.latest();
      for (int attempt = 0;
          attempt < 60 && (latest.numRows() == 0 || latest.column("LAST_PRICE").isNull(0));
          attempt++) {
        Thread.sleep(1000);
        latest = subscription.latest();
      }
      assertEquals(1, latest.numRows());
      assertTrue(latest.columnNames().contains("LAST_PRICE"));
    }
  }

  @Test
  void unsubscribeEndsTheStream() throws Exception {
    Subscription subscription = Xbbg.await(Blp.subscribe("IBM US Equity", "LAST_PRICE"));
    Xbbg.await(subscription.unsubscribe());
    assertFalse(subscription.isActive());
    assertEquals(Optional.empty(), subscription.next(Duration.ofSeconds(5)));
  }

  @Test
  void closingTheEngineFailsItsSubscriptionsWithAnInternalError() throws Exception {
    Engine own = Engine.open();
    Subscription subscription = Xbbg.await(own.subscribe(IBM, LAST_PRICE));
    assertTrue(subscription.next(Duration.ofSeconds(60)).isPresent());
    own.close();

    BlpInternalException shutDown = null;
    long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
    while (shutDown == null && System.nanoTime() < deadline) {
      try {
        if (!subscription.next(Duration.ofSeconds(5)).isPresent()) {
          break;
        }
      } catch (TimeoutException stillOpen) {
        // keep waiting for the shutdown to reach the stream
      } catch (BlpInternalException error) {
        shutDown = error;
      }
    }
    assertNotNull(shutDown, "the subscription did not fail after the engine closed");
    assertTrue(
        shutDown.getMessage().contains("subscription engine shut down"), shutDown.getMessage());
    assertFalse(subscription.isActive());
    assertEquals(Optional.empty(), subscription.next(Duration.ofSeconds(5)));
  }
}
