package io.github.underloam.xbbg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.underloam.xbbg.internal.Json;
import io.github.underloam.xbbg.internal.JsonValue;
import io.github.underloam.xbbg.internal.NativeBridge;
import io.github.underloam.xbbg.internal.NativeTestSupport;
import io.github.underloam.xbbg.internal.ResultKind;
import io.github.underloam.xbbg.internal.SubscriptionBridge;
import io.github.underloam.xbbg.subscription.Subscription;
import io.github.underloam.xbbg.subscription.Tick;
import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Engine release (garbage collection) versus close, on the subscription fixture's test engine
 * ({@code fixture.engine}): an engine nobody references any more is only released, so its
 * subscriptions keep reading; closing it ends them. Runs with {@code XBBG_TEST_SUBSCRIPTIONS=1}.
 */
class EngineLifecycleNativeTest {
  private static final String TOPIC = "SYNTHETIC Equity";

  @BeforeAll
  static void requireFixture() {
    Assumptions.assumeTrue(
        "1".equals(System.getenv("XBBG_TEST_SUBSCRIPTIONS")),
        "set XBBG_TEST_SUBSCRIPTIONS=1 to run the subscription fixture tests");
    NativeTestSupport.assumeAvailable();
  }

  private static long createFixture() {
    return (Long)
        Xbbg.await(
            NativeTestSupport.invokeAsync(
                "fixture.create", JsonValue.EMPTY_OBJECT, ResultKind.HANDLE));
  }

  private static JsonValue fixtureArgs(long fixture) {
    return Json.object().put("fixture", fixture).build();
  }

  private static long fixtureEngine(long fixture) {
    return (Long)
        NativeTestSupport.invoke("fixture.engine", fixtureArgs(fixture), ResultKind.HANDLE);
  }

  private static Subscription subscribe(long fixture) {
    JsonValue args =
        Json.object()
            .put("fixture", fixture)
            .put("tickers", Collections.singletonList(TOPIC))
            .put("fields", Collections.singletonList("BID"))
            .build();
    long handle =
        (Long)
            Xbbg.await(NativeTestSupport.invokeAsync("fixture.subscribe", args, ResultKind.HANDLE));
    return SubscriptionBridge.subscription(SubscriptionBridge.forHandle(handle));
  }

  private static void publish(long fixture, double bid) {
    NativeTestSupport.invoke(
        "fixture.publishJson",
        Json.object()
            .put("fixture", fixture)
            .put("topic", TOPIC)
            .put("json", "{\"BID\":" + bid + "}")
            .build(),
        ResultKind.VOID);
  }

  private static void releaseFixture(long fixture) {
    NativeTestSupport.invoke("fixture.release", fixtureArgs(fixture), ResultKind.VOID);
  }

  /** Engines (real or fixture) not yet torn down in this process. */
  private static long liveEngines() {
    String json =
        (String)
            NativeTestSupport.invoke("core.liveEngines", JsonValue.EMPTY_OBJECT, ResultKind.JSON);
    return ((Number) Json.parse(json)).longValue();
  }

  /** Whether the handle left the native engine table. */
  private static boolean released(long handle) {
    try {
      NativeBridge.engineCallSync(handle, "keyColumn", JsonValue.EMPTY_OBJECT, ResultKind.STRING);
      return false;
    } catch (IllegalStateException closed) {
      return true;
    }
  }

  /** Wraps the handle in an engine and returns only a weak reference to it. */
  private static WeakReference<Engine> unreferencedEngine(long handle) {
    return new WeakReference<Engine>(Engine.create(handle));
  }

  @Test
  @Timeout(60)
  void aCollectedEngineKeepsItsSubscriptionsReading() throws Exception {
    long liveBefore = liveEngines();
    long fixture = createFixture();
    try {
      readThroughACollectedEngine(fixture, liveBefore);
    } finally {
      releaseFixture(fixture);
    }
    // The closed subscription is unreachable now; once its cleaner releases the handle, nothing
    // holds the native engine and it is torn down.
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (liveEngines() > liveBefore && System.nanoTime() < deadline) {
      System.gc();
      Thread.sleep(10);
    }
    assertEquals(liveBefore, liveEngines());
  }

  private static void readThroughACollectedEngine(long fixture, long liveBefore) throws Exception {
    long handle = fixtureEngine(fixture);
    Subscription subscription = subscribe(fixture);
    try {
      WeakReference<Engine> engine = unreferencedEngine(handle);
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (!released(handle) && System.nanoTime() < deadline) {
        System.gc();
        Thread.sleep(10);
      }
      assertNull(engine.get(), "the engine object was collected");
      assertTrue(released(handle), "its cleaner released the handle");
      // Released, not closed: the subscription holds the native engine and keeps reading.
      assertEquals(liveBefore + 1, liveEngines());
      publish(fixture, 4.5);
      Optional<Tick> tick = subscription.next(Duration.ofSeconds(10));
      assertEquals(4.5, tick.get().getDouble("BID").getAsDouble());
    } finally {
      subscription.close();
    }
  }

  @Test
  @Timeout(60)
  void closingTheEngineEndsItsSubscriptions() throws Exception {
    long fixture = createFixture();
    Engine engine = Engine.create(fixtureEngine(fixture));
    Subscription subscription = subscribe(fixture);
    try {
      engine.close();
      assertTrue(engine.isClosed());
      CompletableFuture<Optional<Tick>> read = subscription.nextAsync();
      ExecutionException failed =
          assertThrows(ExecutionException.class, () -> read.get(10, TimeUnit.SECONDS));
      assertInstanceOf(BlpInternalException.class, failed.getCause());
      IllegalStateException closed =
          assertThrows(IllegalStateException.class, engine::signalShutdown);
      assertEquals("engine is closed", closed.getMessage());
    } finally {
      CompletableFuture<?> unused = subscription.unsubscribe();
      releaseFixture(fixture);
    }
  }
}
