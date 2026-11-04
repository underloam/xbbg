package io.github.underloam.xbbg.subscription;

import io.github.underloam.xbbg.Xbbg;
import io.github.underloam.xbbg.internal.Json;
import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import io.github.underloam.xbbg.internal.JsonValue;
import io.github.underloam.xbbg.internal.NativeSubscription;
import io.github.underloam.xbbg.internal.NativeTestSupport;
import io.github.underloam.xbbg.internal.ResultKind;
import io.github.underloam.xbbg.internal.SubscriptionBridge;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Test-only synthetic market-data source ({@code fixture.*} methods of the test-support natives):
 * subscriptions run through the real engine against an in-process publisher.
 */
final class SubscriptionFixture implements AutoCloseable {
  private final long id;
  private final IdentityHashMap<Object, Long> handles = new IdentityHashMap<Object, Long>();

  private SubscriptionFixture(long id) {
    this.id = id;
  }

  /** Creates a fixture with the engine's default stream capacity and overflow policy. */
  static SubscriptionFixture create() {
    return create(null, null);
  }

  /** Creates a fixture with an engine stream capacity and overflow policy (null = default). */
  static SubscriptionFixture create(Integer streamCapacity, String overflowPolicy) {
    JsonObjectBuilder args = Json.object();
    if (streamCapacity != null) {
      args.put("streamCapacity", streamCapacity.longValue());
    }
    args.put("overflowPolicy", overflowPolicy);
    Object handle =
        Xbbg.await(
            NativeTestSupport.invokeAsync("fixture.create", args.build(), ResultKind.HANDLE));
    return new SubscriptionFixture((Long) handle);
  }

  private long subscribeHandle(
      List<String> tickers, List<String> fields, Map<String, Object> options) {
    JsonObjectBuilder opts = Json.object();
    for (Map.Entry<String, Object> option : options.entrySet()) {
      opts.put(option.getKey(), option.getValue());
    }
    JsonValue args =
        Json.object()
            .put("fixture", id)
            .put("tickers", tickers)
            .put("fields", fields)
            .put("options", opts)
            .build();
    return (Long)
        Xbbg.await(NativeTestSupport.invokeAsync("fixture.subscribe", args, ResultKind.HANDLE));
  }

  private void remember(Object subscription, long handle) {
    synchronized (handles) {
      handles.put(subscription, handle);
    }
  }

  /** Subscribes through the engine; {@code options} uses the fixture's option names. */
  Subscription subscribe(List<String> tickers, List<String> fields, Map<String, Object> options) {
    long handle = subscribeHandle(tickers, fields, options);
    Subscription subscription =
        SubscriptionBridge.subscription(SubscriptionBridge.forHandle(handle));
    remember(subscription, handle);
    return subscription;
  }

  /**
   * Subscribes and returns the bare native subscription (no views), like the JavaScript tests that
   * drive the native object directly. Release it when done.
   */
  NativeSubscription subscribeNative(
      List<String> tickers, List<String> fields, Map<String, Object> options) {
    long handle = subscribeHandle(tickers, fields, options);
    NativeSubscription subscription = SubscriptionBridge.forHandle(handle);
    remember(subscription, handle);
    return subscription;
  }

  /** Publishes one update ({@code json} is an object of field values). */
  void publishJson(String topic, String json) {
    NativeTestSupport.invoke(
        "fixture.publishJson",
        Json.object().put("fixture", id).put("topic", topic).put("json", json).build(),
        ResultKind.VOID);
  }

  /** Signals Bloomberg data loss for a topic. */
  void dataLoss(String topic) {
    NativeTestSupport.invoke(
        "fixture.dataLoss",
        Json.object().put("fixture", id).put("topic", topic).build(),
        ResultKind.VOID);
  }

  /** Terminates the synthetic session (releasing outstanding pauses first). */
  void terminate() {
    NativeTestSupport.invoke(
        "fixture.terminate", Json.object().put("fixture", id).build(), ResultKind.VOID);
  }

  /** Waits until every published event has been routed. */
  void settle() {
    Xbbg.await(
        NativeTestSupport.invokeAsync(
            "fixture.settle", Json.object().put("fixture", id).build(), ResultKind.VOID));
  }

  /**
   * Arms a pause after the next delivery's native read and before its publication.
   *
   * @param subscription a {@link Subscription} or {@link NativeSubscription} from this fixture
   */
  PublicationPause pauseNextDelivery(Object subscription) {
    long handle;
    synchronized (handles) {
      handle = handles.get(subscription);
    }
    Object pause =
        NativeTestSupport.invoke(
            "fixture.pauseNextDelivery",
            Json.object().put("fixture", id).put("subscription", handle).build(),
            ResultKind.HANDLE);
    return new PublicationPause((Long) pause);
  }

  @Override
  public void close() {
    NativeTestSupport.invoke(
        "fixture.release", Json.object().put("fixture", id).build(), ResultKind.VOID);
  }

  /** A delivery publication held between the native read and its publication. */
  static final class PublicationPause {
    private final long id;

    PublicationPause(long id) {
      this.id = id;
    }

    /** Waits until a read reached the pause. */
    void ready() {
      Xbbg.await(
          NativeTestSupport.invokeAsync(
              "fixture.pauseReady", Json.object().put("pause", id).build(), ResultKind.VOID));
    }

    /** Lets the paused read continue. */
    void release() {
      NativeTestSupport.invoke(
          "fixture.pauseRelease", Json.object().put("pause", id).build(), ResultKind.VOID);
    }
  }
}
