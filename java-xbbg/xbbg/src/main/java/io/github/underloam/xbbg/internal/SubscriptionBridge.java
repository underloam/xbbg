package io.github.underloam.xbbg.internal;

import io.github.underloam.xbbg.BlpInternalException;
import io.github.underloam.xbbg.SubscriptionDeliveryMode;
import io.github.underloam.xbbg.subscription.AuctionStreamOptions;
import io.github.underloam.xbbg.subscription.StreamOptions;
import io.github.underloam.xbbg.subscription.Subscription;
import io.github.underloam.xbbg.table.Table;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * The subscription side of the native bridge: the factories {@code xbbg_jni} calls to build tick
 * batches, deliveries and drains (contract §7), the JNI implementation of {@link
 * NativeSubscription}, subscription construction for the engine layers, and the JSON form of stream
 * options.
 *
 * <p>Internal API: not covered by compatibility guarantees.
 */
public final class SubscriptionBridge {
  private static volatile Views views;

  private SubscriptionBridge() {}

  // ---------------------------------------------------------------- upcalls (native -> Java)

  static TickBatch ticks(
      int layoutVersion,
      String[] layoutFields,
      byte[] layoutKinds,
      String[] topics,
      long[] timestampsUs,
      int[] rowOffsets,
      char[] fieldIndices,
      byte[] valueKinds,
      long[] valueBits,
      String[] strings) {
    return new TickBatch(
        layoutVersion,
        layoutFields,
        layoutKinds,
        topics,
        timestampsUs,
        rowOffsets,
        fieldIndices,
        valueKinds,
        valueBits,
        strings);
  }

  static NativeDelivery delivery(
      int kind, Object data, boolean hasGap, long droppedUpdates, long receipt) {
    return NativeDelivery.data(
        kind == 1 ? SubscriptionDeliveryMode.STATE : SubscriptionDeliveryMode.DELTAS,
        data,
        hasGap,
        droppedUpdates,
        receipt);
  }

  static NativeDelivery deliveryError(long receipt) {
    return NativeDelivery.error(receipt);
  }

  static NativeDelivery deliveryConversionFailure(Throwable cause) {
    return NativeDelivery.conversionFailure(cause);
  }

  static NativeDrain drain(Object[] deliveries, long receipt) {
    List<NativeDelivery> list = new ArrayList<NativeDelivery>(deliveries.length);
    for (Object delivery : deliveries) {
      list.add((NativeDelivery) delivery);
    }
    return NativeDrain.of(list, receipt);
  }

  static NativeDrain drainConversionFailure(Throwable cause) {
    return NativeDrain.conversionFailure(cause);
  }

  // ---------------------------------------------------------------- subscription construction

  /** Creates the public subscription for a native one; implemented by the subscription package. */
  public interface Views {
    /**
     * Creates the subscription.
     *
     * @param subscription the native subscription
     * @return the public subscription
     */
    Subscription open(NativeSubscription subscription);
  }

  /**
   * Installs the subscription factory; called once by {@link Subscription}'s class initializer.
   *
   * @param installed the factory
   */
  public static void installViews(Views installed) {
    if (views != null) {
      throw new IllegalStateException("subscription views already installed");
    }
    views = installed;
  }

  /**
   * Creates the public subscription for a native one.
   *
   * @param subscription the native subscription
   * @return the public subscription
   */
  public static Subscription subscription(NativeSubscription subscription) {
    Views factory = views;
    if (factory == null) {
      Backends.initialize(Subscription.class);
      factory = views;
    }
    return factory.open(subscription);
  }

  /**
   * Wraps a native subscription handle once it arrives. If the returned future was cancelled or
   * completed meanwhile, the handle is released instead, so it can never leak.
   *
   * @param handle the future native handle (a {@code Long})
   * @return the subscription
   */
  public static CompletableFuture<Subscription> open(CompletableFuture<Object> handle) {
    final CompletableFuture<Subscription> result = new CompletableFuture<Subscription>();
    Futures.onComplete(
        handle,
        (value, error) -> {
          if (error != null) {
            result.completeExceptionally(Futures.unwrap(error));
            return;
          }
          if (!(value instanceof Long)) {
            result.completeExceptionally(
                new BlpInternalException("native subscribe returned no subscription handle"));
            return;
          }
          long id = (Long) value;
          if (result.isDone()) {
            NativeBridge.subscriptionRelease(id);
            return;
          }
          Subscription subscription;
          try {
            subscription = subscription(forHandle(id));
          } catch (Throwable failure) {
            NativeBridge.subscriptionRelease(id);
            result.completeExceptionally(failure);
            return;
          }
          if (!result.complete(subscription)) {
            abandon(subscription);
          }
        });
    return result;
  }

  /** Closes a subscription that its caller gave up on before it opened. */
  @SuppressWarnings("FutureReturnValueIgnored")
  private static void abandon(Subscription subscription) {
    // Suppressed: nobody holds this subscription any more, so nobody can be told how its close
    // ends.
    subscription.unsubscribe(false);
  }

  /**
   * Returns the JNI implementation for a native subscription handle.
   *
   * @param handle the handle
   * @return the native subscription
   */
  public static NativeSubscription forHandle(long handle) {
    return new JniSubscription(handle);
  }

  // ---------------------------------------------------------------- argument JSON

  /**
   * Returns the JSON object of stream options (absent options are omitted).
   *
   * @param options the options
   * @return the {@code options} object of the native subscribe calls
   */
  public static JsonObjectBuilder streamOptions(StreamOptions options) {
    JsonObjectBuilder json = Json.object();
    if (options.delivery().isPresent()) {
      json.put("delivery", options.delivery().get().wireValue());
    }
    json.put("options", options.options().orElse(null));
    json.put("conflate", options.conflate().orElse(null));
    if (options.flushThreshold().isPresent()) {
      json.put("flushThreshold", options.flushThreshold().getAsInt());
    }
    if (options.overflowPolicy().isPresent()) {
      json.put("overflowPolicy", options.overflowPolicy().get().wireValue());
    }
    if (options.streamCapacity().isPresent()) {
      json.put("streamCapacity", options.streamCapacity().getAsInt());
    }
    json.put("allFields", options.allFields().orElse(null));
    json.put("fields", options.fields().orElse(null));
    json.put("aliases", options.aliases().orElse(null));
    if (options.onDelayed().isPresent()) {
      json.put("onDelayed", options.onDelayed().get().wireValue());
    }
    json.put("isolated", options.isolated().orElse(null));
    json.put("rows", options.rows().orElse(null));
    if (options.onFieldError().isPresent()) {
      json.put("onFieldError", options.onFieldError().get().wireValue());
    }
    json.put("zeroAsNull", options.zeroAsNull().orElse(null));
    return json;
  }

  /**
   * Returns the arguments of {@code subscribeAuction}.
   *
   * @param securities the input securities
   * @param options the auction options
   * @param streamAuction whether this is {@code streamAuction} (which rejects {@code rows})
   * @return the argument object
   */
  public static JsonValue auctionArgs(
      List<String> securities, AuctionStreamOptions options, boolean streamAuction) {
    JsonObjectBuilder json = Json.object();
    json.put("securities", securities);
    if (options.pcsOverrides().isPresent()) {
      json.put("pcsOverrides", options.pcsOverrides().get());
    }
    if (streamAuction) {
      json.put("streamAuction", true);
    }
    json.put("options", streamOptions(options.stream()));
    return json.build();
  }

  // ---------------------------------------------------------------- JNI subscription

  /** {@link NativeSubscription} over {@code NativeXbbg.subscriptionCall*}. */
  private static final class JniSubscription implements NativeSubscription {
    private final long handle;
    private final boolean deliversRows;
    private final boolean requiresDeliveries;
    private final SubscriptionDeliveryMode delivery;

    JniSubscription(long handle) {
      this.handle = handle;
      Object info =
          JsonFields.parse(
              (String)
                  NativeBridge.subscriptionCallSync(
                      handle, "info", JsonValue.EMPTY_OBJECT, ResultKind.JSON),
              "subscription info");
      Map<String, Object> fields = JsonFields.object(info, "subscription info");
      this.deliversRows = JsonFields.bool(fields, "deliversRows");
      this.requiresDeliveries = JsonFields.bool(fields, "requiresDeliveries");
      this.delivery = SubscriptionDeliveryMode.parse(JsonFields.string(fields, "delivery"));
    }

    private CompletableFuture<Object> call(String method, JsonValue args, ResultKind kind) {
      return call(method, args, kind, null);
    }

    /**
     * Starts an async subscription call; cancelling the returned future (or a future mapped from it
     * with {@link Futures#map}) cancels the native call. With {@code onLateValue}, a result that
     * arrives after the cancellation goes to it, so its receipt is settled instead of dropped.
     */
    private CompletableFuture<Object> call(
        final String method,
        final JsonValue args,
        final ResultKind kind,
        Consumer<Object> onLateValue) {
      return NativeBridge.startXbbg(
          token -> NativeXbbg.subscriptionCall(handle, method, args.text(), kind.code(), token),
          onLateValue);
    }

    /** Returns a delivery whose read was cancelled to the native source for the next read. */
    private void rollbackLateDelivery(Object late) {
      if (late instanceof NativeDelivery && ((NativeDelivery) late).receipt() != 0) {
        rollbackDelivery(((NativeDelivery) late).receipt());
      }
    }

    /** Keeps a drain whose close was cancelled for a retry. */
    private void rollbackLateDrain(Object late) {
      if (late instanceof NativeDrain && ((NativeDrain) late).receipt() != 0) {
        rollbackDrain(((NativeDrain) late).receipt());
      }
    }

    private Object callSync(String method, JsonValue args, ResultKind kind) {
      return NativeBridge.subscriptionCallSync(handle, method, args, kind);
    }

    private static <T> T expect(Object value, Class<T> type, String method) {
      if (value == null || type.isInstance(value)) {
        return type.cast(value);
      }
      throw new BlpInternalException(
          "native " + method + " returned " + value.getClass().getName());
    }

    private static String format(ResultKind kind) {
      switch (kind) {
        case TABLE:
          return "table";
        case IPC_STREAM:
          return "ipcStream";
        case IPC_FILE:
          return "ipcFile";
        default:
          throw new IllegalArgumentException("delivery format " + kind + " is not a table kind");
      }
    }

    private static JsonValue drainArgs(boolean drain) {
      return Json.object().put("drain", drain).build();
    }

    private static JsonValue receiptArgs(long receipt) {
      return Json.object().put("receipt", receipt).build();
    }

    @Override
    public boolean deliversRows() {
      return deliversRows;
    }

    @Override
    public boolean requiresDeliveries() {
      return requiresDeliveries;
    }

    @Override
    public SubscriptionDeliveryMode delivery() {
      return delivery;
    }

    @Override
    public void signalClosing() {
      callSync("signalClosing", JsonValue.EMPTY_OBJECT, ResultKind.VOID);
    }

    @Override
    public CompletableFuture<TickBatch> nextTicks() {
      return Futures.map(
          call("nextTicks", JsonValue.EMPTY_OBJECT, ResultKind.TICKS),
          value -> expect(value, TickBatch.class, "nextTicks"));
    }

    @Override
    public CompletableFuture<Table> nextArrow() {
      return Futures.map(
          call("nextArrow", JsonValue.EMPTY_OBJECT, ResultKind.TABLE),
          value -> expect(value, Table.class, "nextArrow"));
    }

    @Override
    public CompletableFuture<NativeDelivery> nextDelivery(ResultKind format) {
      JsonValue args = Json.object().put("format", format(format)).build();
      return Futures.map(
          call("nextDelivery", args, ResultKind.DELIVERY, this::rollbackLateDelivery),
          value -> expect(value, NativeDelivery.class, "nextDelivery"));
    }

    @Override
    public boolean commitDelivery(long receipt) {
      return (Boolean) callSync("commitDelivery", receiptArgs(receipt), ResultKind.BOOL);
    }

    @Override
    public void rollbackDelivery(long receipt) {
      callSync("rollbackDelivery", receiptArgs(receipt), ResultKind.VOID);
    }

    @Override
    public CompletableFuture<List<TickBatch>> unsubscribeTicks(boolean drain) {
      return Futures.map(
          call("unsubscribeTicks", drainArgs(drain), ResultKind.TICKS_LIST),
          value -> {
            Object[] batches = expect(value, Object[].class, "unsubscribeTicks");
            List<TickBatch> list = new ArrayList<TickBatch>();
            if (batches != null) {
              for (Object batch : batches) {
                list.add(expect(batch, TickBatch.class, "unsubscribeTicks"));
              }
            }
            return list;
          });
    }

    @Override
    public CompletableFuture<List<Table>> unsubscribeArrow(boolean drain) {
      return Futures.map(
          call("unsubscribeArrow", drainArgs(drain), ResultKind.TABLE_LIST),
          value -> {
            Table[] tables = expect(value, Table[].class, "unsubscribeArrow");
            return tables == null ? Collections.<Table>emptyList() : Arrays.asList(tables);
          });
    }

    @Override
    public CompletableFuture<NativeDrain> unsubscribeDeliveries(boolean drain, ResultKind format) {
      JsonValue args = Json.object().put("drain", drain).put("format", format(format)).build();
      return Futures.map(
          call("unsubscribeDeliveries", args, ResultKind.DELIVERY_LIST, this::rollbackLateDrain),
          value -> {
            NativeDrain result = expect(value, NativeDrain.class, "unsubscribeDeliveries");
            if (result == null) {
              throw new BlpInternalException("native unsubscribeDeliveries returned null");
            }
            return result;
          });
    }

    @Override
    public boolean commitDrain(long receipt) {
      return (Boolean) callSync("commitDrain", receiptArgs(receipt), ResultKind.BOOL);
    }

    @Override
    public void rollbackDrain(long receipt) {
      callSync("rollbackDrain", receiptArgs(receipt), ResultKind.VOID);
    }

    @Override
    public CompletableFuture<Void> add(List<String> tickers, Map<String, String> aliases) {
      JsonValue args =
          Json.object().put("tickers", tickers).putNonEmpty("aliases", aliases).build();
      return Futures.map(call("add", args, ResultKind.VOID), value -> (Void) null);
    }

    @Override
    public CompletableFuture<Void> addFields(List<String> fields) {
      JsonValue args = Json.object().put("fields", fields).build();
      return Futures.map(call("addFields", args, ResultKind.VOID), value -> (Void) null);
    }

    @Override
    public CompletableFuture<Void> remove(List<String> tickers) {
      JsonValue args = Json.object().put("tickers", tickers).build();
      return Futures.map(call("remove", args, ResultKind.VOID), value -> (Void) null);
    }

    @Override
    public Object latest(ResultKind kind) {
      return callSync("latest", JsonValue.EMPTY_OBJECT, kind);
    }

    @Override
    public String takeWarnings() {
      return (String) callSync("takeWarnings", JsonValue.EMPTY_OBJECT, ResultKind.JSON);
    }

    @Override
    public boolean isActive() {
      return (Boolean) callSync("isActive", JsonValue.EMPTY_OBJECT, ResultKind.BOOL);
    }

    @Override
    public String snapshot(String name) {
      return (String) callSync(name, JsonValue.EMPTY_OBJECT, ResultKind.JSON);
    }

    @Override
    public void release() {
      NativeBridge.subscriptionRelease(handle);
    }
  }
}
