package io.github.underloam.xbbg.subscription;

import io.github.underloam.xbbg.SubscriptionDeliveryMode;
import io.github.underloam.xbbg.internal.NativeDelivery;
import io.github.underloam.xbbg.internal.NativeDrain;
import io.github.underloam.xbbg.internal.NativeSubscription;
import io.github.underloam.xbbg.internal.ResultKind;
import io.github.underloam.xbbg.internal.TickBatch;
import io.github.underloam.xbbg.table.Table;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A scripted {@link NativeSubscription}: every read returns a new pending future the test
 * completes, and every native call is recorded.
 */
final class FakeNativeSubscription implements NativeSubscription {
  final List<CompletableFuture<TickBatch>> tickReads = synchronizedList();
  final List<CompletableFuture<Table>> arrowReads = synchronizedList();
  final List<CompletableFuture<NativeDelivery>> deliveryReads = synchronizedList();
  final List<String> closes = synchronizedList();
  final List<Long> commits = synchronizedList();
  final List<Long> rollbacks = synchronizedList();
  final AtomicInteger signalled = new AtomicInteger();
  final AtomicInteger released = new AtomicInteger();
  volatile List<TickBatch> tickDrain = Collections.emptyList();
  volatile NativeDrain deliveryDrain = NativeDrain.of(new ArrayList<NativeDelivery>(), 0);
  private final boolean deliversRows;
  private final boolean requiresDeliveries;

  FakeNativeSubscription() {
    this(true, false);
  }

  FakeNativeSubscription(boolean deliversRows, boolean requiresDeliveries) {
    this.deliversRows = deliversRows;
    this.requiresDeliveries = requiresDeliveries;
  }

  private static <T> List<T> synchronizedList() {
    return Collections.synchronizedList(new ArrayList<T>());
  }

  /** A batch of {@code BID} updates for one topic; the layout is sent when {@code withLayout}. */
  static TickBatch ticks(boolean withLayout, String topic, double... values) {
    int rows = values.length;
    String[] topics = new String[rows];
    long[] timestamps = new long[rows];
    int[] offsets = new int[rows + 1];
    char[] fields = new char[rows];
    byte[] kinds = new byte[rows];
    long[] bits = new long[rows];
    for (int row = 0; row < rows; row++) {
      topics[row] = topic;
      timestamps[row] = 1_000_000L + row;
      offsets[row] = row;
      kinds[row] = TickBatch.VALUE_F64;
      bits[row] = Double.doubleToRawLongBits(values[row]);
    }
    offsets[rows] = rows;
    return new TickBatch(
        1,
        withLayout ? new String[] {"BID"} : null,
        withLayout ? new byte[] {TickBatch.KIND_F64} : null,
        topics,
        timestamps,
        offsets,
        fields,
        kinds,
        bits,
        new String[0]);
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
    return SubscriptionDeliveryMode.DELTAS;
  }

  @Override
  public void signalClosing() {
    signalled.incrementAndGet();
  }

  @Override
  public CompletableFuture<TickBatch> nextTicks() {
    CompletableFuture<TickBatch> read = new CompletableFuture<TickBatch>();
    tickReads.add(read);
    return read;
  }

  @Override
  public CompletableFuture<Table> nextArrow() {
    CompletableFuture<Table> read = new CompletableFuture<Table>();
    arrowReads.add(read);
    return read;
  }

  @Override
  public CompletableFuture<NativeDelivery> nextDelivery(ResultKind format) {
    CompletableFuture<NativeDelivery> read = new CompletableFuture<NativeDelivery>();
    deliveryReads.add(read);
    return read;
  }

  @Override
  public boolean commitDelivery(long receipt) {
    commits.add(receipt);
    return true;
  }

  @Override
  public void rollbackDelivery(long receipt) {
    rollbacks.add(receipt);
  }

  @Override
  public CompletableFuture<List<TickBatch>> unsubscribeTicks(boolean drain) {
    closes.add("ticks:" + drain);
    return CompletableFuture.completedFuture(
        drain ? tickDrain : Collections.<TickBatch>emptyList());
  }

  @Override
  public CompletableFuture<List<Table>> unsubscribeArrow(boolean drain) {
    closes.add("arrow:" + drain);
    return CompletableFuture.completedFuture(Collections.<Table>emptyList());
  }

  @Override
  public CompletableFuture<NativeDrain> unsubscribeDeliveries(boolean drain, ResultKind format) {
    closes.add("deliveries:" + drain);
    return CompletableFuture.completedFuture(
        drain ? deliveryDrain : NativeDrain.of(new ArrayList<NativeDelivery>(), 0));
  }

  @Override
  public boolean commitDrain(long receipt) {
    commits.add(receipt);
    return true;
  }

  @Override
  public void rollbackDrain(long receipt) {
    rollbacks.add(receipt);
  }

  @Override
  public CompletableFuture<Void> add(List<String> tickers, Map<String, String> aliases) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> addFields(List<String> fields) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> remove(List<String> tickers) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public Object latest(ResultKind kind) {
    throw new UnsupportedOperationException("latest");
  }

  @Override
  public String takeWarnings() {
    return "[]";
  }

  @Override
  public boolean isActive() {
    return closes.isEmpty();
  }

  @Override
  public String snapshot(String name) {
    return "[]";
  }

  @Override
  public void release() {
    released.incrementAndGet();
  }
}
