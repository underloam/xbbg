package io.github.underloam.xbbg.internal;

import io.github.underloam.xbbg.SubscriptionDeliveryMode;
import io.github.underloam.xbbg.table.Table;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * The native operations behind one subscription (the napi {@code JsSubscription} surface). The
 * subscription views serialize reads, claim the read format and coordinate closes on top of it.
 * Futures complete on the callback executor; read futures are never cancelled by the views.
 *
 * <p>Internal API: not covered by compatibility guarantees.
 */
public interface NativeSubscription {
  /**
   * Whether the consumer delivers rows ({@code rows: true}); fixed at creation.
   *
   * @return the flag
   */
  boolean deliversRows();

  /**
   * Whether only marked deliveries may read ({@code state} or {@code drop_oldest}); fixed at
   * creation.
   *
   * @return the flag
   */
  boolean requiresDeliveries();

  /**
   * Returns the delivery mode; fixed at creation.
   *
   * @return the mode
   */
  SubscriptionDeliveryMode delivery();

  /** Makes pending native completions resolve without publishing (close or abort started). */
  void signalClosing();

  /**
   * Reads the next scalar batch.
   *
   * @return the batch, or {@code null} at the end of the stream
   */
  CompletableFuture<TickBatch> nextTicks();

  /**
   * Reads the next Arrow batch.
   *
   * @return the table, or {@code null} at the end of the stream
   */
  CompletableFuture<Table> nextArrow();

  /**
   * Reads the next marked delivery.
   *
   * @param format {@code TABLE}, {@code IPC_STREAM} or {@code IPC_FILE}
   * @return the delivery, or {@code null} at the end of the stream (or when close won)
   */
  CompletableFuture<NativeDelivery> nextDelivery(ResultKind format);

  /**
   * Acknowledges a delivery receipt.
   *
   * @param receipt the receipt
   * @return {@code false} when close won and the delivery went back to the source
   * @throws RuntimeException the terminal stream error of an error receipt (once)
   */
  boolean commitDelivery(long receipt);

  /**
   * Rolls a delivery receipt back: its data, gap or error returns to the source.
   *
   * @param receipt the receipt
   */
  void rollbackDelivery(long receipt);

  /**
   * Closes and returns the unread scalar batches when draining.
   *
   * @param drain whether to return unread rows
   * @return the batches (empty without drain)
   */
  CompletableFuture<List<TickBatch>> unsubscribeTicks(boolean drain);

  /**
   * Closes and returns queued and unread Arrow batches when draining.
   *
   * @param drain whether to return unread rows
   * @return the tables (empty without drain)
   */
  CompletableFuture<List<Table>> unsubscribeArrow(boolean drain);

  /**
   * Closes and freezes the marked drain.
   *
   * @param drain whether to return unread deliveries
   * @param format {@code TABLE}, {@code IPC_STREAM} or {@code IPC_FILE}
   * @return the drain
   */
  CompletableFuture<NativeDrain> unsubscribeDeliveries(boolean drain, ResultKind format);

  /**
   * Acknowledges a drain receipt.
   *
   * @param receipt the receipt
   * @return {@code false} when it was already settled
   */
  boolean commitDrain(long receipt);

  /**
   * Rolls a drain receipt back; the frozen drain stays available for a retry.
   *
   * @param receipt the receipt
   */
  void rollbackDrain(long receipt);

  /**
   * Adds upstream topics.
   *
   * @param tickers the topics
   * @param aliases topic to label, possibly empty
   * @return completion
   */
  CompletableFuture<Void> add(List<String> tickers, Map<String, String> aliases);

  /**
   * Grows the field projection.
   *
   * @param fields the fields
   * @return completion
   */
  CompletableFuture<Void> addFields(List<String> fields);

  /**
   * Removes consumer labels.
   *
   * @param tickers the labels
   * @return completion
   */
  CompletableFuture<Void> remove(List<String> tickers);

  /**
   * Returns the latest images.
   *
   * @param kind {@code TABLE}, {@code IPC_STREAM} or {@code IPC_FILE}
   * @return a {@code Table} or IPC bytes
   */
  Object latest(ResultKind kind);

  /**
   * Returns and clears the new delayed-data and rejected-field events.
   *
   * @return a JSON array of events
   */
  String takeWarnings();

  /**
   * Whether the subscription is open, streaming and has an active topic.
   *
   * @return the flag
   */
  boolean isActive();

  /**
   * Returns a JSON status snapshot: {@code tickers}, {@code fields}, {@code stats}, {@code status},
   * {@code events}, {@code failures}, {@code failedTickers}, {@code topicStates}, {@code
   * fieldErrors}, {@code sessionStatus}, {@code serviceStatus} or {@code adminStatus}.
   *
   * @param name the snapshot name
   * @return JSON text
   */
  String snapshot(String name);

  /** Releases the native handle; later calls fail. Idempotent. */
  void release();
}
