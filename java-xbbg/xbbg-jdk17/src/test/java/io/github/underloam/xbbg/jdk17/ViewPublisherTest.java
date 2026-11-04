package io.github.underloam.xbbg.jdk17;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.underloam.xbbg.BlpSubscriptionDataLossException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class ViewPublisherTest {
  /** Records signals; the test drives demand through {@link #subscription}. */
  private static final class Recorder<T> implements Flow.Subscriber<T> {
    final List<T> values = new ArrayList<>();
    Flow.Subscription subscription;
    Throwable error;
    int completions;
    int subscribes;
    Consumer<Recorder<T>> onNext = r -> {};

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      subscribes++;
      this.subscription = subscription;
    }

    @Override
    public void onNext(T item) {
      values.add(item);
      onNext.accept(this);
    }

    @Override
    public void onError(Throwable throwable) {
      error = throwable;
    }

    @Override
    public void onComplete() {
      completions++;
    }
  }

  private static <T> Recorder<T> subscribe(FlowSource<T> source) {
    Recorder<T> recorder = new Recorder<>();
    XbbgFlow.publisher(source).subscribe(recorder);
    return recorder;
  }

  @Test
  void readsOnlyWithDemandAndOneAtATime() {
    FakeSource<String> source = new FakeSource<>();
    Recorder<String> recorder = subscribe(source);
    assertEquals(1, recorder.subscribes);
    assertEquals(0, source.readCount(), "no read before request");

    recorder.subscription.request(2);
    assertEquals(1, source.readCount(), "one read in flight for a demand of 2");
    source.emit("a");
    assertEquals(List.of("a"), recorder.values);
    assertEquals(2, source.readCount());
    source.emit("b");
    assertEquals(List.of("a", "b"), recorder.values);
    assertEquals(2, source.readCount(), "demand exhausted: no further read");

    recorder.subscription.request(1);
    assertEquals(3, source.readCount());
    source.emit("c");
    assertEquals(List.of("a", "b", "c"), recorder.values);
    assertEquals(3, source.readCount());
    assertEquals(1, source.maxInFlight.get());
    assertEquals(0, source.closes.get());
  }

  @Test
  void endOfStreamCompletesAndCloses() {
    FakeSource<String> source = new FakeSource<>();
    Recorder<String> recorder = subscribe(source);
    recorder.subscription.request(10);
    source.emit("x");
    source.end();
    assertEquals(List.of("x"), recorder.values);
    assertEquals(1, recorder.completions);
    assertNull(recorder.error);
    assertEquals(1, source.closes.get());
    recorder.subscription.request(1);
    assertEquals(2, source.readCount(), "no reads after completion");
  }

  @Test
  void readFailureSignalsTheOriginalExceptionAndCloses() {
    FakeSource<String> source = new FakeSource<>();
    Recorder<String> recorder = subscribe(source);
    recorder.subscription.request(1);
    BlpSubscriptionDataLossException loss = new BlpSubscriptionDataLossException("gap");
    source.fail(new CompletionException(loss));
    assertSame(loss, recorder.error);
    assertEquals(0, recorder.completions);
    assertEquals(1, source.closes.get());
  }

  @Test
  void synchronousSourceFailureSignalsOnError() {
    IllegalStateException closed = new IllegalStateException("subscription is closed");
    Recorder<String> recorder =
        subscribe(
            FlowSource.<String>of(
                () -> {
                  throw closed;
                },
                () -> CompletableFuture.completedFuture(null)));
    recorder.subscription.request(1);
    assertSame(closed, recorder.error);
  }

  @Test
  void nullReadResultIsAnError() {
    Recorder<String> recorder =
        subscribe(
            FlowSource.<String>of(
                () -> CompletableFuture.completedFuture(null),
                () -> CompletableFuture.completedFuture(null)));
    recorder.subscription.request(1);
    assertInstanceOf(NullPointerException.class, recorder.error);
  }

  @Test
  void cancelClosesTheViewAndDropsTheInFlightRead() {
    FakeSource<String> source = new FakeSource<>();
    Recorder<String> recorder = subscribe(source);
    recorder.subscription.request(5);
    recorder.subscription.cancel();
    assertEquals(1, source.closes.get());
    source.emit("late");
    assertEquals(List.of(), recorder.values);
    recorder.subscription.request(5);
    recorder.subscription.cancel();
    assertEquals(1, source.readCount());
    assertEquals(1, source.closes.get(), "closed once");
    assertNull(recorder.error);
    assertEquals(0, recorder.completions);
  }

  @Test
  void cancelFromOnNextStopsReading() {
    FakeSource<String> source = new FakeSource<>();
    Recorder<String> recorder = subscribe(source);
    recorder.onNext = r -> r.subscription.cancel();
    recorder.subscription.request(Long.MAX_VALUE);
    source.emit("only");
    assertEquals(List.of("only"), recorder.values);
    assertEquals(1, source.readCount());
    assertEquals(1, source.closes.get());
  }

  @Test
  void secondSubscriberIsRejected() {
    FakeSource<String> source = new FakeSource<>();
    Flow.Publisher<String> publisher = XbbgFlow.publisher(source);
    Recorder<String> first = new Recorder<>();
    Recorder<String> second = new Recorder<>();
    publisher.subscribe(first);
    publisher.subscribe(second);
    assertEquals(1, second.subscribes);
    assertInstanceOf(IllegalStateException.class, second.error);
    assertNull(first.error);
    first.subscription.request(1);
    source.emit("v");
    assertEquals(List.of("v"), first.values);
    assertEquals(List.of(), second.values);
  }

  @Test
  void nonPositiveRequestFailsAndCloses() {
    FakeSource<String> source = new FakeSource<>();
    Recorder<String> recorder = subscribe(source);
    recorder.subscription.request(0);
    assertInstanceOf(IllegalArgumentException.class, recorder.error);
    assertEquals(1, source.closes.get());
    assertEquals(0, source.readCount());
  }

  @Test
  void demandSaturatesAtLongMaxValue() {
    FakeSource<Integer> source = new FakeSource<>();
    Recorder<Integer> recorder = subscribe(source);
    recorder.subscription.request(Long.MAX_VALUE - 1);
    recorder.subscription.request(Long.MAX_VALUE);
    for (int i = 0; i < 3; i++) {
      source.emit(i);
    }
    assertEquals(List.of(0, 1, 2), recorder.values);
    assertEquals(4, source.readCount(), "demand stays unbounded");
  }

  @Test
  void synchronousReadsDoNotRecurse() {
    int[] produced = {0};
    int total = 100_000;
    Recorder<Integer> recorder =
        subscribe(
            FlowSource.<Integer>of(
                () ->
                    CompletableFuture.completedFuture(
                        produced[0] < total ? Optional.of(produced[0]++) : Optional.empty()),
                () -> CompletableFuture.completedFuture(null)));
    recorder.onNext = r -> r.subscription.request(1);
    recorder.subscription.request(1);
    assertEquals(total, recorder.values.size());
    assertEquals(1, recorder.completions);
  }

  @Test
  void rejectsNullArguments() {
    FakeSource<String> source = new FakeSource<>();
    assertThrows(NullPointerException.class, () -> XbbgFlow.publisher(source).subscribe(null));
    assertThrows(NullPointerException.class, () -> XbbgFlow.ticks(null));
    assertThrows(NullPointerException.class, () -> XbbgFlow.arrow(null));
    assertThrows(NullPointerException.class, () -> XbbgFlow.deliveries(null));
  }
}
