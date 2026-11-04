package io.github.underloam.xbbg.jdk17;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;
import org.reactivestreams.tck.TestEnvironment;
import org.reactivestreams.tck.flow.FlowPublisherVerification;
import org.testng.annotations.AfterClass;

/**
 * The Reactive Streams TCK (TestNG, run by the JUnit Platform's TestNG engine) against the view
 * publisher. Even-numbered reads complete on another thread, like native completions on the
 * callback executor; odd ones complete synchronously, like buffered values.
 */
public class ViewPublisherTckTest extends FlowPublisherVerification<Long> {
  private final ExecutorService callbacks = Executors.newCachedThreadPool();

  /** Creates the verification with timeouts that tolerate a loaded build machine. */
  public ViewPublisherTckTest() {
    super(new TestEnvironment(2_000, 200), 2_000);
  }

  @AfterClass
  public void shutdown() {
    callbacks.shutdownNow();
  }

  @Override
  public Flow.Publisher<Long> createFlowPublisher(long elements) {
    AtomicLong next = new AtomicLong();
    return XbbgFlow.publisher(
        FlowSource.of(
            () -> {
              long index = next.getAndIncrement();
              Optional<Long> value = index < elements ? Optional.of(index) : Optional.empty();
              return index % 2 == 0
                  ? CompletableFuture.supplyAsync(() -> value, callbacks)
                  : CompletableFuture.completedFuture(value);
            },
            () -> CompletableFuture.completedFuture(null)));
  }

  /**
   * A view publisher that already has its subscriber: it fails every new subscriber with {@code
   * onError(IllegalStateException)} right after {@code onSubscribe}, without demand. (A failing
   * read can only surface with demand, since the publisher never reads ahead.)
   */
  @Override
  public Flow.Publisher<Long> createFailedFlowPublisher() {
    Flow.Publisher<Long> publisher =
        XbbgFlow.publisher(
            FlowSource.<Long>of(
                CompletableFuture::new, () -> CompletableFuture.completedFuture(null)));
    publisher.subscribe(
        new Flow.Subscriber<Long>() {
          @Override
          public void onSubscribe(Flow.Subscription subscription) {}

          @Override
          public void onNext(Long item) {}

          @Override
          public void onError(Throwable throwable) {}

          @Override
          public void onComplete() {}
        });
    return publisher;
  }

  @Override
  public long maxElementsFromPublisher() {
    return Long.MAX_VALUE - 1;
  }
}
