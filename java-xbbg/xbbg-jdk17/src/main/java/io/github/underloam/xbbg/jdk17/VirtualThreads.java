package io.github.underloam.xbbg.jdk17;

import io.github.underloam.xbbg.Xbbg;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Runs xbbg callbacks on virtual threads (Java 21+).
 *
 * <p>xbbg completes every future on its callback executor ({@link Xbbg#callbackExecutor()}), and
 * dependent stages ({@code thenApply}, {@code thenAccept}, ...) run there too. With {@link
 * #useForCallbacks()} each completion runs on its own virtual thread, so callbacks may block (on
 * {@link Xbbg#await}, I/O, or a further xbbg call) without tying up a platform thread.
 *
 * <p>Blocking on virtual threads is safe with xbbg because no xbbg call waits inside a native (JNI)
 * frame, where a virtual thread would pin its carrier: native methods only parse arguments and
 * start work, Bloomberg traffic runs on the engine's own native threads, and results come back by
 * completing a {@link java.util.concurrent.CompletableFuture}. {@link Xbbg#await} and the
 * subscription views' blocking reads wait on that future, which unmounts the virtual thread. The
 * same holds for virtual threads you create yourself (for example with {@code
 * Executors.newVirtualThreadPerTaskExecutor()}) that call xbbg's blocking helpers.
 *
 * <p>This module targets Java 17, so the virtual-thread executor is created reflectively and the
 * methods are no-ops on Java 17 to 20.
 */
public final class VirtualThreads {
  private static final Object LOCK = new Object();
  private static ExecutorService executor;

  private VirtualThreads() {}

  /**
   * Whether this runtime has virtual threads (Java 21+).
   *
   * @return {@code true} on Java 21 or later
   */
  public static boolean isSupported() {
    return Runtime.version().feature() >= 21;
  }

  /**
   * Makes xbbg complete its futures on virtual threads: sets {@link Xbbg#setCallbackExecutor} to a
   * virtual-thread-per-task executor when running on Java 21+. The executor is created once and
   * reused by later calls. Restore the default with {@code Xbbg.setCallbackExecutor(null)}.
   *
   * @return {@code true} if the executor was installed; {@code false} on Java 17 to 20, where
   *     nothing changes
   */
  public static boolean useForCallbacks() {
    Optional<ExecutorService> virtual = executor();
    virtual.ifPresent(Xbbg::setCallbackExecutor);
    return virtual.isPresent();
  }

  private static Optional<ExecutorService> executor() {
    if (!isSupported()) {
      return Optional.empty();
    }
    synchronized (LOCK) {
      if (executor == null) {
        executor = create();
      }
      return Optional.of(executor);
    }
  }

  private static ExecutorService create() {
    try {
      Method factory = Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
      return (ExecutorService) factory.invoke(null);
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new IllegalStateException(
          "virtual threads are unavailable on Java " + Runtime.version(), e);
    } catch (InvocationTargetException e) {
      throw new IllegalStateException("could not create a virtual-thread executor", e.getCause());
    }
  }
}
