package io.github.underloam.xbbg.internal;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The executor that completes every native future, so user callbacks never run on a native bridge
 * thread.
 *
 * <p>Internal API: not covered by compatibility guarantees.
 */
public final class Callbacks {
  private static final Logger LOG = Logger.getLogger("io.github.underloam.xbbg");
  private static final ExecutorService DEFAULT = Executors.newCachedThreadPool(new DaemonFactory());
  private static volatile Executor current = DEFAULT;

  /** Set while {@link #withInternalCompletions} starts calls on this thread. */
  private static final ThreadLocal<Boolean> INTERNAL = new ThreadLocal<Boolean>();

  private Callbacks() {}

  /**
   * Returns the executor that completes native futures.
   *
   * @return the current executor
   */
  public static Executor executor() {
    return current;
  }

  /**
   * Returns the built-in executor: a cached pool of daemon threads named {@code xbbg-callback-N}.
   *
   * @return the default executor
   */
  public static Executor defaultExecutor() {
    return DEFAULT;
  }

  /**
   * Replaces the executor that completes native futures.
   *
   * @param executor the executor, or {@code null} for the default
   */
  public static void setExecutor(Executor executor) {
    current = executor == null ? DEFAULT : executor;
  }

  /**
   * Starts native calls whose futures complete on the {@linkplain #defaultExecutor() default
   * executor} instead of the user's: for results xbbg itself waits for (provisioning while loading
   * the natives), which a user executor whose thread is running that very wait would never deliver.
   *
   * @param start starts the calls on the current thread
   * @param <T> the result type
   * @return what {@code start} returns
   */
  public static <T> T withInternalCompletions(Supplier<T> start) {
    Boolean outer = INTERNAL.get();
    INTERNAL.set(Boolean.TRUE);
    try {
      return start.get();
    } finally {
      if (outer == null) {
        INTERNAL.remove();
      }
    }
  }

  /** Whether a call started now on this thread completes on the default executor. */
  static boolean internalCompletions() {
    return INTERNAL.get() != null;
  }

  /**
   * Runs a completion on the current executor. If that executor rejects it, the default executor
   * runs it; if both reject it, it runs on the calling thread, so a completion is never lost.
   *
   * @param completion the completion
   */
  public static void dispatch(Runnable completion) {
    dispatch(completion, current);
  }

  /**
   * Runs a completion on {@code executor} ({@link #dispatch(Runnable)} for the current executor,
   * the default executor for internal calls), with the same fallbacks.
   */
  static void dispatch(Runnable completion, Executor executor) {
    try {
      executor.execute(completion);
      return;
    } catch (Throwable rejected) {
      // Executors compare by identity: retrying the very executor that rejected it is pointless.
      if (!DEFAULT.equals(executor)) {
        LOG.log(
            Level.WARNING,
            "xbbg callback executor rejected a completion; using the default executor",
            rejected);
        try {
          DEFAULT.execute(completion);
          return;
        } catch (Throwable alsoRejected) {
          rejected.addSuppressed(alsoRejected);
        }
      }
      LOG.log(Level.WARNING, "xbbg completion runs on the calling thread", rejected);
    }
    completion.run();
  }

  private static final class DaemonFactory implements ThreadFactory {
    private final AtomicInteger next = new AtomicInteger();

    @Override
    public Thread newThread(Runnable runnable) {
      Thread thread = new Thread(runnable, "xbbg-callback-" + next.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    }
  }
}
