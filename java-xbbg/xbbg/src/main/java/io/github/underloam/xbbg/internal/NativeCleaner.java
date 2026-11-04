package io.github.underloam.xbbg.internal;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A Java 8 replacement for {@code java.lang.ref.Cleaner}: runs a release action once, either when
 * {@link Cleanable#clean()} is called or after the owner became unreachable. A daemon thread named
 * {@code xbbg-cleaner} runs the actions for collected owners.
 *
 * <p>The action must not reference the owner, or the owner never becomes unreachable.
 *
 * <p>Internal API: not covered by compatibility guarantees.
 */
public final class NativeCleaner {
  private static final Logger LOG = Logger.getLogger("io.github.underloam.xbbg");
  private static final ReferenceQueue<Object> QUEUE = new ReferenceQueue<Object>();
  private static final Set<Ref> LIVE =
      Collections.newSetFromMap(new ConcurrentHashMap<Ref, Boolean>());
  private static final AtomicBoolean STARTED = new AtomicBoolean();

  private NativeCleaner() {}

  /** A registered release action. */
  public interface Cleanable {
    /** Runs the action now unless it already ran; later calls do nothing. */
    void clean();
  }

  /**
   * Registers an action to run once the owner is unreachable, unless it runs earlier through {@link
   * Cleanable#clean()}.
   *
   * @param owner the object whose reachability is tracked
   * @param action the release action; must not reference {@code owner}
   * @return the handle that runs the action early
   */
  public static Cleanable register(Object owner, Runnable action) {
    if (owner == null || action == null) {
      throw new NullPointerException(owner == null ? "owner" : "action");
    }
    startThread();
    Ref ref = new Ref(owner, action);
    LIVE.add(ref);
    return ref;
  }

  static int liveCount() {
    return LIVE.size();
  }

  private static void startThread() {
    if (STARTED.get() || !STARTED.compareAndSet(false, true)) {
      return;
    }
    Thread thread =
        new Thread(
            new Runnable() {
              @Override
              public void run() {
                while (true) {
                  try {
                    Reference<?> ref = QUEUE.remove();
                    if (ref instanceof Ref) {
                      ((Ref) ref).clean();
                    }
                  } catch (InterruptedException interrupted) {
                    // The cleaner lives as long as the JVM; ignore spurious interrupts.
                  } catch (Throwable error) {
                    LOG.log(Level.WARNING, "xbbg native cleanup failed", error);
                  }
                }
              }
            },
            "xbbg-cleaner");
    thread.setDaemon(true);
    thread.start();
  }

  private static final class Ref extends PhantomReference<Object> implements Cleanable {
    private final AtomicBoolean done = new AtomicBoolean();
    private final Runnable action;

    Ref(Object owner, Runnable action) {
      super(owner, QUEUE);
      this.action = action;
    }

    @Override
    public void clean() {
      if (done.compareAndSet(false, true)) {
        LIVE.remove(this);
        clear();
        action.run();
      }
    }
  }
}
