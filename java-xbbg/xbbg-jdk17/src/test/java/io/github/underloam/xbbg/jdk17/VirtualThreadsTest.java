package io.github.underloam.xbbg.jdk17;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.underloam.xbbg.Xbbg;
import io.github.underloam.xbbg.internal.Callbacks;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class VirtualThreadsTest {
  @AfterEach
  void restoreDefaultExecutor() {
    Xbbg.setCallbackExecutor(null);
  }

  @Test
  void installsVirtualThreadCallbacksOnlyOn21AndLater() throws Exception {
    Executor before = Xbbg.callbackExecutor();
    boolean modern = Runtime.version().feature() >= 21;
    assertEquals(modern, VirtualThreads.isSupported());
    assertEquals(modern, VirtualThreads.useForCallbacks());
    if (!modern) {
      assertSame(before, Xbbg.callbackExecutor(), "Java 17-20: executor unchanged");
      return;
    }
    Executor installed = Xbbg.callbackExecutor();
    assertNotSame(before, installed);

    // the completion path every xbbg future uses now runs on a virtual thread
    Method isVirtual = Thread.class.getMethod("isVirtual");
    CompletableFuture<Boolean> ranVirtual = new CompletableFuture<>();
    Callbacks.dispatch(() -> {
      try {
        ranVirtual.complete((Boolean) isVirtual.invoke(Thread.currentThread()));
      } catch (ReflectiveOperationException e) {
        ranVirtual.completeExceptionally(e);
      }
    });
    assertTrue(ranVirtual.get(10, TimeUnit.SECONDS), "callbacks run on virtual threads");

    // later calls reuse the same executor
    assertTrue(VirtualThreads.useForCallbacks());
    assertSame(installed, Xbbg.callbackExecutor());
  }
}
