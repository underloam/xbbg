package io.github.underloam.xbbg.subscription;

/**
 * A retryable backend conversion failure: the native source was rolled back, the subscription stays
 * open, and the original failure reaches the caller (the JavaScript {@code
 * SubscriptionProjectionError}).
 */
final class ProjectionError extends RuntimeException {
  private static final long serialVersionUID = 1L;

  private final Throwable original;

  ProjectionError(Throwable original) {
    super("Subscription backend conversion failed", original, false, false);
    this.original = original;
  }

  Throwable original() {
    return original;
  }
}
