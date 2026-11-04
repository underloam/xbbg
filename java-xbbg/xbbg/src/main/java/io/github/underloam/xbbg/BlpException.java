package io.github.underloam.xbbg;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Base class of every error raised by the Bloomberg engine.
 *
 * <p>Futures returned by this library complete exceptionally with a subclass; {@link
 * Xbbg#await(java.util.concurrent.CompletableFuture)} rethrows it unwrapped.
 */
public abstract class BlpException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  /** The error code supplied with the error, or {@code null} to report the kind's name. */
  private final String code;

  /** Bloomberg support request ids, possibly empty. */
  private final String[] bloombergRequestIds;

  /**
   * Creates an exception.
   *
   * @param message the message
   * @param code the error code, or {@code null} for the kind's name
   * @param bloombergRequestIds Bloomberg support request ids, or {@code null} for none
   * @param cause the cause, or {@code null}
   */
  protected BlpException(
      String message, String code, Collection<String> bloombergRequestIds, Throwable cause) {
    super(message, cause);
    this.code = code;
    this.bloombergRequestIds =
        bloombergRequestIds == null
            ? new String[0]
            : bloombergRequestIds.toArray(new String[bloombergRequestIds.size()]);
  }

  /**
   * Returns the error category.
   *
   * @return the kind
   */
  public abstract BlpErrorKind kind();

  /**
   * Returns the error code: the code supplied with the error, or the kind's name ({@code SESSION},
   * {@code REQUEST}, {@code LIMIT}, {@code DATALOSS}, {@code VALIDATION}, {@code TIMEOUT}, {@code
   * INTERNAL}, {@code SDK_NOT_FOUND}).
   *
   * @return the code
   */
  public String code() {
    return code != null ? code : kind().name();
  }

  /**
   * Returns the Bloomberg support request id when exactly one request contributed to the error.
   *
   * @return the single request id, or empty
   */
  public Optional<String> bloombergRequestId() {
    return bloombergRequestIds.length == 1
        ? Optional.of(bloombergRequestIds[0])
        : Optional.<String>empty();
  }

  /**
   * Returns every Bloomberg support request id attached to the error. These ids are assigned by
   * Bloomberg (quote them to Bloomberg support); they differ from xbbg's own {@code requestId}
   * label and are never invented.
   *
   * @return the ids, possibly empty
   */
  public List<String> bloombergRequestIds() {
    return Collections.unmodifiableList(Arrays.asList(bloombergRequestIds.clone()));
  }
}
