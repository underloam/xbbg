package io.github.underloam.xbbg;

import java.util.Collection;
import java.util.Optional;

/**
 * A subscription lost data it cannot recover from and was closed. Resubscribe for a fresh image.
 * {@link #code()} is always {@code DATALOSS}.
 */
public class BlpSubscriptionDataLossException extends BlpException {
  private static final long serialVersionUID = 1L;

  /** The topic that lost data, or {@code null}. */
  private final String topic;

  /** The description of the loss, or {@code null}. */
  private final String detail;

  /**
   * Creates an exception.
   *
   * @param message the message
   */
  public BlpSubscriptionDataLossException(String message) {
    this(message, null, null, null, null);
  }

  /**
   * Creates an exception.
   *
   * @param message the message
   * @param topic the affected topic, or {@code null}
   * @param detail what was lost, or {@code null}
   * @param bloombergRequestIds Bloomberg support request ids, or {@code null}
   * @param cause the cause, or {@code null}
   */
  public BlpSubscriptionDataLossException(
      String message,
      String topic,
      String detail,
      Collection<String> bloombergRequestIds,
      Throwable cause) {
    super(message, null, bloombergRequestIds, cause);
    this.topic = topic;
    this.detail = detail;
  }

  @Override
  public BlpErrorKind kind() {
    return BlpErrorKind.DATALOSS;
  }

  /**
   * Returns the topic that lost data.
   *
   * @return the topic, or empty
   */
  public Optional<String> topic() {
    return Optional.ofNullable(topic);
  }

  /**
   * Returns the description of the loss.
   *
   * @return the detail, or empty
   */
  public Optional<String> detail() {
    return Optional.ofNullable(detail);
  }
}
