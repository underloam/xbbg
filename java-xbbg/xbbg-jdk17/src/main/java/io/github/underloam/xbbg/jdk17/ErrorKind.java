package io.github.underloam.xbbg.jdk17;

import io.github.underloam.xbbg.BlpInternalException;
import io.github.underloam.xbbg.BlpLimitException;
import io.github.underloam.xbbg.BlpRequestException;
import io.github.underloam.xbbg.BlpSessionException;
import io.github.underloam.xbbg.BlpSubscriptionDataLossException;
import io.github.underloam.xbbg.BlpTimeoutException;
import io.github.underloam.xbbg.BlpValidationException;
import io.github.underloam.xbbg.BlpapiNotFoundException;
import java.util.Objects;
import java.util.concurrent.CancellationException;

/**
 * The classes of failure an xbbg call can report, as a sealed type for exhaustive pattern matching.
 * Each case carries the typed exception; build one with {@link Errors#classify}.
 */
public sealed interface ErrorKind
    permits ErrorKind.Session,
        ErrorKind.Request,
        ErrorKind.Limit,
        ErrorKind.DataLoss,
        ErrorKind.Validation,
        ErrorKind.Timeout,
        ErrorKind.Internal,
        ErrorKind.SdkNotFound,
        ErrorKind.Argument,
        ErrorKind.State,
        ErrorKind.Cancelled,
        ErrorKind.Other {

  /**
   * Returns the classified exception.
   *
   * @return the exception
   */
  Throwable error();

  /**
   * Session start, connection and worker failures.
   *
   * @param error the exception
   */
  record Session(BlpSessionException error) implements ErrorKind {
    /**
     * Validates the component.
     *
     * @param error the exception
     */
    public Session {
      Objects.requireNonNull(error, "error");
    }
  }

  /**
   * Bloomberg request failures other than limits.
   *
   * @param error the exception
   */
  record Request(BlpRequestException error) implements ErrorKind {
    /**
     * Validates the component.
     *
     * @param error the exception
     */
    public Request {
      Objects.requireNonNull(error, "error");
    }
  }

  /**
   * Request failures caused by a capacity or usage limit.
   *
   * @param error the exception
   */
  record Limit(BlpLimitException error) implements ErrorKind {
    /**
     * Validates the component.
     *
     * @param error the exception
     */
    public Limit {
      Objects.requireNonNull(error, "error");
    }
  }

  /**
   * Terminal subscription data loss.
   *
   * @param error the exception
   */
  record DataLoss(BlpSubscriptionDataLossException error) implements ErrorKind {
    /**
     * Validates the component.
     *
     * @param error the exception
     */
    public DataLoss {
      Objects.requireNonNull(error, "error");
    }
  }

  /**
   * Rejected configuration, request or subscription input.
   *
   * @param error the exception
   */
  record Validation(BlpValidationException error) implements ErrorKind {
    /**
     * Validates the component.
     *
     * @param error the exception
     */
    public Validation {
      Objects.requireNonNull(error, "error");
    }
  }

  /**
   * Request and identity timeouts.
   *
   * @param error the exception
   */
  record Timeout(BlpTimeoutException error) implements ErrorKind {
    /**
     * Validates the component.
     *
     * @param error the exception
     */
    public Timeout {
      Objects.requireNonNull(error, "error");
    }
  }

  /**
   * Internal failures: a closed channel, a native panic, or a broken native contract.
   *
   * @param error the exception
   */
  record Internal(BlpInternalException error) implements ErrorKind {
    /**
     * Validates the component.
     *
     * @param error the exception
     */
    public Internal {
      Objects.requireNonNull(error, "error");
    }
  }

  /**
   * No usable Bloomberg runtime library.
   *
   * @param error the exception
   */
  record SdkNotFound(BlpapiNotFoundException error) implements ErrorKind {
    /**
     * Validates the component.
     *
     * @param error the exception
     */
    public SdkNotFound {
      Objects.requireNonNull(error, "error");
    }
  }

  /**
   * Malformed arguments.
   *
   * @param error the exception
   */
  record Argument(IllegalArgumentException error) implements ErrorKind {
    /**
     * Validates the component.
     *
     * @param error the exception
     */
    public Argument {
      Objects.requireNonNull(error, "error");
    }
  }

  /**
   * Use of a closed engine or subscription, or another invalid state.
   *
   * @param error the exception
   */
  record State(IllegalStateException error) implements ErrorKind {
    /**
     * Validates the component.
     *
     * @param error the exception
     */
    public State {
      Objects.requireNonNull(error, "error");
    }
  }

  /**
   * A cancelled call or an interrupted wait.
   *
   * @param error the exception
   */
  record Cancelled(CancellationException error) implements ErrorKind {
    /**
     * Validates the component.
     *
     * @param error the exception
     */
    public Cancelled {
      Objects.requireNonNull(error, "error");
    }
  }

  /**
   * Any other failure, including {@link io.github.underloam.xbbg.BlpException} subclasses defined
   * outside xbbg.
   *
   * @param error the exception
   */
  record Other(Throwable error) implements ErrorKind {
    /**
     * Validates the component.
     *
     * @param error the exception
     */
    public Other {
      Objects.requireNonNull(error, "error");
    }
  }
}
