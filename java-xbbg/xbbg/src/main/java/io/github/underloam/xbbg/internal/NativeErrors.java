package io.github.underloam.xbbg.internal;

import io.github.underloam.xbbg.BlpInternalException;
import io.github.underloam.xbbg.BlpLimitException;
import io.github.underloam.xbbg.BlpRequestException;
import io.github.underloam.xbbg.BlpSessionException;
import io.github.underloam.xbbg.BlpSubscriptionDataLossException;
import io.github.underloam.xbbg.BlpTimeoutException;
import io.github.underloam.xbbg.BlpValidationException;
import io.github.underloam.xbbg.BlpapiNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;

/** Builds Java exceptions from a native error kind, message and context JSON. */
final class NativeErrors {
  static final int SESSION = 1;
  static final int REQUEST = 2;
  static final int LIMIT = 3;
  static final int DATALOSS = 4;
  static final int VALIDATION = 5;
  static final int TIMEOUT = 6;
  static final int INTERNAL = 7;
  static final int ARGUMENT = 8;
  static final int STATE = 9;
  static final int CANCELLED = 10;
  static final int SDK_NOT_FOUND = 11;

  private NativeErrors() {}

  static Throwable build(int kind, String message, String contextJson) {
    String text = message == null ? "" : message;
    Context context = Context.EMPTY;
    RuntimeException contextProblem = null;
    if (contextJson != null) {
      try {
        context = Context.parse(contextJson);
      } catch (RuntimeException e) {
        contextProblem = e;
      }
    }
    Throwable error = create(kind, text, context);
    if (contextProblem != null) {
      error.addSuppressed(contextProblem);
    }
    return error;
  }

  private static Throwable create(int kind, String message, Context c) {
    switch (kind) {
      case SESSION:
        return new BlpSessionException(message, c.code, c.ids, null);
      case REQUEST:
        return new BlpRequestException(
            message, c.service, c.operation, c.requestId, c.code, c.ids, null);
      case LIMIT:
        return new BlpLimitException(
            message, c.service, c.operation, c.requestId, c.code, c.ids, null);
      case DATALOSS:
        return new BlpSubscriptionDataLossException(message, c.topic, c.detail, c.ids, null);
      case VALIDATION:
        return new BlpValidationException(message, c.element, c.suggestion, c.code, c.ids, null);
      case TIMEOUT:
        return new BlpTimeoutException(message, c.requestId, c.code, c.ids, null);
      case INTERNAL:
        return new BlpInternalException(message, c.code, c.ids, null);
      case ARGUMENT:
        return new IllegalArgumentException(message);
      case STATE:
        return new IllegalStateException(message);
      case CANCELLED:
        return new CancellationException(message);
      case SDK_NOT_FOUND:
        return new BlpapiNotFoundException(message, c.code, null);
      default:
        return new BlpInternalException("native error of unknown kind " + kind + ": " + message);
    }
  }

  private static final class Context {
    static final Context EMPTY = new Context(null);

    final String service;
    final String operation;
    final String requestId;
    final List<String> ids;
    final String topic;
    final String detail;
    final String element;
    final String suggestion;
    final String code;

    private Context(Map<String, Object> object) {
      if (object == null) {
        service = null;
        operation = null;
        requestId = null;
        ids = null;
        topic = null;
        detail = null;
        element = null;
        suggestion = null;
        code = null;
        return;
      }
      service = JsonFields.optString(object, "service");
      operation = JsonFields.optString(object, "operation");
      requestId = JsonFields.optString(object, "requestId");
      ids = JsonFields.optStrings(object, "bloombergRequestIds");
      topic = JsonFields.optString(object, "topic");
      detail = JsonFields.optString(object, "detail");
      element = JsonFields.optString(object, "element");
      suggestion = JsonFields.optString(object, "suggestion");
      code = JsonFields.optString(object, "code");
    }

    static Context parse(String json) {
      return new Context(
          JsonFields.object(JsonFields.parse(json, "error context"), "error context"));
    }
  }
}
