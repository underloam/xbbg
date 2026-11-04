package io.github.underloam.xbbg.ext;

import io.github.underloam.xbbg.internal.JsonFields;
import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import io.github.underloam.xbbg.internal.NativeBridge;
import io.github.underloam.xbbg.internal.ResultKind;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Typed wrappers around the synchronous native {@code ext} calls and their JSON results. */
final class ExtCalls {
  private ExtCalls() {}

  static Object json(String function, JsonObjectBuilder args) {
    String text = (String) NativeBridge.extCall(function, args.build(), ResultKind.JSON);
    return JsonFields.parse(text, function);
  }

  static String string(String function, JsonObjectBuilder args) {
    return (String) NativeBridge.extCall(function, args.build(), ResultKind.STRING);
  }

  static boolean bool(String function, JsonObjectBuilder args) {
    return ((Boolean) NativeBridge.extCall(function, args.build(), ResultKind.BOOL)).booleanValue();
  }

  static void run(String function, JsonObjectBuilder args) {
    NativeBridge.extCall(function, args.build(), ResultKind.VOID);
  }

  static Optional<String> optionalString(Object parsed, String what) {
    if (parsed == null) {
      return Optional.empty();
    }
    if (parsed instanceof String) {
      return Optional.of((String) parsed);
    }
    throw JsonFields.malformed(what, "must be a string or null");
  }

  /** {@code [{key, value}]} as an unmodifiable, insertion-ordered map (later keys win). */
  static Map<String, String> pairs(Object parsed, String what) {
    Map<String, String> map = new LinkedHashMap<String, String>();
    for (Object item : JsonFields.array(parsed, what)) {
      Map<String, Object> pair = JsonFields.object(item, what);
      map.put(JsonFields.string(pair, "key"), JsonFields.string(pair, "value"));
    }
    return Collections.unmodifiableMap(map);
  }

  /** A map as {@code [{key, value}]} in iteration order. */
  static List<Map<String, Object>> pairsJson(Map<String, String> map, String what) {
    List<Map<String, Object>> pairs = new ArrayList<Map<String, Object>>(map.size());
    for (Map.Entry<String, String> entry : map.entrySet()) {
      if (entry.getKey() == null || entry.getValue() == null) {
        throw new NullPointerException(what + " must not contain null keys or values");
      }
      Map<String, Object> pair = new LinkedHashMap<String, Object>();
      pair.put("key", entry.getKey());
      pair.put("value", entry.getValue());
      pairs.add(pair);
    }
    return pairs;
  }

  static <T> List<T> copy(Collection<T> values, String what) {
    if (values == null) {
      throw new NullPointerException(what);
    }
    return new ArrayList<T>(values);
  }

  static int intOf(Object parsed, String what) {
    long value = JsonFields.longOf(parsed, what);
    if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
      throw JsonFields.malformed(what, "is out of the 32-bit range");
    }
    return (int) value;
  }
}
