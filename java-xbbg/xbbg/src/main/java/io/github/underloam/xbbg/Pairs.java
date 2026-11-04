package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.Json;
import io.github.underloam.xbbg.internal.JsonArrayBuilder;
import io.github.underloam.xbbg.internal.JsonValue;
import io.github.underloam.xbbg.internal.Text;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Wire helpers for {@link StringPair} lists, shared by the request layers. */
final class Pairs {
  private Pairs() {}

  /**
   * Serializes pairs as {@code [{"key": ..., "value": ...}, ...]}.
   *
   * @param pairs the pairs
   * @return the JSON array
   */
  static JsonValue json(Collection<StringPair> pairs) {
    JsonArrayBuilder array = Json.array();
    for (StringPair pair : pairs) {
      array.add(Json.object().put("key", pair.key()).put("value", pair.value()));
    }
    return array.build();
  }

  /**
   * Converts a map to pairs in iteration order, values through {@link Text#toRequestString}
   * (JavaScript's {@code String(value)}); like the TS {@code mapObjectToPairs}.
   *
   * @param map the map, or {@code null}
   * @return the pairs, or {@code null} when {@code map} is null
   * @throws IllegalArgumentException if a key or value is null
   */
  static List<StringPair> fromMap(Map<String, ?> map) {
    if (map == null) {
      return null;
    }
    List<StringPair> pairs = new ArrayList<StringPair>(map.size());
    for (Map.Entry<String, ?> entry : map.entrySet()) {
      if (entry.getKey() == null) {
        throw new IllegalArgumentException("keys must not be null");
      }
      if (entry.getValue() == null) {
        throw new IllegalArgumentException("value for '" + entry.getKey() + "' must not be null");
      }
      pairs.add(new StringPair(entry.getKey(), Text.toRequestString(entry.getValue())));
    }
    return Collections.unmodifiableList(pairs);
  }

  /**
   * Replaces the value of the first pair with the key, else appends the pair (TS {@code
   * upsertStringPair}).
   *
   * @param pairs the pairs, or {@code null}
   * @param key the key
   * @param value the value
   * @return the updated pairs
   */
  static List<StringPair> upsert(List<StringPair> pairs, String key, String value) {
    List<StringPair> result = new ArrayList<StringPair>();
    boolean replaced = false;
    if (pairs != null) {
      for (StringPair pair : pairs) {
        if (!replaced && pair.key().equals(key)) {
          result.add(new StringPair(key, value));
          replaced = true;
        } else {
          result.add(pair);
        }
      }
    }
    if (!replaced) {
      result.add(new StringPair(key, value));
    }
    return Collections.unmodifiableList(result);
  }
}
