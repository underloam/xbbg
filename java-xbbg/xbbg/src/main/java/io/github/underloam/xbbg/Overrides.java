package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.Json;
import io.github.underloam.xbbg.internal.JsonArrayBuilder;
import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import io.github.underloam.xbbg.internal.Text;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Immutable Bloomberg field overrides: global pairs plus per-security overrides (a port of the
 * JavaScript {@code ovr()} / {@code OverrideSpec}).
 *
 * <p>Sources merge left to right: a later value replaces an earlier one but the key keeps its first
 * position. Values are converted to request text: dates and {@code java.time} values to {@code
 * YYYYMMDD} (see {@link Dates#formatDate(TemporalAccessor)}), numbers as JavaScript prints them
 * ({@code 40.0} is {@code "40"}), everything else with {@code String.valueOf}. A value that is
 * itself an {@code Overrides}, a {@link Map} or an {@link Iterable} of entries defines overrides
 * for the security named by its key (only its global pairs are used; a security with no pairs is
 * dropped). {@code null} values are rejected.
 *
 * <pre>{@code
 * Overrides o = Overrides.of("EQY_FUND_CRNCY", "EUR").forSecurity("IBM US Equity",
 *     Overrides.of("EQY_FUND_CRNCY", "USD"));
 * }</pre>
 */
public final class Overrides implements Iterable<StringPair> {
  private static final String SOURCE_ERROR =
      "Overrides sources must be Overrides, Maps, or Iterables of override entries";
  private static final String ENTRY_ERROR =
      "override entries must be StringPair, Map.Entry, or two-element key/value arrays or lists";
  private static final Overrides EMPTY =
      new Overrides(Collections.<StringPair>emptyList(), Collections.<SecurityOverride>emptyList());

  private final List<StringPair> pairs;
  private final List<SecurityOverride> securityOverrides;

  private Overrides(List<StringPair> pairs, List<SecurityOverride> securityOverrides) {
    this.pairs = Collections.unmodifiableList(pairs);
    this.securityOverrides = Collections.unmodifiableList(securityOverrides);
  }

  /**
   * Creates overrides from lists the caller no longer uses. {@link State} builds through this
   * factory rather than the private constructor, so Java 8 bytecode has no synthetic non-private
   * constructor next to the {@link #empty()} instance.
   */
  private static Overrides create(
      List<StringPair> pairs, List<SecurityOverride> securityOverrides) {
    return new Overrides(pairs, securityOverrides);
  }

  /**
   * Returns overrides with no pairs.
   *
   * @return the empty overrides
   */
  public static Overrides empty() {
    return EMPTY;
  }

  /**
   * Creates overrides from a map, in iteration order (use a {@link LinkedHashMap} to control it).
   *
   * @param source field to value; a nested {@code Overrides}, {@code Map} or entry {@code Iterable}
   *     value applies to the security named by its key
   * @return the overrides
   * @throws IllegalArgumentException if a key or value is null or a value cannot be converted
   */
  public static Overrides of(Map<String, ?> source) {
    return combine(source);
  }

  /**
   * Creates overrides with one pair.
   *
   * @param field the override field
   * @param value the value
   * @return the overrides
   * @throws IllegalArgumentException if the value is null or cannot be converted
   */
  public static Overrides of(String field, Object value) {
    State state = new State();
    state.addPair(field, value);
    return state.build();
  }

  /**
   * Creates overrides from entries: {@link StringPair}s, {@link Map.Entry}s, or two-element {@code
   * [key, value]} arrays or lists.
   *
   * @param entries the entries
   * @return the overrides
   * @throws IllegalArgumentException if an entry has another shape
   */
  public static Overrides ofEntries(Iterable<?> entries) {
    return combine(entries);
  }

  /**
   * Merges sources left to right: {@code Overrides}, {@code Map}s and entry {@code Iterable}s.
   *
   * @param sources the sources
   * @return the merged overrides
   * @throws IllegalArgumentException if a source has another type
   */
  public static Overrides combine(Object... sources) {
    State state = new State();
    for (Object source : sources) {
      state.addSource(source);
    }
    return state.build();
  }

  /**
   * Returns these overrides merged with more sources (later values win).
   *
   * @param sources {@code Overrides}, {@code Map}s or entry {@code Iterable}s
   * @return the merged overrides
   */
  public Overrides merge(Object... sources) {
    Object[] all = new Object[sources.length + 1];
    all[0] = this;
    System.arraycopy(sources, 0, all, 1, sources.length);
    return combine(all);
  }

  /**
   * Returns these overrides with one more pair (replacing the value of an existing key).
   *
   * @param field the override field
   * @param value the value
   * @return the merged overrides
   */
  public Overrides with(String field, Object value) {
    return combine(this, of(field, value));
  }

  /**
   * Returns these overrides plus overrides that apply to one security only.
   *
   * @param security the security
   * @param sources the security's overrides ({@code Overrides}, {@code Map}s or entry {@code
   *     Iterable}s, merged left to right)
   * @return the merged overrides
   */
  public Overrides forSecurity(String security, Object... sources) {
    if (security == null) {
      throw new IllegalArgumentException("security must not be null");
    }
    Map<String, Object> nested = new LinkedHashMap<String, Object>();
    nested.put(security, combine(sources));
    return combine(this, nested);
  }

  /**
   * Returns the global pairs.
   *
   * @return an unmodifiable list, in key order of first appearance
   */
  public List<StringPair> pairs() {
    return pairs;
  }

  /**
   * Returns the per-security overrides.
   *
   * @return an unmodifiable list, in security order of first appearance
   */
  public List<SecurityOverride> securityOverrides() {
    return securityOverrides;
  }

  /**
   * Returns the global pairs as a map.
   *
   * @return an unmodifiable insertion-ordered map
   */
  public Map<String, String> toMap() {
    Map<String, String> map = new LinkedHashMap<String, String>();
    for (StringPair pair : pairs) {
      map.put(pair.key(), pair.value());
    }
    return Collections.unmodifiableMap(map);
  }

  /**
   * Whether there are neither global nor per-security overrides.
   *
   * @return {@code true} when empty
   */
  public boolean isEmpty() {
    return pairs.isEmpty() && securityOverrides.isEmpty();
  }

  /**
   * Whether any per-security overrides are present.
   *
   * @return {@code true} if {@link #securityOverrides()} is not empty
   */
  public boolean hasSecurityOverrides() {
    return !securityOverrides.isEmpty();
  }

  /**
   * Iterates over the global pairs.
   *
   * @return the iterator
   */
  @Override
  public Iterator<StringPair> iterator() {
    return pairs.iterator();
  }

  /**
   * Adds {@code overrides} and {@code securityOverrides} to a request object, each only when
   * non-empty (TS {@code mapOverridesToRequestParts}).
   */
  void encodeRequestParts(JsonObjectBuilder request) {
    if (!pairs.isEmpty()) {
      request.put("overrides", Pairs.json(pairs));
    }
    if (!securityOverrides.isEmpty()) {
      JsonArrayBuilder array = Json.array();
      for (SecurityOverride entry : securityOverrides) {
        array.add(
            Json.object()
                .put("security", entry.security())
                .put("overrides", Pairs.json(entry.overrides())));
      }
      request.put("securityOverrides", array.build());
    }
  }

  /**
   * Returns the global pairs for requests that do not support per-security overrides (TS {@code
   * mapOverridesToPairs}).
   *
   * @throws IllegalArgumentException if per-security overrides are present
   */
  List<StringPair> globalPairsOnly() {
    if (!securityOverrides.isEmpty()) {
      throw new IllegalArgumentException(
          "Per-security overrides are only supported by bdp(), bdh(), and bds()");
    }
    return pairs;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Overrides)) {
      return false;
    }
    Overrides that = (Overrides) other;
    return pairs.equals(that.pairs) && securityOverrides.equals(that.securityOverrides);
  }

  @Override
  public int hashCode() {
    return 31 * pairs.hashCode() + securityOverrides.hashCode();
  }

  @Override
  public String toString() {
    return "Overrides{pairs=" + pairs + ", securityOverrides=" + securityOverrides + "}";
  }

  /** Merge state: global pairs and per-security pairs, both in first-seen order. */
  private static final class State {
    private final Map<String, String> merged = new LinkedHashMap<String, String>();
    private final Map<String, Map<String, String>> securities =
        new LinkedHashMap<String, Map<String, String>>();

    void addSource(Object source) {
      if (source instanceof Overrides) {
        Overrides overrides = (Overrides) source;
        for (StringPair pair : overrides.pairs) {
          merged.put(pair.key(), pair.value());
        }
        for (SecurityOverride entry : overrides.securityOverrides) {
          addSecurity(entry.security(), entry.overrides());
        }
      } else if (source instanceof Map) {
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) source).entrySet()) {
          if (entry.getKey() == null) {
            throw new IllegalArgumentException("override keys must not be null");
          }
          addPair(String.valueOf(entry.getKey()), entry.getValue());
        }
      } else if (source instanceof Iterable && !(source instanceof Overrides)) {
        for (Object entry : (Iterable<?>) source) {
          addEntry(entry);
        }
      } else {
        throw new IllegalArgumentException(SOURCE_ERROR);
      }
    }

    void addEntry(Object entry) {
      if (entry instanceof StringPair) {
        StringPair pair = (StringPair) entry;
        addPair(pair.key(), pair.value());
      } else if (entry instanceof Map.Entry) {
        Map.Entry<?, ?> mapEntry = (Map.Entry<?, ?>) entry;
        if (mapEntry.getKey() == null) {
          throw new IllegalArgumentException("override keys must not be null");
        }
        addPair(String.valueOf(mapEntry.getKey()), mapEntry.getValue());
      } else if (entry instanceof Object[] && ((Object[]) entry).length == 2) {
        Object[] tuple = (Object[]) entry;
        addPair(keyText(tuple[0]), tuple[1]);
      } else if (entry instanceof List && ((List<?>) entry).size() == 2) {
        List<?> tuple = (List<?>) entry;
        addPair(keyText(tuple.get(0)), tuple.get(1));
      } else {
        throw new IllegalArgumentException(ENTRY_ERROR);
      }
    }

    void addPair(String key, Object value) {
      if (key == null) {
        throw new IllegalArgumentException("override keys must not be null");
      }
      if (value == null) {
        throw new IllegalArgumentException("override value for '" + key + "' must not be null");
      }
      if (value instanceof Overrides || value instanceof Map) {
        addSecurity(key, combine(value).pairs);
      } else if (value instanceof Iterable && !(value instanceof CharSequence)) {
        addSecurity(key, combine(value).pairs);
      } else if (value instanceof Object[]) {
        addSecurity(key, combine(Arrays.asList((Object[]) value)).pairs);
      } else {
        merged.put(key, valueText(key, value));
      }
    }

    private void addSecurity(String security, List<StringPair> pairs) {
      if (pairs.isEmpty()) {
        return;
      }
      Map<String, String> target = securities.get(security);
      if (target == null) {
        target = new LinkedHashMap<String, String>();
        securities.put(security, target);
      }
      for (StringPair pair : pairs) {
        target.put(pair.key(), pair.value());
      }
    }

    Overrides build() {
      if (merged.isEmpty() && securities.isEmpty()) {
        return EMPTY;
      }
      List<StringPair> pairs = new ArrayList<StringPair>(merged.size());
      for (Map.Entry<String, String> entry : merged.entrySet()) {
        pairs.add(new StringPair(entry.getKey(), entry.getValue()));
      }
      List<SecurityOverride> perSecurity = new ArrayList<SecurityOverride>(securities.size());
      for (Map.Entry<String, Map<String, String>> entry : securities.entrySet()) {
        List<StringPair> securityPairs = new ArrayList<StringPair>(entry.getValue().size());
        for (Map.Entry<String, String> pair : entry.getValue().entrySet()) {
          securityPairs.add(new StringPair(pair.getKey(), pair.getValue()));
        }
        perSecurity.add(new SecurityOverride(entry.getKey(), securityPairs));
      }
      return create(pairs, perSecurity);
    }

    private static String keyText(Object key) {
      if (key == null) {
        throw new IllegalArgumentException("override keys must not be null");
      }
      return String.valueOf(key);
    }

    private static String valueText(String key, Object value) {
      if (value instanceof TemporalAccessor || value instanceof Date) {
        try {
          return Dates.formatDateValue(value);
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException(
              "override value for '" + key + "' is not a date: " + e.getMessage(), e);
        }
      }
      return Text.toRequestString(value);
    }
  }
}
