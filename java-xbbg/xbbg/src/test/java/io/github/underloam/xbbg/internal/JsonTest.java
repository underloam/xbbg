package io.github.underloam.xbbg.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.underloam.xbbg.BlpInternalException;
import io.github.underloam.xbbg.KeyColumn;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;

class JsonTest {
  @Test
  void parsesEveryValueTypeIntoTheDocumentedJavaTypes() {
    Object parsed =
        Json.parse(
            " {\"s\":\"x\",\"i\":-12,\"big\":9223372036854775808,\"min\":-9223372036854775808,"
                + "\"d\":1.5e3,\"t\":true,\"f\":false,\"n\":null,\"a\":[1,[2]],\"o\":{}} ");
    Map<String, Object> object = JsonFields.object(parsed, "root");
    assertEquals(
        Arrays.asList("s", "i", "big", "min", "d", "t", "f", "n", "a", "o"),
        Arrays.asList(object.keySet().toArray()));
    assertEquals("x", object.get("s"));
    assertEquals(Long.valueOf(-12), object.get("i"));
    assertEquals(new BigInteger("9223372036854775808"), object.get("big"));
    assertEquals(Long.valueOf(Long.MIN_VALUE), object.get("min"));
    assertEquals(Double.valueOf(1500.0), object.get("d"));
    assertEquals(true, object.get("t"));
    assertEquals(false, object.get("f"));
    assertTrue(object.containsKey("n"));
    assertNull(object.get("n"));
    assertEquals(Arrays.asList(1L, Collections.singletonList(2L)), object.get("a"));
    assertEquals(Collections.emptyMap(), object.get("o"));
  }

  @Test
  void parsedContainersAreUnmodifiable() {
    // Unchecked: Json.parse returns a JSON object as Map<String, Object>.
    @SuppressWarnings("unchecked")
    Map<String, Object> object = (Map<String, Object>) Json.parse("{\"a\":[1]}");
    assertThrows(UnsupportedOperationException.class, () -> object.put("b", 1));
    // Unchecked: Json.parse returns a JSON array as List<Object>.
    @SuppressWarnings("unchecked")
    List<Object> list = (List<Object>) object.get("a");
    assertThrows(UnsupportedOperationException.class, () -> list.add(2));
  }

  @Test
  void decodesEscapesAndSurrogatePairs() {
    assertEquals(
        "a\"b\\c/d\b\f\n\r\t\u00e9\ud83d\ude00",
        Json.parse("\"a\\\"b\\\\c\\/d\\b\\f\\n\\r\\t\\u00E9\\ud83d\\ude00\""));
  }

  @Test
  void rejectsInvalidJsonWithOffsets() {
    String[] invalid = {
      "",
      "  ",
      "{",
      "[1,]",
      "{\"a\":1,}",
      "{\"a\" 1}",
      "{a:1}",
      "01",
      "-",
      "1.",
      ".5",
      "1e",
      "1e+",
      "+1",
      "NaN",
      "Infinity",
      "tru",
      "nul",
      "\"abc",
      "\"a\nb\"",
      "\"\\x\"",
      "\"\\u12\"",
      "\"\\u12G4\"",
      "1 2",
      "{\"a\":1,\"a\":2}",
      "1e400",
      "[1]]",
      "'a'"
    };
    for (String text : invalid) {
      IllegalArgumentException error =
          assertThrows(IllegalArgumentException.class, () -> Json.parse(text), text);
      assertTrue(error.getMessage().startsWith("invalid JSON at offset "), error.getMessage());
    }
  }

  @Test
  void reportsTheOffsetOfDuplicateKeys() {
    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> Json.parse("{\"a\":1,\"a\":2}"));
    assertEquals("invalid JSON at offset 7: duplicate key \"a\"", error.getMessage());
  }

  @Test
  void limitsNestingDepth() {
    StringBuilder deep = new StringBuilder();
    for (int i = 0; i < 600; i++) {
      deep.append('[');
    }
    assertThrows(IllegalArgumentException.class, () -> Json.parse(deep.toString()));
  }

  @Test
  void writesCompactAsciiJson() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("s", "caf\u00e9 \"q\" \\ \n\u0001");
    map.put("skipped", null);
    map.put("i", 3);
    map.put("l", Long.MIN_VALUE);
    map.put("d", 2.0);
    map.put("f", 0.5f);
    map.put("neg", -0.0);
    map.put("frac", 0.1);
    map.put("big", new BigInteger("123456789012345678901234567890"));
    map.put("dec", new BigDecimal("1.50"));
    map.put("b", true);
    map.put("list", Arrays.asList("a", null, 1));
    map.put("ints", new int[] {1, 2});
    map.put("opt", Optional.of("x"));
    map.put("optEmpty", Optional.empty());
    map.put("optInt", OptionalInt.of(7));
    map.put("raw", JsonValue.parse("{\"k\":[true]}"));
    assertEquals(
        "{\"s\":\"caf\\u00e9 \\\"q\\\" \\\\ \\n\\u0001\",\"i\":3,\"l\":-9223372036854775808,"
            + "\"d\":2,\"f\":0.5,\"neg\":-0.0,\"frac\":0.1,"
            + "\"big\":123456789012345678901234567890,\"dec\":1.50,\"b\":true,"
            + "\"list\":[\"a\",null,1],\"ints\":[1,2],\"opt\":\"x\",\"optInt\":7,"
            + "\"raw\":{\"k\":[true]}}",
        Json.write(map));
  }

  @Test
  void writeThenParseRoundTrips() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("text", "\u00e9\ud83d\ude00\u2028");
    map.put("n", 1.25e-7);
    map.put("list", Arrays.asList(1L, "two", false));
    assertEquals(map, Json.parse(Json.write(map)));
  }

  @Test
  void rejectsValuesWithoutAnExplicitJsonForm() {
    assertThrows(IllegalArgumentException.class, () -> Json.write(KeyColumn.TICKER));
    assertThrows(IllegalArgumentException.class, () -> Json.write(new byte[] {1}));
    assertThrows(IllegalArgumentException.class, () -> Json.write(Double.NaN));
    assertThrows(IllegalArgumentException.class, () -> Json.write(Float.POSITIVE_INFINITY));
    assertThrows(IllegalArgumentException.class, () -> Json.write(new Object()));
    assertThrows(
        IllegalArgumentException.class, () -> Json.write(Collections.singletonMap(null, "value")));
  }

  @Test
  void objectBuilderOmitsNullsAndRejectsDuplicateKeys() {
    JsonObjectBuilder builder =
        Json.object().put("a", "x").put("absent", null).put("n", 5L).put("flag", false);
    builder.putNonEmpty("emptyList", Collections.emptyList());
    builder.putNonEmpty("list", Collections.singletonList("v"));
    assertTrue(builder.has("a"));
    assertFalse(builder.has("absent"));
    assertEquals("{\"a\":\"x\",\"n\":5,\"flag\":false,\"list\":[\"v\"]}", builder.build().text());
    IllegalArgumentException duplicate =
        assertThrows(IllegalArgumentException.class, () -> builder.put("a", "y"));
    assertEquals("duplicate JSON key \"a\"", duplicate.getMessage());
    assertThrows(IllegalArgumentException.class, () -> builder.put("nan", Double.NaN));
  }

  @Test
  void arrayBuilderWritesNullsAndNestedValues() {
    JsonValue value =
        Json.array()
            .add("a")
            .add((Object) null)
            .add(2L)
            .add(true)
            .add(Json.object().put("k", 1))
            .build();
    assertEquals("[\"a\",null,2,true,{\"k\":1}]", value.text());
  }

  @Test
  void jsonValueParseValidates() {
    assertEquals("[1]", JsonValue.parse("[1]").text());
    assertThrows(IllegalArgumentException.class, () -> JsonValue.parse("[1"));
    assertEquals(JsonValue.of(Collections.singletonList(1)), JsonValue.parse("[1]"));
  }

  @Test
  void fieldReadersReportMalformedNativeValues() {
    Map<String, Object> object = JsonFields.object(Json.parse("{\"s\":1,\"l\":\"x\"}"), "root");
    BlpInternalException wrongType =
        assertThrows(BlpInternalException.class, () -> JsonFields.string(object, "s"));
    assertEquals("malformed native JSON: s must be a string", wrongType.getMessage());
    BlpInternalException missing =
        assertThrows(BlpInternalException.class, () -> JsonFields.bool(object, "missing"));
    assertEquals("malformed native JSON: missing is missing", missing.getMessage());
    assertThrows(BlpInternalException.class, () -> JsonFields.longValue(object, "l"));
    assertNull(JsonFields.optString(object, "absent"));
    assertThrows(BlpInternalException.class, () -> JsonFields.parse("{", "result"));
    assertThrows(
        BlpInternalException.class,
        () -> JsonFields.intValue(JsonFields.object(Json.parse("{\"i\":4294967296}"), "r"), "i"));
  }
}
