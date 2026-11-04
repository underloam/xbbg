package io.github.underloam.xbbg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.underloam.xbbg.internal.Json;
import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.time.Instant;
import java.time.LocalDate;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OverridesTest {
  private static Map<String, Object> map(Object... keysAndValues) {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      map.put((String) keysAndValues[i], keysAndValues[i + 1]);
    }
    return map;
  }

  private static List<StringPair> pairs(String... keysAndValues) {
    StringPair[] result = new StringPair[keysAndValues.length / 2];
    for (int i = 0; i < keysAndValues.length; i += 2) {
      result[i / 2] = StringPair.of(keysAndValues[i], keysAndValues[i + 1]);
    }
    return Arrays.asList(result);
  }

  @Test
  void laterValuesWinButKeepTheFirstPosition() {
    Overrides merged =
        Overrides.of(map("A", "EUR", "D", LocalDate.of(2023, 1, 17))).merge(map("A", "USD"));
    assertEquals(pairs("A", "USD", "D", "20230117"), merged.pairs());
    assertEquals(map("A", "USD", "D", "20230117"), merged.toMap());
  }

  @Test
  void valuesAreConvertedLikeTheJavaScriptPackage() {
    Overrides overrides =
        Overrides.of(
            map(
                "INT",
                3,
                "DOUBLE",
                40.0,
                "FRACTION",
                0.5,
                "TINY",
                1e-7,
                "HUGE",
                1e21,
                "BOOL",
                true,
                "INSTANT",
                Instant.parse("2023-01-17T23:00:00Z"),
                "DATE",
                Date.from(Instant.parse("2023-01-18T01:00:00Z")),
                "TEXT",
                "x"));
    assertEquals(
        pairs(
            "INT",
            "3",
            "DOUBLE",
            "40",
            "FRACTION",
            "0.5",
            "TINY",
            "1e-7",
            "HUGE",
            "1e+21",
            "BOOL",
            "true",
            "INSTANT",
            "20230117",
            "DATE",
            "20230118",
            "TEXT",
            "x"),
        overrides.pairs());
  }

  @Test
  void nestedSourcesBecomePerSecurityOverrides() {
    Overrides overrides =
        Overrides.of(
            map(
                "EQY_FUND_CRNCY", "EUR",
                "IBM US Equity", map("EQY_FUND_CRNCY", "USD", "NESTED", map("X", "dropped")),
                "MSFT US Equity", Overrides.of("BEST_FPERIOD_OVERRIDE", "1BF"),
                "EMPTY US Equity", Collections.emptyMap()));
    assertEquals(pairs("EQY_FUND_CRNCY", "EUR"), overrides.pairs());
    assertEquals(
        Arrays.asList(
            new SecurityOverride("IBM US Equity", pairs("EQY_FUND_CRNCY", "USD")),
            new SecurityOverride("MSFT US Equity", pairs("BEST_FPERIOD_OVERRIDE", "1BF"))),
        overrides.securityOverrides());
    assertTrue(overrides.hasSecurityOverrides());
  }

  @Test
  void forSecurityMergesInFirstSeenSecurityOrder() {
    Overrides overrides =
        Overrides.of("A", "1")
            .forSecurity("SEC1", Overrides.of("X", "1"))
            .forSecurity("SEC2", map("Y", "2"))
            .forSecurity("SEC1", Overrides.of("X", "3"), map("Z", "4"));
    assertEquals(
        Arrays.asList(
            new SecurityOverride("SEC1", pairs("X", "3", "Z", "4")),
            new SecurityOverride("SEC2", pairs("Y", "2"))),
        overrides.securityOverrides());
  }

  @Test
  void acceptsEveryEntryShape() {
    Overrides overrides =
        Overrides.ofEntries(
            Arrays.asList(
                StringPair.of("A", "1"),
                new AbstractMap.SimpleEntry<String, Object>("B", 2),
                new Object[] {"C", 3.0},
                Arrays.asList("D", false)));
    assertEquals(pairs("A", "1", "B", "2", "C", "3", "D", "false"), overrides.pairs());
    IllegalArgumentException badEntry =
        assertThrows(
            IllegalArgumentException.class,
            () -> Overrides.ofEntries(Collections.singletonList(new Object[] {"only key"})));
    assertEquals(
        "override entries must be StringPair, Map.Entry, or two-element key/value arrays or lists",
        badEntry.getMessage());
  }

  @Test
  void rejectsBadSourcesAndNullValues() {
    IllegalArgumentException badSource =
        assertThrows(IllegalArgumentException.class, () -> Overrides.combine("A=1"));
    assertEquals(
        "Overrides sources must be Overrides, Maps, or Iterables of override entries",
        badSource.getMessage());
    IllegalArgumentException nullValue =
        assertThrows(IllegalArgumentException.class, () -> Overrides.of(map("A", null)));
    assertEquals("override value for 'A' must not be null", nullValue.getMessage());
  }

  @Test
  void emptyOverridesAreShared() {
    assertSame(Overrides.empty(), Overrides.of(Collections.<String, Object>emptyMap()));
    assertTrue(Overrides.empty().isEmpty());
    assertFalse(Overrides.of("A", "1").isEmpty());
    assertTrue(Overrides.of(map("SEC", Collections.emptyMap())).isEmpty());
  }

  @Test
  void writesRequestPartsOnlyWhenPresent() {
    JsonObjectBuilder none = Json.object();
    Overrides.empty().encodeRequestParts(none);
    assertEquals("{}", none.build().text());

    JsonObjectBuilder both = Json.object();
    Overrides.of("A", "1")
        .forSecurity("IBM US Equity", Overrides.of("B", "2"))
        .encodeRequestParts(both);
    assertEquals(
        "{\"overrides\":[{\"key\":\"A\",\"value\":\"1\"}],"
            + "\"securityOverrides\":[{\"security\":\"IBM US Equity\","
            + "\"overrides\":[{\"key\":\"B\",\"value\":\"2\"}]}]}",
        both.build().text());

    JsonObjectBuilder securityOnly = Json.object();
    Overrides.empty().forSecurity("S", Overrides.of("B", "2")).encodeRequestParts(securityOnly);
    assertEquals(
        "{\"securityOverrides\":[{\"security\":\"S\",\"overrides\":[{\"key\":\"B\",\"value\":\"2\"}]}]}",
        securityOnly.build().text());
  }

  @Test
  void globalPairsOnlyRejectsPerSecurityOverrides() {
    assertEquals(pairs("A", "1"), Overrides.of("A", "1").globalPairsOnly());
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> Overrides.empty().forSecurity("S", Overrides.of("B", "2")).globalPairsOnly());
    assertEquals(
        "Per-security overrides are only supported by bdp(), bdh(), and bds()", error.getMessage());
  }

  @Test
  void equalityFollowsContent() {
    assertEquals(Overrides.of("A", "1"), Overrides.of(map("A", 1)));
    assertEquals(Overrides.of("A", "1").hashCode(), Overrides.of(map("A", 1)).hashCode());
    assertFalse(Overrides.of("A", "1").equals(Overrides.of("A", "2")));
  }
}
