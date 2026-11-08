package io.github.underloam.xbbg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import org.junit.jupiter.api.Test;

class DatesTest {
  private static final String AMBIGUOUS_SUFFIX =
      ": month/day order cannot be inferred. Use ISO 8601 (YYYY-MM-DD), Bloomberg-native"
          + " (YYYYMMDD), or pass a Date / Luxon DateTime.";

  @Test
  void formatDateReturnsNullForMissingInput() {
    assertNull(Dates.formatDate((String) null));
    assertNull(Dates.formatDate("   "));
    assertNull(Dates.formatDate("\u00a0\ufeff"));
    assertNull(Dates.formatDate((TemporalAccessor) null));
    assertNull(Dates.formatDate((Date) null));
  }

  @Test
  void formatDateHandlesBloombergAndIsoText() {
    assertEquals("20240117", Dates.formatDate("20240117"));
    assertEquals("20241399", Dates.formatDate(" 20241399 "), "8-digit text is not validated");
    assertEquals("20240117", Dates.formatDate("2024-01-17"));
    assertEquals("20240117", Dates.formatDate("2024/01/17"));
  }

  @Test
  void formatDateReadsDateTimeTextInItsOwnZone() {
    assertEquals("20240117", Dates.formatDate("2024-01-17T23:30:00"));
    assertEquals("20240117", Dates.formatDate("2024-01-17 23:30"));
    assertEquals("20240117", Dates.formatDate("2024-01-17T23:30:00-05:00"));
    assertEquals("20240118", Dates.formatDate("2024-01-18T01:30:00Z"));
    assertEquals("20240117", Dates.formatDate("2024-01-17T23:30:00-05:00[America/New_York]"));
    assertEquals("20240117", Dates.formatDate("2024/01/17 10:30"));
  }

  @Test
  void formatDateRejectsAmbiguousAndUnparseableText() {
    IllegalArgumentException ambiguous =
        assertThrows(IllegalArgumentException.class, () -> Dates.formatDate("01/17/2024"));
    assertEquals("Ambiguous date format \"01/17/2024\"" + AMBIGUOUS_SUFFIX, ambiguous.getMessage());
    assertThrows(IllegalArgumentException.class, () -> Dates.formatDate("1-2-23"));
    assertThrows(IllegalArgumentException.class, () -> Dates.formatDate("17/01/2024 10:30"));
    IllegalArgumentException unparseable =
        assertThrows(IllegalArgumentException.class, () -> Dates.formatDate("next tuesday"));
    assertEquals(
        "Cannot parse \"next tuesday\" as a date. Expected ISO 8601, Bloomberg-native YYYYMMDD,"
            + " or a java.time value.",
        unparseable.getMessage());
  }

  @Test
  void ambiguityMessageQuotesLikeJsonStringify() {
    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> Dates.formatDate("01/02/2023 \"x\""));
    assertEquals(
        "Ambiguous date format \"01/02/2023 \\\"x\\\"\"" + AMBIGUOUS_SUFFIX, error.getMessage());
  }

  @Test
  void formatDateUsesTheValuesOwnCalendarDate() {
    assertEquals("20240117", Dates.formatDate(LocalDate.of(2024, 1, 17)));
    assertEquals("20240117", Dates.formatDate(LocalDateTime.of(2024, 1, 17, 23, 59)));
    assertEquals(
        "20240117",
        Dates.formatDate(
            ZonedDateTime.of(2024, 1, 17, 23, 30, 0, 0, ZoneId.of("America/New_York"))));
    assertEquals(
        "20240117",
        Dates.formatDate(OffsetDateTime.of(2024, 1, 17, 23, 30, 0, 0, ZoneOffset.ofHours(-5))));
    assertEquals("20240118", Dates.formatDate(Instant.parse("2024-01-18T04:30:00Z")));
    assertEquals("20240118", Dates.formatDate(Date.from(Instant.parse("2024-01-18T04:30:00Z"))));
    assertEquals("00050101", Dates.formatDate(LocalDate.of(5, 1, 1)));
  }

  @Test
  void formatDateRejectsValuesWithoutADate() {
    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> Dates.formatDate(YearMonth.of(2024, 1)));
    assertEquals("Cannot convert java.time.YearMonth value 2024-01 to a date.", error.getMessage());
    assertThrows(IllegalArgumentException.class, () -> Dates.formatDate(LocalTime.NOON));
  }

  @Test
  void formatDateTimeKeepsNaiveTextNaive() {
    assertNull(Dates.formatDateTime((String) null));
    assertNull(Dates.formatDateTime(" "));
    assertEquals("2024-01-17T00:00:00", Dates.formatDateTime("20240117"));
    assertEquals("2024-01-17T10:30", Dates.formatDateTime(" 2024-01-17 10:30 "));
    assertEquals("2024-01-17T10:30 extra", Dates.formatDateTime("2024-01-17 10:30 extra"));
    assertEquals("anything", Dates.formatDateTime("anything"));
    IllegalArgumentException ambiguous =
        assertThrows(
            IllegalArgumentException.class, () -> Dates.formatDateTime("01/17/2024 10:30"));
    assertEquals(
        "Ambiguous date format \"01/17/2024 10:30\"" + AMBIGUOUS_SUFFIX, ambiguous.getMessage());
  }

  @Test
  void formatDateTimeFormatsJavaTimeValues() {
    assertEquals(
        "2024-01-17T10:30:00", Dates.formatDateTime(LocalDateTime.of(2024, 1, 17, 10, 30)));
    assertEquals(
        "2024-01-17T10:30:05.123",
        Dates.formatDateTime(LocalDateTime.of(2024, 1, 17, 10, 30, 5, 123_000_000)));
    assertEquals(
        "2024-01-17T10:30:05.123456",
        Dates.formatDateTime(LocalDateTime.of(2024, 1, 17, 10, 30, 5, 123_456_000)));
    assertEquals(
        "2024-01-17T10:30:05.000000001",
        Dates.formatDateTime(LocalDateTime.of(2024, 1, 17, 10, 30, 5, 1)));
    assertEquals("2024-01-17T00:00:00", Dates.formatDateTime(LocalDate.of(2024, 1, 17)));
    assertEquals(
        "2024-01-17T10:30:00-05:00",
        Dates.formatDateTime(OffsetDateTime.of(2024, 1, 17, 10, 30, 0, 0, ZoneOffset.ofHours(-5))));
    assertEquals(
        "2024-01-17T10:30:00Z",
        Dates.formatDateTime(OffsetDateTime.of(2024, 1, 17, 10, 30, 0, 0, ZoneOffset.UTC)));
    assertEquals(
        "2024-07-17T10:30:00-04:00",
        Dates.formatDateTime(
            ZonedDateTime.of(2024, 7, 17, 10, 30, 0, 0, ZoneId.of("America/New_York"))));
    assertEquals(
        "2024-01-17T15:30:00.000Z", Dates.formatDateTime(Instant.parse("2024-01-17T15:30:00Z")));
    assertEquals(
        "2024-01-17T15:30:00.123456Z",
        Dates.formatDateTime(Instant.parse("2024-01-17T15:30:00.123456Z")));
    assertEquals(
        "2024-01-17T15:30:00.250Z",
        Dates.formatDateTime(Date.from(Instant.parse("2024-01-17T15:30:00.25Z"))));
    assertEquals(
        "+010000-01-01T00:00:00.000Z",
        Dates.formatDateTime(LocalDateTime.of(10000, 1, 1, 0, 0).toInstant(ZoneOffset.UTC)));
    assertThrows(IllegalArgumentException.class, () -> Dates.formatDateTime(YearMonth.of(2024, 1)));
  }
}
