package io.github.underloam.xbbg.jdk17;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.underloam.xbbg.internal.TickBatch;
import io.github.underloam.xbbg.subscription.FieldHandle;
import io.github.underloam.xbbg.subscription.Jdk17Fixtures;
import io.github.underloam.xbbg.subscription.Tick;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class TickValuesTest {
  private static final String[] FIELDS = {
    "FLAG", "COUNT", "VOLUME", "PX", "NAME", "DT", "TM", "TS", "UNK", "CLEARED", "ABSENT"
  };
  private static final byte[] KINDS = {
    TickBatch.KIND_BOOL,
    TickBatch.KIND_I32,
    TickBatch.KIND_I64,
    TickBatch.KIND_F64,
    TickBatch.KIND_STR,
    TickBatch.KIND_DATE32,
    TickBatch.KIND_TIME64_US,
    TickBatch.KIND_TIMESTAMP_US,
    TickBatch.KIND_UNKNOWN,
    TickBatch.KIND_F64,
    TickBatch.KIND_F64
  };
  private static final long TIME_US = 34_200_000_000L + 5L; // 09:30:00.000005
  private static final long TIMESTAMP_US = -1L; // one microsecond before the epoch

  private static Tick tick() {
    return Jdk17Fixtures.tick(
        FIELDS,
        KINDS,
        new char[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
        new byte[] {
          TickBatch.VALUE_BOOL,
          TickBatch.VALUE_I32,
          TickBatch.VALUE_I64,
          TickBatch.VALUE_F64,
          TickBatch.VALUE_STR,
          TickBatch.VALUE_I32,
          TickBatch.VALUE_I64,
          TickBatch.VALUE_I64,
          TickBatch.VALUE_I64,
          TickBatch.VALUE_NULL
        },
        new long[] {
          1,
          -7,
          Long.MAX_VALUE,
          Double.doubleToRawLongBits(101.25),
          0,
          19_000,
          TIME_US,
          TIMESTAMP_US,
          42,
          0
        },
        new String[] {"International Business Machines"});
  }

  @Test
  void typesEachPresentFieldByItsLayoutKind() {
    Tick tick = tick();
    assertEquals(Optional.of(new TickValue.BoolValue(true)), TickValues.get(tick, "FLAG"));
    assertEquals(Optional.of(new TickValue.IntValue(-7)), TickValues.get(tick, "COUNT"));
    assertEquals(
        Optional.of(new TickValue.LongValue(Long.MAX_VALUE)), TickValues.get(tick, "VOLUME"));
    assertEquals(Optional.of(new TickValue.DoubleValue(101.25)), TickValues.get(tick, "PX"));
    assertEquals(
        Optional.of(new TickValue.StringValue("International Business Machines")),
        TickValues.get(tick, "NAME"));
    assertEquals(
        Optional.of(new TickValue.DateValue(LocalDate.ofEpochDay(19_000))),
        TickValues.get(tick, "DT"));
    assertEquals(Optional.of(new TickValue.TimeValue(TIME_US)), TickValues.get(tick, "TM"));
    assertEquals(
        Optional.of(new TickValue.TimestampValue(TIMESTAMP_US)), TickValues.get(tick, "TS"));
    // a 64-bit payload in a field of unknown kind is a plain long, not a time
    assertEquals(Optional.of(new TickValue.LongValue(42)), TickValues.get(tick, "UNK"));
  }

  @Test
  void separatesClearedFromAbsent() {
    Tick tick = tick();
    assertEquals(Optional.of(new TickValue.Cleared()), TickValues.get(tick, "CLEARED"));
    assertEquals(Optional.empty(), TickValues.get(tick, "ABSENT"));
    assertEquals(Optional.empty(), TickValues.get(tick, "NOT_IN_LAYOUT"));
  }

  @Test
  void fieldHandlesMatchNames() {
    Tick tick = tick();
    for (String field : FIELDS) {
      assertEquals(TickValues.get(tick, field), TickValues.get(tick, FieldHandle.of(field)));
    }
    assertEquals(Optional.empty(), TickValues.get(tick, FieldHandle.of("NOT_IN_LAYOUT")));
  }

  @Test
  void convertsTimesAndTimestamps() {
    assertEquals(LocalTime.of(9, 30, 0, 5_000), new TickValue.TimeValue(TIME_US).toLocalTime());
    assertEquals(
        Instant.parse("1969-12-31T23:59:59.999999Z"),
        new TickValue.TimestampValue(TIMESTAMP_US).toInstant());
    assertEquals(
        Instant.parse("2023-11-14T22:13:20.000001Z"),
        new TickValue.TimestampValue(1_700_000_000_000_001L).toInstant());
    assertThrows(DateTimeException.class, () -> new TickValue.TimeValue(-1).toLocalTime());
    assertThrows(
        DateTimeException.class, () -> new TickValue.TimeValue(86_400_000_000L).toLocalTime());
    assertThrows(
        DateTimeException.class, () -> new TickValue.TimeValue(Long.MAX_VALUE).toLocalTime());
  }
}
