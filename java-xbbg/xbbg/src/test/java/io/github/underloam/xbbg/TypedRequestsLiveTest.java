package io.github.underloam.xbbg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.underloam.xbbg.table.RowsResult;
import io.github.underloam.xbbg.table.Table;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Read-only typed requests against a running Bloomberg Terminal (localhost:8194). Excluded by
 * default; run with the {@code live} group.
 */
@Tag("live")
class TypedRequestsLiveTest {
  private static final List<String> IBM = Collections.singletonList("IBM US Equity");
  private static final List<String> PX_LAST = Collections.singletonList("PX_LAST");

  /** IBM trades in New York, so "today" and its trading days are New York dates. */
  private static final ZoneId NEW_YORK = ZoneId.of("America/New_York");

  private static Engine engine;

  @BeforeAll
  static void connect() {
    engine = Engine.open();
  }

  @AfterAll
  static void close() {
    if (engine != null) {
      engine.close();
    }
  }

  /** Weekdays before today, most recent first (a holiday simply returns no rows). */
  private static LocalDate[] recentWeekdays(int count) {
    LocalDate[] days = new LocalDate[count];
    LocalDate day = LocalDate.now(NEW_YORK);
    for (int i = 0; i < count; ) {
      day = day.minusDays(1);
      if (day.getDayOfWeek() != DayOfWeek.SATURDAY && day.getDayOfWeek() != DayOfWeek.SUNDAY) {
        days[i++] = day;
      }
    }
    return days;
  }

  @Test
  void bdpReturnsOneValuePerTickerAndField() {
    Table table = Xbbg.await(engine.bdp(IBM, PX_LAST));
    assertEquals(1, table.numRows());
    assertTrue(table.hasColumn("field") && table.hasColumn("value"), table.toString());
    assertEquals("PX_LAST", table.column("field").getString(0));
  }

  @Test
  void bdpRowsBackendCarriesTheSameData() {
    RowsResult rows =
        Xbbg.await(
            engine.bdp(
                IBM, Collections.singletonList("NAME"), BdpOptions.defaults(), Backend.ROWS));
    assertEquals(1, rows.size());
    assertEquals("NAME", rows.get(0).get("field"));
  }

  @Test
  void bdhReturnsRecentHistory() {
    Table table =
        Xbbg.await(
            engine.bdh(
                IBM,
                PX_LAST,
                BdhOptions.builder()
                    .start(LocalDate.now(NEW_YORK).minusDays(14))
                    .end(LocalDate.now(NEW_YORK))
                    .build()));
    assertTrue(table.numRows() > 0, table.toString());
    assertTrue(table.hasColumn("date"), table.toString());
  }

  @Test
  void bdsReturnsBulkRows() {
    Table table =
        Xbbg.await(
            engine.bds(
                Collections.singletonList("INDU Index"),
                Collections.singletonList("INDX_MEMBERS")));
    assertEquals(30, table.numRows(), table.toString());
  }

  @Test
  void bdibReturnsBarsForARecentDay() {
    int rows = 0;
    for (LocalDate day : recentWeekdays(5)) {
      Table table =
          Xbbg.await(
              engine.bdib("IBM US Equity", BdibOptions.builder().day(day).interval(60).build()));
      rows = table.numRows();
      if (rows > 0) {
        assertTrue(table.hasColumn("time"), table.toString());
        break;
      }
    }
    assertTrue(rows > 0, "no bars in the last five weekdays");
  }

  @Test
  void bdtickReturnsTicksForARecentWindow() {
    int rows = 0;
    for (LocalDate day : recentWeekdays(5)) {
      Table table =
          Xbbg.await(
              engine.bdtick(
                  "IBM US Equity",
                  BdtickOptions.builder()
                      .start(day + "T10:00:00")
                      .end(day + "T10:05:00")
                      .requestTz("NY")
                      .build()));
      rows = table.numRows();
      if (rows > 0) {
        break;
      }
    }
    assertTrue(rows > 0, "no ticks in the last five weekdays");
  }

  @Test
  void bqlReturnsAValue() {
    Table table = Xbbg.await(engine.bql("get(px_last) for('IBM US Equity')"));
    assertTrue(table.numRows() > 0, table.toString());
  }

  @Test
  void fieldInfoDescribesAField() {
    Table table = Xbbg.await(engine.fieldInfo("PX_LAST"));
    assertTrue(table.numRows() > 0, table.toString());
    assertTrue(table.hasColumn("id"), table.toString());
  }

  @Test
  void blpFunctionsUseTheConfiguredEngine() {
    Xbbg.configure(EngineConfig.defaults());
    try {
      Table table = Xbbg.await(Blp.bdp("IBM US Equity", "PX_LAST"));
      assertEquals(1, table.numRows());
    } finally {
      Xbbg.resetConfiguration();
    }
  }
}
