package io.github.underloam.xbbg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.underloam.xbbg.internal.ArrowIpcSchema;
import io.github.underloam.xbbg.table.RowsResult;
import io.github.underloam.xbbg.table.Table;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Read-only recipes against a running Bloomberg Terminal (localhost:8194). Excluded by default; run
 * with the {@code live} group.
 */
@Tag("live")
class RecipesLiveTest {
  private static Engine engine;

  @BeforeAll
  static void connect() {
    engine = Engine.open();
  }

  @AfterAll
  static void close() {
    Xbbg.resetConfiguration();
    if (engine != null) {
      engine.close();
    }
  }

  @Test
  void delayStatusReportsOneRowPerUniqueSecurity() {
    Table table = Xbbg.await(engine.delayStatus(Arrays.asList("IBM US Equity", "IBM US Equity")));
    assertEquals(1, table.numRows(), table.toString());
    for (String column :
        Arrays.asList("delayed", "delay_minutes", "exchange_delay", "realtime_eid1", "error")) {
      assertTrue(table.hasColumn(column), column + " in " + table.columnNames());
    }
  }

  @Test
  void blpDelayStatusUsesTheConfiguredEngine() {
    RowsResult rows = Xbbg.await(Blp.delayStatus("IBM US Equity", Backend.ROWS));
    assertEquals(1, rows.size());
  }

  @Test
  void resolveIsinsFindsAnEquity() {
    Table table = Xbbg.await(engine.resolveIsins("US0378331005"));
    assertEquals(1, table.numRows(), table.toString());
  }

  @Test
  void futTickerResolvesTheFrontContract() {
    Table table = Xbbg.await(engine.futTicker("ES1 Index", LocalDate.now(ZoneId.systemDefault())));
    assertEquals(1, table.numRows(), table.toString());
    String ticker = table.column("ticker").getString(0);
    assertTrue(ticker.startsWith("ES") && ticker.endsWith(" Index"), ticker);
    assertFalse(ticker.startsWith("ES1"), ticker);
  }

  @Test
  void recipesHonourTheIpcBackend() {
    byte[] ipc = Xbbg.await(engine.resolveVenues("VOD LN Equity", Backend.IPC_STREAM));
    assertTrue(
        ArrowIpcSchema.read(ipc).fieldNames().containsAll(Arrays.asList("input_order", "status")),
        ArrowIpcSchema.read(ipc).fieldNames().toString());
  }

  @Test
  void recipeArgumentErrorsFailBeforeAnyRequest() {
    CompletionException failure =
        assertThrows(
            CompletionException.class,
            () ->
                engine
                    .yas(
                        "T 4 11/15/34 Govt",
                        "YAS_BOND_YLD",
                        YasOptions.builder().yieldType(10).build())
                    .join());
    assertTrue(failure.getCause() instanceof BlpValidationException, failure.toString());
  }
}
