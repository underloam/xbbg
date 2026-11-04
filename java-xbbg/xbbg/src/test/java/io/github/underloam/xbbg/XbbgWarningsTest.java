package io.github.underloam.xbbg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.underloam.xbbg.table.DelayedDataInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

@Isolated
class XbbgWarningsTest {
  private final List<XbbgWarning> received = new ArrayList<XbbgWarning>();
  private final Consumer<XbbgWarning> listener =
      new Consumer<XbbgWarning>() {
        @Override
        public void accept(XbbgWarning warning) {
          received.add(warning);
        }
      };

  @BeforeEach
  void register() {
    XbbgWarnings.addListener(listener);
    XbbgWarnings.resetTickerKeyColumnWarning();
  }

  @AfterEach
  void unregister() {
    XbbgWarnings.removeListener(listener);
    XbbgWarnings.resetTickerKeyColumnWarning();
  }

  @Test
  void delayedWarningsMatchTheJavaScriptText() {
    Map<String, DelayedDataInfo> delayed = new LinkedHashMap<String, DelayedDataInfo>();
    delayed.put("IBM US Equity", new DelayedDataInfo(true, 15));
    delayed.put("MSFT US Equity", new DelayedDataInfo(false, null));
    delayed.put("VOD LN Equity", new DelayedDataInfo(true, null));
    XbbgWarnings.emitDelayedData(delayed);
    assertEquals(
        Arrays.asList(
            new XbbgWarning(
                "BlpDelayedDataWarning",
                "XBBG_DELAYED_DATA",
                "IBM US Equity: Bloomberg is delivering delayed data (15 minutes behind real time)",
                null),
            new XbbgWarning(
                "BlpDelayedDataWarning",
                "XBBG_DELAYED_DATA",
                "VOD LN Equity: Bloomberg is delivering delayed data",
                null)),
        received);
  }

  @Test
  void tickerDeprecationFiresOncePerJvmAndOnlyForTickerResults() {
    XbbgWarnings.warnTickerKeyColumn(KeyColumn.SECURITY, Arrays.asList("ticker", "field"));
    XbbgWarnings.warnTickerKeyColumn(KeyColumn.TICKER, Arrays.asList("security", "field"));
    assertTrue(received.isEmpty());
    XbbgWarnings.warnTickerKeyColumn(KeyColumn.TICKER, Arrays.asList("x", "ticker"));
    XbbgWarnings.warnTickerKeyColumn(KeyColumn.TICKER, Collections.singletonList("ticker"));
    assertEquals(1, received.size());
    XbbgWarning warning = received.get(0);
    assertEquals("DeprecationWarning", warning.type());
    assertEquals("XBBG_TICKER_KEY_COLUMN", warning.code());
    assertTrue(warning.message().contains("'ticker'"), warning.message());
    assertTrue(warning.message().contains("KeyColumn.SECURITY"), warning.message());
  }

  @Test
  void failingListenersDoNotStopDelivery() {
    Consumer<XbbgWarning> failing =
        new Consumer<XbbgWarning>() {
          @Override
          public void accept(XbbgWarning warning) {
            throw new IllegalStateException("listener bug");
          }
        };
    XbbgWarnings.addListener(failing);
    try {
      XbbgWarnings.emit(new XbbgWarning("T", "C", "m", "d"));
    } finally {
      XbbgWarnings.removeListener(failing);
    }
    assertEquals(Collections.singletonList(new XbbgWarning("T", "C", "m", "d")), received);
    assertEquals("d", received.get(0).detail().get());
  }

  @Test
  void withoutListenersWarningsGoToJavaUtilLogging() {
    XbbgWarnings.removeListener(listener);
    Logger logger = Logger.getLogger("io.github.underloam.xbbg");
    final List<LogRecord> records = new ArrayList<LogRecord>();
    Handler handler =
        new Handler() {
          @Override
          public void publish(LogRecord record) {
            records.add(record);
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    logger.addHandler(handler);
    try {
      XbbgWarnings.emit(new XbbgWarning("BlpFieldWarning", "XBBG_FIELD_EXCEPTION", "msg", null));
    } finally {
      logger.removeHandler(handler);
    }
    assertEquals(1, records.size());
    assertEquals(Level.WARNING, records.get(0).getLevel());
    assertEquals("BlpFieldWarning [XBBG_FIELD_EXCEPTION]: msg", records.get(0).getMessage());
  }

  @Test
  void rawDelayedMetadataFollowsTheSnakeCaseShape() {
    Map<String, DelayedDataInfo> parsed =
        EngineBase.rawDelayed(
            "{\"IBM US Equity\":{\"delayed\":true,\"delay_minutes\":15},"
                + "\"MSFT US Equity\":{\"delayed\":false,\"delay_minutes\":null}}");
    assertEquals(new DelayedDataInfo(true, 15), parsed.get("IBM US Equity"));
    assertEquals(new DelayedDataInfo(false, null), parsed.get("MSFT US Equity"));
    assertNull(EngineBase.rawDelayed(null));
    assertNull(EngineBase.rawDelayed("not json"));
    assertNull(EngineBase.rawDelayed("{\"X\":{\"delayed\":\"yes\",\"delay_minutes\":1}}"));
    assertNull(EngineBase.rawDelayed("{\"X\":{\"delayed\":true}}"));
    assertNull(EngineBase.rawDelayed("[]"));
  }
}
