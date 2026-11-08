package io.github.underloam.xbbg.ext;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.underloam.xbbg.internal.ArrowIpcSchema;
import io.github.underloam.xbbg.internal.Json;
import io.github.underloam.xbbg.internal.NativeTestSupport;
import io.github.underloam.xbbg.internal.ResultKind;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/** Every {@link Ext} function through the native library: real outputs and error messages. */
class ExtTest {
  @BeforeAll
  static void natives() {
    NativeTestSupport.assumeAvailable();
  }

  private static void assertRejected(String message, Executable call) {
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class, call);
    assertEquals(message, error.getMessage());
  }

  @Test
  void parseDateAcceptsTheFiveFormats() {
    for (String input :
        Arrays.asList("2024-01-15", "20240115", "2024/01/15", "15-01-2024", " 15/01/2024 ")) {
      assertEquals(LocalDate.of(2024, 1, 15), Ext.parseDate(input), input);
    }
    assertRejected(
        "failed to parse date 'Jan 15': expected format YYYY-MM-DD, YYYYMMDD, or YYYY/MM/DD",
        () -> Ext.parseDate("Jan 15"));
  }

  @Test
  void fmtDateFormatsAndRejectsInvalidDatesAndPatterns() {
    assertEquals("20240105", Ext.fmtDate(2024, 1, 5));
    assertEquals("05/01/2024", Ext.fmtDate(2024, 1, 5, "%d/%m/%Y"));
    assertEquals("20240229", Ext.fmtDate(2024, 2, 29, null));
    assertRejected("invalid date: 2023-2-29", () -> Ext.fmtDate(2023, 2, 29));
    assertRejected("invalid date format '%H:%M'", () -> Ext.fmtDate(2024, 1, 5, "%H:%M"));
    assertThrows(IllegalArgumentException.class, () -> Ext.fmtDate(2024, -1, 5));
  }

  @Test
  void defaultRanges() {
    assertEquals(
        new TimeRange("2024-05-16", "2024-06-15"), Ext.defaultTurnoverDates(null, "2024-06-15"));
    assertEquals(
        new TimeRange("2024-01-01", "2024-01-31"),
        Ext.defaultTurnoverDates("20240101", "20240131"));
    TimeRange turnover = Ext.defaultTurnoverDates();
    assertEquals(LocalDate.parse(turnover.end()).minusDays(30), LocalDate.parse(turnover.start()));

    assertEquals(
        new TimeRange("2024-01-15T09:00:00", "2024-01-15T10:00:00"),
        Ext.defaultBqrDatetimes("2024-01-15 09:00", "2024-01-15T10:00"));
    assertEquals(
        new TimeRange("2024-01-15T09:30:00", "2024-01-15T10:30:00"),
        Ext.defaultBqrDatetimes(null, "2024-01-15 10:30"));
    TimeRange bqr = Ext.defaultBqrDatetimes();
    assertEquals(19, bqr.start().length());
    assertTrue(bqr.start().compareTo(bqr.end()) < 0);
  }

  @Test
  void pivotToWideAndIsLongFormatReadIpcStreams() {
    for (String key : Arrays.asList("ticker", "security")) {
      byte[] longTable =
          (byte[])
              NativeTestSupport.invoke(
                  "recipes.extLongTable",
                  Json.object().put("key", key).build(),
                  ResultKind.IPC_STREAM);
      assertTrue(Ext.isLongFormat(longTable), key);
      byte[] wide = Ext.pivotToWide(longTable);
      assertFalse(Ext.isLongFormat(wide));
      assertEquals(Arrays.asList(key, "PX_LAST", "VOLUME"), ArrowIpcSchema.read(wide).fieldNames());
      assertArrayEquals(wide, Ext.pivotToWide(wide), "a wide table is returned unchanged");
    }
    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> Ext.pivotToWide(new byte[] {1, 2, 3}));
    assertTrue(error.getMessage().startsWith("invalid Arrow IPC: "), error.getMessage());
    assertThrows(NullPointerException.class, () -> Ext.isLongFormat(null));
  }

  @Test
  void tickerUtilities() {
    assertEquals(new TickerParts("ES", 1, "Index", null), Ext.parseTicker("ES1 Index"));
    TickerParts equity = Ext.parseTicker("SPY1 US Equity");
    assertEquals(Optional.of("US"), equity.exchange());
    assertEquals("SPY", equity.prefix());
    assertRejected(
        "invalid ticker format 'AAPL': expected 'PREFIX ASSET' (e.g., 'ES1 Index')",
        () -> Ext.parseTicker("AAPL"));
    assertTrue(Ext.isSpecificContract("ESH24 Index"));
    assertFalse(Ext.isSpecificContract("ES1 Index"));
    assertEquals("CLZ25 Comdty", Ext.buildFuturesTicker("CL", "Z", "25", "Comdty"));
    List<String> tickers =
        Arrays.asList("IBM US Equity", "ES1 Index", "X=Y US Equity", "VOD LN Equity");
    assertEquals(tickers, Ext.normalizeTickers(tickers));
    assertEquals(Arrays.asList("IBM US Equity", "VOD LN Equity"), Ext.filterEquityTickers(tickers));
  }

  @Test
  void futuresResolution() {
    List<FuturesCandidate> quarterly =
        Ext.generateFuturesCandidates("ES1 Index", 2024, 1, 15, "Q", null);
    assertEquals(4, quarterly.size());
    assertEquals(2024, quarterly.get(0).year());
    assertEquals(3, quarterly.get(0).month());
    assertTrue(quarterly.get(0).ticker().startsWith("ESH"), quarterly.get(0).ticker());
    List<FuturesCandidate> monthly =
        Ext.generateFuturesCandidates("CL1 Comdty", 2024, 1, 15, "bogus", 2);
    assertEquals(2, monthly.size());
    assertEquals(2, monthly.get(1).month(), "an unknown frequency is monthly");
    assertEquals(4, Ext.generateFuturesCandidates("CL1 Comdty", 2024, 1, 15).size());
    assertRejected(
        "futures candidate count 10001 exceeds the maximum of 10000",
        () -> Ext.generateFuturesCandidates("ES1 Index", 2024, 1, 15, null, 10001));
    assertRejected(
        "invalid date: 2024-13-1", () -> Ext.generateFuturesCandidates("ES1 Index", 2024, 13, 1));

    Ext.validateGenericTicker("ES1 Index");
    assertRejected(
        "'ESH24 Index' appears to be a specific contract, not generic. "
            + "Use generic ticker like 'ES1 Index' instead",
        () -> Ext.validateGenericTicker("ESH24 Index"));
    assertEquals(2, Ext.contractIndex("ES3 Index"));

    List<FuturesCandidate> cycle =
        Ext.filterCandidatesByCycle(
            Arrays.asList(
                new FuturesCandidate("ESF24 Index", 2024, 1),
                new FuturesCandidate("ESH24 Index", 2024, 3),
                new FuturesCandidate("ESM24 Index", 2024, 6)),
            "hm");
    assertEquals(
        Arrays.asList(
            new FuturesCandidate("ESH24 Index", 2024, 3),
            new FuturesCandidate("ESM24 Index", 2024, 6)),
        cycle);

    Map<String, String> contracts = new LinkedHashMap<String, String>();
    contracts.put("ESM24 Index", "2024-06-21");
    contracts.put("ESZ23 Index", "2023-12-15");
    contracts.put("ESH24 Index", "20240315");
    contracts.put("BAD Index", "soon");
    assertEquals(
        Arrays.asList("ESH24 Index", "ESM24 Index"),
        Ext.filterValidContracts(contracts, 2024, 1, 15));
  }

  @Test
  void cdxUtilities() {
    assertEquals(
        new CdxTickerInfo("CDX IG CDSI", "GEN", null, "5Y", "Corp", true, null),
        Ext.parseCdxTicker("CDX IG CDSI GEN 5Y Corp"));
    CdxTickerInfo specific = Ext.parseCdxTicker("CDX HY CDSI S45 V2 5Y Corp");
    assertEquals(2L, specific.version().getAsLong());
    assertEquals(45L, specific.seriesNum().getAsLong());
    assertFalse(specific.isGeneric());
    assertRejected(
        "invalid ticker format 'CDX IG 5Y': expected 'PREFIX ASSET' (e.g., 'ES1 Index')",
        () -> Ext.parseCdxTicker("CDX IG 5Y"));
    assertEquals(
        Optional.of("CDX IG CDSI S44 5Y Corp"),
        Ext.previousCdxSeries("CDX IG CDSI S45 V3 5Y Corp"));
    assertEquals(Optional.empty(), Ext.previousCdxSeries("CDX IG CDSI GEN 5Y Corp"));
    assertEquals("CDX IG CDSI S46 5Y Corp", Ext.cdxGenToSpecific("CDX IG CDSI GEN 5Y Corp", 46));
    assertRejected(
        "invalid input: CDX series must be positive",
        () -> Ext.cdxGenToSpecific("CDX IG CDSI GEN 5Y Corp", 0));
    assertThrows(
        IllegalArgumentException.class,
        () -> Ext.cdxGenToSpecific("CDX IG CDSI GEN 5Y Corp", 1L << 32));
  }

  @Test
  void currencyAndColumnUtilities() {
    assertEquals(
        new FxPairInfo("USDGBP Curncy", 100.0, "GBP", "USD"), Ext.buildFxPair("GBp", "usd"));
    assertTrue(Ext.sameCurrency("GBP", "GBp"));
    assertFalse(Ext.sameCurrency("USD", "EUR"));
    assertEquals(
        Arrays.asList("EUR", "GBP"),
        Ext.currenciesNeedingConversion(Arrays.asList("usd", "GBP", "eur", "gbp"), "USD"));

    Map<String, String> dividend =
        Ext.renameDividendColumns(Arrays.asList("ticker", "Declared Date", "Ex-Date"));
    assertEquals(
        Arrays.asList("Declared Date", "Ex-Date"), new ArrayList<String>(dividend.keySet()));
    assertEquals("dec_date", dividend.get("Declared Date"));
    assertEquals(
        Collections.singletonMap("Holding Ticker", "ticker"),
        Ext.renameEtfColumns(Arrays.asList("Holding Ticker", "other")));
  }

  @Test
  void constantTables() {
    assertEquals(Optional.of("H"), Ext.getMonthCode("Mar"));
    assertEquals(Optional.empty(), Ext.getMonthCode("March"));
    assertEquals(Optional.of("Dec"), Ext.getMonthName("Z"));
    assertEquals(Optional.empty(), Ext.getMonthName("z"));
    assertEquals(Optional.of("DVD_Hist_All"), Ext.getDvdType("all"));
    assertEquals(Optional.empty(), Ext.getDvdType("ALL"));
    Map<String, String> months = Ext.getFuturesMonths();
    assertEquals(12, months.size());
    assertEquals("F", months.get("Jan"));
    assertEquals("DVD_Hist_All", Ext.getDvdTypes().get("all"));
    assertEquals("ex_date", Ext.getDvdCols().get("Ex-Date"));
    assertEquals("name", Ext.getEtfCols().get("Holding Name"));
    assertThrows(UnsupportedOperationException.class, () -> months.put("x", "y"));
  }

  @Test
  void yasOverrides() {
    Map<String, String> overrides =
        Ext.buildYasOverrides("20240115", 5, 25.0, 4.25, 99.5, "T 4 11/15/34 Govt");
    assertEquals(
        Arrays.asList(
            "YAS_SETTLE_DT",
            "YAS_YLD_FLAG",
            "YAS_YLD_SPREAD",
            "YAS_BOND_YLD",
            "YAS_BOND_PX",
            "YAS_BNCHMRK_BOND"),
        new ArrayList<String>(overrides.keySet()));
    assertEquals("5", overrides.get("YAS_YLD_FLAG"));
    assertEquals("25", overrides.get("YAS_YLD_SPREAD"));
    assertEquals("99.5", overrides.get("YAS_BOND_PX"));
    assertTrue(Ext.buildYasOverrides(null, null, null, null, null, null).isEmpty());
    for (final int code : new int[] {0, 10, 257}) {
      assertRejected(
          "unknown yield type '"
              + code
              + "': expected YTM, YTC, YTR, YTP, YTW, YTWR, EYTW, EYTWR, or YTAL",
          () -> Ext.buildYasOverrides(null, code, null, null, null, null));
    }
  }

  @Test
  void earningsUtilities() {
    Map<String, String> header = new LinkedHashMap<String, String>();
    header.put("Period 1 Header", "FY 2023");
    header.put("level Header", "Level");
    Map<String, String> renames =
        Ext.buildEarningHeaderRename(
            header, Arrays.asList("ticker", "Period 1 Value", "Period 2 Value", "level"));
    assertEquals(Arrays.asList("Period 1 Value", "level"), new ArrayList<String>(renames.keySet()));
    assertEquals("fy2023", renames.get("Period 1 Value"));

    assertEquals(
        Arrays.asList(60.0, 40.0, 75.0, 25.0, null),
        Ext.calculateLevelPercentages(
            Arrays.asList(60.0, 40.0, 30.0, 10.0, null), Arrays.asList(1L, 1L, 2L, 2L, 2L)));
    assertEquals(
        Arrays.asList(null, null),
        Ext.calculateLevelPercentages(Arrays.asList(1.0, 2.0), Collections.singletonList(1L)));
  }

  @Test
  void bqlQueryBuilders() {
    assertEquals(
        "get(id, name) for(filter(debt(['BAC US Equity'], CONSOLIDATEDUPLICATES='N'), "
            + "SRCH_ASSET_CLASS=='Preferreds'))",
        Ext.buildPreferredsQuery("BAC"));
    assertEquals(
        "get(id, cpn) for(filter(debt(['AAPL US Equity'], CONSOLIDATEDUPLICATES='N'), "
            + "SRCH_ASSET_CLASS=='Corporates' AND CRNCY=='USD'))",
        Ext.buildCorporateBondsQuery("AAPL", "USD", Arrays.asList("cpn", "ID")));
    assertEquals(
        "get(id) for(filter(debt(['9984 JT Equity'], CONSOLIDATEDUPLICATES='N'), "
            + "SRCH_ASSET_CLASS=='Corporates'))",
        Ext.buildCorporateBondsQuery("9984 JT Equity"));
    assertEquals(
        "get(id_isin, weights, id().position, name) for(holdings('SPY US Equity'))",
        Ext.buildEtfHoldingsQuery("SPY", Collections.singletonList("name")));
  }

  @Test
  void marketSessions() {
    SessionWindows us = Ext.deriveSessions("09:30", "16:00", "XNYS", "US");
    assertEquals(Optional.of(new TimeRange("09:30", "16:00")), us.day());
    assertEquals(Optional.of(new TimeRange("04:00", "09:30")), us.pre());
    assertEquals(Optional.of(new TimeRange("16:01", "20:00")), us.post());
    assertEquals(Optional.of(new TimeRange("04:00", "20:00")), us.allday());
    assertFalse(us.am().isPresent());
    SessionWindows tokyo = Ext.deriveSessions("09:00", "15:00", "XTKS", null);
    assertEquals(Optional.of(new TimeRange("09:00", "11:30")), tokyo.am());
    assertEquals(Optional.of(new TimeRange("12:30", "15:30")), tokyo.pm());
    assertEquals(
        new SessionWindows(null, null, null, null, null, null), Ext.deriveSessions("", "16:00"));

    assertEquals(
        Optional.of(new MarketRule(330, 240, null, null, false)), Ext.getMarketRule("XNYS", null));
    assertEquals(Optional.empty(), Ext.getMarketRule("XXXX", "ZZ"));
    assertEquals(Optional.of("Asia/Tokyo"), Ext.inferTimezone(" jp "));
    assertEquals(Optional.empty(), Ext.inferTimezone("QQ"));
  }

  @Test
  void sessionTimesToUtc() {
    assertEquals(
        new TimeRange("2024-01-15T14:30:00", "2024-01-15T21:00:00"),
        Ext.sessionTimesToUtc("09:30", "16:00", "America/New_York", "2024-01-15"));
    assertRejected(
        "invalid date '20240115', expected YYYY-MM-DD",
        () -> Ext.sessionTimesToUtc("09:30", "16:00", "UTC", "20240115"));
    assertRejected(
        "invalid input: invalid timezone: Mars/Base",
        () -> Ext.sessionTimesToUtc("09:30", "16:00", "Mars/Base", "2024-01-15"));
  }

  /** The registry is process-wide, so the whole lifecycle runs in one test with its own tickers. */
  @Test
  void exchangeOverrideLifecycle() {
    final String ticker = "XBBG_JAVA_EXT_TEST Equity";
    final String other = "XBBG_JAVA_EXT_OTHER Equity";
    assertRejected(
        "invalid input: override patch must include at least one field",
        () -> Ext.setExchangeOverride(ticker, ExchangeOverride.builder().build()));
    assertRejected(
        "invalid input: ticker cannot be empty",
        () -> Ext.setExchangeOverride("  ", ExchangeOverride.builder().timezone("UTC").build()));

    Ext.setExchangeOverride(
        ticker,
        ExchangeOverride.builder().timezone("Europe/London").mic("XLON").exchCode("LN").build());
    Ext.setExchangeOverride(
        ticker,
        ExchangeOverride.builder()
            .day(new TimeRange("08:00", "16:30"))
            .pre(new TimeRange("07:00", "08:00"))
            .build());
    ExchangeInfo info = Ext.getExchangeOverride(ticker).get();
    assertEquals(ticker, info.ticker());
    assertEquals(Optional.of("XLON"), info.mic());
    assertEquals(Optional.of("LN"), info.exchCode());
    assertEquals("Europe/London", info.timezone());
    assertEquals("override", info.source());
    assertEquals(Optional.of(new TimeRange("08:00", "16:30")), info.day());
    assertEquals(Optional.of(new TimeRange("07:00", "08:00")), info.pre());
    assertFalse(info.post().isPresent());
    assertFalse(info.utcOffset().isPresent());

    Ext.setExchangeOverride(other, ExchangeOverride.builder().timezone("Asia/Tokyo").build());
    List<String> listed = new ArrayList<String>();
    for (ExchangeInfo override : Ext.listExchangeOverrides()) {
      if (override.ticker().startsWith("XBBG_JAVA_EXT_")) {
        listed.add(override.ticker());
      }
    }
    assertEquals(Arrays.asList(other, ticker), listed, "sorted by ticker");

    Ext.clearExchangeOverride(other);
    assertEquals(Optional.empty(), Ext.getExchangeOverride(other));
    assertTrue(Ext.getExchangeOverride(ticker).isPresent());
    Ext.clearExchangeOverride();
    assertEquals(Optional.empty(), Ext.getExchangeOverride(ticker));
  }

  @Test
  void requiredArgumentsMustNotBeNull() {
    assertThrows(NullPointerException.class, () -> Ext.parseTicker(null));
    assertThrows(NullPointerException.class, () -> Ext.normalizeTickers(null));
    assertThrows(NullPointerException.class, () -> Ext.setExchangeOverride("X", null));
  }
}
