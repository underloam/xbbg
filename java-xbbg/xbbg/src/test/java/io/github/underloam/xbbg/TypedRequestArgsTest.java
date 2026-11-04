package io.github.underloam.xbbg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Option builders and the argument objects sent to the native typed-request planner. */
class TypedRequestArgsTest {
  private static final List<String> IBM = Collections.singletonList("IBM US Equity");
  private static final List<String> PX_LAST = Collections.singletonList("PX_LAST");

  private static Map<String, Object> ordered(Object... keysAndValues) {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      map.put((String) keysAndValues[i], keysAndValues[i + 1]);
    }
    return map;
  }

  @Test
  void defaultOptionsSendOnlyThePositionalArguments() {
    assertEquals(
        "{\"tickers\":[\"IBM US Equity\"],\"fields\":[\"PX_LAST\"]}",
        TypedRequests.bdp(IBM, PX_LAST, BdpOptions.defaults()).text());
    assertEquals(
        "{\"tickers\":[\"IBM US Equity\"],\"fields\":[\"PX_LAST\"]}",
        TypedRequests.bds(IBM, PX_LAST, BdsOptions.defaults()).text());
    assertEquals(
        "{\"tickers\":[\"IBM US Equity\"],\"fields\":[\"PX_LAST\"]}",
        TypedRequests.bdh(IBM, PX_LAST, BdhOptions.defaults()).text());
    assertEquals(
        "{\"ticker\":\"IBM US Equity\"}",
        TypedRequests.bdib("IBM US Equity", BdibOptions.defaults()).text());
    assertEquals(
        "{\"ticker\":\"IBM US Equity\"}",
        TypedRequests.bdtick("IBM US Equity", BdtickOptions.defaults()).text());
    assertEquals("{\"query\":\"q\"}", TypedRequests.bql("q", BqlOptions.defaults()).text());
    assertEquals("{\"screen\":\"S\"}", TypedRequests.beqs("S", BeqsOptions.defaults()).text());
    assertEquals(
        "{\"searchSpec\":\"FI:SRCH\"}",
        TypedRequests.bsrch("FI:SRCH", BsrchOptions.defaults()).text());
    assertEquals(
        "{\"ticker\":\"T\",\"study\":\"rsi\"}",
        TypedRequests.bta("T", "rsi", BtaOptions.defaults()).text());
    assertEquals("{}", TypedRequests.bflds(BfldsOptions.defaults()).text());
    assertEquals(
        "{\"fields\":[\"PX_LAST\"]}",
        TypedRequests.fieldInfo(PX_LAST, BfldsOptions.defaults()).text());
    assertEquals(
        "{\"searchSpec\":\"vwap\"}",
        TypedRequests.fieldSearch("vwap", BfldsOptions.defaults()).text());
    assertEquals("{\"query\":\"IBM\"}", TypedRequests.blkp("IBM", BlkpOptions.defaults()).text());
    assertEquals(
        "{\"portfolio\":\"P\",\"fields\":[\"PORTFOLIO_MWEIGHT\"]}",
        TypedRequests.bport(
                "P", Collections.singletonList("PORTFOLIO_MWEIGHT"), RequestOptions.defaults())
            .text());
    assertEquals(
        "{\"ticker\":\"US\"}",
        TypedRequests.instrumentList("US", RequestOptions.defaults()).text());
    assertEquals(
        "{\"ticker\":\"CDX\"}", TypedRequests.acdxInfo("CDX", BdpOptions.defaults()).text());
    assertEquals("{\"ticker\":\"CDX\"}", TypedRequests.cdx("CDX", CdxOptions.defaults()).text());
  }

  @Test
  void bdpOptionsWriteEveryFieldInOrderWithWireValues() {
    BdpOptions options =
        BdpOptions.builder()
            .overrides(
                Overrides.of("EQY_FUND_CRNCY", "USD")
                    .forSecurity("IBM US Equity", Overrides.of("EQY_FUND_CRNCY", "EUR")))
            .kwargs(ordered("b", 2.0d, "a", true, "c", 'x', "d", new BigDecimal("1.50")))
            .format(Format.LONG_WITH_METADATA)
            .includeSecurityErrors(true)
            .returnEids(false)
            .onDelayed(DelayedPolicy.WARN)
            .validateFields(true)
            .build();
    assertEquals(
        "{\"tickers\":[\"IBM US Equity\"],\"fields\":[\"PX_LAST\"],"
            + "\"overrides\":[{\"key\":\"EQY_FUND_CRNCY\",\"value\":\"USD\"}],"
            + "\"securityOverrides\":[{\"security\":\"IBM US Equity\","
            + "\"overrides\":[{\"key\":\"EQY_FUND_CRNCY\",\"value\":\"EUR\"}]}],"
            + "\"kwargs\":{\"b\":2,\"a\":true,\"c\":\"x\",\"d\":1.50},"
            + "\"format\":\"long_metadata\",\"includeSecurityErrors\":true,\"returnEids\":false,"
            + "\"onDelayed\":\"warn\",\"validateFields\":true}",
        TypedRequests.bdp(IBM, PX_LAST, options).text());
    assertTrue(TypedRequests.warns(options.onDelayed().orElse(null)));
    assertFalse(TypedRequests.warns(DelayedPolicy.RAISE));
    assertFalse(TypedRequests.warns(null));
  }

  @Test
  void emptyOverridesAreNotSent() {
    assertEquals(
        "{\"tickers\":[\"IBM US Equity\"],\"fields\":[\"PX_LAST\"]}",
        TypedRequests.bdp(IBM, PX_LAST, BdpOptions.builder().overrides(Overrides.empty()).build())
            .text());
  }

  @Test
  void dateOptionsAreFormattedWhenSet() {
    BdhOptions fromText = BdhOptions.builder().start("2024-01-02").end("20240131").build();
    BdhOptions fromDates =
        BdhOptions.builder()
            .start(LocalDate.of(2024, 1, 2))
            .end(ZonedDateTime.of(2024, 1, 31, 23, 0, 0, 0, ZoneOffset.ofHours(-5)))
            .build();
    assertEquals(fromText, fromDates);
    assertEquals(
        "{\"tickers\":[\"IBM US Equity\"],\"fields\":[\"PX_LAST\"],"
            + "\"start\":\"20240102\",\"end\":\"20240131\"}",
        TypedRequests.bdh(IBM, PX_LAST, fromText).text());
    assertEquals("20240102", fromText.start().get());

    IllegalArgumentException ambiguous =
        assertThrows(
            IllegalArgumentException.class, () -> BdhOptions.builder().start("01/02/2024"));
    assertTrue(ambiguous.getMessage().startsWith("Ambiguous date format \"01/02/2024\""));
    assertFalse(BdhOptions.builder().start("   ").build().start().isPresent());
    assertFalse(
        BdhOptions.builder().start("2024-01-02").start((String) null).build().start().isPresent());

    BeqsOptions asof = BeqsOptions.builder().asof(LocalDate.of(2024, 3, 28)).build();
    assertEquals("{\"screen\":\"S\",\"asof\":\"20240328\"}", TypedRequests.beqs("S", asof).text());
  }

  @Test
  void bdibOptionsCarryDateTimesAndTheWholeDay() {
    BdibOptions range =
        BdibOptions.builder()
            .start(LocalDateTime.of(2024, 1, 2, 9, 30))
            .end("2024-01-02 10:00:00")
            .requestTz("NY")
            .outputTz("exchange")
            .eventType("BID")
            .interval(5)
            .kwarg("gapFillInitialBar", true)
            .returnEids(true)
            .onDelayed(DelayedPolicy.IGNORE)
            .build();
    assertEquals(
        "{\"ticker\":\"IBM US Equity\",\"start\":\"2024-01-02T09:30:00\","
            + "\"end\":\"2024-01-02T10:00:00\",\"requestTz\":\"NY\",\"outputTz\":\"exchange\","
            + "\"eventType\":\"BID\",\"interval\":5,\"kwargs\":{\"gapFillInitialBar\":true},"
            + "\"returnEids\":true,\"onDelayed\":\"ignore\"}",
        TypedRequests.bdib("IBM US Equity", range).text());

    BdibOptions day = BdibOptions.builder().day(LocalDate.of(2024, 1, 2)).build();
    assertEquals(
        "{\"ticker\":\"IBM US Equity\",\"dt\":\"20240102\"}",
        TypedRequests.bdib("IBM US Equity", day).text());
    assertEquals(day, BdibOptions.builder().day("2024-01-02").build());
    IllegalArgumentException blank =
        assertThrows(IllegalArgumentException.class, () -> BdibOptions.builder().day(" "));
    assertEquals("dt must be a non-empty date-like value", blank.getMessage());
    assertFalse(BdibOptions.builder().day((String) null).build().day().isPresent());
    IllegalArgumentException negative =
        assertThrows(IllegalArgumentException.class, () -> BdibOptions.builder().interval(-1));
    assertEquals("interval must not be negative, got -1", negative.getMessage());
  }

  @Test
  void bdtickOptionsWriteTypedFlagsAndEventTypes() {
    BdtickOptions options =
        BdtickOptions.builder()
            .start(Instant.parse("2024-01-02T14:30:00Z"))
            .end("2024-01-02T15:00:00")
            .eventTypes("BID", "ASK")
            .includeConditionCodes(true)
            .includeExchangeCodes(false)
            .includeBrokerCodes(true)
            .includeRpsCodes(false)
            .includeBicMicCodes(true)
            .includeNonPlottableEvents(false)
            .includeBloombergStandardConditionCodes(true)
            .kwarg("custom", 1)
            .build();
    assertEquals(
        "{\"ticker\":\"T\",\"start\":\"2024-01-02T14:30:00.000Z\","
            + "\"end\":\"2024-01-02T15:00:00\",\"eventTypes\":[\"BID\",\"ASK\"],"
            + "\"includeConditionCodes\":true,\"includeExchangeCodes\":false,"
            + "\"includeBrokerCodes\":true,\"includeRpsCodes\":false,"
            + "\"includeBicMicCodes\":true,\"includeNonPlottableEvents\":false,"
            + "\"includeBloombergStandardConditionCodes\":true,\"kwargs\":{\"custom\":1}}",
        TypedRequests.bdtick("T", options).text());
    assertEquals(
        "{\"ticker\":\"T\",\"eventTypes\":[]}",
        TypedRequests.bdtick(
                "T", BdtickOptions.builder().eventTypes(Collections.<String>emptyList()).build())
            .text());
    assertThrows(
        NullPointerException.class,
        () -> BdtickOptions.builder().eventTypes(Arrays.asList("TRADE", null)));
  }

  @Test
  void kwargsKeepOrderAndRejectNonScalars() {
    BqlOptions options =
        BqlOptions.builder()
            .kwarg("z", 1)
            .kwarg("a", "x")
            .kwarg("z", 2L)
            .appName("app")
            .format(Format.LONG)
            .build();
    assertEquals(
        "{\"query\":\"q\",\"appName\":\"app\",\"kwargs\":{\"z\":2,\"a\":\"x\"},\"format\":\"long\"}",
        TypedRequests.bql("q", options).text());
    assertEquals(Arrays.asList("z", "a"), Arrays.asList(options.kwargs().get().keySet().toArray()));
    assertThrows(UnsupportedOperationException.class, () -> options.kwargs().get().put("b", "y"));

    IllegalArgumentException nullValue =
        assertThrows(IllegalArgumentException.class, () -> BqlOptions.builder().kwarg("a", null));
    assertEquals("kwargs.a must not be null", nullValue.getMessage());
    IllegalArgumentException nan =
        assertThrows(
            IllegalArgumentException.class, () -> BqlOptions.builder().kwarg("a", Double.NaN));
    assertEquals("kwargs.a must be a finite number, got NaN", nan.getMessage());
    IllegalArgumentException object =
        assertThrows(
            IllegalArgumentException.class,
            () -> BqlOptions.builder().kwargs(Collections.singletonMap("a", LocalDate.MIN)));
    assertEquals(
        "kwargs.a must be a string, number or boolean, got java.time.LocalDate",
        object.getMessage());
    assertThrows(
        IllegalArgumentException.class,
        () -> BqlOptions.builder().kwargs(Collections.singletonMap((String) null, "x")));
  }

  @Test
  void optionsAreValuesWithOrderSensitiveKwargs() {
    BdsOptions first =
        BdsOptions.builder().kwargs(ordered("a", 1, "b", 2)).returnEids(true).build();
    BdsOptions same = BdsOptions.builder().kwargs(ordered("a", 1, "b", 2)).returnEids(true).build();
    BdsOptions reordered =
        BdsOptions.builder().kwargs(ordered("b", 2, "a", 1)).returnEids(true).build();
    assertEquals(first, same);
    assertEquals(first.hashCode(), same.hashCode());
    assertNotEquals(first, reordered);
    assertEquals(first, first.toBuilder().build());
    assertEquals("BdsOptions{kwargs={a=1, b=2}, returnEids=true}", first.toString());
    assertSame(BdsOptions.defaults(), BdsOptions.defaults());
    assertEquals(BdsOptions.defaults(), BdsOptions.builder().build());
    assertEquals(
        BdsOptions.builder().validateFields(false).build(),
        first.toBuilder().kwargs(null).returnEids(null).validateFields(false).build());
  }

  @Test
  void btaStudyParamsKeepNullsAndKwargDatesAreFormatted() {
    BtaOptions options =
        BtaOptions.builder()
            .studyParam("lowerBand", null)
            .studyParam("period", 30)
            .kwarg("start_date", "2023-01-02")
            .kwarg("endDate", "  ")
            .kwarg("startDate", 20240101)
            .kwarg("nonTradingDayFillOption", "ALL_CALENDAR_DAYS")
            .startDate(LocalDate.of(2022, 5, 6))
            .endDate("2022-06-07")
            .periodicity("weekly")
            .interval(15)
            .format(Format.LONG)
            .build();
    assertEquals(
        "{\"ticker\":\"T\",\"study\":\"boll\","
            + "\"studyParams\":{\"lowerBand\":null,\"period\":30},"
            + "\"kwargs\":{\"start_date\":\"20230102\",\"endDate\":\"  \",\"startDate\":20240101,"
            + "\"nonTradingDayFillOption\":\"ALL_CALENDAR_DAYS\"},"
            + "\"startDate\":\"20220506\",\"endDate\":\"20220607\",\"periodicity\":\"weekly\","
            + "\"interval\":15,\"format\":\"long\"}",
        TypedRequests.bta("T", "boll", options).text());
    assertEquals(options, options.toBuilder().build());
    assertThrows(
        IllegalArgumentException.class,
        () -> BtaOptions.builder().kwarg("startDate", "01/02/2024").build());
  }

  @Test
  void btaStudyObjectsCarryTheirSettingsInOrder() {
    BtaStudy study =
        BtaStudy.builder("Fear Greed")
            .calcInterval("INTRADAY")
            .interval(5)
            .period(7)
            .param("priceSourceClose", "PX_OPEN")
            .param("lowerBand", null)
            .build();
    assertEquals(
        "{\"ticker\":\"T\",\"study\":{\"studyType\":\"Fear Greed\",\"calcInterval\":\"INTRADAY\","
            + "\"interval\":5,\"period\":7,\"priceSourceClose\":\"PX_OPEN\",\"lowerBand\":null}}",
        TypedRequests.bta("T", study, BtaOptions.defaults()).text());
    assertEquals("INTRADAY", study.calcInterval().get());
    assertEquals(5, study.interval().get());
    assertEquals(
        study,
        BtaStudy.builder("Fear Greed")
            .calcInterval("INTRADAY")
            .interval(5)
            .period(7)
            .param("priceSourceClose", "PX_OPEN")
            .param("lowerBand", null)
            .build());
    assertEquals(
        "{\"ticker\":\"T\",\"study\":{\"studyType\":\"rsi\"}}",
        TypedRequests.bta("T", BtaStudy.of("rsi"), BtaOptions.defaults()).text());
    assertThrows(
        IllegalArgumentException.class, () -> BtaStudy.builder("rsi").param("studyType", "x"));
    assertThrows(
        IllegalArgumentException.class, () -> BtaStudy.builder("rsi").param("interval", 5));
    assertThrows(NullPointerException.class, () -> BtaStudy.builder(null));
  }

  @Test
  void fieldInfoAndFieldSearchRejectConflictingOptions() {
    BfldsOptions withFields = BfldsOptions.builder().fields("PX_LAST").build();
    BfldsOptions withSearch = BfldsOptions.builder().searchSpec("vwap").build();
    for (BfldsOptions options : Arrays.asList(withFields, withSearch)) {
      IllegalArgumentException info =
          assertThrows(
              IllegalArgumentException.class, () -> TypedRequests.fieldInfo(PX_LAST, options));
      assertTrue(info.getMessage().startsWith("fieldInfo() takes its fields as an argument"));
      IllegalArgumentException search =
          assertThrows(
              IllegalArgumentException.class, () -> TypedRequests.fieldSearch("vwap", options));
      assertTrue(search.getMessage().startsWith("fieldSearch() takes its search as an argument"));
    }
    assertEquals(
        "{\"fields\":[\"PX_LAST\",\"NAME\"],\"searchSpec\":\"vwap\",\"kwargs\":{\"x\":1}}",
        TypedRequests.bflds(
                BfldsOptions.builder()
                    .fields(Arrays.asList("PX_LAST", "NAME"))
                    .searchSpec("vwap")
                    .kwarg("x", 1)
                    .build())
            .text());
  }

  @Test
  void cdxOptionsSendTheRecoveryRate() {
    CdxOptions options =
        CdxOptions.builder()
            .recoveryRate(40.0)
            .overrides(Overrides.of("CDS_RR", "25"))
            .onDelayed(DelayedPolicy.RAISE)
            .build();
    assertEquals(
        "{\"ticker\":\"CDX\",\"recoveryRate\":40,"
            + "\"overrides\":[{\"key\":\"CDS_RR\",\"value\":\"25\"}],\"onDelayed\":\"raise\"}",
        TypedRequests.cdx("CDX", options).text());
    assertEquals(
        "{\"ticker\":\"CDX\",\"recoveryRate\":37.5}",
        TypedRequests.cdx("CDX", CdxOptions.builder().recoveryRate(37.5).build()).text());
    assertThrows(
        IllegalArgumentException.class,
        () -> CdxOptions.builder().recoveryRate(Double.POSITIVE_INFINITY));
  }

  @Test
  void positionalArgumentsAreRequired() {
    assertThrows(
        NullPointerException.class, () -> TypedRequests.bdp(null, PX_LAST, BdpOptions.defaults()));
    assertThrows(
        NullPointerException.class,
        () -> TypedRequests.bdp(Arrays.asList("A", null), PX_LAST, BdpOptions.defaults()));
    assertThrows(NullPointerException.class, () -> TypedRequests.bdp(IBM, PX_LAST, null));
    assertThrows(
        NullPointerException.class, () -> TypedRequests.bdib(null, BdibOptions.defaults()));
    assertThrows(
        NullPointerException.class,
        () -> TypedRequests.bta("T", (String) null, BtaOptions.defaults()));
    assertThrows(NullPointerException.class, () -> RequestArgs.single("ticker", null));
  }

  @Test
  void naiveDateTimesStayNaive() {
    assertEquals(
        "2024-01-02T09:30:00",
        BdtickOptions.builder()
            .start(LocalDateTime.of(LocalDate.of(2024, 1, 2), LocalTime.of(9, 30)))
            .build()
            .start()
            .get());
  }
}
