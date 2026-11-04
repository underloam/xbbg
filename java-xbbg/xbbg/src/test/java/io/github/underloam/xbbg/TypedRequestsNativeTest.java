package io.github.underloam.xbbg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.underloam.xbbg.internal.Json;
import io.github.underloam.xbbg.internal.JsonValue;
import io.github.underloam.xbbg.internal.NativeTestSupport;
import io.github.underloam.xbbg.internal.ResultKind;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Round trips through the native planner: Java options → argument object → Rust plan → the generic
 * request the JavaScript wrapper builds. Needs the test-support natives; no session.
 */
class TypedRequestsNativeTest {
  private static final List<String> IBM = Collections.singletonList("IBM US Equity");
  private static final List<String> PX_LAST = Collections.singletonList("PX_LAST");

  @BeforeAll
  static void natives() {
    NativeTestSupport.assumeAvailable();
  }

  /** Plans {@code method} natively and compares the request with {@code expected} (JSON text). */
  private static void assertPlan(String expected, String method, JsonValue args) {
    Object planned = NativeTestSupport.invoke("requests.plan." + method, args, ResultKind.JSON);
    assertEquals(Json.parse(expected), Json.parse((String) planned), method + " " + args);
  }

  private static RuntimeException planError(String method, JsonValue args) {
    return assertThrows(
        RuntimeException.class,
        () -> NativeTestSupport.invoke("requests.plan." + method, args, ResultKind.JSON));
  }

  private static Map<String, Object> ordered(Object... keysAndValues) {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      map.put((String) keysAndValues[i], keysAndValues[i + 1]);
    }
    return map;
  }

  @Test
  void bdpPlansAReferenceDataRequest() {
    assertPlan(
        "{\"service\":\"//blp/refdata\",\"operation\":\"ReferenceDataRequest\","
            + "\"extractor\":\"refdata\",\"securities\":[\"IBM US Equity\"],"
            + "\"fields\":[\"PX_LAST\"],\"includeSecurityErrors\":false}",
        "bdp",
        TypedRequests.bdp(IBM, PX_LAST, BdpOptions.defaults()));
    BdpOptions options =
        BdpOptions.builder()
            .overrides(
                Overrides.of("EQY_FUND_CRNCY", "USD")
                    .forSecurity(
                        "MSFT US Equity",
                        Overrides.of("USER_LOCAL_TRADE_DATE", LocalDate.of(2024, 1, 2))))
            .kwargs(ordered("b", 2.0d, "a", true))
            .format(Format.LONG_TYPED)
            .includeSecurityErrors(true)
            .returnEids(true)
            .onDelayed(DelayedPolicy.WARN)
            .validateFields(true)
            .build();
    assertPlan(
        "{\"service\":\"//blp/refdata\",\"operation\":\"ReferenceDataRequest\","
            + "\"extractor\":\"refdata\",\"securities\":[\"IBM US Equity\",\"MSFT US Equity\"],"
            + "\"fields\":[\"PX_LAST\"],"
            + "\"overrides\":[{\"key\":\"EQY_FUND_CRNCY\",\"value\":\"USD\"}],"
            + "\"securityOverrides\":[{\"security\":\"MSFT US Equity\","
            + "\"overrides\":[{\"key\":\"USER_LOCAL_TRADE_DATE\",\"value\":\"20240102\"}]}],"
            + "\"kwargs\":[{\"key\":\"b\",\"value\":\"2\"},{\"key\":\"a\",\"value\":\"true\"}],"
            + "\"includeSecurityErrors\":true,\"returnEids\":true,\"onDelayed\":\"warn\","
            + "\"validateFields\":true,\"format\":\"long_typed\"}",
        "bdp",
        TypedRequests.bdp(Arrays.asList("IBM US Equity", "MSFT US Equity"), PX_LAST, options));
  }

  @Test
  void bdsAndBdhPlanReferenceAndHistoricalRequests() {
    assertPlan(
        "{\"service\":\"//blp/refdata\",\"operation\":\"ReferenceDataRequest\","
            + "\"extractor\":\"bulk\",\"securities\":[\"IBM US Equity\"],"
            + "\"fields\":[\"DVD_HIST_ALL\"],\"securityOverrides\":[{\"security\":\"IBM US Equity\","
            + "\"overrides\":[{\"key\":\"DVD_Start_Dt\",\"value\":\"20240101\"}]}],"
            + "\"returnEids\":true}",
        "bds",
        TypedRequests.bds(
            IBM,
            Collections.singletonList("DVD_HIST_ALL"),
            BdsOptions.builder()
                .overrides(
                    Overrides.empty()
                        .forSecurity("IBM US Equity", Overrides.of("DVD_Start_Dt", "20240101")))
                .returnEids(true)
                .build()));
    assertPlan(
        "{\"service\":\"//blp/refdata\",\"operation\":\"HistoricalDataRequest\","
            + "\"extractor\":\"histdata\",\"securities\":[\"IBM US Equity\"],"
            + "\"fields\":[\"PX_LAST\"],\"kwargs\":[{\"key\":\"periodicitySelection\","
            + "\"value\":\"WEEKLY\"}],\"startDate\":\"20240101\",\"endDate\":\"20240131\","
            + "\"onDelayed\":\"raise\",\"validateFields\":false}",
        "bdh",
        TypedRequests.bdh(
            IBM,
            PX_LAST,
            BdhOptions.builder()
                .start("2024-01-01")
                .end(LocalDate.of(2024, 1, 31))
                .kwarg("periodicitySelection", "WEEKLY")
                .onDelayed(DelayedPolicy.RAISE)
                .validateFields(false)
                .build()));
  }

  @Test
  void intradayPlansApplyTheTsDefaults() {
    assertPlan(
        "{\"service\":\"//blp/refdata\",\"operation\":\"IntradayBarRequest\","
            + "\"extractor\":\"intraday_bar\",\"security\":\"IBM US Equity\","
            + "\"eventType\":\"TRADE\",\"interval\":1}",
        "bdib",
        TypedRequests.bdib("IBM US Equity", BdibOptions.defaults()));
    assertPlan(
        "{\"service\":\"//blp/refdata\",\"operation\":\"IntradayBarRequest\","
            + "\"extractor\":\"intraday_bar\",\"security\":\"IBM US Equity\","
            + "\"startDatetime\":\"2024-01-02T00:00:00\",\"endDatetime\":\"2024-01-02T23:59:59\","
            + "\"requestTz\":\"NY\",\"eventType\":\"TRADE\",\"interval\":5}",
        "bdib",
        TypedRequests.bdib(
            "IBM US Equity",
            BdibOptions.builder().day("2024-01-02").interval(5).requestTz("NY").build()));
    assertPlan(
        "{\"service\":\"//blp/refdata\",\"operation\":\"IntradayTickRequest\","
            + "\"extractor\":\"intraday_tick\",\"security\":\"IBM US Equity\","
            + "\"kwargs\":[{\"key\":\"includeConditionCodes\",\"value\":\"false\"},"
            + "{\"key\":\"customOption\",\"value\":\"customValue\"},"
            + "{\"key\":\"includeExchangeCodes\",\"value\":\"true\"}],"
            + "\"startDatetime\":\"2024-01-02T09:30:00\",\"endDatetime\":\"2024-01-02T10:00:00\","
            + "\"outputTz\":\"exchange\",\"eventTypes\":[\"TRADE\"],\"returnEids\":true}",
        "bdtick",
        TypedRequests.bdtick(
            "IBM US Equity",
            BdtickOptions.builder()
                .start("2024-01-02T09:30:00")
                .end("2024-01-02 10:00:00")
                .outputTz("exchange")
                .kwarg("includeConditionCodes", true)
                .kwarg("customOption", "customValue")
                .includeConditionCodes(false)
                .includeExchangeCodes(true)
                .returnEids(true)
                .build()));
  }

  @Test
  void queryAndScreenPlansBuildTheirElements() {
    assertPlan(
        "{\"service\":\"//blp/bqlsvc\",\"operation\":\"sendQuery\",\"extractor\":\"bql\","
            + "\"elements\":[{\"key\":\"expression\",\"value\":\"get(px_last) for('IBM US Equity')\"}],"
            + "\"kwargs\":[{\"key\":\"clientContext.appName\",\"value\":\"per-call\"}],"
            + "\"format\":\"long\"}",
        "bql",
        TypedRequests.bql(
            "get(px_last) for('IBM US Equity')",
            BqlOptions.builder()
                .kwarg("clientContext.appName", "from-kwargs")
                .appName("per-call")
                .format(Format.LONG)
                .build()));
    assertPlan(
        "{\"service\":\"//blp/refdata\",\"operation\":\"BeqsRequest\",\"extractor\":\"generic\","
            + "\"elements\":[{\"key\":\"screenName\",\"value\":\"Core Capital Ratios\"},"
            + "{\"key\":\"screenType\",\"value\":\"PRIVATE\"},{\"key\":\"Group\",\"value\":\"General\"},"
            + "{\"key\":\"asOfDate\",\"value\":\"20240328\"}]}",
        "beqs",
        TypedRequests.beqs(
            "Core Capital Ratios", BeqsOptions.builder().asof(LocalDate.of(2024, 3, 28)).build()));
    assertPlan(
        "{\"service\":\"//blp/exrsvc\",\"operation\":\"ExcelGetGridRequest\","
            + "\"extractor\":\"bsrch\",\"elements\":[{\"key\":\"Domain\",\"value\":\"FI:SRCHEX\"},"
            + "{\"key\":\"LIMIT\",\"value\":\"20\"},{\"key\":\"Ticker\",\"value\":\"IBM\"}]}",
        "bsrch",
        TypedRequests.bsrch(
            "FI:SRCHEX",
            BsrchOptions.builder()
                .overrides(Overrides.of("LIMIT", 20))
                .kwarg("Ticker", "IBM")
                .build()));
  }

  @Test
  void btaPlansMergeStudyDefaults() {
    assertPlan(
        "{\"service\":\"//blp/tasvc\",\"operation\":\"studyRequest\",\"extractor\":\"generic\","
            + "\"elements\":[{\"key\":\"priceSource.securityName\",\"value\":\"SYNTHETIC Equity\"},"
            + "{\"key\":\"priceSource.dataRange.historical.periodicitySelection\","
            + "\"value\":\"DAILY\"},"
            + "{\"key\":\"studyAttributes.rsiStudyAttributes.period\",\"value\":\"7\"},"
            + "{\"key\":\"studyAttributes.rsiStudyAttributes.priceSourceClose\","
            + "\"value\":\"PX_LAST\"}]}",
        "bta",
        TypedRequests.bta(
            "SYNTHETIC Equity", "rsi", BtaOptions.builder().studyParam("period", 7).build()));
    assertPlan(
        "{\"service\":\"//blp/tasvc\",\"operation\":\"studyRequest\",\"extractor\":\"generic\","
            + "\"elements\":[{\"key\":\"priceSource.securityName\",\"value\":\"T\"},"
            + "{\"key\":\"priceSource.dataRange.intraday.startDate\",\"value\":\"20240102\"},"
            + "{\"key\":\"priceSource.dataRange.intraday.eventType\",\"value\":\"TRADE\"},"
            + "{\"key\":\"priceSource.dataRange.intraday.interval\",\"value\":\"5\"},"
            + "{\"key\":\"studyAttributes.bollStudyAttributes.period\",\"value\":\"10\"},"
            + "{\"key\":\"studyAttributes.bollStudyAttributes.upperBand\",\"value\":\"2.5\"},"
            + "{\"key\":\"studyAttributes.bollStudyAttributes.priceSourceClose\","
            + "\"value\":\"PX_LAST\"},{\"key\":\"maxDataPoints\",\"value\":\"100\"}]}",
        "bta",
        TypedRequests.bta(
            "T",
            BtaStudy.builder("BB")
                .calcInterval("intraday")
                .interval(5)
                .period(10)
                .param("upperBand", 2.5)
                .param("lowerBand", null)
                .build(),
            BtaOptions.builder()
                .kwarg("startDate", "2024-01-02")
                .kwarg("maxDataPoints", 100)
                .periodicity("DAILY")
                .interval(60)
                .build()));
  }

  @Test
  void fieldMetadataPlansPickSearchOrInfo() {
    String info =
        "{\"service\":\"//blp/apiflds\",\"operation\":\"FieldInfoRequest\","
            + "\"fieldIds\":[\"PX_LAST\",\"NAME\"]}";
    assertPlan(
        info,
        "bflds",
        TypedRequests.bflds(BfldsOptions.builder().fields("PX_LAST", "NAME").build()));
    assertPlan(
        info,
        "fieldInfo",
        TypedRequests.fieldInfo(Arrays.asList("PX_LAST", "NAME"), BfldsOptions.defaults()));
    String search =
        "{\"service\":\"//blp/apiflds\",\"operation\":\"FieldSearchRequest\","
            + "\"kwargs\":[{\"key\":\"returnFieldDocumentation\",\"value\":\"false\"}],"
            + "\"searchSpec\":\"last price\"}";
    BfldsOptions kwargs = BfldsOptions.builder().kwarg("returnFieldDocumentation", false).build();
    assertPlan(
        search,
        "bflds",
        TypedRequests.bflds(kwargs.toBuilder().searchSpec("last price").fields("IGNORED").build()));
    assertPlan(search, "fieldSearch", TypedRequests.fieldSearch("last price", kwargs));
    assertPlan(
        "{\"service\":\"//blp/apiflds\",\"operation\":\"FieldInfoRequest\",\"fieldIds\":[]}",
        "bflds",
        TypedRequests.bflds(BfldsOptions.defaults()));
  }

  @Test
  void instrumentAndPortfolioPlans() {
    assertPlan(
        "{\"service\":\"//blp/instruments\",\"operation\":\"instrumentListRequest\","
            + "\"elements\":[{\"key\":\"query\",\"value\":\"IBM\"}],"
            + "\"kwargs\":[{\"key\":\"maxResults\",\"value\":\"10\"}]}",
        "blkp",
        TypedRequests.blkp("IBM", BlkpOptions.builder().kwarg("maxResults", 10).build()));
    for (String[] method :
        new String[][] {{"bcurves", "curveListRequest"}, {"bgovts", "govtListRequest"}}) {
      assertPlan(
          "{\"service\":\"//blp/instruments\",\"operation\":\""
              + method[1]
              + "\","
              + "\"elements\":[{\"key\":\"query\",\"value\":\"US\"}],\"format\":\"long\"}",
          method[0],
          TypedRequests.instrumentList("US", RequestOptions.builder().format(Format.LONG).build()));
    }
    assertPlan(
        "{\"service\":\"//blp/refdata\",\"operation\":\"PortfolioDataRequest\","
            + "\"security\":\"U12345-1 Client\",\"fields\":[\"PORTFOLIO_MWEIGHT\"],"
            + "\"overrides\":[{\"key\":\"REFERENCE_DATE\",\"value\":\"20240131\"}]}",
        "bport",
        TypedRequests.bport(
            "U12345-1 Client",
            RequestArgs.single("field", "PORTFOLIO_MWEIGHT"),
            RequestOptions.builder()
                .overrides(Overrides.of("REFERENCE_DATE", LocalDate.of(2024, 1, 31)))
                .build()));
  }

  @Test
  void cdxPlansUseTheGeneratedFieldBundles() {
    String fields = Json.write(CdxFields.PRICING);
    assertPlan(
        "{\"service\":\"//blp/refdata\",\"operation\":\"ReferenceDataRequest\","
            + "\"extractor\":\"refdata\",\"securities\":[\"CDX IG CDSI GEN 5Y Corp\"],"
            + "\"fields\":"
            + fields
            + ","
            + "\"overrides\":[{\"key\":\"CDS_RR\",\"value\":\"40\"},{\"key\":\"X\",\"value\":\"1\"}],"
            + "\"includeSecurityErrors\":false}",
        "acdxPricing",
        TypedRequests.cdx(
            "CDX IG CDSI GEN 5Y Corp",
            CdxOptions.builder()
                .overrides(Overrides.of("CDS_RR", 25).with("X", 1))
                .recoveryRate(40.0)
                .build()));
    assertPlan(
        "{\"service\":\"//blp/refdata\",\"operation\":\"ReferenceDataRequest\","
            + "\"extractor\":\"refdata\",\"securities\":[\"T\"],"
            + "\"fields\":"
            + Json.write(CdxFields.RISK)
            + ","
            + "\"overrides\":[{\"key\":\"CDS_RR\",\"value\":\"37.5\"}],"
            + "\"includeSecurityErrors\":false}",
        "acdxRisk",
        TypedRequests.cdx("T", CdxOptions.builder().recoveryRate(37.5).build()));
    assertPlan(
        "{\"service\":\"//blp/refdata\",\"operation\":\"ReferenceDataRequest\","
            + "\"extractor\":\"refdata\",\"securities\":[\"T\"],"
            + "\"fields\":"
            + Json.write(CdxFields.INFO)
            + ",\"includeSecurityErrors\":false,"
            + "\"onDelayed\":\"ignore\"}",
        "acdxInfo",
        TypedRequests.acdxInfo("T", BdpOptions.builder().onDelayed(DelayedPolicy.IGNORE).build()));
  }

  @Test
  void rejectedCombinationsAreIllegalArguments() {
    RuntimeException security =
        planError(
            "beqs",
            TypedRequests.beqs(
                "S",
                BeqsOptions.builder()
                    .overrides(Overrides.empty().forSecurity("IBM US Equity", Overrides.of("A", 1)))
                    .build()));
    assertEquals(IllegalArgumentException.class, security.getClass());
    assertEquals(
        "Per-security overrides are only supported by bdp(), bdh(), and bds()",
        security.getMessage());

    RuntimeException curves =
        planError(
            "bcurves",
            TypedRequests.instrumentList(
                "US", RequestOptions.builder().overrides(Overrides.of("A", 1)).build()));
    assertEquals(IllegalArgumentException.class, curves.getClass());
    assertEquals("bcurves() does not support overrides", curves.getMessage());

    RuntimeException day =
        planError(
            "bdib",
            TypedRequests.bdib(
                "T", BdibOptions.builder().day("2024-01-02").start("2024-01-02T09:30:00").build()));
    assertEquals(IllegalArgumentException.class, day.getClass());
    assertEquals("bdib dt cannot be combined with start/end options", day.getMessage());

    RuntimeException unknown =
        planError(
            "bdp", JsonValue.parse("{\"tickers\":[],\"fields\":[]," + "\"backend\":\"arrow\"}"));
    assertEquals(IllegalArgumentException.class, unknown.getClass());
    assertEquals("unknown field 'backend'", unknown.getMessage());
  }

  @Test
  void engineValidationErrorsKeepTheirElement() {
    RuntimeException delayed =
        planError("bdp", JsonValue.parse("{\"tickers\":[],\"fields\":[],\"onDelayed\":\"WARN\"}"));
    BlpValidationException validation = (BlpValidationException) delayed;
    assertEquals("onDelayed must be warn, raise, or ignore", validation.getMessage());
    assertEquals("onDelayed", validation.element().get());

    RuntimeException json =
        assertThrows(
            RuntimeException.class,
            () ->
                NativeTestSupport.invoke(
                    "requests.input",
                    JsonValue.parse(
                        "{\"service\":\"//blp/refdata\",\"operation\":\"ReferenceDataRequest\","
                            + "\"jsonElements\":\"[1]\"}"),
                    ResultKind.JSON));
    assertEquals(BlpValidationException.class, json.getClass());
    assertEquals("jsonElements must be a JSON object at the top level", json.getMessage());
    assertEquals("jsonElements", ((BlpValidationException) json).element().get());
  }
}
