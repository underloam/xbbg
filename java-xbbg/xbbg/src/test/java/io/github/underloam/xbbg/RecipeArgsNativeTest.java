package io.github.underloam.xbbg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
 * Recipe arguments as the native layer parses them: the Java options are serialized by {@link
 * RecipeArgs}, parsed and defaulted natively, and returned as the normalized call that the recipe
 * would run with. Nothing here needs Bloomberg; argument errors surface before any request.
 */
class RecipeArgsNativeTest {
  @BeforeAll
  static void natives() {
    NativeTestSupport.assumeAvailable();
  }

  // Unchecked: the prepared request is a JSON object, which Json.parse returns as a
  // Map<String, Object>.
  @SuppressWarnings("unchecked")
  private static Map<String, Object> prepare(String method, JsonValue args) {
    String text =
        (String) NativeTestSupport.invoke("recipes.prepare." + method, args, ResultKind.JSON);
    Map<String, Object> call = (Map<String, Object>) Json.parse(text);
    assertEquals(method, call.get("method"));
    return call;
  }

  private static List<String> list(String... values) {
    return Arrays.asList(values);
  }

  @Test
  void bqrKeepsBrokerCodesOnAndLeavesTheWindowToTheRecipe() {
    Map<String, Object> defaults =
        prepare("bqr", RecipeArgs.bqr("XS1 Corp", BqrOptions.defaults()));
    assertEquals(true, defaults.get("includeBrokerCodes"));
    assertNull(defaults.get("startDatetime"), "the recipe defaults to the last hour");
    assertNull(defaults.get("endDatetime"));
    assertNull(defaults.get("eventTypes"));

    Map<String, Object> set =
        prepare(
            "bqr",
            RecipeArgs.bqr(
                "XS1 Corp",
                BqrOptions.builder()
                    .startDatetime("2024-01-15 09:00")
                    .endDatetime("20240116")
                    .eventTypes(list("TRADE"))
                    .includeBrokerCodes(Boolean.FALSE)
                    .build()));
    assertEquals("2024-01-15T09:00", set.get("startDatetime"));
    assertEquals("2024-01-16T00:00:00", set.get("endDatetime"));
    assertEquals(list("TRADE"), set.get("eventTypes"));
    assertEquals(false, set.get("includeBrokerCodes"));
  }

  @Test
  void yasSendsEveryOverrideValueAndValidatesTheYieldType() {
    Map<String, Object> call =
        prepare(
            "yas",
            RecipeArgs.yas(
                list("T 4 11/15/34 Govt"),
                list("YAS_BOND_YLD", "YAS_YLD_SPREAD"),
                YasOptions.builder()
                    .settleDt(LocalDate.of(2024, 1, 15))
                    .yieldType(9)
                    .spread(25.0)
                    .yieldVal(4.25)
                    .price(99.5)
                    .benchmark("T 4 11/15/34 Govt")
                    .build()));
    assertEquals("20240115", call.get("settleDt"));
    assertEquals(9L, call.get("yieldType"));
    assertEquals(99.5, call.get("price"));
    assertEquals(list("YAS_BOND_YLD", "YAS_YLD_SPREAD"), call.get("fields"));

    for (final int code : new int[] {0, 10, 300}) {
      BlpValidationException error =
          assertThrows(
              BlpValidationException.class,
              () ->
                  prepare(
                      "yas",
                      RecipeArgs.yas(
                          list("X Govt"),
                          list("YAS_BOND_YLD"),
                          YasOptions.builder().yieldType(code).build())));
      assertEquals(
          "unknown yield type '"
              + code
              + "': expected YTM, YTC, YTR, YTP, YTW, YTWR, EYTW, EYTWR, or YTAL",
          error.getMessage());
      assertEquals("yieldType", error.element().get());
    }
  }

  @Test
  void positionalDatesAreFormattedAndMissingOnesAreEmpty() {
    Map<String, Object> dividend =
        prepare(
            "dividend",
            RecipeArgs.dividend(
                list("IBM US Equity"),
                null,
                Dates.formatDate(LocalDate.of(2024, 12, 31)),
                DividendOptions.builder().dvdType("all").build()));
    assertEquals("", dividend.get("startDate"), "JavaScript sends '' for a missing date");
    assertEquals("20241231", dividend.get("endDate"));
    assertEquals("all", dividend.get("dvdType"));

    Map<String, Object> futures =
        prepare(
            "futTicker",
            RecipeArgs.futuresResolve(
                "ES1 Index",
                Dates.formatDate("2024-03-01"),
                FuturesResolveOptions.builder().freq("Q").build()));
    assertEquals("20240301", futures.get("dt"));
    assertEquals("Q", futures.get("freq"));
    assertEquals(
        "",
        prepare(
                "activeFutures",
                RecipeArgs.futuresResolve("ES1 Index", null, FuturesResolveOptions.defaults()))
            .get("dt"));
  }

  @Test
  void cdxAndFuturesOptions() {
    Map<String, Object> cdx =
        prepare(
            "cdxTicker",
            RecipeArgs.cdxTicker(
                "CDX IG CDSI GEN 5Y Corp", "20250922", CdxResolveOptions.defaults()));
    assertEquals(false, cdx.get("versionless"));
    Map<String, Object> active =
        prepare(
            "activeCdx",
            RecipeArgs.activeCdx(
                "CDX HY CDSI GEN 5Y Corp",
                "20260325",
                ActiveCdxOptions.builder().lookbackDays(10).versionless(Boolean.TRUE).build()));
    assertEquals(10L, active.get("lookbackDays"));
    assertEquals(true, active.get("versionless"));
    Map<String, Object> curve =
        prepare(
            "futuresCurve",
            RecipeArgs.futuresCurve(
                "CL1 Comdty",
                FuturesCurveOptions.builder()
                    .asof("2024-01-15")
                    .chainField("FUT_CHAIN")
                    .fields(list("OPEN_INT"))
                    .maxContracts(6)
                    .build()));
    assertEquals("20240115", curve.get("asof"));
    assertEquals("FUT_CHAIN", curve.get("chainField"));
    assertEquals(list("OPEN_INT"), curve.get("fields"));
    assertEquals(6L, curve.get("maxContracts"));
  }

  @Test
  void volSurfaceDefaultsPresetsAndEncodesFieldSpecs() {
    Map<String, Object> defaults =
        prepare(
            "volSurface",
            RecipeArgs.volSurface(
                list("SPX Index"), "20240101", "20240131", VolSurfaceOptions.defaults()));
    assertEquals(list("MONEYNESS_30D"), defaults.get("presets"));
    assertNull(defaults.get("fieldSpecs"));
    assertEquals(true, defaults.get("asDecimal"));
    assertEquals(false, defaults.get("includeDerived"));

    Map<String, VolFieldSpec> specs = new LinkedHashMap<String, VolFieldSpec>();
    specs.put(
        "ZETA_FIELD",
        VolFieldSpec.builder()
            .metric("implied_volatility")
            .tenor("1M")
            .pointType("delta_put")
            .point(-25.0)
            .build());
    specs.put("ALPHA_FIELD", null);
    specs.put("POINT_FIELD", VolFieldSpec.builder().point(97.5).build());
    Map<String, Object> custom =
        prepare(
            "volSurface",
            RecipeArgs.volSurface(
                list("SPX Index"),
                "20240101",
                "20240131",
                VolSurfaceOptions.builder()
                    .noPreset()
                    .fieldSpecs(specs)
                    .asDecimal(Boolean.FALSE)
                    .includeDerived(Boolean.TRUE)
                    .riskFreeRate(4.5)
                    .dividendYieldField("EQY_DVD_YLD_IND")
                    .build()));
    assertEquals(Collections.emptyList(), custom.get("presets"), "no preset, unlike JS null");
    assertEquals(
        list(
            "ZETA_FIELD|implied_volatility|1M|delta_put|-25", "ALPHA_FIELD", "POINT_FIELD||||97.5"),
        custom.get("fieldSpecs"));
    assertEquals(false, custom.get("asDecimal"));
    assertEquals(true, custom.get("includeDerived"));
    assertEquals(4.5, custom.get("riskFreeRate"));
    assertEquals("EQY_DVD_YLD_IND", custom.get("dividendYieldField"));

    Map<String, Object> presets =
        prepare(
            "volSurface",
            RecipeArgs.volSurface(
                list("SPX Index"),
                null,
                null,
                VolSurfaceOptions.builder()
                    .preset(VolSurfacePreset.DELTA_1M_2M)
                    .fields(list("PX_LAST"))
                    .build()));
    assertEquals(list("DELTA_1M_2M"), presets.get("presets"));
    assertEquals(list("PX_LAST"), presets.get("fieldSpecs"));
    assertEquals("", presets.get("startDate"));
  }

  @Test
  void listAndIdentityRecipes() {
    Map<String, Object> members =
        prepare(
            "indexMembers",
            RecipeArgs.indexMembers(
                "INDU Index",
                IndexMembersOptions.builder()
                    .field(IndexMemberField.INDX_MEMBERS3)
                    .asof(LocalDate.of(2024, 1, 2))
                    .build()));
    assertEquals("INDX_MEMBERS3", members.get("field"));
    assertEquals("20240102", members.get("asof"));

    Map<String, String> pcs = Collections.singletonMap("London", "LSE1");
    assertEquals(
        pcs,
        prepare(
                "resolveVenues",
                RecipeArgs.resolveVenues(
                    list("ABC LN Equity"),
                    ResolveVenuesOptions.builder().pcsOverrides(pcs).build()))
            .get("pcsOverrides"));
    Map<String, Object> snapshot =
        prepare(
            "auctionSnapshot",
            RecipeArgs.auctionSnapshot(list("VOD LN Equity"), AuctionSnapshotOptions.defaults()));
    assertEquals(
        Collections.emptyList(), snapshot.get("fields"), "empty selects the default group");
    assertEquals(Collections.emptyMap(), snapshot.get("pcsOverrides"));

    assertEquals(
        true,
        prepare(
                "delayStatus",
                RecipeArgs.delayStatus(
                    list("IBM US Equity"),
                    DelayStatusOptions.builder().checkIdentity(Boolean.TRUE).build()))
            .get("checkIdentity"));
    assertEquals(
        list("US0378331005"),
        prepare("resolveIsins", RecipeArgs.resolveIsins(list("US0378331005"))).get("isins"));
    assertEquals(
        list("US037833AK68"),
        prepare("issuerIsins", RecipeArgs.issuerIsins(list("US037833AK68"))).get("bondIsins"));
  }

  @Test
  void etfNavTickersAreTrimmed() {
    assertEquals(
        list("SPY US Equity", "QQQ US Equity"),
        prepare("etfNavSnapshot", RecipeArgs.etfNav(list(" SPY US Equity ", "QQQ US Equity")))
            .get("tickers"));
    assertEquals(
        list("SPY US Equity"),
        prepare("etfNavRelationships", RecipeArgs.etfNav(list("\tSPY US Equity"))).get("tickers"));
    Map<String, Object> history =
        prepare(
            "etfNavHistory", RecipeArgs.etfNavHistory(list("SPY US Equity "), "20240101", null));
    assertEquals(list("SPY US Equity"), history.get("tickers"));
    assertEquals("", history.get("endDate"));
  }

  @Test
  void otherRecipesCarryTheirArguments() {
    assertEquals(
        list("cpn"),
        prepare(
                "corporateBonds",
                RecipeArgs.corporateBonds(
                    "AAPL", CorporateBondsOptions.builder().ccy("USD").fields(list("cpn")).build()))
            .get("fields"));
    assertEquals(
        "BAC US Equity",
        prepare("preferreds", RecipeArgs.preferreds("BAC US Equity", PreferredsOptions.defaults()))
            .get("equityTicker"));
    assertEquals(
        list("name"),
        prepare(
                "etfHoldings",
                RecipeArgs.etfHoldings(
                    "SPY US Equity", EtfHoldingsOptions.builder().fields(list("name")).build()))
            .get("fields"));
    Map<String, Object> yield =
        prepare(
            "dividendYield",
            RecipeArgs.dividendYield(
                list("IBM US Equity"),
                "20240101",
                "20241231",
                DividendYieldOptions.builder()
                    .dividendTypes(list("Regular Cash"))
                    .windowDays(90)
                    .build()));
    assertEquals(list("Regular Cash"), yield.get("dividendTypes"));
    assertEquals(90L, yield.get("windowDays"));
    Map<String, Object> turnover =
        prepare(
            "turnover",
            RecipeArgs.turnover(
                list("VOD LN Equity"),
                null,
                null,
                TurnoverOptions.builder().ccy("USD").factor(1e6).build()));
    assertEquals("USD", turnover.get("ccy"));
    assertEquals(1e6, ((Number) turnover.get("factor")).doubleValue());
    assertEquals("", turnover.get("startDate"), "empty dates use the recipe's default range");
    Map<String, Object> conversion =
        prepare(
            "currencyConversion",
            RecipeArgs.currencyConversion("VOD LN Equity", "USD", "20240101", "20240131"));
    assertEquals("USD", conversion.get("targetCcy"));
    assertEquals("20240131", conversion.get("endDate"));
  }

  @Test
  void malformedArgumentObjectsAreArgumentErrors() {
    IllegalArgumentException unknown =
        assertThrows(
            IllegalArgumentException.class,
            () -> prepare("bqr", Json.object().put("ticker", "X").put("backend", "json").build()));
    assertEquals("unknown field 'backend'", unknown.getMessage());
    IllegalArgumentException missing =
        assertThrows(
            IllegalArgumentException.class,
            () -> prepare("currencyConversion", Json.object().put("ticker", "X").build()));
    assertEquals("missing field 'targetCcy'", missing.getMessage());
    assertThrows(NullPointerException.class, () -> RecipeArgs.bqr(null, BqrOptions.defaults()));
  }

  @Test
  void everyRecipeNameIsKnownToTheNativeLayer() {
    for (String method :
        list(
            "bqr",
            "yas",
            "preferreds",
            "corporateBonds",
            "futTicker",
            "activeFutures",
            "futuresCurve",
            "cdxTicker",
            "activeCdx",
            "dividend",
            "dividendYield",
            "turnover",
            "etfHoldings",
            "volSurface",
            "indexMembers",
            "resolveIsins",
            "resolveVenues",
            "auctionSnapshot",
            "delayStatus",
            "issuerIsins",
            "etfNavRelationships",
            "etfNavSnapshot",
            "etfNavHistory",
            "currencyConversion")) {
      IllegalArgumentException error =
          assertThrows(
              IllegalArgumentException.class,
              () -> prepare(method, Json.object().put("x", 1).build()));
      assertTrue(
          error.getMessage().startsWith("unknown field 'x'"), method + ": " + error.getMessage());
    }
    BlpInternalException unknown =
        assertThrows(BlpInternalException.class, () -> prepare("bdp", JsonValue.EMPTY_OBJECT));
    assertEquals("unknown engine method 'bdp'", unknown.getMessage());
    assertFalse(unknown.getMessage().isEmpty());
  }
}
