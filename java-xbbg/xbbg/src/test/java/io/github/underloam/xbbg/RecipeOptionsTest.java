package io.github.underloam.xbbg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.underloam.xbbg.internal.Json;
import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Recipe option builders and the argument fields they write (no native library needed). */
class RecipeOptionsTest {
  // Unchecked: encodeInto writes JSON objects, which Json.parse returns as Map<String, Object>.
  @SuppressWarnings("unchecked")
  private static Map<String, Object> written(VolSurfaceOptions options) {
    JsonObjectBuilder args = Json.object();
    options.encodeInto(args);
    return (Map<String, Object>) Json.parse(args.build().text());
  }

  @Test
  void unsetOptionsWriteNothing() {
    JsonObjectBuilder args = Json.object();
    VolSurfaceOptions.defaults().encodeInto(args);
    BqrOptions.defaults().encodeInto(args);
    YasOptions.defaults().encodeInto(args);
    FuturesCurveOptions.defaults().encodeInto(args);
    AuctionSnapshotOptions.defaults().encodeInto(args);
    assertTrue(args.isEmpty(), args.toString());
  }

  @Test
  void volSurfacePresetsAreWrittenAsWireValuesAndNoPresetIsAnEmptyList() {
    Map<String, Object> listed =
        written(
            VolSurfaceOptions.builder()
                .presets(Arrays.asList(VolSurfacePreset.DELTA_1M_2M, VolSurfacePreset.MONEYNESS_3M))
                .build());
    assertEquals(Arrays.asList("DELTA_1M_2M", "MONEYNESS_3M"), listed.get("preset"));

    Map<String, Object> none = written(VolSurfaceOptions.builder().noPreset().build());
    assertEquals(Collections.emptyList(), none.get("preset"));

    VolSurfaceOptions unset =
        VolSurfaceOptions.builder().preset(VolSurfacePreset.MONEYNESS_60D).preset(null).build();
    assertFalse(unset.presets().isPresent());
    assertFalse(written(unset).containsKey("preset"));

    assertThrows(
        NullPointerException.class,
        () ->
            VolSurfaceOptions.builder()
                .presets(Arrays.asList(VolSurfacePreset.MONEYNESS_6M, null)));
  }

  @Test
  void volSurfaceFieldSpecsKeepOrderAndNullSpecs() {
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
    specs.put("MID_FIELD", VolFieldSpec.builder().tenor("3M").build());
    Map<String, Object> args = written(VolSurfaceOptions.builder().fieldSpecs(specs).build());

    // Unchecked: "fields" is a JSON object (Map<String, Object> after parsing).
    @SuppressWarnings("unchecked")
    Map<String, Object> fields = (Map<String, Object>) args.get("fields");
    assertEquals(
        Arrays.asList("ZETA_FIELD", "ALPHA_FIELD", "MID_FIELD"),
        Arrays.asList(fields.keySet().toArray()));
    assertNull(fields.get("ALPHA_FIELD"));
    assertTrue(fields.containsKey("ALPHA_FIELD"), "a null spec is written as JSON null");
    // Unchecked: each field spec is a JSON object.
    @SuppressWarnings("unchecked")
    Map<String, Object> zeta = (Map<String, Object>) fields.get("ZETA_FIELD");
    assertEquals("delta_put", zeta.get("pointType"));
    assertEquals(-25.0, ((Number) zeta.get("point")).doubleValue());
    // Unchecked: each field spec is a JSON object.
    @SuppressWarnings("unchecked")
    Map<String, Object> mid = (Map<String, Object>) fields.get("MID_FIELD");
    assertEquals(Collections.singletonMap("tenor", "3M"), mid);
  }

  @Test
  void volSurfacePlainFieldsAndSpecsReplaceEachOther() {
    Map<String, VolFieldSpec> specs = Collections.singletonMap("F", null);
    VolSurfaceOptions specsLast =
        VolSurfaceOptions.builder().fields(Arrays.asList("A", "B")).fieldSpecs(specs).build();
    assertFalse(specsLast.fields().isPresent());
    assertEquals(specs, specsLast.fieldSpecs().get());

    VolSurfaceOptions fieldsLast =
        specsLast.toBuilder().fields(Collections.singletonList("PX_LAST")).build();
    assertFalse(fieldsLast.fieldSpecs().isPresent());
    assertEquals(Collections.singletonList("PX_LAST"), written(fieldsLast).get("fields"));
  }

  @Test
  void dateOptionsAreFormattedAndAmbiguousTextIsRejected() {
    assertEquals(
        "20240115",
        FuturesCurveOptions.builder().asof(LocalDate.of(2024, 1, 15)).build().asof().get());
    assertEquals("20240115", IndexMembersOptions.builder().asof("2024-01-15").build().asof().get());
    assertEquals("20240115", YasOptions.builder().settleDt("20240115").build().settleDt().get());
    assertFalse(YasOptions.builder().settleDt("  ").build().settleDt().isPresent());
    assertThrows(IllegalArgumentException.class, () -> YasOptions.builder().settleDt("01/02/2024"));
  }

  @Test
  void listOptionsRejectNullItemsAndAreImmutable() {
    assertThrows(
        NullPointerException.class,
        () -> PreferredsOptions.builder().fields(Arrays.asList("name", null)));
    List<String> source = new java.util.ArrayList<String>(Arrays.asList("cpn"));
    CorporateBondsOptions options = CorporateBondsOptions.builder().fields(source).build();
    source.add("maturity");
    assertEquals(Collections.singletonList("cpn"), options.fields().get());
    assertThrows(UnsupportedOperationException.class, () -> options.fields().get().add("x"));
    Map<String, String> pcs = new LinkedHashMap<String, String>();
    pcs.put("London", null);
    assertThrows(
        NullPointerException.class, () -> ResolveVenuesOptions.builder().pcsOverrides(pcs));
  }

  @Test
  void toBuilderRoundTripsAndEqualityCoversEveryOption() {
    ActiveCdxOptions options =
        ActiveCdxOptions.builder().lookbackDays(10).versionless(Boolean.TRUE).build();
    assertEquals(options, options.toBuilder().build());
    assertEquals(options.hashCode(), options.toBuilder().build().hashCode());
    assertNotEquals(options, options.toBuilder().versionless(Boolean.FALSE).build());
    assertNotEquals(options, options.toBuilder().lookbackDays(null).build());
    assertEquals("ActiveCdxOptions{lookbackDays=10, versionless=true}", options.toString());

    DelayStatusOptions identity = DelayStatusOptions.builder().checkIdentity(Boolean.TRUE).build();
    assertEquals(true, identity.checkIdentity().get());
    assertEquals(DelayStatusOptions.defaults(), identity.toBuilder().checkIdentity(null).build());
  }
}
