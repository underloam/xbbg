package io.github.underloam.xbbg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

/** The generated enums parse exactly like the Rust parsers that own each set. */
class ClosedSetsTest {
  @Test
  void formatParsingIsExact() {
    assertEquals(Format.LONG, Format.parse("long"));
    assertEquals(Format.LONG_TYPED, Format.parse("typed"));
    assertEquals(Format.LONG_WITH_METADATA, Format.parse("with_metadata"));
    assertEquals(Format.SEMI_LONG, Format.parse("wide"));
    assertEquals("long_metadata", Format.LONG_WITH_METADATA.wireValue());
    assertEquals(Arrays.asList("metadata", "with_metadata"), Format.LONG_WITH_METADATA.aliases());
    assertEquals(Format.LONG, Format.defaultValue());
    assertEquals(
        "Unknown format: LONG",
        assertThrows(IllegalArgumentException.class, () -> Format.parse("LONG")).getMessage());
    assertThrows(IllegalArgumentException.class, () -> Format.parse(" long"));
  }

  @Test
  void overflowPolicyIgnoresCaseButNotWhitespace() {
    assertEquals(OverflowPolicy.DROP_NEWEST, OverflowPolicy.parse("DROP_NEWEST"));
    assertEquals(OverflowPolicy.DROP_OLDEST, OverflowPolicy.parse("DropOldest"));
    assertEquals(OverflowPolicy.BLOCK, OverflowPolicy.parse("bloc\u212a"), "Unicode lowercase");
    assertEquals(OverflowPolicy.DROP_NEWEST, OverflowPolicy.defaultValue());
    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> OverflowPolicy.parse(" block"));
    assertEquals(
        "unknown overflow policy ' block': expected 'drop_newest', 'drop_oldest', or 'block'",
        error.getMessage());
  }

  @Test
  void deliveryModeTrimsAndIgnoresAsciiCase() {
    assertEquals(SubscriptionDeliveryMode.STATE, SubscriptionDeliveryMode.parse("\u00a0 State\t"));
    assertEquals(SubscriptionDeliveryMode.DELTAS, SubscriptionDeliveryMode.defaultValue());
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class, () -> SubscriptionDeliveryMode.parse("\u017ftate"));
    assertEquals("delivery must be 'deltas' or 'state'", error.getMessage());
  }

  @Test
  void validationModeAcceptsAliasesInAnyCase() {
    assertEquals(ValidationMode.DISABLED, ValidationMode.parse("NONE"));
    assertEquals(ValidationMode.DISABLED, ValidationMode.parse("off"));
    assertEquals(ValidationMode.STRICT, ValidationMode.parse("Strict"));
    assertEquals(
        "unknown validation mode 'loose': expected strict, lenient, or disabled",
        assertThrows(IllegalArgumentException.class, () -> ValidationMode.parse("loose"))
            .getMessage());
  }

  @Test
  void keyColumnAndSdkLogLevelUseAsciiCaseOnly() {
    assertEquals(KeyColumn.SECURITY, KeyColumn.parse("SECURITY"));
    assertEquals(KeyColumn.TICKER, KeyColumn.defaultValue());
    assertEquals(
        "unknown key column 'T\u0130CKER': expected security or ticker",
        assertThrows(IllegalArgumentException.class, () -> KeyColumn.parse("T\u0130CKER"))
            .getMessage());
    assertEquals(SdkLogLevel.WARN, SdkLogLevel.parse("WARNING"));
    assertEquals(SdkLogLevel.OFF, SdkLogLevel.defaultValue());
    assertEquals(
        "invalid SDK log level: verbose (expected off/fatal/error/warn/info/debug/trace)",
        assertThrows(IllegalArgumentException.class, () -> SdkLogLevel.parse("verbose"))
            .getMessage());
  }

  @Test
  void presetEnumsAreExact() {
    assertEquals(VolSurfacePreset.MONEYNESS_30D, VolSurfacePreset.parse("MONEYNESS_30D"));
    assertEquals(IndexMemberField.INDX_MEMBERS3, IndexMemberField.parse("INDX_MEMBERS3"));
    assertEquals(Collections.emptyList(), IndexMemberField.INDX_MWEIGHT.aliases());
    assertThrows(IllegalArgumentException.class, () -> VolSurfacePreset.parse("moneyness_30d"));
    assertEquals(8, CdxFields.INFO.size());
    assertEquals(10, CdxFields.PRICING.size());
    assertEquals(6, CdxFields.RISK.size());
    assertThrows(UnsupportedOperationException.class, () -> CdxFields.RISK.add("X"));
  }

  @Test
  void enumTextIsTheWireValue() {
    assertEquals("drop_oldest", OverflowPolicy.DROP_OLDEST.toString());
    assertEquals("semi_long", Format.SEMI_LONG.toString());
  }

  @Test
  void logLevelParsesLikeTheNativeLog() {
    assertEquals(LogLevel.TRACE, LogLevel.parse("0"));
    assertEquals(LogLevel.ERROR, LogLevel.parse("4"));
    assertEquals(LogLevel.WARN, LogLevel.parse("Warning"));
    assertEquals(
        "Invalid log level 'off'. Expected: trace, debug, info, warn, error",
        assertThrows(IllegalArgumentException.class, () -> LogLevel.parse("off")).getMessage());
    assertEquals(SeatType.NONBPS, SeatType.parse("NONBPS"));
    assertThrows(IllegalArgumentException.class, () -> SeatType.parse("bps"));
  }
}
