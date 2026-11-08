package io.github.underloam.xbbg.ext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.underloam.xbbg.internal.NativeTestSupport;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** {@link AuctionFields} and {@link ImbalanceSide} against the native shared definition. */
class AuctionFieldsTest {
  @BeforeAll
  static void natives() {
    NativeTestSupport.assumeAvailable();
  }

  @Test
  void defaultIsTheConcatenationOfFiveGroups() {
    List<String> expected = new ArrayList<String>();
    expected.addAll(AuctionFields.imbalance());
    expected.addAll(AuctionFields.indicative());
    expected.addAll(AuctionFields.state());
    expected.addAll(AuctionFields.halts());
    expected.addAll(AuctionFields.results());
    assertEquals(expected, AuctionFields.defaults());
    assertEquals(39, AuctionFields.defaults().size());
    assertTrue(AuctionFields.composite().contains("PRIMARY_MARKET_CLOSING_PRICE_RT"));
  }

  @Test
  void quotesAndZeroPriceFields() {
    assertEquals(Arrays.asList("BID", "ASK", "BID_SIZE", "ASK_SIZE"), AuctionFields.quotes());
    assertEquals(
        Arrays.asList(
            "THEO_PRICE",
            "INDICATIVE_NEAR",
            "INDICATIVE_FAR",
            "IMBALANCE_BUY",
            "IMBALANCE_SELL",
            "REFERENCE_PRICE_RT"),
        AuctionFields.zeroPriceFields());
  }

  @Test
  void groupsAreLoadedOnceAndUnmodifiable() {
    assertSame(AuctionFields.defaults(), AuctionFields.defaults());
    assertThrows(UnsupportedOperationException.class, () -> AuctionFields.quotes().add("X"));
  }

  @Test
  void groupLookupIsTrimmedAndCaseInsensitive() {
    assertEquals(Optional.of(AuctionFields.quotes()), AuctionFields.group("  Quotes "));
    assertEquals(Optional.of(AuctionFields.defaults()), AuctionFields.group("DEFAULT"));
    assertEquals(Optional.empty(), AuctionFields.group("bogus"));
  }

  @Test
  void imbalanceSideInterpretsBloombergCodes() {
    for (String code : Arrays.asList("BUY", " mbuy ", "RBUY")) {
      assertEquals(Optional.of(ImbalanceSide.BUY), AuctionFields.imbalanceSide(code), code);
    }
    for (String code : Arrays.asList("SELL", "msel", "RSEL")) {
      assertEquals(Optional.of(ImbalanceSide.SELL), AuctionFields.imbalanceSide(code), code);
    }
    for (String code : Arrays.asList("NOIM", "nimb")) {
      assertEquals(Optional.of(ImbalanceSide.NONE), AuctionFields.imbalanceSide(code), code);
    }
    for (String code : Arrays.asList("INOR", "NODS", "", "N.A.")) {
      assertEquals(Optional.empty(), AuctionFields.imbalanceSide(code), code);
    }
    assertEquals("buy", ImbalanceSide.BUY.wireValue());
    assertThrows(NullPointerException.class, () -> AuctionFields.imbalanceSide(null));
  }
}
