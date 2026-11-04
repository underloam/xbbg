package io.github.underloam.xbbg.jdk17;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.underloam.xbbg.SubscriptionDeliveryMode;
import io.github.underloam.xbbg.subscription.Jdk17Fixtures;
import io.github.underloam.xbbg.subscription.SubscriptionGap;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class DeliveryViewTest {
  @Test
  void deltasKeepTheGapBefore() {
    SubscriptionGap gap = Jdk17Fixtures.gap(-1L); // u64::MAX dropped updates
    DeliveryView<String> view =
        DeliveryView.of(Jdk17Fixtures.delivery(SubscriptionDeliveryMode.DELTAS, "rows", gap));
    assertInstanceOf(DeliveryView.Deltas.class, view);
    if (view instanceof DeliveryView.Deltas<String> deltas) {
      assertEquals("rows", deltas.data());
      assertEquals(Optional.of(gap), deltas.gapBefore());
      assertEquals(
          "18446744073709551615",
          Long.toUnsignedString(deltas.gapBefore().orElseThrow().droppedUpdates()));
    }
  }

  @Test
  void continuousDeltasHaveNoGap() {
    DeliveryView<String> view =
        DeliveryView.of(Jdk17Fixtures.delivery(SubscriptionDeliveryMode.DELTAS, "rows", null));
    assertEquals(new DeliveryView.Deltas<>("rows", Optional.empty()), view);
  }

  @Test
  void stateDeliveriesBecomeState() {
    DeliveryView<String> view =
        DeliveryView.of(Jdk17Fixtures.delivery(SubscriptionDeliveryMode.STATE, "images", null));
    assertEquals(new DeliveryView.State<>("images"), view);
    assertEquals("images", view.data());
  }

  @Test
  void rejectsMissingComponents() {
    assertThrows(NullPointerException.class, () -> DeliveryView.of(null));
    assertThrows(NullPointerException.class, () -> new DeliveryView.State<String>(null));
    assertThrows(NullPointerException.class, () -> new DeliveryView.Deltas<>("rows", null));
  }
}
