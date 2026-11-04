package io.github.underloam.xbbg.ext;

import io.github.underloam.xbbg.BlpInternalException;
import io.github.underloam.xbbg.internal.Json;
import io.github.underloam.xbbg.internal.JsonFields;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Stream-valid auction field groups and imbalance codes, from the native shared definition (the
 * {@code @xbbg/core} {@code AuctionFields} object and {@code imbalanceSide} function).
 *
 * <p>The groups load from the native library on first use; a failed load (for example, no Bloomberg
 * runtime yet) is not remembered, so a later call can succeed. Every list is unmodifiable.
 */
public final class AuctionFields {
  private static volatile Groups groups;

  private AuctionFields() {}

  /**
   * Returns the order imbalance fields.
   *
   * @return the {@code imbalance} group
   */
  public static List<String> imbalance() {
    return groups().imbalance;
  }

  /**
   * Returns the indicative price fields.
   *
   * @return the {@code indicative} group
   */
  public static List<String> indicative() {
    return groups().indicative;
  }

  /**
   * Returns the auction state fields.
   *
   * @return the {@code state} group
   */
  public static List<String> state() {
    return groups().state;
  }

  /**
   * Returns the trading halt fields.
   *
   * @return the {@code halts} group
   */
  public static List<String> halts() {
    return groups().halts;
  }

  /**
   * Returns the auction result fields.
   *
   * @return the {@code results} group
   */
  public static List<String> results() {
    return groups().results;
  }

  /**
   * Returns the primary-market composite fields.
   *
   * @return the {@code composite} group
   */
  public static List<String> composite() {
    return groups().composite;
  }

  /**
   * Returns the quote fields ({@code BID}, {@code ASK}, {@code BID_SIZE}, {@code ASK_SIZE}).
   *
   * @return the {@code quotes} group
   */
  public static List<String> quotes() {
    return groups().quotes;
  }

  /**
   * Returns the default auction fields (JavaScript {@code AuctionFields.default}): the imbalance,
   * indicative, state, halts and results groups in that order.
   *
   * @return the {@code default} group
   */
  public static List<String> defaults() {
    return groups().defaults;
  }

  /**
   * Returns the price fields whose numeric zero is a "no value" sentinel.
   *
   * @return the zero-price fields
   */
  public static List<String> zeroPriceFields() {
    return groups().zeroPriceFields;
  }

  /**
   * Looks a field group up by name, ignoring case and surrounding whitespace.
   *
   * @param name a group name: {@code imbalance}, {@code indicative}, {@code state}, {@code halts},
   *     {@code results}, {@code composite}, {@code quotes} or {@code default}
   * @return the group, or empty for an unknown name
   * @throws NullPointerException if {@code name} is null
   */
  public static Optional<List<String>> group(String name) {
    Objects.requireNonNull(name, "name");
    Object parsed = ExtCalls.json("auctionFieldGroup", Json.object().put("name", name));
    return parsed == null
        ? Optional.<List<String>>empty()
        : Optional.of(JsonFields.stringList(parsed, "auctionFieldGroup"));
  }

  /**
   * Interprets a Bloomberg imbalance code, ignoring case and surrounding whitespace (JavaScript
   * {@code imbalanceSide}).
   *
   * @param code the imbalance code, for example {@code BUY}, {@code MSEL} or {@code NOIM}
   * @return the side, or empty for undisclosed and unknown codes ({@code INOR}, {@code NODS},
   *     {@code ""}, …)
   * @throws NullPointerException if {@code code} is null
   */
  public static Optional<ImbalanceSide> imbalanceSide(String code) {
    Objects.requireNonNull(code, "code");
    Optional<String> side =
        ExtCalls.optionalString(
            ExtCalls.json("imbalanceSide", Json.object().put("code", code)), "imbalanceSide");
    return side.isPresent()
        ? Optional.of(ImbalanceSide.fromWire(side.get()))
        : Optional.<ImbalanceSide>empty();
  }

  private static Groups groups() {
    Groups loaded = groups;
    if (loaded == null) {
      synchronized (AuctionFields.class) {
        loaded = groups;
        if (loaded == null) {
          loaded = new Groups();
          groups = loaded;
        }
      }
    }
    return loaded;
  }

  private static List<String> load(String name) {
    Object parsed = ExtCalls.json("auctionFieldGroup", Json.object().put("name", name));
    if (parsed == null) {
      throw new BlpInternalException("native auction field group is missing: " + name);
    }
    return JsonFields.stringList(parsed, "auctionFieldGroup(" + name + ")");
  }

  private static final class Groups {
    final List<String> imbalance = load("imbalance");
    final List<String> indicative = load("indicative");
    final List<String> state = load("state");
    final List<String> halts = load("halts");
    final List<String> results = load("results");
    final List<String> composite = load("composite");
    final List<String> quotes = load("quotes");
    final List<String> defaults = load("default");
    final List<String> zeroPriceFields =
        JsonFields.stringList(
            ExtCalls.json("auctionZeroPriceFields", Json.object()), "auctionZeroPriceFields");
  }
}
