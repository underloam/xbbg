package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.Json;
import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import io.github.underloam.xbbg.internal.JsonValue;
import io.github.underloam.xbbg.internal.ResultKind;
import io.github.underloam.xbbg.internal.SubscriptionBridge;
import io.github.underloam.xbbg.subscription.AuctionStreamOptions;
import io.github.underloam.xbbg.subscription.StreamOptions;
import io.github.underloam.xbbg.subscription.Subscription;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Real-time subscriptions ({@code subscribe}, {@code stream}, {@code subscribeAuction}, ...).
 *
 * <p>Every entry point validates its {@link StreamOptions} with the JavaScript messages ({@link
 * BlpValidationException} naming the option in {@link BlpValidationException#element()}) before
 * anything is subscribed. Market-data feeds are shared within the engine when the service, topic
 * and normalized options match; aliases are per subscription. A subscription keeps the native
 * engine running after the {@link Engine} object is no longer referenced, until the subscription is
 * closed; {@link Engine#close()} ends every subscription of the engine.
 */
abstract class EngineSubscriptions extends EngineRecipes {
  private static final String MKTDATA = "//blp/mktdata";

  EngineSubscriptions(long handle, KeyColumn keyColumn) {
    super(handle, keyColumn);
  }

  /** The unified {@code subscribe} arguments. */
  static JsonValue subscribeArgs(
      String service,
      List<String> tickers,
      List<String> fields,
      StreamOptions options,
      boolean rawOptions) {
    JsonObjectBuilder json = Json.object();
    json.put("service", service);
    json.put("tickers", tickers);
    json.put("fields", fields);
    if (rawOptions) {
      json.put("rawOptions", true);
    }
    json.put("options", SubscriptionBridge.streamOptions(options));
    return json.build();
  }

  private CompletableFuture<Subscription> open(String method, JsonValue args) {
    return SubscriptionBridge.open(call(method, args, ResultKind.HANDLE));
  }

  private CompletableFuture<Subscription> named(
      String service,
      Collection<String> tickers,
      Collection<String> fields,
      StreamOptions options) {
    return open(
        "subscribe",
        subscribeArgs(
            service,
            RequestArgs.strings("tickers", tickers),
            RequestArgs.strings("fields", fields),
            Objects.requireNonNull(options, "options"),
            false));
  }

  private CompletableFuture<Subscription> single(
      String service, String ticker, StreamOptions options) {
    Objects.requireNonNull(options, "options");
    return named(
        service,
        RequestArgs.single("ticker", ticker),
        options.fields().orElse(Collections.<String>emptyList()),
        options);
  }

  /**
   * Subscribes to {@code //blp/mktdata} topics with default options.
   *
   * @param tickers the topics
   * @param fields the fields
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Subscription> subscribe(
      Collection<String> tickers, Collection<String> fields) {
    return subscribe(tickers, fields, StreamOptions.defaults());
  }

  /**
   * Subscribes to {@code //blp/mktdata} topics. {@code options.fields} is ignored.
   *
   * @param tickers the topics
   * @param fields the fields
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Subscription> subscribe(
      Collection<String> tickers, Collection<String> fields, StreamOptions options) {
    return named(MKTDATA, tickers, fields, options);
  }

  /**
   * Streams {@code //blp/mktdata} topics with default options (same as {@code subscribe(tickers,
   * fields)}).
   *
   * @param tickers the topics
   * @param fields the fields
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Subscription> stream(
      Collection<String> tickers, Collection<String> fields) {
    return stream(tickers, fields, StreamOptions.defaults());
  }

  /**
   * Streams {@code //blp/mktdata} topics. {@code options.fields} is ignored.
   *
   * @param tickers the topics
   * @param fields the fields
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Subscription> stream(
      Collection<String> tickers, Collection<String> fields, StreamOptions options) {
    return named(MKTDATA, tickers, fields, options);
  }

  /**
   * Streams volume-weighted average prices ({@code //blp/mktvwap}) with default options.
   *
   * @param tickers the topics
   * @param fields the VWAP fields
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Subscription> vwap(
      Collection<String> tickers, Collection<String> fields) {
    return vwap(tickers, fields, StreamOptions.defaults());
  }

  /**
   * Streams volume-weighted average prices ({@code //blp/mktvwap}); VWAP parameters go in {@code
   * options.options}. {@code options.fields} is ignored.
   *
   * @param tickers the topics
   * @param fields the VWAP fields
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Subscription> vwap(
      Collection<String> tickers, Collection<String> fields, StreamOptions options) {
    return named("//blp/mktvwap", tickers, fields, options);
  }

  /**
   * Streams intraday bars ({@code //blp/mktbar}) with default options.
   *
   * @param ticker the topic
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Subscription> mktbar(String ticker) {
    return mktbar(ticker, StreamOptions.defaults());
  }

  /**
   * Streams intraday bars ({@code //blp/mktbar}); the fields come from {@code options.fields} (none
   * when unset) and bar settings such as {@code interval=5} from {@code options.options}.
   *
   * @param ticker the topic
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Subscription> mktbar(String ticker, StreamOptions options) {
    return single("//blp/mktbar", ticker, options);
  }

  /**
   * Streams market depth ({@code //blp/mktdepthdata}, B-PIPE) with default options.
   *
   * @param ticker the topic
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Subscription> depth(String ticker) {
    return depth(ticker, StreamOptions.defaults());
  }

  /**
   * Streams market depth ({@code //blp/mktdepthdata}, B-PIPE); the fields come from {@code
   * options.fields}.
   *
   * @param ticker the topic
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Subscription> depth(String ticker, StreamOptions options) {
    return single("//blp/mktdepthdata", ticker, options);
  }

  /**
   * Streams option and futures chains ({@code //blp/mktlist}, B-PIPE) with default options.
   *
   * @param ticker the topic
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Subscription> chains(String ticker) {
    return chains(ticker, StreamOptions.defaults());
  }

  /**
   * Streams option and futures chains ({@code //blp/mktlist}, B-PIPE); the fields come from {@code
   * options.fields}.
   *
   * @param ticker the topic
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Subscription> chains(String ticker, StreamOptions options) {
    return single("//blp/mktlist", ticker, options);
  }

  /**
   * Subscribes to any service with Bloomberg options passed through unchanged (no trimming, no
   * {@code &} stripping, no {@code conflate}, which is rejected here). {@code options.fields} is
   * ignored.
   *
   * @param service the service, such as {@code //blp/mktdata}
   * @param tickers the topics
   * @param fields the fields
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Subscription> subscribeWithOptions(
      String service,
      Collection<String> tickers,
      Collection<String> fields,
      StreamOptions options) {
    return open(
        "subscribe",
        subscribeArgs(
            Objects.requireNonNull(service, "service"),
            RequestArgs.strings("tickers", tickers),
            RequestArgs.strings("fields", fields),
            Objects.requireNonNull(options, "options"),
            true));
  }

  /**
   * Resolves the input to its primary auction venue, then subscribes to the venue with the default
   * auction fields.
   *
   * @param security the input security
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Subscription> subscribeAuction(String security) {
    return subscribeAuction(
        RequestArgs.single("securities", security), AuctionStreamOptions.defaults());
  }

  /**
   * Resolves the input to its primary auction venue, then subscribes to the venue.
   *
   * @param security the input security
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Subscription> subscribeAuction(
      String security, AuctionStreamOptions options) {
    return subscribeAuction(RequestArgs.single("securities", security), options);
  }

  /**
   * Resolves every input to its primary auction venue first (atomic preflight), then opens one
   * subscription over the venue topics with the default auction fields.
   *
   * @param securities the input securities (duplicates are dropped)
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Subscription> subscribeAuction(Collection<String> securities) {
    return subscribeAuction(securities, AuctionStreamOptions.defaults());
  }

  /**
   * Resolves every input to its primary auction venue first (atomic preflight: any unresolved,
   * failed or ambiguous input fails the call with {@link BlpValidationException} before anything is
   * subscribed), then opens one subscription over the venue topics. Ticks are labelled with the
   * input securities; {@code add} takes venue topics with aliases.
   *
   * @param securities the input securities (duplicates are dropped)
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Subscription> subscribeAuction(
      Collection<String> securities, AuctionStreamOptions options) {
    return open(
        "subscribeAuction",
        SubscriptionBridge.auctionArgs(
            RequestArgs.strings("securities", securities),
            Objects.requireNonNull(options, "options"),
            false));
  }

  /**
   * Like {@code subscribeAuction(String)}, for row-delivering auction streams.
   *
   * @param security the input security
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Subscription> streamAuction(String security) {
    return streamAuction(
        RequestArgs.single("securities", security), AuctionStreamOptions.defaults());
  }

  /**
   * Like {@code subscribeAuction(String, AuctionStreamOptions)}; {@code rows} must stay unset.
   *
   * @param security the input security
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Subscription> streamAuction(
      String security, AuctionStreamOptions options) {
    return streamAuction(RequestArgs.single("securities", security), options);
  }

  /**
   * Like {@code subscribeAuction(Collection)}, for row-delivering auction streams.
   *
   * @param securities the input securities
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Subscription> streamAuction(Collection<String> securities) {
    return streamAuction(securities, AuctionStreamOptions.defaults());
  }

  /**
   * Like {@code subscribeAuction(Collection, AuctionStreamOptions)}, but always delivers rows:
   * setting {@code rows} fails with {@link BlpValidationException}.
   *
   * @param securities the input securities
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Subscription> streamAuction(
      Collection<String> securities, AuctionStreamOptions options) {
    return open(
        "subscribeAuction",
        SubscriptionBridge.auctionArgs(
            RequestArgs.strings("securities", securities),
            Objects.requireNonNull(options, "options"),
            true));
  }

  /**
   * Resolves the ETF's validated iNAV ticker first, then streams {@code LAST_PRICE} of the iNAV.
   *
   * @param ticker the ETF
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Subscription> subscribeEtfInav(String ticker) {
    return subscribeEtfInav(RequestArgs.single("tickers", ticker), StreamOptions.defaults());
  }

  /**
   * Resolves the ETF's validated iNAV ticker first, then streams it.
   *
   * @param ticker the ETF
   * @param options the options ({@code fields} defaults to {@code LAST_PRICE})
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Subscription> subscribeEtfInav(String ticker, StreamOptions options) {
    return subscribeEtfInav(RequestArgs.single("tickers", ticker), options);
  }

  /**
   * Resolves every ETF's validated iNAV ticker first, then streams {@code LAST_PRICE} of the iNAVs.
   *
   * @param tickers the ETFs (trimmed; duplicates are rejected)
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Subscription> subscribeEtfInav(Collection<String> tickers) {
    return subscribeEtfInav(tickers, StreamOptions.defaults());
  }

  /**
   * Resolves every ETF's validated iNAV ticker first (atomic preflight: an invalid, missing or
   * ambiguous relationship fails the call with {@link BlpValidationException} before anything is
   * subscribed), then opens one subscription over the iNAV tickers. Topics are the iNAV tickers, so
   * {@code add} and {@code remove} take iNAV tickers.
   *
   * @param tickers the ETFs (trimmed; duplicates are rejected)
   * @param options the options ({@code fields} defaults to {@code LAST_PRICE}; an empty list stays
   *     empty)
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public CompletableFuture<Subscription> subscribeEtfInav(
      Collection<String> tickers, StreamOptions options) {
    JsonObjectBuilder json = Json.object();
    json.put("tickers", RequestArgs.strings("tickers", tickers));
    json.put(
        "options", SubscriptionBridge.streamOptions(Objects.requireNonNull(options, "options")));
    return open("subscribeEtfInav", json.build());
  }
}
