package io.github.underloam.xbbg;

import io.github.underloam.xbbg.subscription.AuctionStreamOptions;
import io.github.underloam.xbbg.subscription.StreamOptions;
import io.github.underloam.xbbg.subscription.Subscription;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Subscriptions on the configured engine ({@code Blp.subscribe}). Each method obtains the
 * configured engine first and then behaves like the {@link Engine} method of the same name.
 */
abstract class BlpSubscriptions extends BlpRecipes {
  BlpSubscriptions() {}

  /**
   * Subscribes to one {@code //blp/mktdata} topic and field.
   *
   * @param ticker the topic
   * @param field the field
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> subscribe(String ticker, String field) {
    return subscribe(ticker, field, StreamOptions.defaults());
  }

  /**
   * Subscribes to one {@code //blp/mktdata} topic and field.
   *
   * @param ticker the topic
   * @param field the field
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> subscribe(
      String ticker, String field, StreamOptions options) {
    return subscribe(
        RequestArgs.single("ticker", ticker), RequestArgs.single("field", field), options);
  }

  /**
   * Subscribes to one {@code //blp/mktdata} topic.
   *
   * @param ticker the topic
   * @param fields the fields
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> subscribe(
      String ticker, Collection<String> fields) {
    return subscribe(ticker, fields, StreamOptions.defaults());
  }

  /**
   * Subscribes to one {@code //blp/mktdata} topic.
   *
   * @param ticker the topic
   * @param fields the fields
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> subscribe(
      String ticker, Collection<String> fields, StreamOptions options) {
    return subscribe(RequestArgs.single("ticker", ticker), fields, options);
  }

  /**
   * Subscribes to {@code //blp/mktdata} topics for one field.
   *
   * @param tickers the topics
   * @param field the field
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> subscribe(
      Collection<String> tickers, String field) {
    return subscribe(tickers, field, StreamOptions.defaults());
  }

  /**
   * Subscribes to {@code //blp/mktdata} topics for one field.
   *
   * @param tickers the topics
   * @param field the field
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> subscribe(
      Collection<String> tickers, String field, StreamOptions options) {
    return subscribe(tickers, RequestArgs.single("field", field), options);
  }

  /**
   * Subscribes to {@code //blp/mktdata} topics.
   *
   * @param tickers the topics
   * @param fields the fields
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> subscribe(
      Collection<String> tickers, Collection<String> fields) {
    return subscribe(tickers, fields, StreamOptions.defaults());
  }

  /**
   * Subscribes to {@code //blp/mktdata} topics.
   *
   * @param tickers the topics
   * @param fields the fields
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> subscribe(
      Collection<String> tickers, Collection<String> fields, StreamOptions options) {
    final List<String> topics = RequestArgs.strings("tickers", tickers);
    final List<String> names = RequestArgs.strings("fields", fields);
    final StreamOptions opts = Objects.requireNonNull(options, "options");
    return withEngine(engine -> engine.subscribe(topics, names, opts));
  }

  /**
   * Streams {@code //blp/mktdata} topics.
   *
   * @param tickers the topics
   * @param fields the fields
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> stream(
      Collection<String> tickers, Collection<String> fields) {
    return stream(tickers, fields, StreamOptions.defaults());
  }

  /**
   * Streams {@code //blp/mktdata} topics.
   *
   * @param tickers the topics
   * @param fields the fields
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> stream(
      Collection<String> tickers, Collection<String> fields, StreamOptions options) {
    final List<String> topics = RequestArgs.strings("tickers", tickers);
    final List<String> names = RequestArgs.strings("fields", fields);
    final StreamOptions opts = Objects.requireNonNull(options, "options");
    return withEngine(engine -> engine.stream(topics, names, opts));
  }

  /**
   * Streams volume-weighted average prices ({@code //blp/mktvwap}).
   *
   * @param tickers the topics
   * @param fields the VWAP fields
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> vwap(
      Collection<String> tickers, Collection<String> fields) {
    return vwap(tickers, fields, StreamOptions.defaults());
  }

  /**
   * Streams volume-weighted average prices ({@code //blp/mktvwap}).
   *
   * @param tickers the topics
   * @param fields the VWAP fields
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> vwap(
      Collection<String> tickers, Collection<String> fields, StreamOptions options) {
    final List<String> topics = RequestArgs.strings("tickers", tickers);
    final List<String> names = RequestArgs.strings("fields", fields);
    final StreamOptions opts = Objects.requireNonNull(options, "options");
    return withEngine(engine -> engine.vwap(topics, names, opts));
  }

  /**
   * Streams intraday bars ({@code //blp/mktbar}).
   *
   * @param ticker the topic
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> mktbar(String ticker) {
    return mktbar(ticker, StreamOptions.defaults());
  }

  /**
   * Streams intraday bars ({@code //blp/mktbar}); fields come from {@code options.fields}.
   *
   * @param ticker the topic
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> mktbar(String ticker, StreamOptions options) {
    Objects.requireNonNull(ticker, "ticker");
    Objects.requireNonNull(options, "options");
    return withEngine(engine -> engine.mktbar(ticker, options));
  }

  /**
   * Streams market depth ({@code //blp/mktdepthdata}, B-PIPE).
   *
   * @param ticker the topic
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> depth(String ticker) {
    return depth(ticker, StreamOptions.defaults());
  }

  /**
   * Streams market depth ({@code //blp/mktdepthdata}, B-PIPE); fields come from {@code
   * options.fields}.
   *
   * @param ticker the topic
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> depth(String ticker, StreamOptions options) {
    Objects.requireNonNull(ticker, "ticker");
    Objects.requireNonNull(options, "options");
    return withEngine(engine -> engine.depth(ticker, options));
  }

  /**
   * Streams option and futures chains ({@code //blp/mktlist}, B-PIPE).
   *
   * @param ticker the topic
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> chains(String ticker) {
    return chains(ticker, StreamOptions.defaults());
  }

  /**
   * Streams option and futures chains ({@code //blp/mktlist}, B-PIPE); fields come from {@code
   * options.fields}.
   *
   * @param ticker the topic
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> chains(String ticker, StreamOptions options) {
    Objects.requireNonNull(ticker, "ticker");
    Objects.requireNonNull(options, "options");
    return withEngine(engine -> engine.chains(ticker, options));
  }

  /**
   * Subscribes to any service with Bloomberg options passed through unchanged.
   *
   * @param service the service
   * @param tickers the topics
   * @param fields the fields
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> subscribeWithOptions(
      String service,
      Collection<String> tickers,
      Collection<String> fields,
      StreamOptions options) {
    Objects.requireNonNull(service, "service");
    final List<String> topics = RequestArgs.strings("tickers", tickers);
    final List<String> names = RequestArgs.strings("fields", fields);
    final StreamOptions opts = Objects.requireNonNull(options, "options");
    return withEngine(engine -> engine.subscribeWithOptions(service, topics, names, opts));
  }

  /**
   * Resolves the input to its primary auction venue, then subscribes to it.
   *
   * @param security the input security
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> subscribeAuction(String security) {
    return subscribeAuction(security, AuctionStreamOptions.defaults());
  }

  /**
   * Resolves the input to its primary auction venue, then subscribes to it.
   *
   * @param security the input security
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> subscribeAuction(
      String security, AuctionStreamOptions options) {
    return subscribeAuction(RequestArgs.single("securities", security), options);
  }

  /**
   * Resolves every input to its primary auction venue first, then subscribes to the venues.
   *
   * @param securities the input securities
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> subscribeAuction(Collection<String> securities) {
    return subscribeAuction(securities, AuctionStreamOptions.defaults());
  }

  /**
   * Resolves every input to its primary auction venue first, then subscribes to the venues.
   *
   * @param securities the input securities
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> subscribeAuction(
      Collection<String> securities, AuctionStreamOptions options) {
    final List<String> inputs = RequestArgs.strings("securities", securities);
    final AuctionStreamOptions opts = Objects.requireNonNull(options, "options");
    return withEngine(engine -> engine.subscribeAuction(inputs, opts));
  }

  /**
   * Row-delivering {@code subscribeAuction(String)}.
   *
   * @param security the input security
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> streamAuction(String security) {
    return streamAuction(security, AuctionStreamOptions.defaults());
  }

  /**
   * Row-delivering {@code subscribeAuction(String, AuctionStreamOptions)}; {@code rows} must stay
   * unset.
   *
   * @param security the input security
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> streamAuction(
      String security, AuctionStreamOptions options) {
    return streamAuction(RequestArgs.single("securities", security), options);
  }

  /**
   * Row-delivering {@code subscribeAuction(Collection)}.
   *
   * @param securities the input securities
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> streamAuction(Collection<String> securities) {
    return streamAuction(securities, AuctionStreamOptions.defaults());
  }

  /**
   * Row-delivering {@code subscribeAuction(Collection, AuctionStreamOptions)}; {@code rows} must
   * stay unset.
   *
   * @param securities the input securities
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> streamAuction(
      Collection<String> securities, AuctionStreamOptions options) {
    final List<String> inputs = RequestArgs.strings("securities", securities);
    final AuctionStreamOptions opts = Objects.requireNonNull(options, "options");
    return withEngine(engine -> engine.streamAuction(inputs, opts));
  }

  /**
   * Resolves the ETF's validated iNAV ticker first, then streams it.
   *
   * @param ticker the ETF
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> subscribeEtfInav(String ticker) {
    return subscribeEtfInav(ticker, StreamOptions.defaults());
  }

  /**
   * Resolves the ETF's validated iNAV ticker first, then streams it.
   *
   * @param ticker the ETF
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> subscribeEtfInav(
      String ticker, StreamOptions options) {
    return subscribeEtfInav(RequestArgs.single("tickers", ticker), options);
  }

  /**
   * Resolves every ETF's validated iNAV ticker first, then streams them.
   *
   * @param tickers the ETFs
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> subscribeEtfInav(Collection<String> tickers) {
    return subscribeEtfInav(tickers, StreamOptions.defaults());
  }

  /**
   * Resolves every ETF's validated iNAV ticker first, then streams them.
   *
   * @param tickers the ETFs
   * @param options the options
   * @return the subscription
   * @throws NullPointerException if an argument is {@code null}
   */
  public static CompletableFuture<Subscription> subscribeEtfInav(
      Collection<String> tickers, StreamOptions options) {
    final List<String> etfs = RequestArgs.strings("tickers", tickers);
    final StreamOptions opts = Objects.requireNonNull(options, "options");
    return withEngine(engine -> engine.subscribeEtfInav(etfs, opts));
  }
}
