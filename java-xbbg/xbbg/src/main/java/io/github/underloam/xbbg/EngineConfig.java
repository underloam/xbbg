package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.Json;
import io.github.underloam.xbbg.internal.JsonArrayBuilder;
import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import io.github.underloam.xbbg.internal.JsonValue;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;

/**
 * Immutable engine configuration. Every field is optional: an omitted field keeps the engine's
 * default, and presence matters where the engine distinguishes it (for example {@code host}/{@code
 * port} cannot be combined with {@code zfpRemote}). The engine validates the configuration when
 * connecting, with the same checks and messages as the JavaScript package.
 */
public final class EngineConfig {
  private static final EngineConfig DEFAULTS = builder().build();

  private final String host;
  private final Integer port;
  private final List<ServerAddress> servers;
  private final String zfpRemote;
  private final Integer requestPoolSize;
  private final Integer subscriptionPoolSize;
  private final Integer runtimeWorkerThreads;
  private final Integer maxSubscriptionSessions;
  private final Boolean shardRequests;
  private final Integer shardThreshold;
  private final Integer shardChunkSize;
  private final Integer shardMaxConcurrent;
  private final ValidationMode validationMode;
  private final KeyColumn keyColumn;
  private final Integer subscriptionFlushThreshold;
  private final Integer maxEventQueueSize;
  private final Integer commandQueueSize;
  private final Integer subscriptionStreamCapacity;
  private final OverflowPolicy overflowPolicy;
  private final List<String> warmupServices;
  private final String fieldCachePath;
  private final String bqlAppName;
  private final AuthConfig auth;
  private final TlsConfig tls;
  private final Integer numStartAttempts;
  private final Boolean autoRestartOnDisconnection;
  private final RetryPolicy retryPolicy;
  private final Long requestTimeoutMs;
  private final Long streamsDeactivatedWarnMs;
  private final Boolean keepAliveEnabled;
  private final Integer keepAliveInactivityMs;
  private final Integer keepAliveResponseTimeoutMs;
  private final Double slowConsumerHiWaterMark;
  private final Double slowConsumerLoWaterMark;
  private final SdkLogLevel sdkLogLevel;
  private final Socks5Config socks5;

  private EngineConfig(Builder b) {
    host = b.host;
    port = b.port;
    servers = Collections.unmodifiableList(new ArrayList<ServerAddress>(b.servers));
    zfpRemote = b.zfpRemote;
    requestPoolSize = b.requestPoolSize;
    subscriptionPoolSize = b.subscriptionPoolSize;
    runtimeWorkerThreads = b.runtimeWorkerThreads;
    maxSubscriptionSessions = b.maxSubscriptionSessions;
    shardRequests = b.shardRequests;
    shardThreshold = b.shardThreshold;
    shardChunkSize = b.shardChunkSize;
    shardMaxConcurrent = b.shardMaxConcurrent;
    validationMode = b.validationMode;
    keyColumn = b.keyColumn;
    subscriptionFlushThreshold = b.subscriptionFlushThreshold;
    maxEventQueueSize = b.maxEventQueueSize;
    commandQueueSize = b.commandQueueSize;
    subscriptionStreamCapacity = b.subscriptionStreamCapacity;
    overflowPolicy = b.overflowPolicy;
    warmupServices =
        b.warmupServices == null
            ? null
            : Collections.unmodifiableList(new ArrayList<String>(b.warmupServices));
    fieldCachePath = b.fieldCachePath;
    bqlAppName = b.bqlAppName;
    auth = b.auth;
    tls = b.tls;
    numStartAttempts = b.numStartAttempts;
    autoRestartOnDisconnection = b.autoRestartOnDisconnection;
    retryPolicy = b.retryPolicy;
    requestTimeoutMs = b.requestTimeoutMs;
    streamsDeactivatedWarnMs = b.streamsDeactivatedWarnMs;
    keepAliveEnabled = b.keepAliveEnabled;
    keepAliveInactivityMs = b.keepAliveInactivityMs;
    keepAliveResponseTimeoutMs = b.keepAliveResponseTimeoutMs;
    slowConsumerHiWaterMark = b.slowConsumerHiWaterMark;
    slowConsumerLoWaterMark = b.slowConsumerLoWaterMark;
    sdkLogLevel = b.sdkLogLevel;
    socks5 = b.socks5;
  }

  /**
   * Starts a configuration.
   *
   * @return a builder with every field omitted
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the configuration with every field omitted (engine defaults: {@code localhost:8194}).
   *
   * @return the default configuration
   */
  public static EngineConfig defaults() {
    return DEFAULTS;
  }

  /**
   * Returns a builder initialized with this configuration's fields.
   *
   * @return a new builder
   */
  public Builder toBuilder() {
    Builder b = new Builder();
    b.host = host;
    b.port = port;
    b.servers.addAll(servers);
    b.zfpRemote = zfpRemote;
    b.requestPoolSize = requestPoolSize;
    b.subscriptionPoolSize = subscriptionPoolSize;
    b.runtimeWorkerThreads = runtimeWorkerThreads;
    b.maxSubscriptionSessions = maxSubscriptionSessions;
    b.shardRequests = shardRequests;
    b.shardThreshold = shardThreshold;
    b.shardChunkSize = shardChunkSize;
    b.shardMaxConcurrent = shardMaxConcurrent;
    b.validationMode = validationMode;
    b.keyColumn = keyColumn;
    b.subscriptionFlushThreshold = subscriptionFlushThreshold;
    b.maxEventQueueSize = maxEventQueueSize;
    b.commandQueueSize = commandQueueSize;
    b.subscriptionStreamCapacity = subscriptionStreamCapacity;
    b.overflowPolicy = overflowPolicy;
    b.warmupServices = warmupServices == null ? null : new ArrayList<String>(warmupServices);
    b.fieldCachePath = fieldCachePath;
    b.bqlAppName = bqlAppName;
    b.auth = auth;
    b.tls = tls;
    b.numStartAttempts = numStartAttempts;
    b.autoRestartOnDisconnection = autoRestartOnDisconnection;
    b.retryPolicy = retryPolicy;
    b.requestTimeoutMs = requestTimeoutMs;
    b.streamsDeactivatedWarnMs = streamsDeactivatedWarnMs;
    b.keepAliveEnabled = keepAliveEnabled;
    b.keepAliveInactivityMs = keepAliveInactivityMs;
    b.keepAliveResponseTimeoutMs = keepAliveResponseTimeoutMs;
    b.slowConsumerHiWaterMark = slowConsumerHiWaterMark;
    b.slowConsumerLoWaterMark = slowConsumerLoWaterMark;
    b.sdkLogLevel = sdkLogLevel;
    b.socks5 = socks5;
    return b;
  }

  /**
   * Returns the Terminal or server host, or empty for {@code localhost}.
   *
   * @return the Terminal or server host, or empty for {@code localhost}
   */
  public Optional<String> host() {
    return Optional.ofNullable(host);
  }

  /**
   * Returns the port, or empty for 8194.
   *
   * @return the port, or empty for 8194
   */
  public OptionalInt port() {
    return optional(port);
  }

  /**
   * Returns the ordered failover servers; empty when {@code host}/{@code port} apply.
   *
   * @return the ordered failover servers; empty when {@code host}/{@code port} apply
   */
  public List<ServerAddress> servers() {
    return servers;
  }

  /**
   * Returns the ZFP leased-line port ({@code 8194} or {@code 8196}), or empty.
   *
   * @return the ZFP leased-line port ({@code 8194} or {@code 8196}), or empty
   */
  public Optional<String> zfpRemote() {
    return Optional.ofNullable(zfpRemote);
  }

  /**
   * Returns the number of request worker sessions, or empty for 2.
   *
   * @return the number of request worker sessions, or empty for 2
   */
  public OptionalInt requestPoolSize() {
    return optional(requestPoolSize);
  }

  /**
   * Returns the number of pre-warmed subscription sessions, or empty for 1.
   *
   * @return the number of pre-warmed subscription sessions, or empty for 1
   */
  public OptionalInt subscriptionPoolSize() {
    return optional(subscriptionPoolSize);
  }

  /**
   * Returns the engine's runtime worker threads, or empty for 2.
   *
   * @return the engine's runtime worker threads, or empty for 2
   */
  public OptionalInt runtimeWorkerThreads() {
    return optional(runtimeWorkerThreads);
  }

  /**
   * Returns the maximum live subscription sessions, or empty for 32.
   *
   * @return the maximum live subscription sessions, or empty for 32
   */
  public OptionalInt maxSubscriptionSessions() {
    return optional(maxSubscriptionSessions);
  }

  /**
   * Returns whether eligible multi-security requests are sharded, or empty for false.
   *
   * @return whether eligible multi-security requests are sharded, or empty for false
   */
  public Optional<Boolean> shardRequests() {
    return Optional.ofNullable(shardRequests);
  }

  /**
   * Returns the minimum securities before sharding applies, or empty for 20.
   *
   * @return the minimum securities before sharding applies, or empty for 20
   */
  public OptionalInt shardThreshold() {
    return optional(shardThreshold);
  }

  /**
   * Returns the maximum securities per shard, or empty for 16.
   *
   * @return the maximum securities per shard, or empty for 16
   */
  public OptionalInt shardChunkSize() {
    return optional(shardChunkSize);
  }

  /**
   * Returns the maximum concurrent shard requests, or empty for 4.
   *
   * @return the maximum concurrent shard requests, or empty for 4
   */
  public OptionalInt shardMaxConcurrent() {
    return optional(shardMaxConcurrent);
  }

  /**
   * Returns the field validation mode, or empty for {@code disabled}.
   *
   * @return the field validation mode, or empty for {@code disabled}
   */
  public Optional<ValidationMode> validationMode() {
    return Optional.ofNullable(validationMode);
  }

  /**
   * Returns the result key column, or empty for the deprecated {@code ticker}.
   *
   * @return the result key column, or empty for the deprecated {@code ticker}
   */
  public Optional<KeyColumn> keyColumn() {
    return Optional.ofNullable(keyColumn);
  }

  /**
   * Returns the default subscription batch size, or empty for 16.
   *
   * @return the default subscription batch size, or empty for 16
   */
  public OptionalInt subscriptionFlushThreshold() {
    return optional(subscriptionFlushThreshold);
  }

  /**
   * Returns the SDK event queue size, or empty for 10000.
   *
   * @return the SDK event queue size, or empty for 10000
   */
  public OptionalInt maxEventQueueSize() {
    return optional(maxEventQueueSize);
  }

  /**
   * Returns the command queue size, or empty for 256.
   *
   * @return the command queue size, or empty for 256
   */
  public OptionalInt commandQueueSize() {
    return optional(commandQueueSize);
  }

  /**
   * Returns the default per-subscription queue capacity, or empty for 256.
   *
   * @return the default per-subscription queue capacity, or empty for 256
   */
  public OptionalInt subscriptionStreamCapacity() {
    return optional(subscriptionStreamCapacity);
  }

  /**
   * Returns the default overflow policy, or empty for {@code drop_newest}.
   *
   * @return the default overflow policy, or empty for {@code drop_newest}
   */
  public Optional<OverflowPolicy> overflowPolicy() {
    return Optional.ofNullable(overflowPolicy);
  }

  /**
   * Returns the services opened at startup, or empty for {@code //blp/refdata} and {@code
   * //blp/apiflds} (an empty list opens none).
   *
   * @return the services opened at startup, or empty for {@code //blp/refdata} and {@code
   *     //blp/apiflds} (an empty list opens none)
   */
  public Optional<List<String>> warmupServices() {
    return Optional.ofNullable(warmupServices);
  }

  /**
   * Returns the field cache file, or empty for {@code ~/.xbbg/field_cache.json}.
   *
   * @return the field cache file, or empty for {@code ~/.xbbg/field_cache.json}
   */
  public Optional<String> fieldCachePath() {
    return Optional.ofNullable(fieldCachePath);
  }

  /**
   * Returns the default BQL {@code clientContext.appName}, or empty.
   *
   * @return the default BQL {@code clientContext.appName}, or empty
   */
  public Optional<String> bqlAppName() {
    return Optional.ofNullable(bqlAppName);
  }

  /**
   * Returns the authorization, or empty.
   *
   * @return the authorization, or empty
   */
  public Optional<AuthConfig> auth() {
    return Optional.ofNullable(auth);
  }

  /**
   * Returns the TLS settings, or empty.
   *
   * @return the TLS settings, or empty
   */
  public Optional<TlsConfig> tls() {
    return Optional.ofNullable(tls);
  }

  /**
   * Returns the session start attempts, or empty for 3.
   *
   * @return the session start attempts, or empty for 3
   */
  public OptionalInt numStartAttempts() {
    return optional(numStartAttempts);
  }

  /**
   * Returns whether sessions restart after a disconnection, or empty for true.
   *
   * @return whether sessions restart after a disconnection, or empty for true
   */
  public Optional<Boolean> autoRestartOnDisconnection() {
    return Optional.ofNullable(autoRestartOnDisconnection);
  }

  /**
   * Returns the retry policy, or empty for no retries.
   *
   * @return the retry policy, or empty for no retries
   */
  public Optional<RetryPolicy> retryPolicy() {
    return Optional.ofNullable(retryPolicy);
  }

  /**
   * Returns the hard per-request timeout in ms, or empty for 0 (disabled).
   *
   * @return the hard per-request timeout in ms, or empty for 0 (disabled)
   */
  public OptionalLong requestTimeoutMs() {
    return optional(requestTimeoutMs);
  }

  /**
   * Returns the deactivated-stream warning threshold in ms, or empty for 30000.
   *
   * @return the deactivated-stream warning threshold in ms, or empty for 30000
   */
  public OptionalLong streamsDeactivatedWarnMs() {
    return optional(streamsDeactivatedWarnMs);
  }

  /**
   * Returns whether keep-alive pings are enabled, or empty for the SDK default (true).
   *
   * @return whether keep-alive pings are enabled, or empty for the SDK default (true)
   */
  public Optional<Boolean> keepAliveEnabled() {
    return Optional.ofNullable(keepAliveEnabled);
  }

  /**
   * Returns the keep-alive inactivity time in ms, or empty for the SDK default (20000).
   *
   * @return the keep-alive inactivity time in ms, or empty for the SDK default (20000)
   */
  public OptionalInt keepAliveInactivityMs() {
    return optional(keepAliveInactivityMs);
  }

  /**
   * Returns the keep-alive response timeout in ms, or empty for the SDK default (10000).
   *
   * @return the keep-alive response timeout in ms, or empty for the SDK default (10000)
   */
  public OptionalInt keepAliveResponseTimeoutMs() {
    return optional(keepAliveResponseTimeoutMs);
  }

  /**
   * Returns the slow-consumer high water mark, or empty for the SDK default (0.75).
   *
   * @return the slow-consumer high water mark, or empty for the SDK default (0.75)
   */
  public OptionalDouble slowConsumerHiWaterMark() {
    return optional(slowConsumerHiWaterMark);
  }

  /**
   * Returns the slow-consumer low water mark, or empty for the SDK default (0.5).
   *
   * @return the slow-consumer low water mark, or empty for the SDK default (0.5)
   */
  public OptionalDouble slowConsumerLoWaterMark() {
    return optional(slowConsumerLoWaterMark);
  }

  /**
   * Returns the Bloomberg SDK log level, or empty for {@code off}.
   *
   * @return the Bloomberg SDK log level, or empty for {@code off}
   */
  public Optional<SdkLogLevel> sdkLogLevel() {
    return Optional.ofNullable(sdkLogLevel);
  }

  /**
   * Returns the SOCKS5 proxy, or empty.
   *
   * @return the SOCKS5 proxy, or empty
   */
  public Optional<Socks5Config> socks5() {
    return Optional.ofNullable(socks5);
  }

  /**
   * Serializes the configuration as the engine's {@code EngineConfig} JSON, omitting absent fields.
   */
  JsonValue toJson() {
    JsonObjectBuilder json = Json.object();
    json.put("host", host);
    json.put("port", port);
    if (!servers.isEmpty()) {
      JsonArrayBuilder array = Json.array();
      for (ServerAddress server : servers) {
        array.add(server.toJson());
      }
      json.put("servers", array.build());
    }
    json.put("zfpRemote", zfpRemote);
    json.put("requestPoolSize", requestPoolSize);
    json.put("subscriptionPoolSize", subscriptionPoolSize);
    json.put("runtimeWorkerThreads", runtimeWorkerThreads);
    json.put("maxSubscriptionSessions", maxSubscriptionSessions);
    json.put("shardRequests", shardRequests);
    json.put("shardThreshold", shardThreshold);
    json.put("shardChunkSize", shardChunkSize);
    json.put("shardMaxConcurrent", shardMaxConcurrent);
    json.put("validationMode", validationMode == null ? null : validationMode.wireValue());
    json.put("keyColumn", keyColumn == null ? null : keyColumn.wireValue());
    json.put("subscriptionFlushThreshold", subscriptionFlushThreshold);
    json.put("maxEventQueueSize", maxEventQueueSize);
    json.put("commandQueueSize", commandQueueSize);
    json.put("subscriptionStreamCapacity", subscriptionStreamCapacity);
    json.put("overflowPolicy", overflowPolicy == null ? null : overflowPolicy.wireValue());
    json.put("warmupServices", warmupServices);
    json.put("fieldCachePath", fieldCachePath);
    json.put("bqlAppName", bqlAppName);
    json.put("auth", auth == null ? null : auth.toJson());
    json.put("tls", tls == null ? null : tls.toJson());
    json.put("numStartAttempts", numStartAttempts);
    json.put("autoRestartOnDisconnection", autoRestartOnDisconnection);
    json.put("retryPolicy", retryPolicy == null ? null : retryPolicy.toJson());
    json.put("requestTimeoutMs", requestTimeoutMs);
    json.put("streamsDeactivatedWarnMs", streamsDeactivatedWarnMs);
    json.put("keepAliveEnabled", keepAliveEnabled);
    json.put("keepAliveInactivityMs", keepAliveInactivityMs);
    json.put("keepAliveResponseTimeoutMs", keepAliveResponseTimeoutMs);
    json.put("slowConsumerHiWaterMark", slowConsumerHiWaterMark);
    json.put("slowConsumerLoWaterMark", slowConsumerLoWaterMark);
    json.put("sdkLogLevel", sdkLogLevel == null ? null : sdkLogLevel.wireValue());
    json.put("socks5", socks5 == null ? null : socks5.toJson());
    return json.build();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof EngineConfig
        && toJson().equals(((EngineConfig) other).toJson())
        && Objects.equals(tls, ((EngineConfig) other).tls);
  }

  @Override
  public int hashCode() {
    return toJson().hashCode();
  }

  /** Shows every present field; TLS secrets and auth tokens are redacted. */
  @Override
  public String toString() {
    if (tls == null && (auth == null || !auth.token().isPresent())) {
      return "EngineConfig" + toJson();
    }
    EngineConfig redacted = toBuilder().tls(null).auth(null).build();
    String text = redacted.toJson().text();
    StringBuilder out = new StringBuilder("EngineConfig").append(text, 0, text.length() - 1);
    if (auth != null) {
      out.append(text.length() > 2 ? "," : "").append("\"auth\":").append(auth);
    }
    if (tls != null) {
      out.append(text.length() > 2 || auth != null ? "," : "").append("\"tls\":").append(tls);
    }
    return out.append('}').toString();
  }

  static int checkPort(String name, int value) {
    if (value < 0 || value > 65535) {
      throw new IllegalArgumentException(name + " must be between 0 and 65535, got " + value);
    }
    return value;
  }

  static int checkUnsigned(String name, int value) {
    if (value < 0) {
      throw new IllegalArgumentException(name + " must not be negative, got " + value);
    }
    return value;
  }

  static double checkFinite(String name, double value) {
    if (Double.isNaN(value) || Double.isInfinite(value)) {
      throw new IllegalArgumentException(name + " must be a finite number, got " + value);
    }
    return value;
  }

  private static OptionalInt optional(Integer value) {
    return value == null ? OptionalInt.empty() : OptionalInt.of(value);
  }

  private static OptionalLong optional(Long value) {
    return value == null ? OptionalLong.empty() : OptionalLong.of(value);
  }

  private static OptionalDouble optional(Double value) {
    return value == null ? OptionalDouble.empty() : OptionalDouble.of(value);
  }

  /** Builds an {@link EngineConfig}. Setting a field to {@code null} omits it again. */
  public static final class Builder {
    private String host;
    private Integer port;
    private final List<ServerAddress> servers = new ArrayList<ServerAddress>();
    private String zfpRemote;
    private Integer requestPoolSize;
    private Integer subscriptionPoolSize;
    private Integer runtimeWorkerThreads;
    private Integer maxSubscriptionSessions;
    private Boolean shardRequests;
    private Integer shardThreshold;
    private Integer shardChunkSize;
    private Integer shardMaxConcurrent;
    private ValidationMode validationMode;
    private KeyColumn keyColumn;
    private Integer subscriptionFlushThreshold;
    private Integer maxEventQueueSize;
    private Integer commandQueueSize;
    private Integer subscriptionStreamCapacity;
    private OverflowPolicy overflowPolicy;
    private List<String> warmupServices;
    private String fieldCachePath;
    private String bqlAppName;
    private AuthConfig auth;
    private TlsConfig tls;
    private Integer numStartAttempts;
    private Boolean autoRestartOnDisconnection;
    private RetryPolicy retryPolicy;
    private Long requestTimeoutMs;
    private Long streamsDeactivatedWarnMs;
    private Boolean keepAliveEnabled;
    private Integer keepAliveInactivityMs;
    private Integer keepAliveResponseTimeoutMs;
    private Double slowConsumerHiWaterMark;
    private Double slowConsumerLoWaterMark;
    private SdkLogLevel sdkLogLevel;
    private Socks5Config socks5;

    private Builder() {}

    /**
     * Sets the Terminal or server host (default {@code localhost}).
     *
     * @param value the host
     * @return this builder
     */
    public Builder host(String value) {
      host = value;
      return this;
    }

    /**
     * Sets the port (default 8194).
     *
     * @param value the port, 0 to 65535
     * @return this builder
     */
    public Builder port(int value) {
      port = checkPort("port", value);
      return this;
    }

    /**
     * Sets ordered failover servers; when present they replace {@code host}/{@code port}.
     *
     * @param values the servers
     * @return this builder
     */
    public Builder servers(Collection<ServerAddress> values) {
      servers.clear();
      if (values != null) {
        for (ServerAddress server : values) {
          servers.add(Objects.requireNonNull(server, "servers must not contain null"));
        }
      }
      return this;
    }

    /**
     * Sets ordered failover servers; when present they replace {@code host}/{@code port}.
     *
     * @param values the servers
     * @return this builder
     */
    public Builder servers(ServerAddress... values) {
      return servers(Arrays.asList(values));
    }

    /**
     * Uses a ZFP leased line ({@code "8194"} or {@code "8196"}); cannot be combined with host,
     * port, servers or SOCKS5, and needs TLS.
     *
     * @param value the remote port
     * @return this builder
     */
    public Builder zfpRemote(String value) {
      zfpRemote = value;
      return this;
    }

    /**
     * Sets the number of request worker sessions (default 2; must be positive).
     *
     * @param value the count
     * @return this builder
     */
    public Builder requestPoolSize(int value) {
      requestPoolSize = checkUnsigned("requestPoolSize", value);
      return this;
    }

    /**
     * Sets the number of pre-warmed subscription sessions (default 1).
     *
     * @param value the count
     * @return this builder
     */
    public Builder subscriptionPoolSize(int value) {
      subscriptionPoolSize = checkUnsigned("subscriptionPoolSize", value);
      return this;
    }

    /**
     * Sets the engine's runtime worker threads (default 2; must be positive).
     *
     * @param value the count
     * @return this builder
     */
    public Builder runtimeWorkerThreads(int value) {
      runtimeWorkerThreads = checkUnsigned("runtimeWorkerThreads", value);
      return this;
    }

    /**
     * Sets the maximum live subscription sessions (default 32; at least the pool size).
     *
     * @param value the count
     * @return this builder
     */
    public Builder maxSubscriptionSessions(int value) {
      maxSubscriptionSessions = checkUnsigned("maxSubscriptionSessions", value);
      return this;
    }

    /**
     * Enables sharding of eligible multi-security {@code bdp}/{@code bdh} requests (default false).
     *
     * @param value the flag
     * @return this builder
     */
    public Builder shardRequests(boolean value) {
      shardRequests = value;
      return this;
    }

    /**
     * Sets the minimum securities before sharding applies (default 20; at least 2).
     *
     * @param value the count
     * @return this builder
     */
    public Builder shardThreshold(int value) {
      shardThreshold = checkUnsigned("shardThreshold", value);
      return this;
    }

    /**
     * Sets the maximum securities per shard (default 16).
     *
     * @param value the count
     * @return this builder
     */
    public Builder shardChunkSize(int value) {
      shardChunkSize = checkUnsigned("shardChunkSize", value);
      return this;
    }

    /**
     * Sets the maximum concurrent shard requests (default 4).
     *
     * @param value the count
     * @return this builder
     */
    public Builder shardMaxConcurrent(int value) {
      shardMaxConcurrent = checkUnsigned("shardMaxConcurrent", value);
      return this;
    }

    /**
     * Sets field validation before requests are sent (default {@code disabled}).
     *
     * @param value the mode
     * @return this builder
     */
    public Builder validationMode(ValidationMode value) {
      validationMode = value;
      return this;
    }

    /**
     * Sets the name of the result column holding each row's security (default {@code ticker},
     * deprecated; {@code security} becomes the default in the next major release).
     *
     * @param value the key column
     * @return this builder
     */
    public Builder keyColumn(KeyColumn value) {
      keyColumn = value;
      return this;
    }

    /**
     * Sets the default number of queued updates combined into one subscription read (default 16;
     * must be positive).
     *
     * @param value the count
     * @return this builder
     */
    public Builder subscriptionFlushThreshold(int value) {
      subscriptionFlushThreshold = checkUnsigned("subscriptionFlushThreshold", value);
      return this;
    }

    /**
     * Sets the SDK event queue size (default 10000).
     *
     * @param value the size
     * @return this builder
     */
    public Builder maxEventQueueSize(int value) {
      maxEventQueueSize = checkUnsigned("maxEventQueueSize", value);
      return this;
    }

    /**
     * Sets the command queue size (default 256; must be positive).
     *
     * @param value the size
     * @return this builder
     */
    public Builder commandQueueSize(int value) {
      commandQueueSize = checkUnsigned("commandQueueSize", value);
      return this;
    }

    /**
     * Sets the default per-subscription queue capacity (default 256; must be positive).
     *
     * @param value the capacity
     * @return this builder
     */
    public Builder subscriptionStreamCapacity(int value) {
      subscriptionStreamCapacity = checkUnsigned("subscriptionStreamCapacity", value);
      return this;
    }

    /**
     * Sets what a subscription does when its consumer stalls (default {@code drop_newest}).
     *
     * @param value the policy
     * @return this builder
     */
    public Builder overflowPolicy(OverflowPolicy value) {
      overflowPolicy = value;
      return this;
    }

    /**
     * Sets the services opened at startup (default {@code //blp/refdata} and {@code
     * //blp/apiflds}); an empty collection opens none.
     *
     * @param values the service URIs, or {@code null} for the default
     * @return this builder
     */
    public Builder warmupServices(Collection<String> values) {
      warmupServices = values == null ? null : new ArrayList<String>(values);
      return this;
    }

    /**
     * Sets the field cache file (default {@code ~/.xbbg/field_cache.json}; the first engine in the
     * process wins).
     *
     * @param value the file
     * @return this builder
     */
    public Builder fieldCachePath(Path value) {
      fieldCachePath = value == null ? null : value.toString();
      return this;
    }

    /**
     * Sets the field cache file.
     *
     * @param value the file name
     * @return this builder
     */
    public Builder fieldCachePath(String value) {
      fieldCachePath = value;
      return this;
    }

    /**
     * Sets the default BQL {@code clientContext.appName} (non-blank; independent of {@code auth}).
     *
     * @param value the application name
     * @return this builder
     */
    public Builder bqlAppName(String value) {
      bqlAppName = value;
      return this;
    }

    /**
     * Sets the authorization.
     *
     * @param value the authorization
     * @return this builder
     */
    public Builder auth(AuthConfig value) {
      auth = value;
      return this;
    }

    /**
     * Sets the TLS settings.
     *
     * @param value the settings
     * @return this builder
     */
    public Builder tls(TlsConfig value) {
      tls = value;
      return this;
    }

    /**
     * Sets the session start attempts (default 3).
     *
     * @param value the count
     * @return this builder
     */
    public Builder numStartAttempts(int value) {
      numStartAttempts = checkUnsigned("numStartAttempts", value);
      return this;
    }

    /**
     * Sets whether sessions restart after a disconnection (default true).
     *
     * @param value the flag
     * @return this builder
     */
    public Builder autoRestartOnDisconnection(boolean value) {
      autoRestartOnDisconnection = value;
      return this;
    }

    /**
     * Sets the retry policy for transient request failures.
     *
     * @param value the policy
     * @return this builder
     */
    public Builder retryPolicy(RetryPolicy value) {
      retryPolicy = value;
      return this;
    }

    /**
     * Sets the hard per-request timeout (default 0, disabled); the engine rejects negative values.
     *
     * @param value milliseconds
     * @return this builder
     */
    public Builder requestTimeoutMs(long value) {
      requestTimeoutMs = value;
      return this;
    }

    /**
     * Sets when to warn about streams that stay deactivated (default 30000; 0 disables); the engine
     * rejects negative values.
     *
     * @param value milliseconds
     * @return this builder
     */
    public Builder streamsDeactivatedWarnMs(long value) {
      streamsDeactivatedWarnMs = value;
      return this;
    }

    /**
     * Enables or disables Bloomberg keep-alive pings (SDK default: enabled).
     *
     * @param value the flag
     * @return this builder
     */
    public Builder keepAliveEnabled(boolean value) {
      keepAliveEnabled = value;
      return this;
    }

    /**
     * Sets the inactivity time before a keep-alive ping (SDK default 20000); the engine rejects
     * negative values.
     *
     * @param value milliseconds
     * @return this builder
     */
    public Builder keepAliveInactivityMs(int value) {
      keepAliveInactivityMs = value;
      return this;
    }

    /**
     * Sets how long to wait for a keep-alive response (SDK default 10000); the engine rejects
     * negative values.
     *
     * @param value milliseconds
     * @return this builder
     */
    public Builder keepAliveResponseTimeoutMs(int value) {
      keepAliveResponseTimeoutMs = value;
      return this;
    }

    /**
     * Sets the slow-consumer high water mark as a fraction of {@code maxEventQueueSize} (SDK
     * default 0.75; 0.0 to 1.0).
     *
     * @param value the fraction
     * @return this builder
     */
    public Builder slowConsumerHiWaterMark(double value) {
      slowConsumerHiWaterMark = checkFinite("slowConsumerHiWaterMark", value);
      return this;
    }

    /**
     * Sets the slow-consumer low water mark as a fraction of {@code maxEventQueueSize} (SDK default
     * 0.5; at least 0.0 and below 1.0).
     *
     * @param value the fraction
     * @return this builder
     */
    public Builder slowConsumerLoWaterMark(double value) {
      slowConsumerLoWaterMark = checkFinite("slowConsumerLoWaterMark", value);
      return this;
    }

    /**
     * Sets the verbosity of Bloomberg's C SDK logging (default {@code off}); it applies from the
     * first engine that starts with a level other than {@code off}.
     *
     * @param value the level
     * @return this builder
     */
    public Builder sdkLogLevel(SdkLogLevel value) {
      sdkLogLevel = value;
      return this;
    }

    /**
     * Routes every server connection through a SOCKS5 proxy (not with ZFP).
     *
     * @param value the proxy
     * @return this builder
     */
    public Builder socks5(Socks5Config value) {
      socks5 = value;
      return this;
    }

    /**
     * Builds the configuration.
     *
     * @return the configuration
     */
    public EngineConfig build() {
      return new EngineConfig(this);
    }
  }
}
