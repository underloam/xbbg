package io.github.underloam.xbbg.sdk;

import io.github.underloam.xbbg.internal.Text;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for {@link BlpapiSdk#provision(ProvisionOptions)}.
 *
 * <p>Build with {@link #builder()}; only {@link Builder#source(BlpapiSource)} is required. TLS
 * trusts the operating system's certificate store (plus {@link Builder#extraCaCertificates}),
 * credentials come only from {@link Builder#basicAuth}, {@link Builder#bearerToken}, {@link
 * Builder#netrc} and credentials embedded in the source URL, and installs go to the shared xbbg
 * cache directory unless {@link Builder#cacheDir} is set.
 *
 * <p>Proxy precedence, first match wins:
 *
 * <ol>
 *   <li>{@link Builder#directConnection()} or {@code proxy(Proxy.NO_PROXY)}: direct connections;
 *   <li>{@link Builder#proxy(String)} or an HTTP/SOCKS {@link Builder#proxy(Proxy)}: that proxy for
 *       every request (hosts in {@link Builder#noProxy} bypass it);
 *   <li>the {@link ProxySelector} ({@link Builder#proxySelector}, default {@link
 *       ProxySelector#getDefault()}, so {@code https.proxyHost}, {@code socksProxyHost} and {@code
 *       java.net.useSystemProxies} apply) when it returns a proxy for the source URL: that proxy;
 *   <li>otherwise (the selector answers {@code DIRECT}, the JVM default): the native client's
 *       system detection, i.e. the {@code HTTPS_PROXY}/{@code HTTP_PROXY}/{@code ALL_PROXY}/ {@code
 *       NO_PROXY} environment variables and the operating system's proxy settings.
 * </ol>
 */
public final class ProvisionOptions {
  /** Platforms the provisioner can install for. */
  static final List<String> PLATFORMS =
      Collections.unmodifiableList(
          Arrays.asList("windows-x86_64", "linux-x86_64", "macos-aarch64"));

  /** Default TCP connect timeout. */
  public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);

  /** Default maximum time between two reads of a response body. */
  public static final Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(60);

  /** Default number of retries for transient network failures. */
  public static final int DEFAULT_RETRIES = 3;

  private static final String DEFAULT_CONDA_CHANNEL_ALIAS = "https://conda.anaconda.org/";

  private final BlpapiSource source;
  private final String version;
  private final String platform;
  private final Path cacheDir;
  private final boolean offline;
  private final String proxyUrl;
  private final boolean direct;
  private final ProxySelector proxySelector;
  private final List<String> noProxy;
  private final List<HostCredential> credentials;
  private final Path netrc;
  private final Path extraCaCertificates;
  private final Duration connectTimeout;
  private final Duration readTimeout;
  private final int retries;
  private final boolean requireArtifactHash;

  private ProvisionOptions(Builder b) {
    this.source = b.source;
    this.version = b.version;
    this.platform = b.platform;
    this.cacheDir = b.cacheDir;
    this.offline = b.offline;
    this.proxyUrl = b.proxyUrl;
    this.direct = b.direct;
    this.proxySelector = b.proxySelector;
    this.noProxy = Collections.unmodifiableList(new ArrayList<>(b.noProxy));
    this.credentials = Collections.unmodifiableList(new ArrayList<>(b.credentials));
    this.netrc = b.netrc;
    this.extraCaCertificates = b.extraCaCertificates;
    this.connectTimeout = b.connectTimeout;
    this.readTimeout = b.readTimeout;
    this.retries = b.retries;
    this.requireArtifactHash = b.requireArtifactHash;
  }

  /**
   * A new builder with default settings.
   *
   * @return the builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Where the runtime comes from.
   *
   * @return the source
   */
  public BlpapiSource source() {
    return source;
  }

  /**
   * The exact version to install; empty means the newest stable release.
   *
   * @return the pinned version
   */
  public Optional<String> version() {
    return Optional.ofNullable(version);
  }

  /**
   * Target platform ({@code windows-x86_64}, {@code linux-x86_64} or {@code macos-aarch64}); empty
   * means the platform of this JVM.
   *
   * @return the platform
   */
  public Optional<String> platform() {
    return Optional.ofNullable(platform);
  }

  /**
   * Cache root; empty means the shared xbbg cache directory (system property {@code
   * xbbg.cache.dir}, environment variable {@code XBBG_CACHE_DIR}, or the platform cache directory).
   *
   * @return the cache root
   */
  public Optional<Path> cacheDir() {
    return Optional.ofNullable(cacheDir);
  }

  /**
   * Whether network access is forbidden; offline provisioning only returns cached installs.
   *
   * @return {@code true} for offline mode
   */
  public boolean offline() {
    return offline;
  }

  /**
   * The explicit proxy URL, if one was set with {@link Builder#proxy(String)} or {@link
   * Builder#proxy(Proxy)}.
   *
   * @return the proxy URL
   */
  public Optional<String> proxy() {
    return Optional.ofNullable(proxyUrl);
  }

  /**
   * Whether connections are forced to be direct ({@link Builder#directConnection()} or {@code
   * proxy(Proxy.NO_PROXY)}).
   *
   * @return {@code true} when no proxy may be used
   */
  public boolean directConnection() {
    return direct;
  }

  /**
   * Hosts that bypass the proxy ({@code NO_PROXY} syntax: host names, {@code .domain} suffixes, IP
   * addresses or CIDR ranges).
   *
   * @return the bypass list
   */
  public List<String> noProxy() {
    return noProxy;
  }

  /**
   * Explicit netrc file used for credentials.
   *
   * @return the netrc path
   */
  public Optional<Path> netrc() {
    return Optional.ofNullable(netrc);
  }

  /**
   * PEM bundle of extra CA certificates trusted in addition to the operating system store.
   *
   * @return the PEM bundle path
   */
  public Optional<Path> extraCaCertificates() {
    return Optional.ofNullable(extraCaCertificates);
  }

  /**
   * TCP connect timeout.
   *
   * @return the timeout
   */
  public Duration connectTimeout() {
    return connectTimeout;
  }

  /**
   * Maximum time between two reads of a response body.
   *
   * @return the timeout
   */
  public Duration readTimeout() {
    return readTimeout;
  }

  /**
   * Retries for transient network failures.
   *
   * @return the retry count
   */
  public int retries() {
    return retries;
  }

  /**
   * Whether remote artifacts must come with an expected SHA-256.
   *
   * @return {@code true} if a remote artifact without a hash is rejected
   */
  public boolean requireArtifactHash() {
    return requireArtifactHash;
  }

  /**
   * Hosts with configured credentials (the secrets themselves are not exposed).
   *
   * @return the hosts, in configuration order
   */
  public List<String> credentialHosts() {
    List<String> hosts = new ArrayList<>(credentials.size());
    for (HostCredential credential : credentials) {
      hosts.add(credential.host);
    }
    return Collections.unmodifiableList(hosts);
  }

  /**
   * The native request object (JSON-ready maps, lists, strings, numbers and booleans).
   *
   * @param resolvedCacheDir cache root to use when {@link #cacheDir()} is empty
   * @return the request
   */
  Map<String, Object> toRequest(Path resolvedCacheDir) {
    Map<String, Object> request = new LinkedHashMap<>();
    request.put("source", sourceRequest(source));
    putIfPresent(request, "version", version);
    putIfPresent(request, "platform", platform);
    Path cache = cacheDir != null ? cacheDir : resolvedCacheDir;
    request.put("cacheDir", cache.toAbsolutePath().toString());
    request.put("offline", offline);
    request.put("proxy", proxyRequest());
    if (!credentials.isEmpty()) {
      List<Object> list = new ArrayList<>(credentials.size());
      for (HostCredential credential : credentials) {
        list.add(credential.toRequest());
      }
      request.put("credentials", list);
    }
    if (netrc != null) {
      request.put("netrc", netrc.toAbsolutePath().toString());
    }
    if (extraCaCertificates != null) {
      request.put("extraCaPem", extraCaCertificates.toAbsolutePath().toString());
    }
    request.put("connectTimeoutMillis", connectTimeout.toMillis());
    request.put("readTimeoutMillis", readTimeout.toMillis());
    request.put("retries", retries);
    request.put("requireArtifactHash", requireArtifactHash);
    return request;
  }

  private static Map<String, Object> sourceRequest(BlpapiSource source) {
    Map<String, Object> out = new LinkedHashMap<>();
    switch (source.kind()) {
      case PYPI:
        out.put("kind", "pypi");
        out.put("url", source.location());
        out.put("project", source.name().orElse(BlpapiSource.DEFAULT_PYPI_PROJECT));
        break;
      case CONDA:
        out.put("kind", "conda");
        out.put("channel", source.location());
        out.put("package", source.name().orElse(BlpapiSource.DEFAULT_CONDA_PACKAGE));
        break;
      default:
        out.put("kind", "artifact");
        out.put("location", source.location());
        source.sha256().ifPresent(sha -> out.put("sha256", sha));
        break;
    }
    return out;
  }

  /** The proxy setting for the native side, following the precedence in the class comment. */
  Map<String, Object> proxyRequest() {
    Map<String, Object> out = new LinkedHashMap<>();
    if (direct) {
      out.put("mode", "direct");
      return out;
    }
    String url = proxyUrl;
    if (url == null) {
      URI target = networkTarget(source);
      ProxySelector selector = proxySelector != null ? proxySelector : ProxySelector.getDefault();
      if (target != null && selector != null) {
        url = proxyUrl(selector.select(target));
      }
    }
    if (url == null) {
      out.put("mode", "system");
      return out;
    }
    out.put("mode", "explicit");
    out.put("url", url);
    if (!noProxy.isEmpty()) {
      out.put("noProxy", new ArrayList<Object>(noProxy));
    }
    return out;
  }

  /** The URL a source is fetched from first, or {@code null} for local files. */
  static URI networkTarget(BlpapiSource source) {
    String location = source.location();
    String lower = location.toLowerCase(Locale.ROOT);
    if (lower.startsWith("http://") || lower.startsWith("https://")) {
      try {
        return URI.create(location);
      } catch (IllegalArgumentException malformed) {
        // The native side reports malformed URLs with context; no proxy decision needed.
        return null;
      }
    }
    boolean name =
        !location.contains(":")
            && !location.contains("\\")
            && !location.startsWith("/")
            && !location.startsWith(".")
            && !location.startsWith("~");
    if (source.kind() == BlpapiSource.Kind.CONDA && name) {
      // A channel name ("conda-forge", "conda-forge/label/main") resolves against the alias.
      return URI.create(DEFAULT_CONDA_CHANNEL_ALIAS + location + "/");
    }
    return null;
  }

  /** First usable proxy of a {@link ProxySelector} answer as a URL, or {@code null} for direct. */
  static String proxyUrl(List<Proxy> proxies) {
    if (proxies == null) {
      return null;
    }
    for (Proxy proxy : proxies) {
      String url = proxyUrl(proxy);
      if (url != null) {
        return url;
      }
    }
    return null;
  }

  private static String proxyUrl(Proxy proxy) {
    if (proxy == null || proxy.type() == Proxy.Type.DIRECT) {
      return null;
    }
    SocketAddress address = proxy.address();
    if (!(address instanceof InetSocketAddress)) {
      return null;
    }
    InetSocketAddress inet = (InetSocketAddress) address;
    String host = inet.getHostString();
    if (host.indexOf(':') >= 0 && !host.startsWith("[")) {
      host = "[" + host + "]";
    }
    String scheme = proxy.type() == Proxy.Type.SOCKS ? "socks5h" : "http";
    return scheme + "://" + host + ":" + inet.getPort();
  }

  private static void putIfPresent(Map<String, Object> map, String key, Object value) {
    if (value != null) {
      map.put(key, value);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof ProvisionOptions)) {
      return false;
    }
    ProvisionOptions that = (ProvisionOptions) other;
    return offline == that.offline
        && direct == that.direct
        && retries == that.retries
        && requireArtifactHash == that.requireArtifactHash
        && source.equals(that.source)
        && Objects.equals(version, that.version)
        && Objects.equals(platform, that.platform)
        && Objects.equals(cacheDir, that.cacheDir)
        && Objects.equals(proxyUrl, that.proxyUrl)
        && Objects.equals(proxySelector, that.proxySelector)
        && noProxy.equals(that.noProxy)
        && credentials.equals(that.credentials)
        && Objects.equals(netrc, that.netrc)
        && Objects.equals(extraCaCertificates, that.extraCaCertificates)
        && connectTimeout.equals(that.connectTimeout)
        && readTimeout.equals(that.readTimeout);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        source,
        version,
        platform,
        cacheDir,
        offline,
        proxyUrl,
        direct,
        proxySelector,
        noProxy,
        credentials,
        netrc,
        extraCaCertificates,
        connectTimeout,
        readTimeout,
        retries,
        requireArtifactHash);
  }

  /**
   * Describes the options without secrets: URL credentials in the source and proxy are replaced by
   * {@code ***} and only the hosts of explicit credentials are listed.
   *
   * @return the description
   */
  @Override
  public String toString() {
    StringBuilder out = new StringBuilder("ProvisionOptions{source=").append(source);
    out.append(", version=").append(version == null ? "latest" : version);
    if (platform != null) {
      out.append(", platform=").append(platform);
    }
    if (cacheDir != null) {
      out.append(", cacheDir=").append(cacheDir);
    }
    if (offline) {
      out.append(", offline");
    }
    if (direct) {
      out.append(", proxy=direct");
    } else if (proxyUrl != null) {
      out.append(", proxy=").append(Text.redactUserInfo(proxyUrl));
    }
    if (!credentials.isEmpty()) {
      out.append(", credentials=").append(credentialHosts());
    }
    return out.append('}').toString();
  }

  /** One per-host credential: basic with a {@code username}, else bearer. */
  private static final class HostCredential {
    final String host;
    final String username;
    final String secret;

    HostCredential(String host, String username, String secret) {
      this.host = host;
      this.username = username;
      this.secret = secret;
    }

    Map<String, Object> toRequest() {
      Map<String, Object> out = new LinkedHashMap<>();
      out.put("host", host);
      if (username != null) {
        out.put("username", username);
        out.put("password", secret);
      } else {
        out.put("token", secret);
      }
      return out;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof HostCredential)) {
        return false;
      }
      HostCredential that = (HostCredential) other;
      return host.equals(that.host)
          && Objects.equals(username, that.username)
          && secret.equals(that.secret);
    }

    @Override
    public int hashCode() {
      return Objects.hash(host, username, secret);
    }

    @Override
    public String toString() {
      return host;
    }
  }

  /** Builder for {@link ProvisionOptions}. */
  public static final class Builder {
    private BlpapiSource source;
    private String version;
    private String platform;
    private Path cacheDir;
    private boolean offline;
    private String proxyUrl;
    private boolean direct;
    private ProxySelector proxySelector;
    private final List<String> noProxy = new ArrayList<>();
    private final List<HostCredential> credentials = new ArrayList<>();
    private Path netrc;
    private Path extraCaCertificates;
    private Duration connectTimeout = DEFAULT_CONNECT_TIMEOUT;
    private Duration readTimeout = DEFAULT_READ_TIMEOUT;
    private int retries = DEFAULT_RETRIES;
    private boolean requireArtifactHash = true;

    private Builder() {}

    /**
     * Sets where the runtime comes from (required).
     *
     * @param source the source
     * @return this builder
     */
    public Builder source(BlpapiSource source) {
      this.source = Objects.requireNonNull(source, "source");
      return this;
    }

    /**
     * Pins an exact version (PEP 440 for PyPI, conda version for channels); {@code null} or {@code
     * "latest"} selects the newest stable release.
     *
     * @param version the version
     * @return this builder
     * @throws IllegalArgumentException if {@code version} is blank
     */
    public Builder version(String version) {
      if (version == null || version.trim().equalsIgnoreCase("latest")) {
        this.version = null;
      } else if (version.trim().isEmpty()) {
        throw new IllegalArgumentException("version must not be blank");
      } else {
        this.version = version.trim();
      }
      return this;
    }

    /**
     * Sets the target platform; {@code null} selects the platform of this JVM.
     *
     * @param platform {@code windows-x86_64}, {@code linux-x86_64} or {@code macos-aarch64}
     * @return this builder
     * @throws IllegalArgumentException for any other value
     */
    public Builder platform(String platform) {
      if (platform != null && !PLATFORMS.contains(platform)) {
        throw new IllegalArgumentException(
            "unsupported platform '"
                + platform
                + "' (supported: "
                + String.join(", ", PLATFORMS)
                + ")");
      }
      this.platform = platform;
      return this;
    }

    /**
     * Sets the cache root; {@code null} selects the shared xbbg cache directory.
     *
     * @param cacheDir the cache root
     * @return this builder
     */
    public Builder cacheDir(Path cacheDir) {
      this.cacheDir = cacheDir;
      return this;
    }

    /**
     * Forbids network access; provisioning then only returns installs already in the cache (the
     * newest one from this source unless a version is pinned).
     *
     * @param offline {@code true} for offline mode
     * @return this builder
     */
    public Builder offline(boolean offline) {
      this.offline = offline;
      return this;
    }

    /**
     * Uses an explicit proxy for every request ({@code http://}, {@code https://}, {@code
     * socks5://} or {@code socks5h://}, optionally with {@code user:password@}); {@code null}
     * returns to the default (see the proxy precedence in {@link ProvisionOptions}).
     *
     * @param proxyUrl the proxy URL
     * @return this builder
     * @throws IllegalArgumentException if the scheme is not supported
     */
    public Builder proxy(String proxyUrl) {
      if (proxyUrl != null) {
        String lower = proxyUrl.trim().toLowerCase(Locale.ROOT);
        if (!(lower.startsWith("http://")
            || lower.startsWith("https://")
            || lower.startsWith("socks5://")
            || lower.startsWith("socks5h://"))) {
          throw new IllegalArgumentException(
              "proxy URL must start with http://, https://, socks5:// or socks5h://: '"
                  + Text.redactUserInfo(proxyUrl)
                  + "'");
        }
        this.proxyUrl = proxyUrl.trim();
      } else {
        this.proxyUrl = null;
      }
      this.direct = false;
      return this;
    }

    /**
     * Uses a {@link Proxy}: {@link Proxy#NO_PROXY} forces direct connections like {@link
     * #directConnection()}, an HTTP proxy becomes {@code http://host:port} and a SOCKS proxy {@code
     * socks5h://host:port}.
     *
     * @param proxy the proxy
     * @return this builder
     */
    public Builder proxy(Proxy proxy) {
      Objects.requireNonNull(proxy, "proxy");
      String url = proxyUrl(proxy);
      this.proxyUrl = url;
      this.direct = url == null;
      return this;
    }

    /**
     * Forces direct connections: no proxy from the JVM, the environment or the operating system is
     * used.
     *
     * @return this builder
     */
    public Builder directConnection() {
      this.proxyUrl = null;
      this.direct = true;
      return this;
    }

    /**
     * Sets the selector consulted for the source URL when no explicit proxy is set; default {@link
     * ProxySelector#getDefault()} at provisioning time. A {@code DIRECT} answer leaves the choice
     * to the native client's system proxy detection.
     *
     * @param selector the selector, or {@code null} for the default
     * @return this builder
     */
    public Builder proxySelector(ProxySelector selector) {
      this.proxySelector = selector;
      return this;
    }

    /**
     * Sets hosts that bypass the proxy ({@code NO_PROXY} syntax).
     *
     * @param hosts host names, {@code .domain} suffixes, IP addresses or CIDR ranges
     * @return this builder
     */
    public Builder noProxy(Collection<String> hosts) {
      noProxy.clear();
      for (String host : Objects.requireNonNull(hosts, "hosts")) {
        noProxy.add(nonBlank(host, "no-proxy host"));
      }
      return this;
    }

    /**
     * Adds HTTP basic credentials for a host.
     *
     * @param host host name, or {@code *.domain} for every subdomain
     * @param username user name
     * @param password password or API key
     * @return this builder
     */
    public Builder basicAuth(String host, String username, String password) {
      credentials.add(
          new HostCredential(
              nonBlank(host, "credential host"),
              Objects.requireNonNull(username, "username"),
              Objects.requireNonNull(password, "password")));
      return this;
    }

    /**
     * Adds a bearer token for a host.
     *
     * @param host host name, or {@code *.domain} for every subdomain
     * @param token the token
     * @return this builder
     */
    public Builder bearerToken(String host, String token) {
      credentials.add(
          new HostCredential(nonBlank(host, "credential host"), null, nonBlank(token, "token")));
      return this;
    }

    /**
     * Reads credentials from this netrc file (never read implicitly).
     *
     * @param netrc the netrc path, or {@code null} for none
     * @return this builder
     */
    public Builder netrc(Path netrc) {
      this.netrc = netrc;
      return this;
    }

    /**
     * Trusts the CA certificates in this PEM bundle in addition to the operating system's store
     * (for TLS-intercepting corporate proxies).
     *
     * @param pemBundle the PEM file, or {@code null} for none
     * @return this builder
     */
    public Builder extraCaCertificates(Path pemBundle) {
      this.extraCaCertificates = pemBundle;
      return this;
    }

    /**
     * Sets the TCP connect timeout (default 10 seconds).
     *
     * @param timeout a positive duration
     * @return this builder
     */
    public Builder connectTimeout(Duration timeout) {
      this.connectTimeout = positive(timeout, "connect timeout");
      return this;
    }

    /**
     * Sets the maximum time between two reads of a response body (default 60 seconds).
     *
     * @param timeout a positive duration
     * @return this builder
     */
    public Builder readTimeout(Duration timeout) {
      this.readTimeout = positive(timeout, "read timeout");
      return this;
    }

    /**
     * Sets the retries for transient network failures (default 3).
     *
     * @param retries zero or more
     * @return this builder
     */
    public Builder retries(int retries) {
      if (retries < 0) {
        throw new IllegalArgumentException("retries must not be negative: " + retries);
      }
      this.retries = retries;
      return this;
    }

    /**
     * Whether a remote artifact must come with an expected SHA-256 (default {@code true}); without
     * it, integrity rests on TLS and the archive's own {@code RECORD} or {@code info/paths.json}.
     *
     * @param require {@code false} to accept remote artifacts without a hash
     * @return this builder
     */
    public Builder requireArtifactHash(boolean require) {
      this.requireArtifactHash = require;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return the options
     * @throws IllegalStateException if no source was set
     */
    public ProvisionOptions build() {
      if (source == null) {
        throw new IllegalStateException("ProvisionOptions needs a source");
      }
      return new ProvisionOptions(this);
    }

    private static String nonBlank(String value, String what) {
      if (value == null || value.trim().isEmpty()) {
        throw new IllegalArgumentException(what + " must not be blank");
      }
      return value.trim();
    }

    private static Duration positive(Duration value, String what) {
      Objects.requireNonNull(value, what);
      if (value.isNegative() || value.isZero()) {
        throw new IllegalArgumentException(what + " must be positive: " + value);
      }
      return value;
    }
  }
}
