package io.github.underloam.xbbg.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ProvisionOptionsTest {
  private static final BlpapiSource BLOOMBERG =
      BlpapiSource.pypi(BlpapiSource.BLOOMBERG_PYPI_INDEX);
  private static final Path CACHE = Paths.get("cache").toAbsolutePath();

  /** A selector that answers {@code answer} and records what it was asked. */
  private static final class FixedSelector extends ProxySelector {
    final List<URI> asked = new ArrayList<>();
    private final Proxy answer;

    FixedSelector(Proxy answer) {
      this.answer = answer;
    }

    @Override
    public List<Proxy> select(URI uri) {
      asked.add(uri);
      return Collections.singletonList(answer);
    }

    @Override
    public void connectFailed(URI uri, SocketAddress address, IOException failure) {}
  }

  private static Proxy http(String host, int port) {
    return new Proxy(Proxy.Type.HTTP, InetSocketAddress.createUnresolved(host, port));
  }

  private static Map<String, Object> proxyOf(ProvisionOptions.Builder builder) {
    return builder.build().proxyRequest();
  }

  @Test
  void proxyPrecedenceDirectThenExplicitThenSelectorThenSystem() {
    FixedSelector selector = new FixedSelector(http("jvm-proxy", 3128));
    ProvisionOptions.Builder base =
        ProvisionOptions.builder().source(BLOOMBERG).proxySelector(selector);

    assertEquals("direct", proxyOf(base.directConnection()).get("mode"));
    assertEquals("direct", proxyOf(base.proxy(Proxy.NO_PROXY)).get("mode"));

    Map<String, Object> explicit = proxyOf(base.proxy("http://corp:8080"));
    assertEquals("explicit", explicit.get("mode"));
    assertEquals("http://corp:8080", explicit.get("url"));
    assertTrue(selector.asked.isEmpty(), "an explicit proxy never consults the selector");

    Map<String, Object> fromSelector = proxyOf(base.proxy((String) null));
    assertEquals("explicit", fromSelector.get("mode"));
    assertEquals("http://jvm-proxy:3128", fromSelector.get("url"));
    assertEquals(URI.create(BlpapiSource.BLOOMBERG_PYPI_INDEX), selector.asked.get(0));

    Map<String, Object> system = proxyOf(base.proxySelector(new FixedSelector(Proxy.NO_PROXY)));
    assertEquals(
        Collections.singletonMap("mode", "system"),
        system,
        "DIRECT from the JVM leaves the choice to the native system detection");
  }

  @Test
  void selectorProxiesBecomeUrlsAndBypassListsTravelWithThem() throws IOException {
    InetAddress loopback = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
    Proxy socks = new Proxy(Proxy.Type.SOCKS, new InetSocketAddress(loopback, 1080));
    Map<String, Object> request =
        proxyOf(
            ProvisionOptions.builder()
                .source(BlpapiSource.conda("conda-forge"))
                .proxySelector(new FixedSelector(socks))
                .noProxy(Arrays.asList("localhost", ".corp")));
    assertEquals("socks5h://127.0.0.1:1080", request.get("url"));
    assertEquals(
        "http://[::1]:8080",
        ProvisionOptions.proxyUrl(
            Collections.singletonList(
                new Proxy(Proxy.Type.HTTP, InetSocketAddress.createUnresolved("::1", 8080)))));
    assertNull(ProvisionOptions.proxyUrl(Collections.singletonList(Proxy.NO_PROXY)));
  }

  @Test
  void selectorIsAskedForTheUrlTheSourceFetchesFirst() {
    assertEquals(
        URI.create("https://conda.anaconda.org/conda-forge/label/main/"),
        ProvisionOptions.networkTarget(BlpapiSource.conda("conda-forge/label/main")));
    assertEquals(
        URI.create("https://h/x.whl"),
        ProvisionOptions.networkTarget(BlpapiSource.artifact("https://h/x.whl")));
    assertNull(ProvisionOptions.networkTarget(BlpapiSource.artifact("C:\\w\\x.whl")));
    assertNull(ProvisionOptions.networkTarget(BlpapiSource.conda("./local-channel")));
    FixedSelector selector = new FixedSelector(http("p", 1));
    Map<String, Object> local =
        proxyOf(
            ProvisionOptions.builder()
                .source(BlpapiSource.artifact("C:\\w\\x.whl"))
                .proxySelector(selector));
    assertEquals("system", local.get("mode"));
    assertTrue(selector.asked.isEmpty(), "local files are never offered to the selector");
  }

  @Test
  void requestCarriesEveryOptionWithJavaDefaults() {
    ProvisionOptions options =
        ProvisionOptions.builder()
            .source(BlpapiSource.artifact("https://h/x.whl"))
            .version("3.26.9.1")
            .platform("linux-x86_64")
            .offline(true)
            .directConnection()
            .basicAuth("repo.example", "user", "secret")
            .bearerToken("*.corp", "token")
            .netrc(Paths.get("netrc"))
            .extraCaCertificates(Paths.get("ca.pem"))
            .connectTimeout(Duration.ofMillis(1500))
            .readTimeout(Duration.ofSeconds(2))
            .retries(0)
            .requireArtifactHash(false)
            .build();
    Map<String, Object> request = options.toRequest(CACHE);
    assertEquals("artifact", ((Map<?, ?>) request.get("source")).get("kind"));
    assertEquals("3.26.9.1", request.get("version"));
    assertEquals("linux-x86_64", request.get("platform"));
    assertEquals(CACHE.toString(), request.get("cacheDir"));
    assertEquals(true, request.get("offline"));
    assertEquals(1500L, request.get("connectTimeoutMillis"));
    assertEquals(2000L, request.get("readTimeoutMillis"));
    assertEquals(0, request.get("retries"));
    assertEquals(false, request.get("requireArtifactHash"));
    List<?> credentials = (List<?>) request.get("credentials");
    assertEquals("secret", ((Map<?, ?>) credentials.get(0)).get("password"));
    assertEquals("token", ((Map<?, ?>) credentials.get(1)).get("token"));
    assertEquals(Paths.get("ca.pem").toAbsolutePath().toString(), request.get("extraCaPem"));
    assertEquals(Arrays.asList("repo.example", "*.corp"), options.credentialHosts());
    assertFalse(options.toString().contains("secret"), options.toString());
    assertFalse(options.toString().contains("token,"), options.toString());

    Map<String, Object> defaults =
        ProvisionOptions.builder().source(BLOOMBERG).build().toRequest(CACHE);
    assertFalse(defaults.containsKey("version"));
    assertFalse(defaults.containsKey("platform"));
    assertFalse(defaults.containsKey("credentials"));
    assertEquals(10_000L, defaults.get("connectTimeoutMillis"));
    assertEquals(60_000L, defaults.get("readTimeoutMillis"));
    assertEquals(3, defaults.get("retries"));
    assertEquals(true, defaults.get("requireArtifactHash"));
    Map<?, ?> source = (Map<?, ?>) defaults.get("source");
    assertEquals("pypi", source.get("kind"));
    assertEquals("blpapi", source.get("project"));
  }

  @Test
  void toStringHidesCredentialsEmbeddedInTheSourceAndProxy() {
    BlpapiSource source = BlpapiSource.pypi("https://user:s3cret@mirror.example/simple/");
    ProvisionOptions options =
        ProvisionOptions.builder()
            .source(source)
            .proxy("http://proxyuser:pr0xy@proxy.example:3128")
            .build();
    String text = options.toString();
    assertFalse(text.contains("s3cret"), text);
    assertFalse(text.contains("user:"), text);
    assertFalse(text.contains("pr0xy"), text);
    assertTrue(text.contains("source=pypi:https://***@mirror.example/simple/"), text);
    assertTrue(text.contains("proxy=http://***@proxy.example:3128"), text);
    // The request handed to the native provisioner keeps the credentials it needs.
    Map<?, ?> request = (Map<?, ?>) options.toRequest(CACHE).get("source");
    assertEquals("https://user:s3cret@mirror.example/simple/", request.get("url"));
  }

  @Test
  void invalidValuesAreRejectedAtBuildTime() {
    ProvisionOptions.Builder builder = ProvisionOptions.builder();
    assertThrows(IllegalStateException.class, builder::build);
    assertThrows(IllegalArgumentException.class, () -> builder.platform("linux-aarch64"));
    assertThrows(IllegalArgumentException.class, () -> builder.version(" "));
    assertThrows(IllegalArgumentException.class, () -> builder.retries(-1));
    assertThrows(IllegalArgumentException.class, () -> builder.readTimeout(Duration.ZERO));
    assertThrows(IllegalArgumentException.class, () -> builder.proxy("ftp://p:21"));
    assertThrows(IllegalArgumentException.class, () -> builder.bearerToken(" ", "t"));
    assertThrows(IllegalArgumentException.class, () -> builder.noProxy(Arrays.asList("ok", "")));
    assertThrows(NullPointerException.class, () -> builder.source(null));
    ProvisionOptions latest = builder.source(BLOOMBERG).version("latest").build();
    assertFalse(latest.version().isPresent());
    IllegalArgumentException secret =
        assertThrows(IllegalArgumentException.class, () -> builder.proxy("ftp://u:pw@p:21"));
    assertFalse(secret.getMessage().contains("pw"), secret.getMessage());
  }
}
