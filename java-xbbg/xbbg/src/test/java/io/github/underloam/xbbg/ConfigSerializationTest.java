package io.github.underloam.xbbg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.underloam.xbbg.internal.Json;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConfigSerializationTest {
  @Test
  void emptyConfigOmitsEveryField() {
    assertEquals("{}", EngineConfig.defaults().toJson().text());
    assertEquals(
        "{}", EngineConfig.builder().servers().warmupServices(null).build().toJson().text());
  }

  @Test
  void everyFieldUsesItsTypeScriptName() {
    EngineConfig config =
        EngineConfig.builder()
            .host("bbg-host")
            .port(8196)
            .servers(ServerAddress.of("a", 8194), ServerAddress.of("b", 8196))
            .zfpRemote("8194")
            .requestPoolSize(3)
            .subscriptionPoolSize(0)
            .runtimeWorkerThreads(4)
            .maxSubscriptionSessions(8)
            .shardRequests(true)
            .shardThreshold(30)
            .shardChunkSize(10)
            .shardMaxConcurrent(2)
            .validationMode(ValidationMode.STRICT)
            .keyColumn(KeyColumn.SECURITY)
            .subscriptionFlushThreshold(32)
            .maxEventQueueSize(5000)
            .commandQueueSize(128)
            .subscriptionStreamCapacity(64)
            .overflowPolicy(OverflowPolicy.DROP_OLDEST)
            .warmupServices(Collections.<String>emptyList())
            .fieldCachePath(Paths.get("cache.json"))
            .bqlAppName("app")
            .auth(AuthConfig.manual("app", "user", "10.0.0.1"))
            .tls(
                TlsConfig.builder()
                    .clientCredentials(TlsMaterial.bytes(new byte[] {1, 2, 3}))
                    .clientCredentialsPassword("secret")
                    .trustMaterial(TlsMaterial.bytes(ByteBuffer.wrap(new byte[] {4})))
                    .handshakeTimeoutMs(1000)
                    .crlFetchTimeoutMs(2000)
                    .build())
            .numStartAttempts(1)
            .autoRestartOnDisconnection(false)
            .retryPolicy(
                RetryPolicy.builder()
                    .maxRetries(2)
                    .initialDelayMs(10)
                    .backoffFactor(1.5)
                    .maxDelayMs(100)
                    .build())
            .requestTimeoutMs(5000)
            .streamsDeactivatedWarnMs(0)
            .keepAliveEnabled(true)
            .keepAliveInactivityMs(20000)
            .keepAliveResponseTimeoutMs(10000)
            .slowConsumerHiWaterMark(0.75)
            .slowConsumerLoWaterMark(0.5)
            .sdkLogLevel(SdkLogLevel.WARN)
            .socks5(Socks5Config.of("proxy", 1080))
            .build();
    assertEquals(
        "{\"host\":\"bbg-host\",\"port\":8196,"
            + "\"servers\":[{\"host\":\"a\",\"port\":8194},{\"host\":\"b\",\"port\":8196}],"
            + "\"zfpRemote\":\"8194\",\"requestPoolSize\":3,\"subscriptionPoolSize\":0,"
            + "\"runtimeWorkerThreads\":4,\"maxSubscriptionSessions\":8,\"shardRequests\":true,"
            + "\"shardThreshold\":30,\"shardChunkSize\":10,\"shardMaxConcurrent\":2,"
            + "\"validationMode\":\"strict\",\"keyColumn\":\"security\","
            + "\"subscriptionFlushThreshold\":32,\"maxEventQueueSize\":5000,"
            + "\"commandQueueSize\":128,\"subscriptionStreamCapacity\":64,"
            + "\"overflowPolicy\":\"drop_oldest\",\"warmupServices\":[],"
            + "\"fieldCachePath\":\"cache.json\",\"bqlAppName\":\"app\","
            + "\"auth\":{\"method\":\"manual\",\"appName\":\"app\",\"userId\":\"user\","
            + "\"ipAddress\":\"10.0.0.1\"},"
            + "\"tls\":{\"clientCredentials\":{\"bytes\":\"AQID\"},"
            + "\"clientCredentialsPassword\":\"secret\",\"trustMaterial\":{\"bytes\":\"BA==\"},"
            + "\"handshakeTimeoutMs\":1000,\"crlFetchTimeoutMs\":2000},"
            + "\"numStartAttempts\":1,\"autoRestartOnDisconnection\":false,"
            + "\"retryPolicy\":{\"maxRetries\":2,\"initialDelayMs\":10,\"backoffFactor\":1.5,"
            + "\"maxDelayMs\":100},"
            + "\"requestTimeoutMs\":5000,\"streamsDeactivatedWarnMs\":0,\"keepAliveEnabled\":true,"
            + "\"keepAliveInactivityMs\":20000,\"keepAliveResponseTimeoutMs\":10000,"
            + "\"slowConsumerHiWaterMark\":0.75,\"slowConsumerLoWaterMark\":0.5,"
            + "\"sdkLogLevel\":\"warn\",\"socks5\":{\"host\":\"proxy\",\"port\":1080}}",
        config.toJson().text());
    assertEquals(config, config.toBuilder().build());
    assertEquals(config.hashCode(), config.toBuilder().build().hashCode());
  }

  @Test
  void toStringRedactsSecrets() {
    EngineConfig config =
        EngineConfig.builder()
            .host("h")
            .auth(AuthConfig.token("token-value"))
            .tls(
                TlsConfig.builder()
                    .clientCredentials(TlsMaterial.path("private-client.p12"))
                    .clientCredentialsPassword("pw-value")
                    .trustMaterial(TlsMaterial.path("private-trust.p7"))
                    .build())
            .build();
    String text = config.toString();
    assertFalse(text.contains("token-value"), text);
    assertFalse(text.contains("pw-value"), text);
    assertFalse(text.contains("private-client"), text);
    assertTrue(text.contains("\"host\":\"h\""), text);
    assertTrue(text.contains("<redacted>"), text);
  }

  @Test
  void rejectsOutOfRangeNumbers() {
    assertThrows(IllegalArgumentException.class, () -> EngineConfig.builder().port(65536));
    assertThrows(IllegalArgumentException.class, () -> EngineConfig.builder().port(-1));
    assertThrows(IllegalArgumentException.class, () -> EngineConfig.builder().requestPoolSize(-1));
    assertThrows(
        IllegalArgumentException.class,
        () -> EngineConfig.builder().slowConsumerHiWaterMark(Double.NaN));
    assertThrows(IllegalArgumentException.class, () -> ServerAddress.of("h", 70000));
    assertThrows(IllegalArgumentException.class, () -> RetryPolicy.builder().maxRetries(-2));
  }

  @Test
  void tlsMaterialCopiesBytesAndHidesThemInText() {
    byte[] bytes = {9, 8};
    TlsMaterial material = TlsMaterial.bytes(bytes);
    bytes[0] = 0;
    assertEquals(9, material.bytes().get()[0]);
    assertEquals("TlsMaterial{2 bytes}", material.toString());
    assertEquals("TlsMaterial{path}", TlsMaterial.path("secret/location.p12").toString());
    assertEquals("{\"path\":\"x.p12\"}", TlsMaterial.path("x.p12").toJson().text());
  }

  @Test
  void minimalRequestSpecSendsServiceAndOperation() {
    assertEquals(
        "{\"service\":\"//blp/refdata\",\"operation\":\"\"}",
        RequestSpec.builder("//blp/refdata").build().toJson().text());
  }

  @Test
  void requestSpecSerializesEveryRequestInputField() {
    Map<String, Object> json = new LinkedHashMap<String, Object>();
    json.put("a", Collections.singletonMap("b", 1));
    Map<String, Object> kwargs = new LinkedHashMap<String, Object>();
    kwargs.put("periodicitySelection", "DAILY");
    kwargs.put("maxDataPoints", 10.0);
    Map<String, String> fieldTypes = new LinkedHashMap<String, String>();
    fieldTypes.put("PX_LAST", "float64");
    RequestSpec spec =
        RequestSpec.builder("//blp/refdata")
            .operation("HistoricalDataRequest")
            .requestOperation("Raw")
            .requestId("req-1")
            .extractor("histdata")
            .securities("IBM US Equity", "MSFT US Equity")
            .security("IBM US Equity")
            .fields("PX_LAST")
            .overrides(Overrides.of("A", "1").forSecurity("IBM US Equity", Overrides.of("B", "2")))
            .element("periodicityAdjustment", "ACTUAL")
            .element("nested.value", "3")
            .kwargs(kwargs)
            .jsonElements(json)
            .startDate(LocalDate.of(2024, 1, 2))
            .endDate("20240131")
            .startDatetime(LocalDateTime.of(2024, 1, 2, 9, 30))
            .endDatetime("2024-01-02 16:00")
            .requestTz("NY")
            .outputTz("UTC")
            .eventType("TRADE")
            .eventTypes("BID", "ASK")
            .interval(5)
            .options(Collections.singletonList(StringPair.of("opt", "1")))
            .fieldTypes(fieldTypes)
            .includeSecurityErrors(true)
            .returnEids(false)
            .onDelayed(DelayedPolicy.WARN)
            .validateFields(true)
            .searchSpec("price")
            .fieldIds(Arrays.asList("PR005"))
            .format(Format.LONG_WITH_METADATA)
            .build();
    assertEquals(
        "{\"service\":\"//blp/refdata\",\"operation\":\"HistoricalDataRequest\","
            + "\"requestOperation\":\"Raw\",\"requestId\":\"req-1\",\"extractor\":\"histdata\","
            + "\"securities\":[\"IBM US Equity\",\"MSFT US Equity\"],"
            + "\"security\":\"IBM US Equity\",\"fields\":[\"PX_LAST\"],"
            + "\"overrides\":[{\"key\":\"A\",\"value\":\"1\"}],"
            + "\"securityOverrides\":[{\"security\":\"IBM US Equity\","
            + "\"overrides\":[{\"key\":\"B\",\"value\":\"2\"}]}],"
            + "\"elements\":[{\"key\":\"periodicityAdjustment\",\"value\":\"ACTUAL\"},"
            + "{\"key\":\"nested.value\",\"value\":\"3\"}],"
            + "\"kwargs\":[{\"key\":\"periodicitySelection\",\"value\":\"DAILY\"},"
            + "{\"key\":\"maxDataPoints\",\"value\":\"10\"}],"
            + "\"jsonElements\":\"{\\\"a\\\":{\\\"b\\\":1}}\","
            + "\"startDate\":\"20240102\",\"endDate\":\"20240131\","
            + "\"startDatetime\":\"2024-01-02T09:30:00\",\"endDatetime\":\"2024-01-02 16:00\","
            + "\"requestTz\":\"NY\",\"outputTz\":\"UTC\",\"eventType\":\"TRADE\","
            + "\"eventTypes\":[\"BID\",\"ASK\"],\"interval\":5,"
            + "\"options\":[{\"key\":\"opt\",\"value\":\"1\"}],"
            + "\"fieldTypes\":[{\"key\":\"PX_LAST\",\"value\":\"float64\"}],"
            + "\"includeSecurityErrors\":true,\"returnEids\":false,\"onDelayed\":\"warn\","
            + "\"validateFields\":true,\"searchSpec\":\"price\",\"fieldIds\":[\"PR005\"],"
            + "\"format\":\"long_metadata\"}",
        spec.toJson().text());
    assertTrue(Json.parse(spec.toJson().text()) instanceof Map, "the request is one JSON object");
    assertFalse(spec.equals(RequestSpec.builder("//blp/refdata").build()));
  }

  @Test
  void requestSpecIsImmutable() {
    java.util.List<String> fields = new java.util.ArrayList<String>(Arrays.asList("A"));
    RequestSpec spec = RequestSpec.builder("s").fields(fields).build();
    fields.add("B");
    assertEquals(Arrays.asList("A"), spec.fields().get());
    assertThrows(UnsupportedOperationException.class, () -> spec.fields().get().add("C"));
  }
}
