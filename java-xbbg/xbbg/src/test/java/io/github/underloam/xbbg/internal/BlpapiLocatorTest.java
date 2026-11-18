package io.github.underloam.xbbg.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.underloam.xbbg.BlpapiNotFoundException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What Java passes to the shared native locator, and how it reports a failed search. The search
 * itself is Rust's ({@code xbbg_host::sdk}) and is tested there.
 */
class BlpapiLocatorTest {
  @TempDir Path temp;

  private final Map<String, String> env = new HashMap<String, String>();
  private final Map<String, String> properties = new HashMap<String, String>();

  private BlpapiLocator locator(Path explicit) {
    properties.put("user.dir", temp.toString());
    return new BlpapiLocator(lookup(env), lookup(properties), explicit, temp.resolve("cache"));
  }

  private static Function<String, String> lookup(final Map<String, String> values) {
    return new Function<String, String>() {
      @Override
      public String apply(String name) {
        return values.get(name);
      }
    };
  }

  private static Map<String, Object> parse(String json) {
    return JsonFields.object(JsonFields.parse(json, "request"), "request");
  }

  @Test
  void theRequestCarriesOnlyTheCacheRootWhenNothingIsConfigured() {
    Map<String, Object> request = parse(locator(null).requestJson());
    assertEquals(Collections.singleton("cacheRoot"), request.keySet());
    assertEquals(temp.resolve("cache").toString(), request.get("cacheRoot"));
  }

  @Test
  void theSetterIsTheExplicitPath() {
    properties.put(BlpapiLocator.PATH_PROPERTY, "ignored-while-the-setter-is-set");
    Path explicit = temp.resolve("sdk").resolve("lib");
    Map<String, Object> request = parse(locator(explicit).requestJson());
    assertEquals(explicit.toString(), request.get("explicit"));
    assertEquals("Xbbg.setSdkPath", locator(explicit).explicitLabel());
  }

  @Test
  void thePropertyIsTheExplicitPathResolvedAgainstTheWorkingDirectory() {
    properties.put(BlpapiLocator.PATH_PROPERTY, "runtime");
    BlpapiLocator locator = locator(null);
    assertEquals(temp.resolve("runtime").toString(), parse(locator.requestJson()).get("explicit"));
    assertEquals("-Dxbbg.blpapi.path", locator.explicitLabel());

    properties.put(BlpapiLocator.PATH_PROPERTY, temp.resolve("abs").resolve("..").toString());
    assertEquals(temp.toString(), parse(locator(null).requestJson()).get("explicit"));
  }

  @Test
  void aBlankPropertyIsNotAnExplicitPath() {
    properties.put(BlpapiLocator.PATH_PROPERTY, "  ");
    BlpapiLocator locator = locator(null);
    assertFalse(parse(locator.requestJson()).containsKey("explicit"));
    assertNull(locator.explicitLabel());
  }

  @Test
  void theRequestNeverCarriesTheEnvironment() {
    env.put("BLPAPI_ROOT", temp.resolve("root").toString());
    assertFalse(parse(locator(null).requestJson()).containsKey("env"));
  }

  @Test
  void quotesAndBackslashesInPathsSurviveTheRequest() {
    Path explicit = temp.resolve("with \"quote\" and \\ backslash");
    assertEquals(explicit.toString(), parse(locator(explicit).requestJson()).get("explicit"));
  }

  @Test
  void provisioningSettingsComeFromThePropertyThenTheEnvironment() {
    assertNull(locator(null).configuredSourceSpec());
    env.put(BlpapiLocator.SOURCE_ENV, "pypi:https://example.invalid/simple/");
    env.put(BlpapiLocator.VERSION_ENV, "3.26.5.1");
    assertEquals("pypi:https://example.invalid/simple/", locator(null).configuredSourceSpec());
    assertEquals("3.26.5.1", locator(null).configuredVersion());
    properties.put(BlpapiLocator.SOURCE_PROPERTY, "conda:conda-forge");
    properties.put(BlpapiLocator.VERSION_PROPERTY, "3.25.0.1");
    assertEquals("conda:conda-forge", locator(null).configuredSourceSpec());
    assertEquals("3.25.0.1", locator(null).configuredVersion());
    properties.put(BlpapiLocator.SOURCE_PROPERTY, " ");
    assertEquals("pypi:https://example.invalid/simple/", locator(null).configuredSourceSpec());
  }

  @Test
  void rejectionsAreReadFromTheNativeErrorAndFormatted() {
    List<Object> values =
        JsonFields.array(
            JsonFields.parse(
                "[{\"path\":\"/opt/sdk/libblpapi3_64.so\",\"source\":\"blpapi_root\","
                    + "\"detail\":\"BLPAPI_ROOT\",\"reason\":\"lacks a required function\"},"
                    + "{\"path\":\"/cache/sdk/3.26.5.1/linux-x86_64/pypi-x\",\"source\":\"cache\","
                    + "\"detail\":\"/cache/sdk/3.26.5.1/linux-x86_64/pypi-x\","
                    + "\"reason\":\"sha256 mismatch\"},"
                    + "{\"path\":\"/x\",\"source\":\"explicit\",\"reason\":\"does not exist\"}]",
                "rejected"),
            "rejected");
    List<BlpapiLocator.Rejection> rejected = BlpapiLocator.parseRejected(values);
    assertEquals(3, rejected.size());
    assertEquals(
        "/opt/sdk/libblpapi3_64.so (blpapi_root: BLPAPI_ROOT): lacks a required function",
        rejected.get(0).toString());
    assertEquals("/x (explicit): does not exist", rejected.get(2).toString());
  }

  @Test
  void theNotFoundMessageListsEveryRejectionAndEveryWayToSupplyARuntime() {
    BlpapiLocator locator = locator(temp.resolve("runtime"));
    List<BlpapiLocator.Rejection> rejected =
        Arrays.asList(
            new BlpapiLocator.Rejection("/a/lib.so", "explicit", "", "does not exist"),
            new BlpapiLocator.Rejection("/b/lib.so", "search_path", "PATH", "cannot be opened"));
    BlpapiNotFoundException error = locator.notFound(rejected);
    String message = error.getMessage();
    assertTrue(message.startsWith("Bloomberg runtime library not found."), message);
    assertTrue(message.contains("\n  - /a/lib.so (explicit): does not exist"), message);
    assertTrue(message.contains("\n  - /b/lib.so (search_path: PATH): cannot be opened"), message);
    assertTrue(message.contains("Xbbg.setSdkPath, BLPAPI_LIB_DIR, BLPAPI_ROOT"), message);
    assertTrue(message.contains(temp.resolve("cache").resolve("sdk").toString()), message);
    assertTrue(message.contains("Xbbg.setSdkSource"), message);
    assertTrue(message.contains(BlpapiLocator.SOURCE_ENV), message);
  }

  @Test
  void theNotFoundMessageOmitsTheRejectedSectionWhenNothingWasTried() {
    String message =
        locator(null).notFound(Collections.<BlpapiLocator.Rejection>emptyList()).getMessage();
    assertFalse(message.contains("Rejected:"), message);
    assertTrue(message.contains("Searched, in order: BLPAPI_LIB_DIR"), message);
  }

  @Test
  void anInvalidPropertyPathIsReported() {
    properties.put(BlpapiLocator.PATH_PROPERTY, "bad\0path");
    assertThrows(IllegalArgumentException.class, () -> locator(null).requestJson());
  }
}
