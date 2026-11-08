package io.github.underloam.xbbg.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class BlpapiSdkCliTest {
  /** An environment with no variables. */
  private static String noEnv(String name) {
    return null;
  }

  private static BlpapiSdk.Cli parse(String... args) {
    return BlpapiSdk.Cli.parse(args, BlpapiSdkCliTest::noEnv);
  }

  private static Function<String, String> sourceEnv(String spec) {
    return name -> "XBBG_SDK_SOURCE".equals(name) ? spec : null;
  }

  /** Runs the CLI; returns the exit code, standard output and standard error. */
  private static String[] run(Function<String, String> env, String... args) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    int code = BlpapiSdk.run(args, env, SdkFixtures.utf8(out), SdkFixtures.utf8(err));
    return new String[] {
      Integer.toString(code),
      new String(out.toByteArray(), StandardCharsets.UTF_8),
      new String(err.toByteArray(), StandardCharsets.UTF_8)
    };
  }

  @Test
  void parsesEveryOption() {
    BlpapiSdk.Cli cli =
        parse(
            "--source",
            "conda:conda-forge",
            "--version",
            "3.26.9.1",
            "--platform",
            "linux-x86_64,macos-aarch64",
            "--cache-dir",
            "c",
            "--offline",
            "--proxy",
            "http://p:1",
            "--netrc",
            "credentials.netrc");
    assertEquals(BlpapiSource.conda("conda-forge"), cli.source);
    assertEquals("3.26.9.1", cli.version);
    assertEquals(Arrays.asList("linux-x86_64", "macos-aarch64"), cli.platformsOrDefault());
    assertEquals(Paths.get("c"), cli.cacheDir);
    assertTrue(cli.offline);
    assertEquals("http://p:1", cli.proxy);
    assertEquals(Paths.get("credentials.netrc"), cli.netrc);
    assertEquals(ProvisionOptions.PLATFORMS, parse("--installed", "--platform", "all").platforms);
    assertEquals(Collections.singletonList(null), parse("--installed").platformsOrDefault());
    assertTrue(parse("--source", "x", "--help").help);
  }

  @Test
  void rejectsBadCommandLines() {
    for (String[] args :
        new String[][] {
          {},
          {"--version", "1"},
          {"--source"},
          {"--source", "--offline"},
          {"--source", "conda-forge", "--platform", "linux-aarch64"},
          {"--source", "conda-forge", "--colour"},
          {"--source", "conda-forge", "--netrc"},
          {"--source", "pypi:"}
        }) {
      assertThrows(IllegalArgumentException.class, () -> parse(args), Arrays.toString(args));
    }
  }

  @Test
  void theSourceFallsBackToXbbgSdkSource() {
    String spec = "pypi:https://user:secret@mirror.example/simple/";
    BlpapiSdk.Cli fromEnv = BlpapiSdk.Cli.parse(new String[] {"--offline"}, sourceEnv(spec));
    assertEquals(BlpapiSource.parse(spec), fromEnv.source);
    assertTrue(fromEnv.offline);

    BlpapiSdk.Cli explicit =
        BlpapiSdk.Cli.parse(new String[] {"--source", "conda-forge"}, sourceEnv(spec));
    assertEquals(BlpapiSource.conda("conda-forge"), explicit.source);

    assertNull(BlpapiSdk.Cli.parse(new String[] {"--installed"}, sourceEnv(spec)).source);

    IllegalArgumentException missing =
        assertThrows(
            IllegalArgumentException.class,
            () -> BlpapiSdk.Cli.parse(new String[0], sourceEnv("  ")));
    assertTrue(missing.getMessage().contains("XBBG_SDK_SOURCE"), missing.getMessage());

    IllegalArgumentException malformed =
        assertThrows(
            IllegalArgumentException.class,
            () -> BlpapiSdk.Cli.parse(new String[0], sourceEnv("pypi:")));
    assertTrue(
        malformed.getMessage().startsWith("XBBG_SDK_SOURCE is not a valid source"),
        malformed.getMessage());
  }

  @Test
  void usageErrorsExitWithTwoWithoutLoadingNatives() {
    String[] bogus = run(BlpapiSdkCliTest::noEnv, "--bogus");
    assertEquals("2", bogus[0]);
    assertTrue(bogus[2].contains("unknown argument '--bogus'"), bogus[2]);
    assertTrue(bogus[2].contains("usage:"), bogus[2]);
    assertEquals("", bogus[1]);

    String[] help = run(BlpapiSdkCliTest::noEnv, "--help");
    assertEquals("0", help[0]);
    assertFalse(help[1].isEmpty());
  }

  @Test
  void invalidOptionValuesAreUsageErrors() {
    String[] proxy =
        run(BlpapiSdkCliTest::noEnv, "--source", "conda-forge", "--proxy", "ftp://u:secret@proxy");
    assertEquals("2", proxy[0], proxy[2]);
    assertTrue(proxy[2].contains("proxy URL must start with"), proxy[2]);
    assertTrue(proxy[2].contains("usage:"), proxy[2]);
    assertFalse(proxy[2].contains("secret"), proxy[2]);
    assertEquals("", proxy[1]);

    String[] version = run(BlpapiSdkCliTest::noEnv, "--source", "conda-forge", "--version", " ");
    assertEquals("2", version[0], version[2]);
    assertTrue(version[2].contains("version must not be blank"), version[2]);
    assertTrue(version[2].contains("usage:"), version[2]);

    String[] noSource = run(BlpapiSdkCliTest::noEnv, "--offline");
    assertEquals("2", noSource[0], noSource[2]);
    assertTrue(noSource[2].contains("XBBG_SDK_SOURCE"), noSource[2]);
  }

  @Test
  void usageWarnsAboutSecretsInArguments() {
    String usage = BlpapiSdk.Cli.USAGE;
    assertTrue(usage.contains("--netrc <file>"), usage);
    assertTrue(usage.contains("without --source, XBBG_SDK_SOURCE"), usage);
    assertTrue(usage.contains("other processes can read command-line arguments"), usage);
    assertTrue(usage.contains("Keep credentials out of --source and --proxy"), usage);
  }
}
