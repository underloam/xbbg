package io.github.underloam.xbbg.internal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.underloam.xbbg.BlpapiNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BlpapiLocatorTest {
  @TempDir Path temp;

  private final Map<String, String> env = new HashMap<String, String>();
  private final Map<String, String> properties = new HashMap<String, String>();

  private BlpapiLocator locator(Platform platform, Path explicit) {
    properties.put("user.dir", temp.toString());
    properties.put("user.home", temp.resolve("home").toString());
    return new BlpapiLocator(
        platform, lookup(env), lookup(properties), explicit, temp.resolve("cache"));
  }

  private static Function<String, String> lookup(final Map<String, String> values) {
    return new Function<String, String>() {
      @Override
      public String apply(String name) {
        return values.get(name);
      }
    };
  }

  private static Path library(Path dir, String name) throws IOException {
    Files.createDirectories(dir);
    Path file = dir.resolve(name);
    Files.write(file, ("runtime in " + dir.getFileName()).getBytes(StandardCharsets.UTF_8));
    return file.toAbsolutePath();
  }

  @Test
  void followsTheDocumentedCandidateOrder() throws IOException {
    String dll = "blpapi3_64.dll";
    Path libDir = library(temp.resolve("libdir"), dll);
    Path rootBin = library(temp.resolve("root").resolve("bin"), dll);
    library(temp.resolve("root").resolve("3.24.10.1").resolve("bin"), dll);
    Path newest = library(temp.resolve("root").resolve("3.25.0").resolve("lib"), dll);
    Path dev = library(temp.resolve("dev"), dll);
    env.put("BLPAPI_LIB_DIR", "libdir");
    env.put("BLPAPI_ROOT", temp.resolve("root").toString());
    env.put("XBBG_DEV_SDK_ROOT", "dev");

    assertEquals(libDir, locator(Platform.WINDOWS_X86_64, null).findLocal());
    Files.delete(libDir);
    assertEquals(rootBin, locator(Platform.WINDOWS_X86_64, null).findLocal());
    Files.delete(rootBin);
    assertEquals(newest, locator(Platform.WINDOWS_X86_64, null).findLocal());
    Files.delete(newest);
    Path older =
        temp.resolve("root").resolve("3.24.10.1").resolve("bin").resolve(dll).toAbsolutePath();
    assertEquals(older, locator(Platform.WINDOWS_X86_64, null).findLocal());
    Files.delete(older);
    assertEquals(dev, locator(Platform.WINDOWS_X86_64, null).findLocal());
  }

  @Test
  void explicitPathWinsAndMustHoldARuntime() throws IOException {
    Path file = library(temp.resolve("explicit"), "custom-name.so");
    env.put(
        "BLPAPI_LIB_DIR", library(temp.resolve("env"), "libblpapi3_64.so").getParent().toString());
    assertEquals(file, locator(Platform.LINUX_X86_64, file).findLocal());

    Path sdkRoot = temp.resolve("sdk");
    Path inLinuxDir = library(sdkRoot.resolve("Linux"), "libblpapi3_64.so");
    assertEquals(inLinuxDir, locator(Platform.LINUX_X86_64, sdkRoot).findLocal());

    Path empty = Files.createDirectories(temp.resolve("empty"));
    BlpapiNotFoundException error =
        assertThrows(
            BlpapiNotFoundException.class, () -> locator(Platform.LINUX_X86_64, empty).findLocal());
    assertEquals(
        "Xbbg.setSdkPath points to "
            + empty
            + ", which holds no Bloomberg runtime library (expected libblpapi3_64.so,"
            + " libblpapi3.so)",
        error.getMessage());
    assertEquals("SDK_NOT_FOUND", error.code());

    properties.put(BlpapiLocator.PATH_PROPERTY, "missing");
    BlpapiNotFoundException missing =
        assertThrows(
            BlpapiNotFoundException.class, () -> locator(Platform.LINUX_X86_64, null).findLocal());
    assertTrue(
        missing.getMessage().startsWith("-Dxbbg.blpapi.path points to "), missing.getMessage());
    assertTrue(missing.getMessage().contains("which does not exist"), missing.getMessage());
  }

  @Test
  void findsTheNewestValidProvisionedInstall() throws IOException {
    Path sdk = temp.resolve("cache").resolve("sdk");
    provisioned(
        sdk.resolve("3.24.10.1").resolve("linux-x86_64").resolve("pypi-aaaaaaaaaaaa"), true);
    Path newest =
        provisioned(
            sdk.resolve("3.25.1.1").resolve("linux-x86_64").resolve("conda-bbbbbbbbbbbb"), true);
    provisioned(
        sdk.resolve("3.26.0.1").resolve("macos-aarch64").resolve("pypi-cccccccccccc"), true);
    Path corrupt =
        provisioned(
            sdk.resolve("9.0.0.0").resolve("linux-x86_64").resolve("pypi-dddddddddddd"), false);
    Files.createDirectories(
        sdk.resolve("9.9.9.9").resolve("linux-x86_64").resolve("pypi-incomplete"));

    BlpapiLocator locator = locator(Platform.LINUX_X86_64, null);
    assertEquals(newest, locator.findLocal());

    Files.delete(newest);
    Files.delete(
        sdk.resolve("3.24.10.1")
            .resolve("linux-x86_64")
            .resolve("pypi-aaaaaaaaaaaa")
            .resolve("lib")
            .resolve("libblpapi3_64.so"));
    assertNull(locator.findLocal());
    String message = locator.notFound().getMessage();
    assertTrue(message.contains("skipped " + corrupt.getParent().getParent()), message);
    assertTrue(message.contains("library sha256 does not match provenance.json"), message);
  }

  @Test
  void skipsProvenanceThatEscapesTheInstall() throws IOException {
    Path install =
        temp.resolve("cache")
            .resolve("sdk")
            .resolve("3.25.1.1")
            .resolve("linux-x86_64")
            .resolve("pypi-x");
    library(temp.resolve("cache").resolve("sdk"), "libblpapi3_64.so");
    writeProvenance(install, "../../../libblpapi3_64.so", Hashes.sha256Hex(new byte[0]));
    BlpapiLocator locator = locator(Platform.LINUX_X86_64, null);
    assertNull(locator.findLocal());
    assertTrue(
        locator.notFound().getMessage().contains("lib is not a relative path inside the install"));
  }

  @Test
  void notFoundListsSearchedPlacesAndEveryOption() {
    env.put("BLPAPI_LIB_DIR", "nowhere");
    BlpapiLocator locator = locator(Platform.LINUX_X86_64, null);
    assertNull(locator.findLocal());
    BlpapiNotFoundException error = locator.notFound();
    String message = error.getMessage();
    assertTrue(
        message.startsWith(
            "Bloomberg runtime library not found (looked for libblpapi3_64.so, libblpapi3.so)."),
        message);
    assertTrue(message.contains(temp.resolve("nowhere") + " (BLPAPI_LIB_DIR)"), message);
    assertTrue(
        message.contains(
            temp.resolve("home").resolve("blp").resolve("DAPI") + " (Bloomberg Terminal)"),
        message);
    for (String option :
        Arrays.asList(
            "Xbbg.setSdkPath(path) or -Dxbbg.blpapi.path=",
            "BLPAPI_LIB_DIR=",
            "BLPAPI_ROOT",
            "XBBG_DEV_SDK_ROOT",
            "Bloomberg Terminal (DAPI)",
            "Xbbg.setSdkSource(BlpapiSource.parse(spec))",
            "-Dxbbg.sdk.source=<spec>",
            "XBBG_SDK_SOURCE=<spec>",
            "io.github.underloam.xbbg.sdk.BlpapiSdk --source <spec>")) {
      assertTrue(message.contains(option), option + " missing from: " + message);
    }
    assertEquals("SDK_NOT_FOUND", error.code());
  }

  @Test
  void sourceSettingsPreferSystemProperties() {
    BlpapiLocator locator = locator(Platform.LINUX_X86_64, null);
    assertNull(locator.configuredSourceSpec());
    env.put(BlpapiLocator.SOURCE_ENV, "conda:env-channel");
    env.put(BlpapiLocator.VERSION_ENV, "3.24.10.1");
    assertEquals("conda:env-channel", locator.configuredSourceSpec());
    assertEquals("3.24.10.1", locator.configuredVersion());
    properties.put(BlpapiLocator.SOURCE_PROPERTY, "pypi:https://example.invalid/simple");
    properties.put(BlpapiLocator.VERSION_PROPERTY, " ");
    assertEquals("pypi:https://example.invalid/simple", locator.configuredSourceSpec());
    assertEquals("3.24.10.1", locator.configuredVersion(), "blank properties are ignored");
  }

  @Test
  void windowsTerminalRootsFollowTheJavaScriptOrder() {
    env.put("SystemDrive", "D:");
    env.put("ProgramFiles", "D:\\Program Files");
    env.put("ProgramFiles(x86)", "D:\\Program Files (x86)\\");
    env.put("USERPROFILE", "D:\\Users\\someone");
    assertEquals(
        Arrays.asList(
            "D:\\blp\\DAPI",
            "C:\\blp\\DAPI",
            "D:\\Program Files\\Bloomberg\\Blp\\DAPI",
            "D:\\Program Files\\Bloomberg\\Blp\\DAPI",
            "D:\\Program Files (x86)\\Bloomberg\\Blp\\DAPI",
            "D:\\Program Files (x86)\\Bloomberg\\Blp\\DAPI",
            "D:\\Users\\someone\\AppData\\Local\\Bloomberg\\DAPI",
            "D:\\Users\\someone\\AppData\\Local\\Bloomberg\\Blp\\DAPI"),
        locator(Platform.WINDOWS_X86_64, null).windowsDapiRoots());
    env.put("LOCALAPPDATA", "E:\\Local");
    env.remove("SystemDrive");
    List<String> roots = locator(Platform.WINDOWS_X86_64, null).windowsDapiRoots();
    assertEquals("C:\\blp\\DAPI", roots.get(0));
    assertEquals("E:\\Local\\Bloomberg\\Blp\\DAPI", roots.get(roots.size() - 1));
  }

  @Test
  void windowsCandidatesAreDeduplicatedIgnoringCaseAndQuotes() {
    env.put("BLPAPI_LIB_DIR", temp.resolve("Runtime").toString());
    env.put(
        "PATH",
        "\"" + temp.resolve("RUNTIME") + "\";" + temp.resolve("other") + ";;C:\\blp\\DAPI\\");
    List<String> sources = new ArrayList<String>();
    List<String> dirs = new ArrayList<String>();
    for (String[] candidate : locator(Platform.WINDOWS_X86_64, null).candidates()) {
      dirs.add(candidate[0]);
      sources.add(candidate[1]);
    }
    assertEquals(temp.resolve("Runtime").toString(), dirs.get(0));
    assertEquals("BLPAPI_LIB_DIR", sources.get(0));
    assertEquals(1, countIgnoringCase(dirs, temp.resolve("runtime").toString()));
    assertEquals(1, countIgnoringCase(dirs, "C:\\blp\\DAPI"));
    assertEquals(temp.resolve("other").toString(), dirs.get(dirs.size() - 1));
    assertEquals("PATH", sources.get(sources.size() - 1));
  }

  @Test
  void versionOrderingPutsNumericVersionsFirst() {
    List<Path> dirs =
        new ArrayList<Path>(
            Arrays.asList(
                temp.resolve("beta"),
                temp.resolve("3.9"),
                temp.resolve("3.24.10.1"),
                temp.resolve("alpha"),
                temp.resolve("3.24.9")));
    dirs.sort(BlpapiLocator.NEWEST_VERSION_FIRST);
    List<String> names = new ArrayList<String>();
    for (Path dir : dirs) {
      names.add(dir.getFileName().toString());
    }
    assertEquals(Arrays.asList("3.24.10.1", "3.24.9", "3.9", "beta", "alpha"), names);
  }

  private static int countIgnoringCase(List<String> values, String target) {
    int count = 0;
    for (String value : values) {
      if (value
          .replace('/', '\\')
          .replaceAll("\\\\+$", "")
          .equalsIgnoreCase(target.replace('/', '\\'))) {
        count++;
      }
    }
    return count;
  }

  private Path provisioned(Path install, boolean validHash) throws IOException {
    Path lib = library(install.resolve("lib"), "libblpapi3_64.so");
    String sha = validHash ? Hashes.sha256Hex(lib) : Hashes.sha256Hex(new byte[] {1});
    writeProvenance(install, "lib/libblpapi3_64.so", sha);
    return lib;
  }

  private void writeProvenance(Path install, String lib, String sha) throws IOException {
    Files.createDirectories(install);
    String platform = install.getParent().getFileName().toString();
    Path provenance = install.resolve("provenance.json");
    Files.write(
        provenance,
        Json.write(
                new java.util.LinkedHashMap<String, Object>() {
                  private static final long serialVersionUID = 1L;

                  {
                    put("version", install.getParent().getParent().getFileName().toString());
                    put("platform", platform);
                    put("source", "pypi:https://example.invalid/simple");
                    put("artifactUrl", "https://example.invalid/blpapi.whl");
                    put("artifactSha256", sha);
                    put("lib", lib);
                    put("libSha256", sha);
                    put("licenses", Arrays.asList("licenses/LICENSE"));
                  }
                })
            .getBytes(StandardCharsets.UTF_8));
    Files.setLastModifiedTime(provenance, FileTime.fromMillis(System.currentTimeMillis()));
  }

  @Test
  void malformedEnvironmentPathsAreSkippedAndTheSearchGoesOn() throws IOException {
    // NUL is invalid in a path on every platform.
    String malformed = "C:\\blp\u0000\\API";
    Path rootBin = library(temp.resolve("root").resolve("bin"), "blpapi3_64.dll");
    env.put("BLPAPI_LIB_DIR", malformed);
    env.put("BLPAPI_ROOT", temp.resolve("root").toString());
    env.put("XBBG_DEV_SDK_ROOT", malformed + "2");
    BlpapiLocator windows = locator(Platform.WINDOWS_X86_64, null);
    assertEquals(rootBin, windows.findLocal());
    assertEquals(
        malformed + " (BLPAPI_LIB_DIR, not a valid path: skipped)", windows.searched().get(0));

    // A malformed root is skipped too, and the later candidates are still searched.
    Files.delete(rootBin);
    env.put("BLPAPI_ROOT", malformed + "3");
    Path linuxLib =
        library(temp.resolve("home").resolve("blp").resolve("DAPI"), "libblpapi3_64.so");
    BlpapiLocator linux = locator(Platform.LINUX_X86_64, null);
    assertEquals(linuxLib, linux.findLocal());
    assertTrue(
        linux.searched().contains(malformed + "3 (BLPAPI_ROOT, not a valid path: skipped)"),
        linux.searched().toString());
    assertTrue(
        linux.searched().contains(malformed + "2 (XBBG_DEV_SDK_ROOT, not a valid path: skipped)"),
        linux.searched().toString());
  }

  @Test
  void versionNamesParseOrGiveNoParts() {
    assertArrayEquals(new int[] {3, 24, 10}, BlpapiLocator.versionParts("3.24.10"));
    assertEquals(0, BlpapiLocator.versionParts("").length);
    assertEquals(0, BlpapiLocator.versionParts("3..1").length);
    assertEquals(0, BlpapiLocator.versionParts("v3.1").length);
    assertEquals(0, BlpapiLocator.versionParts("1234567890").length);
  }
}
