package io.github.underloam.xbbg.internal;

import io.github.underloam.xbbg.BlpapiNotFoundException;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Finds the Bloomberg runtime library (blpapi) without loading anything. Candidates, in order:
 *
 * <ol>
 *   <li>the explicit path ({@code Xbbg.setSdkPath}, else system property {@code xbbg.blpapi.path}):
 *       a library file or a directory; an explicit path without a runtime is an error;
 *   <li>{@code BLPAPI_LIB_DIR}; {@code BLPAPI_ROOT} and {@code XBBG_DEV_SDK_ROOT} as SDK roots (the
 *       root, {@code bin}, {@code lib}, {@code Lib}, {@code lib/win64}, {@code lib/win32} on
 *       Windows; the root, {@code lib}, {@code Linux} or {@code Darwin} elsewhere; then the same
 *       layouts under versioned subdirectories, newest first);
 *   <li>the Bloomberg Terminal DAPI directories, then (Windows) each {@code PATH} entry;
 *   <li>the newest valid install provisioned under {@code <cache>/sdk/<version>/<platform>/}, whose
 *       library still matches the sha256 recorded in {@code provenance.json}.
 * </ol>
 *
 * The first directory holding one of {@link Platform#blpapiLibraryNames()} wins. Environment and
 * system-property lookups are injectable for tests.
 *
 * <p>Internal API: not covered by compatibility guarantees.
 */
public final class BlpapiLocator {
  /** System property naming the runtime library or its directory. */
  public static final String PATH_PROPERTY = "xbbg.blpapi.path";

  /** System property naming a provisioning source spec. */
  public static final String SOURCE_PROPERTY = "xbbg.sdk.source";

  /** Environment variable naming a provisioning source spec. */
  public static final String SOURCE_ENV = "XBBG_SDK_SOURCE";

  /** System property naming the runtime version to provision. */
  public static final String VERSION_PROPERTY = "xbbg.sdk.version";

  /** Environment variable naming the runtime version to provision. */
  public static final String VERSION_ENV = "XBBG_SDK_VERSION";

  private final Platform platform;
  private final Function<String, String> env;
  private final Function<String, String> properties;
  private final Path explicitPath;
  private final Path cacheRoot;
  private final List<String> searched = new ArrayList<String>();
  private final List<String> skippedInstalls = new ArrayList<String>();

  /**
   * Creates a locator.
   *
   * @param platform the platform whose library names and layouts apply
   * @param env environment lookup (returns {@code null} when unset)
   * @param properties system-property lookup (returns {@code null} when unset)
   * @param explicitPath the path set with {@code Xbbg.setSdkPath}, or {@code null}
   * @param cacheRoot the xbbg cache root
   */
  public BlpapiLocator(
      Platform platform,
      Function<String, String> env,
      Function<String, String> properties,
      Path explicitPath,
      Path cacheRoot) {
    this.platform = platform;
    this.env = env;
    this.properties = properties;
    this.explicitPath = explicitPath;
    this.cacheRoot = cacheRoot;
  }

  /**
   * Creates a locator for the running process.
   *
   * @param explicitPath the path set with {@code Xbbg.setSdkPath}, or {@code null}
   * @param cacheRoot the xbbg cache root
   * @return the locator
   */
  public static BlpapiLocator forCurrentProcess(Path explicitPath, Path cacheRoot) {
    return new BlpapiLocator(
        Platform.current(),
        new Function<String, String>() {
          @Override
          public String apply(String name) {
            return System.getenv(name);
          }
        },
        new Function<String, String>() {
          @Override
          public String apply(String name) {
            return System.getProperty(name);
          }
        },
        explicitPath,
        cacheRoot);
  }

  /**
   * Searches the local candidates and the provisioned cache.
   *
   * @return the library file, or {@code null} when none was found
   * @throws BlpapiNotFoundException if an explicit path holds no runtime library
   */
  public Path findLocal() {
    searched.clear();
    skippedInstalls.clear();
    Path explicit = explicitPath;
    String explicitLabel = "Xbbg.setSdkPath";
    if (explicit == null) {
      String property = blankToNull(properties.apply(PATH_PROPERTY));
      if (property != null) {
        explicit = resolve(property);
        explicitLabel = "-D" + PATH_PROPERTY;
      }
    }
    if (explicit != null) {
      return findExplicit(explicit, explicitLabel);
    }
    for (String[] candidate : candidates()) {
      Path found = libraryIn(candidate[0]);
      searched.add(candidate[0] + " (" + candidate[1] + ")");
      if (found != null) {
        return found;
      }
    }
    return findProvisioned();
  }

  /**
   * Returns the configured provisioning source spec ({@code xbbg.sdk.source}, else {@code
   * XBBG_SDK_SOURCE}).
   *
   * @return the spec, or {@code null}
   */
  public String configuredSourceSpec() {
    String property = blankToNull(properties.apply(SOURCE_PROPERTY));
    return property != null ? property : blankToNull(env.apply(SOURCE_ENV));
  }

  /**
   * Returns the configured runtime version to provision ({@code xbbg.sdk.version}, else {@code
   * XBBG_SDK_VERSION}).
   *
   * @return the version, or {@code null} for the newest
   */
  public String configuredVersion() {
    String property = blankToNull(properties.apply(VERSION_PROPERTY));
    return property != null ? property : blankToNull(env.apply(VERSION_ENV));
  }

  /**
   * Builds the error raised when no runtime was found and none is configured for provisioning.
   *
   * @return the exception, listing what was searched and every way to supply a runtime
   */
  public BlpapiNotFoundException notFound() {
    StringBuilder out = new StringBuilder();
    out.append("Bloomberg runtime library not found (looked for ")
        .append(String.join(", ", platform.blpapiLibraryNames()))
        .append(").");
    if (!searched.isEmpty()) {
      out.append("\nSearched:");
      for (String entry : searched) {
        out.append("\n  - ").append(entry);
      }
    }
    out.append("\nSearched provisioned runtimes under ").append(cacheRoot.resolve("sdk"));
    for (String skipped : skippedInstalls) {
      out.append("\n  - skipped ").append(skipped);
    }
    out.append("\nProvide the runtime with any of these:")
        .append("\n  1. Xbbg.setSdkPath(path) or -D")
        .append(PATH_PROPERTY)
        .append("=<library file or directory>")
        .append("\n  2. BLPAPI_LIB_DIR=<directory holding the library>, or BLPAPI_ROOT /")
        .append(" XBBG_DEV_SDK_ROOT=<Bloomberg C++ SDK root>")
        .append("\n  3. Bloomberg Terminal (DAPI): detected automatically when installed")
        .append("\n  4. A provisioned runtime: Xbbg.setSdkSource(BlpapiSource.parse(spec)), -D")
        .append(SOURCE_PROPERTY)
        .append("=<spec> or ")
        .append(SOURCE_ENV)
        .append("=<spec> (optional -D")
        .append(VERSION_PROPERTY)
        .append(" / ")
        .append(VERSION_ENV)
        .append(
            "), for example pypi:https://blpapi.bloomberg.com/repository/releases/python/simple/;")
        .append(" or download ahead of time with")
        .append(" java -cp xbbg.jar io.github.underloam.xbbg.sdk.BlpapiSdk --source <spec>");
    return new BlpapiNotFoundException(out.toString());
  }

  /**
   * Returns the directories searched by the last {@link #findLocal()}, each with its source.
   *
   * @return the searched directories
   */
  public List<String> searched() {
    return Collections.unmodifiableList(searched);
  }

  /**
   * Returns the ordered local candidate directories (steps 2 and 3), each as {@code {directory,
   * source}}. An environment value that is not a valid path is listed as skipped (it can hold no
   * runtime) and the search goes on, as in JavaScript.
   *
   * @return the candidates, deduplicated
   */
  List<String[]> candidates() {
    List<String[]> candidates = new ArrayList<String[]>();
    Set<String> seen = new HashSet<String>();
    String libDir = blankToNull(env.apply("BLPAPI_LIB_DIR"));
    if (libDir != null) {
      Path dir = resolveOrNull(libDir);
      if (dir == null) {
        add(candidates, seen, libDir, "BLPAPI_LIB_DIR, not a valid path: skipped");
      } else {
        add(candidates, seen, dir.toString(), "BLPAPI_LIB_DIR");
      }
    }
    addEnvSdkRoot(candidates, seen, "BLPAPI_ROOT");
    addEnvSdkRoot(candidates, seen, "XBBG_DEV_SDK_ROOT");
    if (platform.isWindows()) {
      for (String dapi : windowsDapiRoots()) {
        add(candidates, seen, dapi, "Bloomberg Terminal");
      }
      String path = env.apply("PATH");
      if (path != null) {
        for (String entry : path.split(";", -1)) {
          String trimmed = stripQuotes(entry.trim());
          if (!trimmed.isEmpty()) {
            add(candidates, seen, trimmed, "PATH");
          }
        }
      }
    } else {
      String home = blankToNull(properties.apply("user.home"));
      Path homeDapi = home == null ? null : resolveOrNull(home);
      if (homeDapi != null) {
        add(
            candidates,
            seen,
            homeDapi.resolve("blp").resolve("DAPI").toString(),
            "Bloomberg Terminal");
      }
      add(candidates, seen, "/opt/bloomberg/DAPI", "Bloomberg Terminal");
    }
    return candidates;
  }

  private void addEnvSdkRoot(List<String[]> candidates, Set<String> seen, String variable) {
    String value = blankToNull(env.apply(variable));
    if (value == null) {
      return;
    }
    Path root = resolveOrNull(value);
    if (root == null) {
      add(candidates, seen, value, variable + ", not a valid path: skipped");
    } else {
      addSdkRoot(candidates, seen, root, variable);
    }
  }

  List<String> windowsDapiRoots() {
    List<String> roots = new ArrayList<String>();
    String systemDrive = windowsDrive(env.apply("SystemDrive"));
    String systemRoot = systemDrive + "\\";
    roots.add(systemRoot + "blp\\DAPI");
    if (!systemDrive.equalsIgnoreCase("c:")) {
      roots.add("C:\\blp\\DAPI");
    }
    String[] programFiles = {
      env.apply("ProgramFiles"),
      systemRoot + "Program Files",
      env.apply("ProgramFiles(x86)"),
      systemRoot + "Program Files (x86)"
    };
    for (String base : programFiles) {
      if (base != null && !base.isEmpty()) {
        roots.add(windowsJoin(base, "Bloomberg\\Blp\\DAPI"));
      }
    }
    String localAppData = blankToNull(env.apply("LOCALAPPDATA"));
    if (localAppData == null) {
      String profile = blankToNull(env.apply("USERPROFILE"));
      if (profile != null) {
        localAppData = windowsJoin(profile, "AppData\\Local");
      }
    }
    if (localAppData != null) {
      roots.add(windowsJoin(localAppData, "Bloomberg\\DAPI"));
      roots.add(windowsJoin(localAppData, "Bloomberg\\Blp\\DAPI"));
    }
    return roots;
  }

  private void addSdkRoot(List<String[]> candidates, Set<String> seen, Path root, String source) {
    for (Path layout : sdkLayouts(root)) {
      add(candidates, seen, layout.toString(), source);
    }
    for (Path versioned : versionedChildren(root)) {
      for (Path layout : sdkLayouts(versioned)) {
        add(candidates, seen, layout.toString(), source);
      }
    }
  }

  private List<Path> sdkLayouts(Path root) {
    List<Path> layouts = new ArrayList<Path>();
    layouts.add(root);
    if (platform.isWindows()) {
      layouts.add(root.resolve("bin"));
      layouts.add(root.resolve("lib"));
      layouts.add(root.resolve("Lib"));
      layouts.add(root.resolve("lib").resolve("win64"));
      layouts.add(root.resolve("lib").resolve("win32"));
    } else {
      layouts.add(root.resolve("lib"));
      layouts.add(root.resolve(platform == Platform.LINUX_X86_64 ? "Linux" : "Darwin"));
    }
    return layouts;
  }

  private static List<Path> versionedChildren(Path root) {
    List<Path> children = new ArrayList<Path>();
    if (!Files.isDirectory(root)) {
      return children;
    }
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(root)) {
      for (Path child : stream) {
        if (Files.isDirectory(child) && versionParts(fileName(child)).length > 0) {
          children.add(child);
        }
      }
    } catch (IOException e) {
      return children;
    }
    Collections.sort(children, NEWEST_VERSION_FIRST);
    return children;
  }

  private Path findExplicit(Path explicit, String label) {
    searched.add(explicit + " (" + label + ")");
    if (Files.isRegularFile(explicit)) {
      return explicit;
    }
    if (Files.isDirectory(explicit)) {
      for (Path layout : sdkLayouts(explicit)) {
        Path found = libraryIn(layout.toString());
        if (found != null) {
          return found;
        }
      }
      for (Path versioned : versionedChildren(explicit)) {
        for (Path layout : sdkLayouts(versioned)) {
          Path found = libraryIn(layout.toString());
          if (found != null) {
            return found;
          }
        }
      }
    }
    throw new BlpapiNotFoundException(
        label
            + " points to "
            + explicit
            + (Files.exists(explicit)
                ? ", which holds no Bloomberg runtime library ("
                : ", which does not exist (")
            + "expected "
            + String.join(", ", platform.blpapiLibraryNames())
            + ")");
  }

  private Path libraryIn(String directory) {
    Path dir;
    try {
      dir = Paths.get(directory);
    } catch (InvalidPathException e) {
      return null;
    }
    for (String name : platform.blpapiLibraryNames()) {
      Path candidate = dir.resolve(name);
      if (Files.isRegularFile(candidate)) {
        return candidate.toAbsolutePath();
      }
    }
    return null;
  }

  private Path findProvisioned() {
    Path sdkRoot = cacheRoot.resolve("sdk");
    if (!Files.isDirectory(sdkRoot)) {
      return null;
    }
    List<Path> versions = new ArrayList<Path>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(sdkRoot)) {
      for (Path child : stream) {
        if (Files.isDirectory(child)) {
          versions.add(child);
        }
      }
    } catch (IOException e) {
      skippedInstalls.add(sdkRoot + ": " + e.getMessage());
      return null;
    }
    Collections.sort(versions, NEWEST_VERSION_FIRST);
    for (Path version : versions) {
      Path platformDir = version.resolve(platform.id());
      if (!Files.isDirectory(platformDir)) {
        continue;
      }
      for (Path install : installsNewestFirst(platformDir)) {
        Path library = validInstall(install);
        if (library != null) {
          return library;
        }
      }
    }
    return null;
  }

  private List<Path> installsNewestFirst(Path platformDir) {
    final List<Path> installs = new ArrayList<Path>();
    final Map<Path, Long> modified = new java.util.HashMap<Path, Long>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(platformDir)) {
      for (Path child : stream) {
        Path provenance = child.resolve("provenance.json");
        if (Files.isRegularFile(provenance)) {
          installs.add(child);
          modified.put(child, Files.getLastModifiedTime(provenance).toMillis());
        }
      }
    } catch (IOException e) {
      skippedInstalls.add(platformDir + ": " + e.getMessage());
      return installs;
    }
    Collections.sort(
        installs,
        (left, right) -> {
          int byTime = Long.compare(modified.get(right), modified.get(left));
          return byTime != 0 ? byTime : fileName(left).compareTo(fileName(right));
        });
    return installs;
  }

  private Path validInstall(Path install) {
    Map<String, Object> provenance;
    try {
      String text =
          new String(
              Files.readAllBytes(install.resolve("provenance.json")), StandardCharsets.UTF_8);
      provenance = JsonFields.object(Json.parse(text), "provenance.json");
    } catch (IOException | RuntimeException e) {
      skippedInstalls.add(install + ": unreadable provenance.json (" + e.getMessage() + ")");
      return null;
    }
    Object platformValue = provenance.get("platform");
    Object libValue = provenance.get("lib");
    Object shaValue = provenance.get("libSha256");
    if (!platform.id().equals(platformValue)) {
      skippedInstalls.add(install + ": provenance.json platform is " + platformValue);
      return null;
    }
    if (!(libValue instanceof String) || !(shaValue instanceof String)) {
      skippedInstalls.add(install + ": provenance.json lacks lib or libSha256");
      return null;
    }
    String relative = (String) libValue;
    if (relative.isEmpty()
        || relative.startsWith("/")
        || relative.contains("\\")
        || relative.contains(":")
        || ("/" + relative + "/").contains("/../")) {
      skippedInstalls.add(
          install + ": provenance.json lib is not a relative path inside the install");
      return null;
    }
    Path library = install.resolve(relative.replace('/', File.separatorChar)).normalize();
    if (!library.startsWith(install.normalize()) || !Files.isRegularFile(library)) {
      skippedInstalls.add(install + ": library " + relative + " is missing");
      return null;
    }
    try {
      String actual = Hashes.sha256Hex(library);
      if (!actual.equalsIgnoreCase((String) shaValue)) {
        skippedInstalls.add(install + ": library sha256 does not match provenance.json");
        return null;
      }
    } catch (IOException e) {
      skippedInstalls.add(install + ": cannot hash library (" + e.getMessage() + ")");
      return null;
    }
    return library.toAbsolutePath();
  }

  private Path resolve(String value) {
    Path path = Paths.get(value);
    if (path.isAbsolute()) {
      return path.normalize();
    }
    String workingDir = properties.apply("user.dir");
    Path base = workingDir == null ? Paths.get("").toAbsolutePath() : Paths.get(workingDir);
    return base.resolve(path).normalize();
  }

  /** {@link #resolve(String)}, or {@code null} for a value that is not a valid path. */
  private Path resolveOrNull(String value) {
    try {
      return resolve(value);
    } catch (InvalidPathException e) {
      return null;
    }
  }

  /** The last name of a path, or the whole path for a root. */
  private static String fileName(Path path) {
    Path name = path.getFileName();
    return name == null ? path.toString() : name.toString();
  }

  private void add(List<String[]> candidates, Set<String> seen, String candidate, String source) {
    if (candidate == null || candidate.isEmpty()) {
      return;
    }
    if (seen.add(key(candidate))) {
      candidates.add(new String[] {candidate, source});
    }
  }

  private String key(String candidate) {
    if (!platform.isWindows()) {
      try {
        return Paths.get(candidate).toAbsolutePath().normalize().toString();
      } catch (InvalidPathException e) {
        return candidate;
      }
    }
    String normalized = candidate.replace('/', '\\');
    StringBuilder out = new StringBuilder(normalized.length());
    for (int i = 0; i < normalized.length(); i++) {
      char c = normalized.charAt(i);
      if (c == '\\' && i > 1 && out.length() > 0 && out.charAt(out.length() - 1) == '\\') {
        continue;
      }
      out.append(c);
    }
    while (out.length() > 3 && out.charAt(out.length() - 1) == '\\') {
      out.setLength(out.length() - 1);
    }
    return out.toString().toLowerCase(Locale.ROOT);
  }

  private static String windowsDrive(String value) {
    if (value != null
        && value.length() >= 2
        && Character.isLetter(value.charAt(0))
        && value.charAt(1) == ':') {
      return value.substring(0, 2);
    }
    return "C:";
  }

  private static String windowsJoin(String base, String child) {
    return base.endsWith("\\") || base.endsWith("/") ? base + child : base + "\\" + child;
  }

  private static String stripQuotes(String value) {
    if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
      return value.substring(1, value.length() - 1);
    }
    return value;
  }

  private static String blankToNull(String value) {
    return value == null || value.trim().isEmpty() ? null : value;
  }

  /** The numeric parts of a dotted version name ({@code 3.24.10}); empty when it is not one. */
  static int[] versionParts(String name) {
    if (name.isEmpty()) {
      return new int[0];
    }
    String[] pieces = name.split("\\.", -1);
    int[] parts = new int[pieces.length];
    for (int i = 0; i < pieces.length; i++) {
      String piece = pieces[i];
      if (piece.isEmpty() || piece.length() > 9) {
        return new int[0];
      }
      for (int j = 0; j < piece.length(); j++) {
        if (piece.charAt(j) < '0' || piece.charAt(j) > '9') {
          return new int[0];
        }
      }
      parts[i] = Integer.parseInt(piece);
    }
    return parts;
  }

  /** Version-named directories newest first, then the others in reverse name order. */
  static final Comparator<Path> NEWEST_VERSION_FIRST = BlpapiLocator::compareNewestVersionFirst;

  private static int compareNewestVersionFirst(Path left, Path right) {
    String leftName = fileName(left);
    String rightName = fileName(right);
    int[] leftParts = versionParts(leftName);
    int[] rightParts = versionParts(rightName);
    if (leftParts.length > 0 && rightParts.length > 0) {
      int length = Math.max(leftParts.length, rightParts.length);
      for (int i = 0; i < length; i++) {
        int l = i < leftParts.length ? leftParts[i] : 0;
        int r = i < rightParts.length ? rightParts[i] : 0;
        if (l != r) {
          return Integer.compare(r, l);
        }
      }
      return rightName.compareTo(leftName);
    }
    if (leftParts.length > 0) {
      return -1;
    }
    if (rightParts.length > 0) {
      return 1;
    }
    return rightName.compareTo(leftName);
  }
}
