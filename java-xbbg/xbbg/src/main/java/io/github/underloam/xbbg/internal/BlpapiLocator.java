package io.github.underloam.xbbg.internal;

import io.github.underloam.xbbg.BlpapiNotFoundException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * What Java alone knows about where the Bloomberg runtime (blpapi) is, and how to report that none
 * was found. The search itself lives in Rust ({@code xbbg_host::sdk}, reached through {@link
 * NativeXbbg#loadSdk}), the same code every other xbbg host uses, in one order: the explicit path
 * (which is authoritative: when it does not load, nothing else is tried), {@code BLPAPI_LIB_DIR},
 * {@code BLPAPI_ROOT}, {@code XBBG_DEV_SDK_ROOT}, the Bloomberg Terminal, the operating system's
 * library search path variables and its own library search, and the verified installs of the xbbg
 * cache.
 *
 * <p>Java contributes:
 *
 * <ul>
 *   <li>the explicit path: {@code Xbbg.setSdkPath}, else system property {@code xbbg.blpapi.path}
 *       (a library file or a directory; relative values resolve against {@code user.dir});
 *   <li>the cache root ({@link NativeLibraries#cacheRoot()}, which honours {@code xbbg.cache.dir});
 *   <li>the provisioning source and version ({@code Xbbg.setSdkSource}, {@code xbbg.sdk.source},
 *       {@code XBBG_SDK_SOURCE}, {@code xbbg.sdk.version}, {@code XBBG_SDK_VERSION}), used by
 *       {@link NativeLibraries} when the search finds nothing.
 * </ul>
 *
 * Environment and system-property lookups are injectable for tests.
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

  private final Function<String, String> env;
  private final Function<String, String> properties;
  private final Path explicitPath;
  private final Path cacheRoot;

  /**
   * Creates a locator.
   *
   * @param env environment lookup (returns {@code null} when unset)
   * @param properties system-property lookup (returns {@code null} when unset)
   * @param explicitPath the path set with {@code Xbbg.setSdkPath}, or {@code null}
   * @param cacheRoot the xbbg cache root
   */
  public BlpapiLocator(
      Function<String, String> env,
      Function<String, String> properties,
      Path explicitPath,
      Path cacheRoot) {
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
   * Builds the request {@code NativeXbbg.loadSdk} takes: a JSON object with {@code explicit} (when
   * the explicit path is set) and {@code cacheRoot}. The environment is left to the native side,
   * which reads the process environment itself.
   *
   * @return the request JSON
   */
  public String requestJson() {
    return Json.object()
        .put("explicit", explicit())
        .put("cacheRoot", cacheRoot.toString())
        .toString();
  }

  /** The explicit location, as an absolute path string, or {@code null}. */
  private String explicit() {
    if (explicitPath != null) {
      return explicitPath.toString();
    }
    String property = blankToNull(properties.apply(PATH_PROPERTY));
    return property == null ? null : resolve(property).toString();
  }

  /**
   * Describes where the explicit location comes from, for messages.
   *
   * @return the setter or system property, or {@code null} when no explicit location is set
   */
  String explicitLabel() {
    if (explicitPath != null) {
      return "Xbbg.setSdkPath";
    }
    return blankToNull(properties.apply(PATH_PROPERTY)) == null ? null : "-D" + PATH_PROPERTY;
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
   * Builds the error raised when no runtime could be loaded and none is configured for
   * provisioning.
   *
   * <p>An explicit location is authoritative: when one is set, it is all that was searched, and the
   * message says so instead of listing an order that was not walked.
   *
   * @param rejected every candidate the search tried and why it was refused
   * @return the exception, listing what was rejected and how to supply a runtime
   */
  public BlpapiNotFoundException notFound(List<Rejection> rejected) {
    StringBuilder out = new StringBuilder("Bloomberg runtime library not found.");
    if (!rejected.isEmpty()) {
      out.append("\nRejected:");
      for (Rejection entry : rejected) {
        out.append("\n  - ").append(entry);
      }
    }
    String rest =
        "BLPAPI_LIB_DIR, BLPAPI_ROOT, XBBG_DEV_SDK_ROOT, the Bloomberg Terminal, the operating"
            + " system's library search path and its own library search, and the verified runtimes"
            + " under "
            + cacheRoot.resolve("sdk");
    String label = explicitLabel();
    if (label != null) {
      out.append("\n")
          .append(label)
          .append(" names the runtime to use, so nothing else was searched. Correct it, or clear")
          .append(" it to search, in order: ")
          .append(rest)
          .append(".");
      return new BlpapiNotFoundException(out.toString());
    }
    out.append("\nSearched, in order: ").append(rest).append(".");
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
   * Reads the {@code rejected} array of a native {@code loadSdk} error.
   *
   * @param values the parsed array
   * @return the rejections, in the order the search tried them
   */
  static List<Rejection> parseRejected(List<Object> values) {
    List<Rejection> rejected = new ArrayList<Rejection>(values.size());
    for (Object value : values) {
      Map<String, Object> entry = JsonFields.object(value, "rejected runtime");
      rejected.add(
          new Rejection(
              JsonFields.string(entry, "path"),
              JsonFields.string(entry, "source"),
              JsonFields.optString(entry, "detail"),
              JsonFields.string(entry, "reason")));
    }
    return Collections.unmodifiableList(rejected);
  }

  private Path resolve(String value) {
    Path path;
    try {
      path = Paths.get(value);
    } catch (InvalidPathException e) {
      throw new IllegalArgumentException(
          PATH_PROPERTY + " is not a valid path: " + e.getMessage(), e);
    }
    if (path.isAbsolute()) {
      return path.normalize();
    }
    String workingDir = properties.apply("user.dir");
    Path base = workingDir == null ? Paths.get("").toAbsolutePath() : Paths.get(workingDir);
    return base.resolve(path).normalize();
  }

  private static String blankToNull(String value) {
    return value == null || value.trim().isEmpty() ? null : value;
  }

  /** A candidate the search refused: where it was, which rule found it, and why it was refused. */
  static final class Rejection {
    final String path;
    final String source;
    final String detail;
    final String reason;

    Rejection(String path, String source, String detail, String reason) {
      this.path = path;
      this.source = source;
      this.detail = detail;
      this.reason = reason;
    }

    @Override
    public String toString() {
      String origin = detail == null || detail.isEmpty() ? source : source + ": " + detail;
      return path + " (" + origin + "): " + reason;
    }
  }
}
