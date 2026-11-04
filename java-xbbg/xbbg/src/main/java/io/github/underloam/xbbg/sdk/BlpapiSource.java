package io.github.underloam.xbbg.sdk;

import io.github.underloam.xbbg.internal.Text;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * Where {@link BlpapiSdk} obtains the Bloomberg BLPAPI runtime.
 *
 * <p>Three kinds of source are supported:
 *
 * <ul>
 *   <li>a PyPI simple index ({@link #pypi(String)}): PEP 503 HTML or PEP 691 JSON, for example
 *       Bloomberg's own index {@value #BLOOMBERG_PYPI_INDEX}; the runtime comes from the {@code
 *       blpapi} wheel for the target platform;
 *   <li>a conda channel ({@link #conda(String)}): a name such as {@code conda-forge} or a channel
 *       URL; the runtime comes from the {@code libblpapi} package;
 *   <li>a single artifact ({@link #artifact(String)}): a {@code .whl}, {@code .conda} or {@code
 *       .tar.bz2} file given as URL or local path.
 * </ul>
 *
 * <p>Instances are immutable. {@link #spec()} renders a source in the syntax that {@link
 * #parse(String)} accepts, so {@code BlpapiSource.parse(source.spec())} equals {@code source}.
 */
public final class BlpapiSource {
  /** Bloomberg's public PyPI-compatible index. */
  public static final String BLOOMBERG_PYPI_INDEX =
      "https://blpapi.bloomberg.com/repository/releases/python/simple/";

  /** Project used by {@link #pypi(String)}. */
  public static final String DEFAULT_PYPI_PROJECT = "blpapi";

  /** Package used by {@link #conda(String)}. */
  public static final String DEFAULT_CONDA_PACKAGE = "libblpapi";

  /** The kind of a {@link BlpapiSource}. */
  public enum Kind {
    /** A PyPI simple index. */
    PYPI,
    /** A conda channel. */
    CONDA,
    /** A single wheel or conda package. */
    ARTIFACT
  }

  private static final String SHA256_FRAGMENT = "#sha256=";

  private final Kind kind;
  private final String location;
  private final String name;
  private final String sha256;

  private BlpapiSource(Kind kind, String location, String name, String sha256) {
    this.kind = kind;
    this.location = location;
    this.name = name;
    this.sha256 = sha256;
  }

  /**
   * A PyPI simple index providing the {@code blpapi} project.
   *
   * @param indexUrl index root such as {@value #BLOOMBERG_PYPI_INDEX}; credentials may be embedded
   *     as {@code https://user:password@host/...}
   * @return the source
   * @throws IllegalArgumentException if {@code indexUrl} is blank
   */
  public static BlpapiSource pypi(String indexUrl) {
    return pypi(indexUrl, DEFAULT_PYPI_PROJECT);
  }

  /**
   * A PyPI simple index providing {@code project}.
   *
   * @param indexUrl index root, for example {@value #BLOOMBERG_PYPI_INDEX}
   * @param project project name on that index
   * @return the source
   * @throws IllegalArgumentException if an argument is blank
   */
  public static BlpapiSource pypi(String indexUrl, String project) {
    return new BlpapiSource(
        Kind.PYPI, required(indexUrl, "index URL"), required(project, "project"), null);
  }

  /**
   * A conda channel providing the {@code libblpapi} package.
   *
   * @param channel channel name ({@code conda-forge}) or URL
   * @return the source
   * @throws IllegalArgumentException if {@code channel} is blank
   */
  public static BlpapiSource conda(String channel) {
    return conda(channel, DEFAULT_CONDA_PACKAGE);
  }

  /**
   * A conda channel providing {@code packageName}.
   *
   * @param channel channel name ({@code conda-forge}) or URL
   * @param packageName package that contains the Bloomberg native library
   * @return the source
   * @throws IllegalArgumentException if an argument is blank
   */
  public static BlpapiSource conda(String channel, String packageName) {
    return new BlpapiSource(
        Kind.CONDA, required(channel, "channel"), required(packageName, "package"), null);
  }

  /**
   * A single {@code .whl}, {@code .conda} or {@code .tar.bz2} artifact.
   *
   * <p>A trailing {@code #sha256=<hex>} is split off and becomes the expected archive hash. Remote
   * ({@code http}/{@code https}) artifacts need that hash unless {@link
   * ProvisionOptions.Builder#requireArtifactHash(boolean)} is disabled; local files are verified
   * against the wheel {@code RECORD} or the conda {@code info/paths.json}.
   *
   * @param urlOrPath URL ({@code https://}, {@code http://}, {@code file:}) or local path
   * @return the source
   * @throws IllegalArgumentException if {@code urlOrPath} is blank or its hash is malformed
   */
  public static BlpapiSource artifact(String urlOrPath) {
    String location = required(urlOrPath, "artifact location");
    int fragment = location.toLowerCase(Locale.ROOT).lastIndexOf(SHA256_FRAGMENT);
    if (fragment >= 0) {
      String hash = location.substring(fragment + SHA256_FRAGMENT.length());
      return artifact(location.substring(0, fragment), hash);
    }
    return new BlpapiSource(Kind.ARTIFACT, location, null, null);
  }

  /**
   * A single artifact with a known SHA-256.
   *
   * @param urlOrPath URL ({@code https://}, {@code http://}, {@code file:}) or local path
   * @param sha256 expected SHA-256 of the archive, 64 hex digits
   * @return the source
   * @throws IllegalArgumentException if {@code urlOrPath} is blank or {@code sha256} is not a
   *     SHA-256 hex digest
   */
  public static BlpapiSource artifact(String urlOrPath, String sha256) {
    String location = required(urlOrPath, "artifact location");
    return new BlpapiSource(Kind.ARTIFACT, location, null, sha256Hex(sha256));
  }

  /**
   * Parses a source specification.
   *
   * <p>Accepted forms: {@code pypi:<index url>[#<project>]}, {@code conda:<channel>[#<package>]},
   * {@code artifact:<url or path>[#sha256=<hex>]}, or a bare URL or path, which is classified by
   * its shape: a name ending in {@code .whl}, {@code .conda} or {@code .tar.bz2} is an artifact, a
   * location containing {@code /simple} is a PyPI index, anything else is a conda channel.
   *
   * @param spec the specification, for example {@code conda:conda-forge}
   * @return the source
   * @throws IllegalArgumentException if {@code spec} is blank or malformed
   */
  public static BlpapiSource parse(String spec) {
    String text = required(spec, "BLPAPI source spec");
    int colon = text.indexOf(':');
    // A one-letter prefix is a Windows drive ("C:\..."), never a source kind.
    String prefix = colon > 1 ? text.substring(0, colon).toLowerCase(Locale.ROOT) : "";
    String rest = colon > 1 ? text.substring(colon + 1).trim() : "";
    if (prefix.equals("pypi")) {
      int hash = rest.indexOf('#');
      return hash < 0 ? pypi(rest) : pypi(rest.substring(0, hash), rest.substring(hash + 1));
    }
    if (prefix.equals("conda")) {
      int hash = rest.indexOf('#');
      return hash < 0 ? conda(rest) : conda(rest.substring(0, hash), rest.substring(hash + 1));
    }
    if (prefix.equals("artifact")) {
      return artifact(rest);
    }
    String shape = text.toLowerCase(Locale.ROOT).replace('\\', '/');
    int end = firstIndexOf(shape, '#', '?');
    String path = end < 0 ? shape : shape.substring(0, end);
    if (path.endsWith(".whl") || path.endsWith(".conda") || path.endsWith(".tar.bz2")) {
      return artifact(text);
    }
    if (path.contains("/simple")) {
      return pypi(text);
    }
    return conda(text);
  }

  /**
   * The kind of this source.
   *
   * @return the kind
   */
  public Kind kind() {
    return kind;
  }

  /**
   * The index URL, channel, or artifact URL or path (without any {@code #sha256=} suffix).
   *
   * @return the location
   */
  public String location() {
    return location;
  }

  /**
   * The PyPI project or conda package; empty for artifacts.
   *
   * @return the project or package name
   */
  public Optional<String> name() {
    return Optional.ofNullable(name);
  }

  /**
   * The expected archive SHA-256 (lowercase hex) of an artifact; empty if none was given.
   *
   * @return the expected hash
   */
  public Optional<String> sha256() {
    return Optional.ofNullable(sha256);
  }

  /**
   * This source in {@link #parse(String)} syntax; default project and package names are omitted.
   * The result keeps credentials embedded in the location; {@link #toString()} hides them.
   *
   * @return the specification
   */
  public String spec() {
    switch (kind) {
      case PYPI:
        return "pypi:" + location + (DEFAULT_PYPI_PROJECT.equals(name) ? "" : "#" + name);
      case CONDA:
        return "conda:" + location + (DEFAULT_CONDA_PACKAGE.equals(name) ? "" : "#" + name);
      default:
        return "artifact:" + location + (sha256 == null ? "" : SHA256_FRAGMENT + sha256);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof BlpapiSource)) {
      return false;
    }
    BlpapiSource that = (BlpapiSource) other;
    return kind == that.kind
        && location.equals(that.location)
        && Objects.equals(name, that.name)
        && Objects.equals(sha256, that.sha256);
  }

  @Override
  public int hashCode() {
    return Objects.hash(kind, location, name, sha256);
  }

  /**
   * Returns the {@link #spec()} with the user-info of any URL replaced by {@code ***}, so logging a
   * source never prints embedded credentials. Use {@link #spec()} to round-trip a source.
   *
   * @return the redacted specification
   */
  @Override
  public String toString() {
    return Text.redactUserInfo(spec());
  }

  private static String required(String value, String what) {
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException(what + " must not be blank");
    }
    return value.trim();
  }

  private static String sha256Hex(String value) {
    String hex = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    if (hex.length() != 64) {
      throw new IllegalArgumentException("not a SHA-256 hex digest: '" + value + "'");
    }
    for (int i = 0; i < hex.length(); i++) {
      char c = hex.charAt(i);
      if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
        throw new IllegalArgumentException("not a SHA-256 hex digest: '" + value + "'");
      }
    }
    return hex;
  }

  private static int firstIndexOf(String text, char first, char second) {
    int a = text.indexOf(first);
    int b = text.indexOf(second);
    if (a < 0) {
      return b;
    }
    return b < 0 ? a : Math.min(a, b);
  }
}
