package io.github.underloam.xbbg.sdk;

import io.github.underloam.xbbg.internal.JsonFields;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An installed Bloomberg runtime in the provisioning cache.
 *
 * <p>Installs are immutable: the directory that holds {@link #libraryPath()} never changes once
 * written, because a loaded native library cannot be replaced on Windows.
 */
public final class ProvisionResult {
  private final String version;
  private final String platform;
  private final Path libraryPath;
  private final List<Path> licensePaths;
  private final String source;
  private final String artifactUrl;
  private final String artifactSha256;
  private final String libSha256;
  private final boolean cacheHit;

  ProvisionResult(
      String version,
      String platform,
      Path libraryPath,
      List<Path> licensePaths,
      String source,
      String artifactUrl,
      String artifactSha256,
      String libSha256,
      boolean cacheHit) {
    this.version = Objects.requireNonNull(version, "version");
    this.platform = Objects.requireNonNull(platform, "platform");
    this.libraryPath = Objects.requireNonNull(libraryPath, "libraryPath");
    this.licensePaths = Collections.unmodifiableList(new ArrayList<>(licensePaths));
    this.source = Objects.requireNonNull(source, "source");
    this.artifactUrl = Objects.requireNonNull(artifactUrl, "artifactUrl");
    this.artifactSha256 = Objects.requireNonNull(artifactSha256, "artifactSha256");
    this.libSha256 = Objects.requireNonNull(libSha256, "libSha256");
    this.cacheHit = cacheHit;
  }

  /** Decodes one result object produced by the native provisioner. */
  static ProvisionResult fromNative(Map<String, Object> json) {
    List<Path> licenses = new ArrayList<>();
    for (String license : JsonFields.strings(json, "licensePaths")) {
      licenses.add(Paths.get(license));
    }
    return new ProvisionResult(
        JsonFields.string(json, "version"),
        JsonFields.string(json, "platform"),
        Paths.get(JsonFields.string(json, "libraryPath")),
        licenses,
        JsonFields.string(json, "source"),
        JsonFields.string(json, "artifactUrl"),
        JsonFields.string(json, "artifactSha256"),
        JsonFields.string(json, "libSha256"),
        JsonFields.bool(json, "cacheHit"));
  }

  /**
   * The runtime version, for example {@code 3.26.9.1}.
   *
   * @return the version
   */
  public String version() {
    return version;
  }

  /**
   * The platform the runtime is built for ({@code windows-x86_64}, {@code linux-x86_64} or {@code
   * macos-aarch64}).
   *
   * @return the platform
   */
  public String platform() {
    return platform;
  }

  /**
   * Absolute path of the Bloomberg native library ({@code blpapi3_64.dll} or {@code
   * libblpapi3_64.so}).
   *
   * @return the library path
   */
  public Path libraryPath() {
    return libraryPath;
  }

  /**
   * Absolute paths of the licence files shipped with the runtime.
   *
   * @return the licence files
   */
  public List<Path> licensePaths() {
    return licensePaths;
  }

  /**
   * The source the runtime came from, in {@link BlpapiSource#parse(String)} syntax (credentials
   * removed).
   *
   * @return the source specification
   */
  public String source() {
    return source;
  }

  /**
   * URL (or {@code file:} URL) of the archive the library was extracted from, without credentials.
   *
   * @return the archive URL
   */
  public String artifactUrl() {
    return artifactUrl;
  }

  /**
   * SHA-256 of that archive, verified against the index or repodata when they publish one.
   *
   * @return the archive hash (lowercase hex)
   */
  public String artifactSha256() {
    return artifactSha256;
  }

  /**
   * SHA-256 of the native library, verified against the wheel {@code RECORD} or the conda {@code
   * info/paths.json} and re-checked on every cache hit.
   *
   * @return the library hash (lowercase hex)
   */
  public String libSha256() {
    return libSha256;
  }

  /**
   * Whether the install already existed in the cache.
   *
   * @return {@code true} if nothing was downloaded
   */
  public boolean cacheHit() {
    return cacheHit;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof ProvisionResult)) {
      return false;
    }
    ProvisionResult that = (ProvisionResult) other;
    return cacheHit == that.cacheHit
        && version.equals(that.version)
        && platform.equals(that.platform)
        && libraryPath.equals(that.libraryPath)
        && licensePaths.equals(that.licensePaths)
        && source.equals(that.source)
        && artifactUrl.equals(that.artifactUrl)
        && artifactSha256.equals(that.artifactSha256)
        && libSha256.equals(that.libSha256);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        version,
        platform,
        libraryPath,
        licensePaths,
        source,
        artifactUrl,
        artifactSha256,
        libSha256,
        cacheHit);
  }

  @Override
  public String toString() {
    return "ProvisionResult{version="
        + version
        + ", platform="
        + platform
        + ", libraryPath="
        + libraryPath
        + ", source="
        + source
        + ", artifactUrl="
        + artifactUrl
        + ", artifactSha256="
        + artifactSha256
        + ", libSha256="
        + libSha256
        + ", cacheHit="
        + cacheHit
        + ", licensePaths="
        + licensePaths
        + '}';
  }
}
