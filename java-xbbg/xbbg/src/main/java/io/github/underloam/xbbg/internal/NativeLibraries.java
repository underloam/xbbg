package io.github.underloam.xbbg.internal;

import io.github.underloam.xbbg.BlpapiNotFoundException;
import io.github.underloam.xbbg.sdk.BlpapiSdk;
import io.github.underloam.xbbg.sdk.BlpapiSource;
import io.github.underloam.xbbg.sdk.ProvisionOptions;
import io.github.underloam.xbbg.sdk.ProvisionResult;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Loads {@code xbbg_jni} and the Bloomberg runtime it links against, once per JVM.
 *
 * <p>{@link #ensureLoaded()} detects the platform, finds blpapi ({@link BlpapiLocator}, then
 * provisioning when a source is configured), extracts the bundled natives into {@code
 * <cache>/jvm/<version>-<sha12>/<platform>/}, loads blpapi and then {@code xbbg_jni} by absolute
 * path, checks the ABI handshake and registers a shutdown hook. A failure is not cached: a later
 * call (for example after {@code Xbbg.setSdkPath}) tries again, unless a library was already loaded
 * into the process, which only a JVM restart undoes.
 *
 * <p>A provisioning download runs without the load lock and completes on the default callback
 * executor, so neither the settings nor a busy user callback executor wait for it, and concurrent
 * callers share one download.
 *
 * <p>Internal API: not covered by compatibility guarantees.
 */
public final class NativeLibraries {
  /** The ABI version shared with {@code xbbg_jni}. */
  public static final int ABI = 1;

  private static final Logger LOG = Logger.getLogger("io.github.underloam.xbbg");
  private static final String RESOURCE_ROOT = "/io/github/underloam/xbbg/native/";
  private static final long SHUTDOWN_TIMEOUT_MILLIS = 10_000L;
  private static final String RESTART_HINT =
      "; the runtime stays loaded in this JVM, so restart the JVM after pointing xbbg at"
          + " Bloomberg API 3.20.0 or newer";

  /** Guards extraction; separate from the load lock so provisioning never waits on it. */
  private static final Object EXTRACT_LOCK = new Object();

  private static volatile boolean loaded;
  private static volatile Path sdkPath;
  private static volatile BlpapiSource sdkSource;
  private static volatile Path loadedBlpapi;
  private static volatile BundledNatives bundled;

  /** The download in flight; guarded by the class monitor. */
  private static Provisioning provisioning;

  private NativeLibraries() {}

  /**
   * Loads the natives unless already loaded. When a runtime must be provisioned, the calling thread
   * waits for the download without holding the load lock (concurrent callers share it), then loads
   * it unless {@code setSdkPath} or {@code setSdkSource} changed the settings meanwhile, in which
   * case the search starts again with the new settings.
   *
   * @throws UnsatisfiedLinkError if the platform is unsupported, the jar has no natives for it, or
   *     a library cannot be loaded
   * @throws BlpapiNotFoundException if no Bloomberg runtime was found or provisioned
   */
  public static void ensureLoaded() {
    if (loaded) {
      return;
    }
    while (true) {
      Provisioning download;
      synchronized (NativeLibraries.class) {
        if (loaded) {
          return;
        }
        Path cache = cacheRoot();
        bundled();
        BlpapiLocator locator = BlpapiLocator.forCurrentProcess(sdkPath, cache);
        Path blpapi = locator.findLocal();
        if (blpapi != null) {
          load(blpapi);
          return;
        }
        download = download(locator, cache);
      }
      Path provisioned = null;
      try {
        provisioned = download.await();
      } finally {
        if (provisioned == null) {
          forget(download);
        }
      }
      synchronized (NativeLibraries.class) {
        if (loaded) {
          return;
        }
        if (download.startedWith(sdkPath, sdkSource)) {
          load(provisioned);
          return;
        }
      }
      // The settings changed during the download: search again with the new ones.
      forget(download);
    }
  }

  /**
   * Whether the natives are loaded.
   *
   * @return {@code true} after a successful {@link #ensureLoaded()}
   */
  public static boolean isLoaded() {
    return loaded;
  }

  /**
   * Returns the Bloomberg runtime library that was loaded.
   *
   * @return the library path, or {@code null} before loading
   */
  public static Path loadedBlpapi() {
    return loadedBlpapi;
  }

  /**
   * Sets the explicit Bloomberg runtime location (a library file or a directory).
   *
   * @param path the location, or {@code null} to clear it
   * @throws IllegalStateException if the natives are already loaded
   */
  public static void setSdkPath(Path path) {
    synchronized (NativeLibraries.class) {
      if (loaded) {
        throw new IllegalStateException(
            "xbbg natives are already loaded with the Bloomberg runtime " + loadedBlpapi);
      }
      sdkPath = path == null ? null : path.toAbsolutePath().normalize();
    }
  }

  /**
   * Returns the explicit Bloomberg runtime location.
   *
   * @return the location, or {@code null}
   */
  public static Path sdkPath() {
    return sdkPath;
  }

  /**
   * Sets the source to provision a Bloomberg runtime from when none is found locally.
   *
   * @param source the source, or {@code null} to clear it
   * @throws IllegalStateException if the natives are already loaded
   */
  public static void setSdkSource(BlpapiSource source) {
    synchronized (NativeLibraries.class) {
      if (loaded) {
        throw new IllegalStateException(
            "xbbg natives are already loaded with the Bloomberg runtime " + loadedBlpapi);
      }
      sdkSource = source;
    }
  }

  /**
   * Returns the configured provisioning source.
   *
   * @return the source, or {@code null}
   */
  public static BlpapiSource sdkSource() {
    return sdkSource;
  }

  /**
   * Returns the xbbg cache root, by the rules of the Rust {@code xbbg_sdk::default_cache_root}:
   * system property {@code xbbg.cache.dir}, else {@code XBBG_CACHE_DIR}, else {@code
   * %LOCALAPPDATA%\xbbg} (falling back to {@code %USERPROFILE%\AppData\Local\xbbg}) on Windows,
   * {@code $HOME/Library/Caches/xbbg} on macOS and {@code $XDG_CACHE_HOME/xbbg} (when absolute) or
   * {@code $HOME/.cache/xbbg} elsewhere. Like Rust, an empty environment variable counts as unset;
   * where Rust finds no home directory, the JVM's {@code user.home} is used.
   *
   * @return the absolute cache root
   */
  public static Path cacheRoot() {
    String os = System.getProperty("os.name", "").toLowerCase(java.util.Locale.ROOT);
    return cacheRoot(
        System.getProperty("xbbg.cache.dir"),
        System.getenv("XBBG_CACHE_DIR"),
        os.startsWith("windows") ? "windows" : os.startsWith("mac") ? "macos" : "linux",
        System.getenv("LOCALAPPDATA"),
        System.getenv("USERPROFILE"),
        System.getenv("XDG_CACHE_HOME"),
        System.getenv("HOME"),
        System.getProperty("user.home"));
  }

  static Path cacheRoot(
      String property,
      String envDir,
      String os,
      String localAppData,
      String userProfile,
      String xdgCacheHome,
      String home,
      String userHome) {
    if (property != null && !property.trim().isEmpty()) {
      return Paths.get(property).toAbsolutePath().normalize();
    }
    if (isSet(envDir)) {
      return Paths.get(envDir).toAbsolutePath().normalize();
    }
    if (os.equals("windows")) {
      if (isSet(localAppData)) {
        return Paths.get(localAppData, "xbbg");
      }
      return Paths.get(isSet(userProfile) ? userProfile : userHome, "AppData", "Local", "xbbg");
    }
    String homeDir = isSet(home) ? home : userHome;
    if (os.equals("macos")) {
      return Paths.get(homeDir, "Library", "Caches", "xbbg");
    }
    if (isSet(xdgCacheHome) && Paths.get(xdgCacheHome).isAbsolute()) {
      return Paths.get(xdgCacheHome, "xbbg");
    }
    return Paths.get(homeDir, ".cache", "xbbg");
  }

  /** Rust's rule for environment values: unset or empty is absent; anything else is used. */
  private static boolean isSet(String value) {
    return value != null && !value.isEmpty();
  }

  /**
   * Extracts a bundled native library for the current platform into the cache (once; an existing
   * file with the right sha256 is reused, never overwritten) and returns its path.
   *
   * @param baseName the library base name, for example {@code xbbg_jni}
   * @return the absolute path of the extracted library
   * @throws UnsatisfiedLinkError if the jar has no such library for this platform or extraction
   *     fails
   */
  public static Path extractBundled(String baseName) {
    BundledNatives natives = bundled();
    String fileName = System.mapLibraryName(baseName);
    String expected = natives.properties.getProperty(fileName + ".sha256");
    if (expected == null) {
      throw new UnsatisfiedLinkError(
          "the xbbg jar has no " + fileName + " for platform " + natives.platform.id());
    }
    Path target = natives.directory.resolve(fileName);
    synchronized (EXTRACT_LOCK) {
      try {
        if (Files.exists(target)) {
          verify(target, expected);
          return target;
        }
        Files.createDirectories(natives.directory);
        Path temp = Files.createTempFile(natives.directory, fileName + ".", ".tmp");
        try {
          String actual;
          try (InputStream in = resource(natives.platform, fileName);
              OutputStream out = Files.newOutputStream(temp)) {
            actual = copyHashing(in, out);
          }
          if (!actual.equalsIgnoreCase(expected)) {
            throw new UnsatisfiedLinkError(
                "the bundled "
                    + fileName
                    + " does not match its recorded sha256; the jar is corrupt");
          }
          moveIntoPlace(temp, target);
        } finally {
          Files.deleteIfExists(temp);
        }
        verify(target, expected);
        return target;
      } catch (IOException e) {
        UnsatisfiedLinkError error =
            new UnsatisfiedLinkError(
                "cannot extract " + fileName + " to " + natives.directory + ": " + e.getMessage());
        error.initCause(e);
        throw error;
      }
    }
  }

  private static void moveIntoPlace(Path temp, Path target) throws IOException {
    if (Files.exists(target)) {
      return;
    }
    try {
      Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE);
    } catch (AtomicMoveNotSupportedException e) {
      try {
        Files.move(temp, target);
      } catch (FileAlreadyExistsException raced) {
        // Another JVM extracted the same file first; its content is verified by the caller.
      }
    } catch (FileAlreadyExistsException | AccessDeniedException raced) {
      if (!Files.exists(target)) {
        throw raced;
      }
    }
  }

  private static void verify(Path file, String expected) throws IOException {
    String actual = Hashes.sha256Hex(file);
    if (!actual.equalsIgnoreCase(expected)) {
      throw new UnsatisfiedLinkError(
          "the extracted native library "
              + file
              + " does not match the jar (sha256 "
              + actual
              + ", expected "
              + expected
              + "); delete it and retry");
    }
  }

  private static String copyHashing(InputStream in, OutputStream out) throws IOException {
    MessageDigest digest = Hashes.sha256();
    byte[] buffer = new byte[64 * 1024];
    int read;
    while ((read = in.read(buffer)) > 0) {
      digest.update(buffer, 0, read);
      out.write(buffer, 0, read);
    }
    return Hashes.hex(digest.digest());
  }

  private static InputStream resource(Platform platform, String fileName) throws IOException {
    InputStream in =
        NativeLibraries.class.getResourceAsStream(RESOURCE_ROOT + platform.id() + "/" + fileName);
    if (in == null) {
      throw new UnsatisfiedLinkError(
          "the xbbg jar has no " + fileName + " for platform " + platform.id());
    }
    return in;
  }

  private static BundledNatives bundled() {
    BundledNatives result = bundled;
    if (result != null) {
      return result;
    }
    synchronized (EXTRACT_LOCK) {
      if (bundled == null) {
        bundled = readBundled(Platform.current(), cacheRoot());
      }
      return bundled;
    }
  }

  static BundledNatives readBundled(Platform platform, Path cacheRoot) {
    byte[] bytes;
    try (InputStream in =
        NativeLibraries.class.getResourceAsStream(
            RESOURCE_ROOT + platform.id() + "/natives.properties")) {
      if (in == null) {
        throw new UnsatisfiedLinkError(
            "the xbbg jar has no native libraries for platform "
                + platform.id()
                + " (missing "
                + RESOURCE_ROOT.substring(1)
                + platform.id()
                + "/natives.properties)");
      }
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buffer = new byte[8192];
      int read;
      while ((read = in.read(buffer)) > 0) {
        out.write(buffer, 0, read);
      }
      bytes = out.toByteArray();
    } catch (IOException e) {
      UnsatisfiedLinkError error =
          new UnsatisfiedLinkError("cannot read the bundled natives.properties: " + e.getMessage());
      error.initCause(e);
      throw error;
    }
    Properties properties = new Properties();
    try {
      properties.load(new ByteArrayInputStream(bytes));
    } catch (IOException e) {
      throw new UnsatisfiedLinkError(
          "the bundled natives.properties is malformed: " + e.getMessage());
    }
    String abi = properties.getProperty("abi");
    if (abi == null || !abi.trim().equals(Integer.toString(ABI))) {
      throw new UnsatisfiedLinkError(
          "the bundled natives have ABI " + abi + " but this xbbg jar needs ABI " + ABI);
    }
    String sha12 = Hashes.sha256Hex(bytes).substring(0, 12);
    Path directory =
        cacheRoot
            .resolve("jvm")
            .resolve(Versions.versionOrUnknown() + "-" + sha12)
            .resolve(platform.id());
    return new BundledNatives(platform, properties, directory);
  }

  /** Loads a found or provisioned runtime and then {@code xbbg_jni}; call with the lock held. */
  private static void load(Path blpapi) {
    Platform platform = Platform.current();
    Path xbbgJni = extractBundled("xbbg_jni");
    try {
      System.load(blpapi.toString());
    } catch (UnsatisfiedLinkError e) {
      throw loadFailure(
          e,
          "cannot load the Bloomberg runtime "
              + blpapi
              + " on "
              + platform.id()
              + ": "
              + e.getMessage()
              + " (xbbg needs Bloomberg API 3.20.0 or newer)");
    }
    try {
      System.load(xbbgJni.toString());
    } catch (UnsatisfiedLinkError e) {
      throw loadFailure(
          e,
          "cannot load "
              + xbbgJni
              + " against the Bloomberg runtime "
              + blpapi
              + ": "
              + e.getMessage()
              + " (the runtime may be older than the minimum, Bloomberg API 3.20.0"
              + RESTART_HINT
              + ")");
    }
    int abi = NativeXbbg.abiVersion();
    if (abi != ABI) {
      throw new UnsatisfiedLinkError(
          "stale xbbg_jni at " + xbbgJni + ": native ABI " + abi + ", this jar needs ABI " + ABI);
    }
    loadedBlpapi = blpapi;
    provisioning = null;
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                new Runnable() {
                  @Override
                  public void run() {
                    try {
                      NativeXbbg.shutdown(SHUTDOWN_TIMEOUT_MILLIS);
                    } catch (Throwable error) {
                      LOG.log(Level.WARNING, "xbbg native shutdown failed", error);
                    }
                  }
                },
                "xbbg-shutdown"));
    loaded = true;
  }

  /**
   * The error for a failed {@code System.load}: names the class loader conflict when another class
   * loader of this JVM already loaded the library (the JVM allows one per native library), else
   * {@code message}.
   */
  static UnsatisfiedLinkError loadFailure(UnsatisfiedLinkError e, String message) {
    String detail = e.getMessage();
    if (detail != null && detail.contains("already loaded in another classloader")) {
      return linkError(
          "xbbg is already loaded by another class loader in this JVM ("
              + detail
              + "); a native library belongs to one class loader, so load the xbbg jar from one"
              + " class loader only, for example a parent shared by the applications that use it",
          e);
    }
    return linkError(message, e);
  }

  /**
   * Returns the download for the configured source, joining the one in flight when it is for the
   * same settings; call with the lock held.
   *
   * @throws BlpapiNotFoundException if no source is configured
   */
  private static Provisioning download(BlpapiLocator locator, Path cache) {
    BlpapiSource source = sdkSource;
    if (source == null) {
      String spec = locator.configuredSourceSpec();
      if (spec == null) {
        throw locator.notFound();
      }
      source = parseSource(spec);
    }
    final ProvisionOptions options =
        ProvisionOptions.builder()
            .source(source)
            .version(locator.configuredVersion())
            .cacheDir(cache)
            .build();
    Provisioning current = provisioning;
    if (current != null
        && current.options.equals(options)
        && current.startedWith(sdkPath, sdkSource)) {
      return current;
    }
    // xbbg waits for this result itself, possibly on a thread of the user's callback executor, so
    // it must not complete on that executor.
    current =
        new Provisioning(
            sdkPath,
            sdkSource,
            options,
            Callbacks.withInternalCompletions(() -> BlpapiSdk.provision(options)));
    provisioning = current;
    return current;
  }

  /** Forgets a finished or abandoned download, unless a newer one replaced it. */
  private static void forget(Provisioning download) {
    synchronized (NativeLibraries.class) {
      if (provisioning == download) {
        provisioning = null;
      }
    }
  }

  /**
   * Parses the source spec of {@code xbbg.sdk.source} / {@code XBBG_SDK_SOURCE}; the error names
   * the spec without the credentials it may embed.
   */
  static BlpapiSource parseSource(String spec) {
    try {
      return BlpapiSource.parse(spec);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "invalid Bloomberg runtime source '" + Text.redactUserInfo(spec) + "': " + e.getMessage(),
          e);
    }
  }

  private static UnsatisfiedLinkError linkError(String message, Throwable cause) {
    UnsatisfiedLinkError error = new UnsatisfiedLinkError(message);
    error.initCause(cause);
    return error;
  }

  /** One download of a Bloomberg runtime, shared by every caller with the same settings. */
  private static final class Provisioning {
    private final Path sdkPath;
    private final BlpapiSource sdkSource;
    final ProvisionOptions options;
    private final CompletableFuture<ProvisionResult> result;

    Provisioning(
        Path sdkPath,
        BlpapiSource sdkSource,
        ProvisionOptions options,
        CompletableFuture<ProvisionResult> result) {
      this.sdkPath = sdkPath;
      this.sdkSource = sdkSource;
      this.options = options;
      this.result = result;
    }

    /** Whether the download was started with these {@code setSdkPath}/{@code setSdkSource}. */
    boolean startedWith(Path path, BlpapiSource source) {
      return Objects.equals(sdkPath, path) && Objects.equals(sdkSource, source);
    }

    /** Waits for the download; rethrows its failure unwrapped. */
    Path await() {
      try {
        return result.join().libraryPath();
      } catch (CompletionException e) {
        Throwable cause = Futures.unwrap(e);
        Futures.throwIfUnchecked(cause);
        throw e;
      }
    }
  }

  static final class BundledNatives {
    final Platform platform;
    final Properties properties;
    final Path directory;

    BundledNatives(Platform platform, Properties properties, Path directory) {
      this.platform = platform;
      this.properties = properties;
      this.directory = directory;
    }
  }
}
