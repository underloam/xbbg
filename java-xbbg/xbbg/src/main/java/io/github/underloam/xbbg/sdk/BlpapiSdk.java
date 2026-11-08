package io.github.underloam.xbbg.sdk;

import io.github.underloam.xbbg.BlpException;
import io.github.underloam.xbbg.internal.BlpapiLocator;
import io.github.underloam.xbbg.internal.Futures;
import io.github.underloam.xbbg.internal.Json;
import io.github.underloam.xbbg.internal.JsonFields;
import io.github.underloam.xbbg.internal.NativeLibraries;
import io.github.underloam.xbbg.internal.NativeSdk;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/**
 * Downloads and installs the Bloomberg BLPAPI runtime into the xbbg cache.
 *
 * <p>Provisioning resolves the requested version from a {@link BlpapiSource}, downloads the
 * archive, verifies its SHA-256 against the index or repodata, verifies the native library against
 * the wheel {@code RECORD} or the conda {@code info/paths.json}, and installs it atomically under
 * {@code <cache>/sdk/<version>/<platform>/}. Installs are shared by every process using the same
 * cache and are never modified once written.
 *
 * <p>The provisioner lives in its own native library, {@code xbbg_sdk_jni}, which never links
 * blpapi and is loaded on first use.
 *
 * <p>Network access follows {@link ProvisionOptions}: a forced direct connection, else an explicit
 * proxy, else the proxy the JVM's {@link java.net.ProxySelector} returns for the source URL, else
 * (the selector answers {@code DIRECT}) the native client's detection from the {@code
 * HTTPS_PROXY}/{@code HTTP_PROXY}/{@code NO_PROXY} environment variables and the operating system's
 * proxy settings.
 *
 * <p>Command line: {@code java -cp xbbg.jar io.github.underloam.xbbg.sdk.BlpapiSdk [--source
 * <spec>] [--version <v>] [--platform <p>[,<p>...]|all] [--cache-dir <dir>] [--offline] [--proxy
 * <url>] [--netrc <file>]} provisions and prints the result; without {@code --source} the spec
 * comes from {@code XBBG_SDK_SOURCE}. {@code --installed [--cache-dir <dir>] [--platform <p>]}
 * lists installs. Command-line arguments are visible to other processes and land in shell history
 * and CI logs, so credentials belong in a netrc file ({@code --netrc}) or in {@code
 * XBBG_SDK_SOURCE}.
 */
public final class BlpapiSdk {
  private BlpapiSdk() {}

  /**
   * Provisions the runtime described by {@code options}.
   *
   * <p>The future fails with {@link io.github.underloam.xbbg.BlpapiNotFoundException} when the
   * source has no matching runtime (or, offline, when none is cached), with {@link
   * IllegalArgumentException} for invalid options, and with {@link
   * io.github.underloam.xbbg.BlpInternalException} for network, integrity and file system failures
   * ({@link BlpException#code()} is {@code SDK_NETWORK}, {@code SDK_INTEGRITY}, {@code
   * SDK_INVALID_ARTIFACT} or {@code SDK_IO}). Cancelling the future stops the download.
   *
   * @param options what to install and how to reach the network
   * @return the installed runtime
   */
  public static CompletableFuture<ProvisionResult> provision(ProvisionOptions options) {
    Objects.requireNonNull(options, "options");
    String request;
    try {
      Path cacheDir =
          options.cacheDir().isPresent() ? options.cacheDir().get() : NativeLibraries.cacheRoot();
      request = Json.write(options.toRequest(cacheDir));
    } catch (RuntimeException e) {
      return Futures.failed(e);
    }
    return Futures.map(NativeSdk.provisionAsync(request), BlpapiSdk::decode);
  }

  private static ProvisionResult decode(Object json) {
    String what = "provisioning result";
    return ProvisionResult.fromNative(
        JsonFields.object(JsonFields.parse((String) json, what), what));
  }

  /**
   * Lists the valid installs in the shared xbbg cache, newest version first.
   *
   * @return the installs for every platform
   */
  public static List<ProvisionResult> installed() {
    return installed(NativeLibraries.cacheRoot(), null);
  }

  /**
   * Lists the valid installs under a cache root, newest version first.
   *
   * @param cacheDir the cache root
   * @return the installs for every platform
   */
  public static List<ProvisionResult> installed(Path cacheDir) {
    return installed(cacheDir, null);
  }

  /**
   * Lists the valid installs for one platform under a cache root, newest version first. Incomplete
   * installs and installs whose library no longer matches its recorded hash are skipped.
   *
   * @param cacheDir the cache root
   * @param platform {@code windows-x86_64}, {@code linux-x86_64}, {@code macos-aarch64}, or {@code
   *     null} for every platform
   * @return the installs
   */
  public static List<ProvisionResult> installed(Path cacheDir, String platform) {
    Objects.requireNonNull(cacheDir, "cacheDir");
    if (platform != null && !ProvisionOptions.PLATFORMS.contains(platform)) {
      throw new IllegalArgumentException(
          "unsupported platform '"
              + platform
              + "' (supported: "
              + String.join(", ", ProvisionOptions.PLATFORMS)
              + ")");
    }
    String what = "install listing";
    List<Object> listed =
        JsonFields.array(JsonFields.parse(NativeSdk.installedJson(cacheDir, platform), what), what);
    List<ProvisionResult> results = new ArrayList<>(listed.size());
    for (Object entry : listed) {
      results.add(ProvisionResult.fromNative(JsonFields.object(entry, "install")));
    }
    return Collections.unmodifiableList(results);
  }

  /**
   * Command-line entry point; exits with 0 on success, 1 when provisioning fails and 2 for a usage
   * error.
   *
   * @param args the arguments (run with {@code --help} for the list)
   */
  public static void main(String[] args) {
    System.exit(run(args, System::getenv, System.out, System.err));
  }

  static int run(String[] args, Function<String, String> env, PrintStream out, PrintStream err) {
    Cli cli;
    List<ProvisionOptions> requests;
    try {
      cli = Cli.parse(args, env);
      requests =
          cli.help || cli.listInstalled
              ? Collections.<ProvisionOptions>emptyList()
              : cli.requests();
    } catch (IllegalArgumentException e) {
      err.println("error: " + e.getMessage());
      err.println(Cli.USAGE);
      return 2;
    }
    if (cli.help) {
      out.println(Cli.USAGE);
      return 0;
    }
    try {
      if (cli.listInstalled) {
        Path cacheDir = cli.cacheDir != null ? cli.cacheDir : NativeLibraries.cacheRoot();
        int count = 0;
        for (String platform : cli.platformsOrDefault()) {
          for (ProvisionResult result : installed(cacheDir, platform)) {
            print(result, out);
            count++;
          }
        }
        if (count == 0) {
          out.println("no BLPAPI runtime installed under " + cacheDir.toAbsolutePath());
        }
        return 0;
      }
      for (ProvisionOptions options : requests) {
        print(await(provision(options)), out);
      }
      return 0;
    } catch (RuntimeException | UnsatisfiedLinkError e) {
      String code = e instanceof BlpException ? " [" + ((BlpException) e).code() + "]" : "";
      err.println("error" + code + ": " + e.getMessage());
      return 1;
    }
  }

  private static void print(ProvisionResult result, PrintStream out) {
    out.println("version          " + result.version());
    out.println("platform         " + result.platform());
    out.println("library          " + result.libraryPath());
    out.println("library sha256   " + result.libSha256());
    out.println("artifact         " + result.artifactUrl());
    out.println("artifact sha256  " + result.artifactSha256());
    out.println("source           " + result.source());
    out.println("cache hit        " + result.cacheHit());
    for (Path license : result.licensePaths()) {
      out.println("license          " + license);
    }
    out.println();
  }

  private static <T> T await(CompletableFuture<T> future) {
    try {
      return future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      future.cancel(true);
      throw new CancellationException("interrupted while provisioning");
    } catch (ExecutionException e) {
      Throwable cause = Futures.unwrap(e);
      Futures.throwIfUnchecked(cause);
      throw new IllegalStateException(cause);
    }
  }

  /** Parsed command line. */
  static final class Cli {
    static final String USAGE =
        "usage: java -cp xbbg.jar io.github.underloam.xbbg.sdk.BlpapiSdk"
            + " [--source <spec>] [--version <v>] [--platform <p>[,<p>...]|all]"
            + " [--cache-dir <dir>] [--offline] [--proxy <url>] [--netrc <file>]\n"
            + "       java -cp xbbg.jar io.github.underloam.xbbg.sdk.BlpapiSdk --installed"
            + " [--cache-dir <dir>] [--platform <p>[,<p>...]|all]\n"
            + "  <spec>: pypi:<index url>[#project] | conda:<channel>[#package]"
            + " | artifact:<url or path>[#sha256=<hex>] | a bare URL or path;"
            + " without --source, "
            + BlpapiLocator.SOURCE_ENV
            + "\n"
            + "  --netrc <file>: credentials per host, read only from this file\n"
            + "  platforms: "
            + String.join(", ", ProvisionOptions.PLATFORMS)
            + "\n"
            + "  Warning: other processes can read command-line arguments, and shells and CI logs"
            + " keep them.\n"
            + "  Keep credentials out of --source and --proxy: use --netrc, or pass a source with"
            + " credentials in "
            + BlpapiLocator.SOURCE_ENV
            + ".";

    boolean help;
    boolean listInstalled;
    BlpapiSource source;
    String version;
    final List<String> platforms = new ArrayList<>();
    Path cacheDir;
    boolean offline;
    String proxy;
    Path netrc;

    static Cli parse(String[] args, Function<String, String> env) {
      Cli cli = new Cli();
      for (int i = 0; i < args.length; i++) {
        String arg = args[i];
        switch (arg) {
          case "-h":
          case "--help":
            cli.help = true;
            return cli;
          case "--installed":
            cli.listInstalled = true;
            break;
          case "--offline":
            cli.offline = true;
            break;
          case "--source":
            cli.source = BlpapiSource.parse(value(args, ++i, arg));
            break;
          case "--version":
            cli.version = value(args, ++i, arg);
            break;
          case "--cache-dir":
            cli.cacheDir = Paths.get(value(args, ++i, arg));
            break;
          case "--proxy":
            cli.proxy = value(args, ++i, arg);
            break;
          case "--netrc":
            cli.netrc = Paths.get(value(args, ++i, arg));
            break;
          case "--platform":
            String platforms = value(args, ++i, arg);
            List<String> requested =
                platforms.equals("all")
                    ? ProvisionOptions.PLATFORMS
                    : Arrays.asList(platforms.split(","));
            for (String platform : requested) {
              String name = platform.trim();
              if (!ProvisionOptions.PLATFORMS.contains(name)) {
                throw new IllegalArgumentException("unsupported platform '" + name + "'");
              }
              cli.platforms.add(name);
            }
            break;
          default:
            throw new IllegalArgumentException("unknown argument '" + arg + "'");
        }
      }
      if (!cli.listInstalled && cli.source == null) {
        cli.source = sourceFromEnvironment(env);
      }
      return cli;
    }

    private static BlpapiSource sourceFromEnvironment(Function<String, String> env) {
      String spec = env.apply(BlpapiLocator.SOURCE_ENV);
      if (spec == null || spec.trim().isEmpty()) {
        throw new IllegalArgumentException(
            "--source is required (or set " + BlpapiLocator.SOURCE_ENV + ")");
      }
      try {
        return BlpapiSource.parse(spec);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            BlpapiLocator.SOURCE_ENV + " is not a valid source: " + e.getMessage(), e);
      }
    }

    /**
     * The provisioning request for each requested platform.
     *
     * @throws IllegalArgumentException if an option value is invalid (a usage error)
     */
    List<ProvisionOptions> requests() {
      List<ProvisionOptions> requests = new ArrayList<>();
      for (String platform : platformsOrDefault()) {
        requests.add(
            ProvisionOptions.builder()
                .source(source)
                .version(version)
                .platform(platform)
                .cacheDir(cacheDir)
                .offline(offline)
                .proxy(proxy)
                .netrc(netrc)
                .build());
      }
      return requests;
    }

    /** The requested platforms, or one {@code null} (host; every platform for --installed). */
    List<String> platformsOrDefault() {
      return platforms.isEmpty() ? Collections.<String>singletonList(null) : platforms;
    }

    private static String value(String[] args, int index, String flag) {
      if (index >= args.length || args[index].startsWith("--")) {
        throw new IllegalArgumentException(flag + " needs a value");
      }
      return args[index];
    }
  }
}
