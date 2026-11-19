//! Unified Bloomberg SDK CLI shared by `xbbg-sdk`, `python -m xbbg.sdk`, `npx xbbg-sdk`,
//! and `xbbg-mcp sdk`.
//!
//! Two modes:
//! - Default (runtime cache): downloads and verifies `blpapi3_64.dll` / `libblpapi3_64.so` into
//!   the shared `<cache>/sdk/<version>/<platform>/` cache, or lists installed runtimes
//!   (`--installed`).
//! - `cpp` subcommand: stages the full Bloomberg C++ SDK (headers + runtime libraries) into
//!   `vendor/blpapi-sdk/<version>/` for local source builds, replacing `scripts/sdktool.{sh,ps1}`.

use std::io::Write;
use std::path::PathBuf;

use crate::cpp_sdk::{
    DEFAULT_ENV_FILE, DEFAULT_VENDOR_DIR, StageOptions, active_version, clean_cache,
    list_installed, remove_version, stage_blocking,
};
use crate::{
    Platform, ProvisionOptions, Provisioned, ProxyConfig, SOURCE_ENV, Source, VERSION_ENV,
    VersionSpec, default_cache_root, installed, redact_user_info,
};

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|s| !s.is_empty())
}

fn parse_proxy(value: &str) -> Result<ProxyConfig, String> {
    let trimmed = value.trim();
    if trimmed.eq_ignore_ascii_case("direct") {
        Ok(ProxyConfig::Direct)
    } else if trimmed.eq_ignore_ascii_case("system") {
        Ok(ProxyConfig::System)
    } else if trimmed.starts_with("http://")
        || trimmed.starts_with("https://")
        || trimmed.starts_with("socks5://")
        || trimmed.starts_with("socks5h://")
    {
        Ok(ProxyConfig::Explicit {
            url: trimmed.to_owned(),
            no_proxy: Vec::new(),
        })
    } else {
        Err(format!(
            "invalid proxy '{}': expected direct, system, or an http/https/socks5 URL",
            redact_user_info(trimmed)
        ))
    }
}

fn cli_usage(prog: &str) -> String {
    let platforms = Platform::ALL.map(Platform::as_str).join(", ");
    format!(
        "usage: {prog} [--source <spec>] [--version <v>] [--platform <p>[,<p>...]|all] \
         [--cache-dir <dir>] [--offline] [--proxy <url>] [--netrc <file>]\n\
         \x20      {prog} --installed [--cache-dir <dir>] [--platform <p>[,<p>...]|all]\n\
         \x20      {prog} cpp [<version> | --version <v>] [--dir <dir>] [--no-set-active] [--force]\n\
         \x20      {prog} cpp --list | --remove <version> | --clean-cache\n\
         \x20 <spec>: pypi:<index url>[#<project>] | conda:<channel>[#<package>] \
         | artifact:<url or path>[#sha256=<hex>] | a bare URL or path; without --source, {SOURCE_ENV}\n\
         \x20 --netrc <file>: credentials per host, read only from this file\n\
         \x20 platforms: {platforms}\n\
         \x20 Run `{prog} cpp --help` for source-build C++ SDK staging options.\n\
         \x20 Warning: other processes can read command-line arguments, and shells and CI logs keep them.\n\
         \x20 Keep credentials out of --source and --proxy: use --netrc, or pass a source with credentials in {SOURCE_ENV}."
    )
}

fn cpp_usage(prog: &str) -> String {
    let platforms = Platform::ALL.map(Platform::as_str).join(", ");
    format!(
        "usage: {prog} cpp [<version> | --version <v>] [--platform <p>] [--dir <dir>] \
         [--env-file <file>] [--no-set-active] [--force] [--proxy <url>] [--netrc <file>]\n\
         \x20      {prog} cpp --list [--dir <dir>] [--env-file <file>]\n\
         \x20      {prog} cpp --remove <version> [--dir <dir>] [--env-file <file>]\n\
         \x20      {prog} cpp --clean-cache [--dir <dir>]\n\
         \x20 Stages the full Bloomberg C++ SDK (headers + libraries) for building from source.\n\
         \x20 defaults: --dir {DEFAULT_VENDOR_DIR}, --env-file {DEFAULT_ENV_FILE}\n\
         \x20 platforms: {platforms}"
    )
}

struct CliArgs {
    help: bool,
    installed: bool,
    source: Option<Source>,
    version: VersionSpec,
    platforms: Vec<Platform>,
    cache_dir: Option<PathBuf>,
    offline: bool,
    proxy: ProxyConfig,
    netrc: Option<PathBuf>,
}

fn parse_cli(args: &[String], get_env: &dyn Fn(&str) -> Option<String>) -> Result<CliArgs, String> {
    let mut help = false;
    let mut installed_flag = false;
    let mut source_arg: Option<String> = None;
    let mut version_arg: Option<String> = None;
    let mut platforms: Vec<Platform> = Vec::new();
    let mut cache_dir: Option<PathBuf> = None;
    let mut offline = false;
    let mut proxy = ProxyConfig::System;
    let mut netrc: Option<PathBuf> = None;

    let mut i = 0;
    while i < args.len() {
        let arg = args[i].as_str();
        let next = |idx: &mut usize, flag: &str| -> Result<String, String> {
            *idx += 1;
            args.get(*idx)
                .cloned()
                .ok_or_else(|| format!("{flag} requires a value"))
        };
        match arg {
            "-h" | "--help" => help = true,
            "--installed" => installed_flag = true,
            "--offline" => offline = true,
            "--source" => source_arg = Some(next(&mut i, "--source")?),
            "--version" => {
                let value = next(&mut i, "--version")?;
                if value.trim().is_empty() {
                    return Err("--version must not be blank".into());
                }
                version_arg = Some(value);
            }
            "--platform" => {
                let value = next(&mut i, "--platform")?;
                for token in value.split(',') {
                    let trimmed = token.trim();
                    if trimmed.eq_ignore_ascii_case("all") {
                        for p in Platform::ALL {
                            if !platforms.contains(&p) {
                                platforms.push(p);
                            }
                        }
                    } else {
                        let parsed = trimmed.parse::<Platform>().map_err(|e| e.to_string())?;
                        if !platforms.contains(&parsed) {
                            platforms.push(parsed);
                        }
                    }
                }
            }
            "--cache-dir" => {
                let value = next(&mut i, "--cache-dir")?;
                if value.trim().is_empty() {
                    return Err("--cache-dir must not be blank".into());
                }
                cache_dir = Some(PathBuf::from(value));
            }
            "--proxy" => {
                let value = next(&mut i, "--proxy")?;
                proxy = parse_proxy(&value)?;
            }
            "--netrc" => {
                let value = next(&mut i, "--netrc")?;
                if value.trim().is_empty() {
                    return Err("--netrc must not be blank".into());
                }
                netrc = Some(PathBuf::from(value));
            }
            other => return Err(format!("unknown option '{other}'")),
        }
        i += 1;
    }

    let source = if help || installed_flag {
        None
    } else if let Some(spec) = source_arg {
        Some(Source::parse(&spec).map_err(|err| {
            format!(
                "--source '{}' is not a valid source: {err}",
                redact_user_info(&spec)
            )
        })?)
    } else if let Some(env_spec) =
        get_env(SOURCE_ENV).and_then(|s| non_empty(Some(&s)).map(str::to_owned))
    {
        Some(Source::parse(&env_spec).map_err(|err| {
            format!(
                "{SOURCE_ENV} is not a valid source ('{}'): {err}",
                redact_user_info(&env_spec)
            )
        })?)
    } else {
        return Err(format!(
            "missing --source (or set {SOURCE_ENV} to a PyPI index, conda channel, or artifact path/URL)"
        ));
    };

    let version_raw = version_arg
        .or_else(|| get_env(VERSION_ENV).and_then(|s| non_empty(Some(&s)).map(str::to_owned)));
    let version = match version_raw.as_deref() {
        None => VersionSpec::Latest,
        Some(v) if v.trim().eq_ignore_ascii_case("latest") => VersionSpec::Latest,
        Some(v) => VersionSpec::exact(v.trim()),
    };

    if cache_dir.is_none()
        && let Some(env_dir) =
            get_env("XBBG_CACHE_DIR").and_then(|s| non_empty(Some(&s)).map(str::to_owned))
    {
        cache_dir = Some(PathBuf::from(env_dir));
    }

    Ok(CliArgs {
        help,
        installed: installed_flag,
        source,
        version,
        platforms,
        cache_dir,
        offline,
        proxy,
        netrc,
    })
}

fn print_provisioned(result: &Provisioned, out: &mut dyn Write) -> std::io::Result<()> {
    writeln!(out, "version          {}", result.version)?;
    writeln!(out, "platform         {}", result.platform.as_str())?;
    writeln!(out, "library          {}", result.lib_path.display())?;
    writeln!(out, "library sha256   {}", result.lib_sha256)?;
    writeln!(out, "artifact         {}", result.artifact_url)?;
    writeln!(out, "artifact sha256  {}", result.artifact_sha256)?;
    writeln!(out, "source           {}", result.source)?;
    writeln!(out, "cache hit        {}", result.cache_hit)?;
    for license in &result.license_paths {
        writeln!(out, "license          {}", license.display())?;
    }
    writeln!(out)?;
    Ok(())
}

fn run_provision_blocking(
    options: ProvisionOptions,
    source: Source,
    platform: Platform,
    version: VersionSpec,
) -> crate::Result<Provisioned> {
    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::scope(|scope| {
            scope
                .spawn(move || {
                    let provisioner = crate::blocking::Provisioner::new(options)?;
                    provisioner.provision(&source, platform, &version)
                })
                .join()
                .map_err(|_| crate::Error::Internal("provisioning thread panicked".into()))?
        })
    } else {
        let provisioner = crate::blocking::Provisioner::new(options)?;
        provisioner.provision(&source, platform, &version)
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CppMode {
    Add,
    List,
    Remove,
    CleanCache,
}

fn run_cpp(prog: &str, args: &[String], out: &mut dyn Write, err: &mut dyn Write) -> i32 {
    let usage = cpp_usage(prog);
    let mut help = false;
    let mut mode = CppMode::Add;
    let mut version: Option<String> = None;
    let mut platform: Option<Platform> = None;
    let mut vendor_dir = PathBuf::from(DEFAULT_VENDOR_DIR);
    let mut env_file = PathBuf::from(DEFAULT_ENV_FILE);
    let mut set_active = true;
    let mut force = false;
    let mut proxy = ProxyConfig::System;
    let mut netrc: Option<PathBuf> = None;
    let mut index_url: Option<String> = None;
    let mut raw_base_url: Option<String> = None;

    let mut i = 0;
    while i < args.len() {
        let arg = args[i].as_str();
        let next = |idx: &mut usize, flag: &str| -> Result<String, String> {
            *idx += 1;
            args.get(*idx)
                .cloned()
                .ok_or_else(|| format!("{flag} requires a value"))
        };
        let parsed = (|| -> Result<(), String> {
            match arg {
                "-h" | "--help" => help = true,
                "--list" => mode = CppMode::List,
                "--clean-cache" => mode = CppMode::CleanCache,
                "--remove" => {
                    mode = CppMode::Remove;
                    if let Some(next_arg) = args.get(i + 1)
                        && !next_arg.starts_with('-')
                    {
                        i += 1;
                        version = Some(next_arg.clone());
                    }
                }
                "--version" => {
                    let v = next(&mut i, "--version")?;
                    if v.trim().is_empty() {
                        return Err("--version must not be blank".into());
                    }
                    version = Some(v);
                }
                "--platform" => {
                    let v = next(&mut i, "--platform")?;
                    platform = Some(v.trim().parse::<Platform>().map_err(|e| e.to_string())?);
                }
                "--dir" => {
                    let v = next(&mut i, "--dir")?;
                    if v.trim().is_empty() {
                        return Err("--dir must not be blank".into());
                    }
                    vendor_dir = PathBuf::from(v);
                }
                "--env-file" => {
                    let v = next(&mut i, "--env-file")?;
                    if v.trim().is_empty() {
                        return Err("--env-file must not be blank".into());
                    }
                    env_file = PathBuf::from(v);
                }
                "--no-set-active" => set_active = false,
                "--set-active" => set_active = true,
                "--force" => force = true,
                "--proxy" => {
                    let v = next(&mut i, "--proxy")?;
                    proxy = parse_proxy(&v)?;
                }
                "--netrc" => {
                    let v = next(&mut i, "--netrc")?;
                    if v.trim().is_empty() {
                        return Err("--netrc must not be blank".into());
                    }
                    netrc = Some(PathBuf::from(v));
                }
                "--index-url" => {
                    let v = next(&mut i, "--index-url")?;
                    index_url = Some(v);
                }
                "--raw-base-url" => {
                    let v = next(&mut i, "--raw-base-url")?;
                    raw_base_url = Some(v);
                }
                other if other.starts_with('-') => {
                    return Err(format!("unknown option '{other}'"));
                }
                positional => {
                    if version.is_none() {
                        version = Some(positional.to_owned());
                    } else {
                        return Err(format!("unexpected argument '{positional}'"));
                    }
                }
            }
            Ok(())
        })();
        if let Err(msg) = parsed {
            let _ = writeln!(err, "error: {msg}");
            let _ = writeln!(err, "{usage}");
            return 2;
        }
        i += 1;
    }

    if help {
        let _ = writeln!(out, "{usage}");
        return 0;
    }

    match mode {
        CppMode::CleanCache => match clean_cache(&vendor_dir) {
            Ok(removed) => {
                if removed.is_empty() {
                    if vendor_dir.join(".cache").is_dir() {
                        let _ = writeln!(out, "Cache is already empty.");
                    } else {
                        let _ = writeln!(out, "No cache directory found. Nothing to clean.");
                    }
                } else {
                    for name in removed {
                        let _ = writeln!(out, "Removed: {name}");
                    }
                }
                0
            }
            Err(error) => {
                let _ = writeln!(err, "error [SDK_{}]: {error}", error.code());
                1
            }
        },
        CppMode::List => match list_installed(&vendor_dir) {
            Ok(versions) => {
                let active = active_version(&env_file);
                if versions.is_empty() {
                    let _ = writeln!(out, "No SDK versions installed.");
                } else {
                    for v in versions {
                        if active.as_deref() == Some(v.as_str()) {
                            let _ = writeln!(out, "{v} (active)");
                        } else {
                            let _ = writeln!(out, "{v}");
                        }
                    }
                }
                0
            }
            Err(error) => {
                let _ = writeln!(err, "error [SDK_{}]: {error}", error.code());
                1
            }
        },
        CppMode::Remove => {
            let Some(v) = version else {
                let _ = writeln!(err, "error: --remove requires a version");
                let _ = writeln!(err, "{usage}");
                return 2;
            };
            match remove_version(&vendor_dir, &env_file, &v, out) {
                Ok(()) => 0,
                Err(error) => {
                    let _ = writeln!(err, "error [SDK_{}]: {error}", error.code());
                    1
                }
            }
        }
        CppMode::Add => {
            let Some(target_platform) = platform.or_else(Platform::host) else {
                let _ = writeln!(
                    err,
                    "error [SDK_UNSUPPORTED_PLATFORM]: this platform has no Bloomberg C++ SDK; pass --platform"
                );
                return 1;
            };
            let mut opts = StageOptions::new(target_platform);
            opts.version = version;
            opts.vendor_dir = vendor_dir;
            opts.env_file = env_file;
            opts.set_active = set_active;
            opts.force = force;
            opts.network.proxy = proxy;
            opts.network.netrc = netrc;
            if let Some(url) = index_url {
                opts.index_url = url;
            }
            if let Some(url) = raw_base_url {
                opts.raw_base_url = url;
            }
            match stage_blocking(opts, out) {
                Ok(_) => 0,
                Err(error) => {
                    let _ = writeln!(err, "error [SDK_{}]: {error}", error.code());
                    1
                }
            }
        }
    }
}

/// Runs the unified xbbg SDK CLI, returning exit code 0 (ok), 1 (failure), or 2 (usage).
pub fn run(
    prog: &str,
    args: &[String],
    get_env: &dyn Fn(&str) -> Option<String>,
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> i32 {
    if args.first().map(String::as_str) == Some("cpp") {
        return run_cpp(prog, &args[1..], out, err);
    }

    let usage = cli_usage(prog);
    let cli = match parse_cli(args, get_env) {
        Ok(cli) => cli,
        Err(message) => {
            let _ = writeln!(err, "error: {message}");
            let _ = writeln!(err, "{usage}");
            return 2;
        }
    };
    if cli.help {
        let _ = writeln!(out, "{usage}");
        return 0;
    }

    let Some(cache_dir) = cli.cache_dir.or_else(default_cache_root) else {
        let _ = writeln!(
            err,
            "error [SDK_INVALID_ARGUMENT]: cannot determine the xbbg cache directory; pass --cache-dir or set XBBG_CACHE_DIR"
        );
        return 1;
    };

    if cli.installed {
        let platforms: Vec<Option<Platform>> = if cli.platforms.is_empty() {
            vec![None]
        } else {
            cli.platforms.into_iter().map(Some).collect()
        };
        let mut count = 0usize;
        for platform in platforms {
            match installed(&cache_dir, platform) {
                Ok(list) => {
                    for item in &list {
                        let _ = print_provisioned(item, out);
                        count += 1;
                    }
                }
                Err(error) => {
                    let _ = writeln!(err, "error [SDK_{}]: {error}", error.code());
                    return 1;
                }
            }
        }
        if count == 0 {
            let abs = std::path::absolute(&cache_dir).unwrap_or(cache_dir);
            let _ = writeln!(out, "no BLPAPI runtime installed under {}", abs.display());
        }
        return 0;
    }

    let source = cli.source.expect("source required when not --installed");
    let platforms = if cli.platforms.is_empty() {
        match Platform::host() {
            Some(host) => vec![host],
            None => {
                let _ = writeln!(
                    err,
                    "error [SDK_UNSUPPORTED_PLATFORM]: this platform has no Bloomberg runtime; pass --platform"
                );
                return 1;
            }
        }
    } else {
        cli.platforms
    };

    for platform in platforms {
        let mut options = ProvisionOptions::new(cache_dir.clone());
        options.network.offline = cli.offline;
        options.network.proxy = cli.proxy.clone();
        options.network.netrc = cli.netrc.clone();
        match run_provision_blocking(options, source.clone(), platform, cli.version.clone()) {
            Ok(result) => {
                let _ = print_provisioned(&result, out);
            }
            Err(error) => {
                let _ = writeln!(err, "error [SDK_{}]: {error}", error.code());
                return 1;
            }
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_env(_: &str) -> Option<String> {
        None
    }

    #[test]
    fn cli_help_exits_zero_and_mentions_env_and_cpp() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = run("xbbg-sdk", &["--help".into()], &no_env, &mut out, &mut err);
        assert_eq!(code, 0);
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains(SOURCE_ENV));
        assert!(text.contains("--netrc"));
        assert!(text.contains("xbbg-sdk cpp"));
        assert!(err.is_empty());
    }

    #[test]
    fn cli_rejects_missing_source_unknown_flag_and_redacts_proxy() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        assert_eq!(run("xbbg-sdk", &[], &no_env, &mut out, &mut err), 2);
        assert!(String::from_utf8(err).unwrap().contains(SOURCE_ENV));

        let mut err = Vec::new();
        assert_eq!(
            run(
                "xbbg-sdk",
                &[
                    "--source".into(),
                    "conda-forge".into(),
                    "--proxy".into(),
                    "ftp://alice:secret@proxy.example".into(),
                ],
                &no_env,
                &mut out,
                &mut err,
            ),
            2
        );
        let text = String::from_utf8(err).unwrap();
        assert!(!text.contains("secret"));
        assert!(text.contains("***@proxy.example"));
    }

    #[test]
    fn cli_installed_on_empty_cache_reports_none() {
        let tmp = tempfile::tempdir().unwrap();
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = run(
            "xbbg-sdk",
            &[
                "--installed".into(),
                "--cache-dir".into(),
                tmp.path().to_string_lossy().into_owned(),
            ],
            &no_env,
            &mut out,
            &mut err,
        );
        assert_eq!(code, 0);
        assert!(
            String::from_utf8(out)
                .unwrap()
                .contains("no BLPAPI runtime installed")
        );
        assert!(err.is_empty());
    }

    #[test]
    fn cpp_subcommand_help_list_clean_and_remove() {
        let tmp = tempfile::tempdir().unwrap();
        let vendor = tmp.path().join("vendor");
        let env_file = tmp.path().join(".env");

        let mut out = Vec::new();
        let mut err = Vec::new();
        assert_eq!(
            run(
                "xbbg-sdk",
                &["cpp".into(), "--help".into()],
                &no_env,
                &mut out,
                &mut err,
            ),
            0
        );
        assert!(String::from_utf8(out).unwrap().contains("--no-set-active"));

        let mut out = Vec::new();
        assert_eq!(
            run(
                "xbbg-sdk",
                &[
                    "cpp".into(),
                    "--list".into(),
                    "--dir".into(),
                    vendor.to_string_lossy().into_owned(),
                    "--env-file".into(),
                    env_file.to_string_lossy().into_owned(),
                ],
                &no_env,
                &mut out,
                &mut err,
            ),
            0
        );
        assert!(
            String::from_utf8(out)
                .unwrap()
                .contains("No SDK versions installed.")
        );

        std::fs::create_dir_all(vendor.join("3.26.5.1/include")).unwrap();
        crate::cpp_sdk::set_active_version(&env_file, &vendor, "3.26.5.1").unwrap();

        let mut out = Vec::new();
        assert_eq!(
            run(
                "xbbg-sdk",
                &[
                    "cpp".into(),
                    "--list".into(),
                    "--dir".into(),
                    vendor.to_string_lossy().into_owned(),
                    "--env-file".into(),
                    env_file.to_string_lossy().into_owned(),
                ],
                &no_env,
                &mut out,
                &mut err,
            ),
            0
        );
        assert!(
            String::from_utf8(out)
                .unwrap()
                .contains("3.26.5.1 (active)")
        );

        let mut out = Vec::new();
        assert_eq!(
            run(
                "xbbg-sdk",
                &[
                    "cpp".into(),
                    "--remove".into(),
                    "3.26.5.1".into(),
                    "--dir".into(),
                    vendor.to_string_lossy().into_owned(),
                    "--env-file".into(),
                    env_file.to_string_lossy().into_owned(),
                ],
                &no_env,
                &mut out,
                &mut err,
            ),
            0
        );
        assert!(!vendor.join("3.26.5.1").exists());
    }
}
