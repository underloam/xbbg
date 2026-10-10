//! Compiler-owned build facts shared by bindings and benchmark executables.

use std::{
    env,
    path::{Path, PathBuf},
    process::Command,
};

pub(crate) fn emit() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    let workspace = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?)
        .join("../..")
        .canonicalize()?;
    println!(
        "cargo:rerun-if-changed={}",
        workspace.join("scripts/rust_build_provenance.rs").display()
    );
    let git_root = git_output(&workspace, &["rev-parse", "--show-toplevel"])
        .and_then(|root| PathBuf::from(root).canonicalize().ok())
        .filter(|root| root == &workspace);
    if let Some(root) = &git_root {
        for name in ["HEAD", "refs", "packed-refs"] {
            if let Some(path) = git_output(
                root,
                &["rev-parse", "--path-format=absolute", "--git-path", name],
            ) {
                if Path::new(&path).exists() {
                    println!("cargo:rerun-if-changed={path}");
                }
            }
        }
    }
    // Immutable commit/tag facts: a cached build script cannot attest worktree dirtiness.
    let describe = git_root
        .as_ref()
        .and_then(|root| git_output(root, &["describe", "--tags", "--always"]))
        .unwrap_or_else(|| "unknown".to_string());
    let commit = git_root
        .as_ref()
        .and_then(|root| git_output(root, &["rev-parse", "HEAD"]))
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=VERGEN_GIT_DESCRIBE={describe}");
    println!("cargo:rustc-env=XBBG_BUILD_GIT_COMMIT={commit}");

    for (key, source) in [
        ("PROFILE", "PROFILE"),
        ("TARGET", "TARGET"),
        ("HOST", "HOST"),
        ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS"),
        ("OPT_LEVEL", "OPT_LEVEL"),
        ("TARGET_FEATURES", "CARGO_CFG_TARGET_FEATURE"),
    ] {
        println!("cargo:rerun-if-env-changed={source}");
        let value = env::var(source).unwrap_or_default();
        println!("cargo:rustc-env=XBBG_BUILD_{key}={value}");
    }
    let flags = env::var("CARGO_ENCODED_RUSTFLAGS").unwrap_or_default();
    let mut flags = flags.split('\u{1f}');
    let mut target_cpu = "default";
    while let Some(flag) = flags.next() {
        let option = if flag == "-C" || flag == "--codegen" {
            flags.next()
        } else {
            flag.strip_prefix("-C")
                .or_else(|| flag.strip_prefix("--codegen="))
        }
        .map(str::trim_start);
        if let Some(cpu) = option.and_then(|option| option.strip_prefix("target-cpu=")) {
            target_cpu = cpu;
        }
    }
    println!("cargo:rustc-env=XBBG_BUILD_TARGET_CPU={target_cpu}");
    let compiler = Command::new(env::var("RUSTC")?).arg("--version").output()?;
    if !compiler.status.success() {
        return Err("could not record the build compiler version".into());
    }
    println!(
        "cargo:rustc-env=XBBG_BUILD_RUSTC_VERSION={}",
        String::from_utf8(compiler.stdout)?.trim()
    );
    let allocator = if env::var_os("CARGO_FEATURE_MIMALLOC").is_some() {
        "mimalloc"
    } else {
        "system"
    };
    println!("cargo:rustc-env=XBBG_BUILD_ALLOCATOR={allocator}");
    Ok(())
}

fn git_output(root: &Path, args: &[&str]) -> Option<String> {
    Command::new("git")
        .current_dir(root)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}
