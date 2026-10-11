use std::env;
use std::fs;
use std::path::{Path, PathBuf};

/// Makes conda-forge's versioned Windows libclang DLL discoverable by bindgen.
///
/// bindgen/clang-sys only probes `clang.dll` and `libclang.dll`, while pixi's
/// conda-forge package currently installs `libclang-13.dll` under
/// `%CONDA_PREFIX%\\Library\\bin`. Copying that DLL to an OUT_DIR-local
/// `libclang.dll` alias keeps the repository self-contained and avoids mutating
/// the pixi environment.
pub(crate) fn prepare_windows_libclang_alias(out_dir: &Path) -> Result<(), String> {
    println!("cargo:rerun-if-env-changed=LIBCLANG_PATH");
    println!("cargo:rerun-if-env-changed=CONDA_PREFIX");

    if !cfg!(windows) {
        return Ok(());
    }

    for dir in libclang_candidate_dirs() {
        if has_bindgen_libclang_name(&dir) {
            // SAFETY: build scripts run in their own process, and this helper runs
            // on the main thread before bindgen or anything else spawns a thread.
            unsafe { env::set_var("LIBCLANG_PATH", &dir) };
            return Ok(());
        }

        if let Some(versioned_dll) = find_versioned_libclang_dll(&dir)? {
            let alias_dir = out_dir.join("libclang");
            fs::create_dir_all(&alias_dir).map_err(|e| {
                format!(
                    "Failed to create libclang alias directory {}: {}",
                    alias_dir.display(),
                    e
                )
            })?;

            let alias = alias_dir.join("libclang.dll");
            fs::copy(&versioned_dll, &alias).map_err(|e| {
                format!(
                    "Failed to copy {} to {}: {}",
                    versioned_dll.display(),
                    alias.display(),
                    e
                )
            })?;

            // SAFETY: build scripts run in their own process, and this helper runs
            // on the main thread before bindgen or anything else spawns a thread.
            unsafe { env::set_var("LIBCLANG_PATH", &alias_dir) };
            println!("cargo:rerun-if-changed={}", versioned_dll.display());
            return Ok(());
        }
    }

    Ok(())
}

fn libclang_candidate_dirs() -> Vec<PathBuf> {
    let mut dirs = Vec::new();

    if let Some(path) = env::var_os("LIBCLANG_PATH").map(PathBuf::from) {
        if path.is_dir() {
            dirs.push(path);
        } else if let Some(parent) = path.parent() {
            dirs.push(parent.to_path_buf());
        }
    }

    if let Some(conda_prefix) = env::var_os("CONDA_PREFIX") {
        let conda_prefix = PathBuf::from(conda_prefix);
        dirs.push(conda_prefix.join("Library").join("bin"));
        dirs.push(conda_prefix.join("lib"));
    }

    dirs.sort();
    dirs.dedup();
    dirs
}

fn has_bindgen_libclang_name(dir: &Path) -> bool {
    dir.join("libclang.dll").is_file() || dir.join("clang.dll").is_file()
}

fn find_versioned_libclang_dll(dir: &Path) -> Result<Option<PathBuf>, String> {
    if !dir.is_dir() {
        return Ok(None);
    }

    let mut matches = Vec::new();
    for entry in fs::read_dir(dir)
        .map_err(|e| format!("Failed to read libclang directory {}: {}", dir.display(), e))?
    {
        let path = entry.map_err(|e| e.to_string())?.path();
        let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };

        if file_name.starts_with("libclang-") && file_name.ends_with(".dll") {
            matches.push(path);
        }
    }

    matches.sort_by(|left, right| {
        version_key(left)
            .cmp(&version_key(right))
            .then(left.cmp(right))
    });
    Ok(matches.pop())
}

fn version_key(path: &Path) -> Vec<u32> {
    path.file_name()
        .and_then(|value| value.to_str())
        .and_then(|value| value.strip_prefix("libclang-"))
        .and_then(|value| value.strip_suffix(".dll"))
        .map(|value| {
            value
                .split(|ch: char| !ch.is_ascii_digit())
                .filter(|part| !part.is_empty())
                .filter_map(|part| part.parse().ok())
                .collect()
        })
        .unwrap_or_default()
}
