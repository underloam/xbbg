use std::env;
use std::fs;
use std::path::{Path, PathBuf};

mod libclang;

fn main() {
    // Ensure rebuilds when env changes
    println!("cargo:rerun-if-env-changed=BLPAPI_INCLUDE_DIR");
    println!("cargo:rerun-if-env-changed=BLPAPI_LIB_DIR");
    println!("cargo:rerun-if-env-changed=BLPAPI_ROOT");
    println!("cargo:rerun-if-env-changed=BLPAPI_PREGENERATED_BINDINGS");
    println!("cargo:rerun-if-env-changed=BLPAPI_BINDINGS_EXPORT_PATH");
    println!("cargo:rerun-if-env-changed=LIBCLANG_PATH");
    // Precise source tracking: Cargo reruns build scripts on their own change
    // only when told to. Without these, editing the allowlist or the libclang
    // helper would not retrigger binding generation.
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=libclang.rs");
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-env-changed=DOCS_RS");

    // docs.rs has no Bloomberg SDK and no network access, so header discovery and
    // bindgen cannot run there and this crate cannot be documented on docs.rs.
    //
    // Emitting an empty stub here was tried and rejected: it makes THIS crate build
    // green with an empty API page, while xbbg_core/src/ffi.rs re-exports ~150
    // BLPAPI symbols unconditionally and still fails with 29 unresolved imports,
    // taking xbbg-async and xbbg-recipes down with it. Fixing docs.rs properly
    // requires committing a pregenerated binding surface, and the Bloomberg SDK
    // license permits distributing copies but not "modifying, adapting", which
    // makes redistributing bindgen-derived declarations a licensing decision rather
    // than a build-script one.
    //
    // So fail, but fail legibly: the docs.rs log states the real reason instead of
    // an opaque "SDK root does not exist" from the resolver below.
    if env::var_os("DOCS_RS").is_some() {
        panic!(
            "blpapi-sys: cannot build on docs.rs. The BLPAPI C SDK is proprietary and \
             is neither bundled nor downloadable in the docs.rs sandbox (no network), \
             so bindgen cannot generate bindings. Documentation for this crate must be \
             built locally with BLPAPI_ROOT set: `cargo doc -p xbbg-blpapi-sys`."
        );
    }

    // Resolve include and lib directories from environment (precedence order)
    let (include_dir, lib_dir) =
        resolve_include_and_lib_dirs().unwrap_or_else(|e| panic!("blpapi-sys: {}", e));

    // Emit link search path
    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    // Consumers use Cargo's resolved SDK rather than maintaining another locator.
    println!(
        "cargo:rustc-env=XBBG_SDK_INCLUDE_DIR={}",
        include_dir
            .canonicalize()
            .expect("resolved SDK include directory must be readable")
            .display()
    );

    // Enforce mutually exclusive static/dynamic features
    let want_static = env::var_os("CARGO_FEATURE_STATIC").is_some();
    let want_dynamic = env::var_os("CARGO_FEATURE_DYNAMIC").is_some() || !want_static;
    if want_static && want_dynamic {
        panic!("Features 'static' and 'dynamic' are mutually exclusive");
    }

    // Determine library base name based on target platform and architecture
    let lib_name = detect_link_lib_name(&lib_dir);

    // Emit link type
    if want_static {
        println!("cargo:rustc-link-lib=static={}", lib_name);
    } else {
        println!("cargo:rustc-link-lib=dylib={}", lib_name);
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));
    let bindings_out = out_dir.join("bindings.rs");

    // Track SDK headers and link inputs so an in-place SDK replacement rebuilds
    // dependent artifacts rather than stamping new provenance onto old bytes.
    println!("cargo:rerun-if-changed={}", include_dir.display());
    println!("cargo:rerun-if-changed={}", lib_dir.display());

    if let Some(pregenerated_bindings) = env::var_os("BLPAPI_PREGENERATED_BINDINGS") {
        let pregenerated_bindings = PathBuf::from(pregenerated_bindings);
        if !pregenerated_bindings.is_file() {
            panic!(
                "blpapi-sys: BLPAPI_PREGENERATED_BINDINGS does not point to a file: {}",
                pregenerated_bindings.display()
            );
        }
        println!("cargo:rerun-if-changed={}", pregenerated_bindings.display());

        copy_bindings(&pregenerated_bindings, &bindings_out)
            .unwrap_or_else(|e| panic!("blpapi-sys: {}", e));

        export_bindings_if_requested(&pregenerated_bindings);
        return;
    }

    // Cargo tracks generation inputs; cross-build reuse is explicit through
    // BLPAPI_PREGENERATED_BINDINGS rather than an SDK-local cache that can
    // outlive changes to headers or the resolved bindgen dependency.
    libclang::prepare_windows_libclang_alias(&out_dir)
        .unwrap_or_else(|e| panic!("blpapi-sys: {}", e));

    // Build bindgen wrapper that includes all blpapi_*.h headers found
    let wrapper =
        generate_wrapper_header(&include_dir).unwrap_or_else(|e| panic!("blpapi-sys: {}", e));

    // Pin the generated code to this crate's MSRV and edition rather than to the
    // rustc running the build script: edition 2024 requires `unsafe extern "C"`
    // blocks, and bindings exported via BLPAPI_BINDINGS_EXPORT_PATH and replayed
    // via BLPAPI_PREGENERATED_BINDINGS must compile on every supported toolchain.
    let rust_target = env!("CARGO_PKG_RUST_VERSION")
        .parse::<bindgen::RustTarget>()
        .expect("blpapi-sys: rust-version must be a valid bindgen Rust target");

    let builder = bindgen::Builder::default()
        .rust_target(rust_target)
        .rust_edition(bindgen::RustEdition::Edition2024)
        .header_contents("wrapper.h", &wrapper)
        .clang_arg(format!("-I{}", include_dir.display()))
        .allowlist_function("^blpapi_.*")
        .allowlist_type("^blpapi_.*")
        .allowlist_var("^(BLPAPI_.*|BLPAPI_SDK_VERSION.*|g_blpapi.*)")
        .ctypes_prefix("cty")
        .use_core()
        .layout_tests(false)
        .derive_default(false)
        .generate_comments(false)
        .formatter(bindgen::Formatter::Rustfmt);

    // Generate and write
    let bindings = builder
        .generate()
        .expect("Unable to generate blpapi bindings via bindgen");

    bindings
        .write_to_file(&bindings_out)
        .unwrap_or_else(|e| panic!("Failed to write bindings: {}", e));

    export_bindings_if_requested(&bindings_out);
}

fn export_bindings_if_requested(bindings: &Path) {
    if let Some(export_path) = env::var_os("BLPAPI_BINDINGS_EXPORT_PATH") {
        let export_path = PathBuf::from(export_path);
        copy_bindings(bindings, &export_path).unwrap_or_else(|e| panic!("blpapi-sys: {}", e));
    }
}

fn copy_bindings(src: &Path, dst: &Path) -> Result<(), String> {
    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            format!(
                "Failed to create parent directory for {}: {}",
                dst.display(),
                e
            )
        })?;
    }

    fs::copy(src, dst).map_err(|e| {
        format!(
            "Failed to copy bindings from {} to {}: {}",
            src.display(),
            dst.display(),
            e
        )
    })?;
    Ok(())
}

fn resolve_env_path(value: std::ffi::OsString) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() || path.exists() {
        return path;
    }

    if let Ok(manifest_dir) = env::var("CARGO_MANIFEST_DIR") {
        let mut dir = PathBuf::from(manifest_dir);
        loop {
            let candidate = dir.join(&path);
            if candidate.exists() {
                return candidate;
            }
            if !dir.pop() {
                break;
            }
        }
    }

    path
}

fn resolve_include_and_lib_dirs() -> Result<(PathBuf, PathBuf), String> {
    // 1) Explicit include/lib
    let include = env::var_os("BLPAPI_INCLUDE_DIR");
    let lib = env::var_os("BLPAPI_LIB_DIR");
    if let (Some(inc), Some(lib)) = (include, lib) {
        let inc = resolve_env_path(inc);
        let lib = resolve_env_path(lib);
        validate_header_exists(&inc)?;
        return Ok((inc, lib));
    }

    // 2) Root
    if let Some(root) = env::var_os("BLPAPI_ROOT") {
        let root = resolve_env_path(root);
        // Adding a newer version beneath a container must invalidate discovery.
        println!("cargo:rerun-if-changed={}", root.display());
        let (inc, lib) = resolve_sdk_layout(&root)?;
        validate_header_exists(&inc)?;
        return Ok((inc, lib));
    }

    Err("Cannot locate Bloomberg SDK. Set BLPAPI_INCLUDE_DIR/BLPAPI_LIB_DIR or BLPAPI_ROOT.".into())
}

fn resolve_sdk_layout(root: &Path) -> Result<(PathBuf, PathBuf), String> {
    let mut last_error = None;

    for candidate in candidate_sdk_roots(root)? {
        match derive_include_lib(&candidate) {
            Ok(layout) => return Ok(layout),
            Err(err) => last_error = Some(err),
        }
    }

    if let Some(err) = last_error {
        Err(err)
    } else {
        Err(format!("No SDK candidates found under {}", root.display()))
    }
}

fn candidate_sdk_roots(root: &Path) -> Result<Vec<PathBuf>, String> {
    if !root.is_dir() {
        return Err(format!(
            "SDK root does not exist or is not a directory: {}",
            root.display()
        ));
    }

    let mut roots = vec![root.to_path_buf()];

    let children = sorted_child_dirs(root)?;

    for child in &children {
        if !roots.iter().any(|existing| existing == child) {
            roots.push(child.clone());
        }
    }

    for child in children {
        for grandchild in sorted_child_dirs(&child)? {
            if !roots.iter().any(|existing| existing == &grandchild) {
                roots.push(grandchild);
            }
        }
    }

    Ok(roots)
}

fn sorted_child_dirs(root: &Path) -> Result<Vec<PathBuf>, String> {
    let mut entries = Vec::new();

    for entry in fs::read_dir(root).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path();
        if path.is_dir() {
            entries.push(path);
        }
    }

    entries.sort_by(|a, b| compare_sdk_dir_names(a.file_name(), b.file_name()));
    Ok(entries)
}

fn compare_sdk_dir_names(
    a: Option<&std::ffi::OsStr>,
    b: Option<&std::ffi::OsStr>,
) -> std::cmp::Ordering {
    let a_name = a.and_then(|value| value.to_str()).unwrap_or_default();
    let b_name = b.and_then(|value| value.to_str()).unwrap_or_default();

    match (
        parse_version_components(a_name),
        parse_version_components(b_name),
    ) {
        (Some(a_version), Some(b_version)) => b_version.cmp(&a_version),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => a_name.cmp(b_name),
    }
}

fn parse_version_components(value: &str) -> Option<Vec<u32>> {
    let mut parts = Vec::new();

    for piece in value.split('.') {
        if piece.is_empty() {
            return None;
        }
        parts.push(piece.parse().ok()?);
    }

    if parts.len() >= 3 && parts.len() <= 4 {
        Some(parts)
    } else {
        None
    }
}

fn derive_include_lib(root: &Path) -> Result<(PathBuf, PathBuf), String> {
    let include_candidates = [root.join("include"), root.join("Include")];

    for include_dir in include_candidates {
        if !include_dir.is_dir() || validate_header_exists(&include_dir).is_err() {
            continue;
        }

        for lib_dir in library_dir_candidates(root) {
            if lib_dir.is_dir() && contains_linkable_blpapi_lib(&lib_dir) {
                return Ok((include_dir.clone(), lib_dir));
            }
        }
    }

    Err(format!(
        "Could not derive include/lib under {}.",
        root.display()
    ))
}

fn library_dir_candidates(root: &Path) -> Vec<PathBuf> {
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
    let mut candidates = vec![
        root.join("lib"),
        root.join("Lib"),
        root.join("lib64"),
        root.join("bin"),
    ];

    if target_os == "windows" {
        let win_lib_subdir = if target_arch == "x86" {
            "win32"
        } else {
            "win64"
        };
        candidates.push(root.join("lib").join(win_lib_subdir));
    } else if target_os == "linux" {
        candidates.push(root.join("Linux"));
        candidates.push(root.join("linux"));
    } else if target_os == "macos" {
        candidates.push(root.join("Darwin"));
        candidates.push(root.join("darwin"));
        candidates.push(root.join("MacOS"));
        candidates.push(root.join("macos"));
    }

    candidates
}

fn contains_linkable_blpapi_lib(lib_dir: &Path) -> bool {
    expected_library_files()
        .iter()
        .any(|file_name| lib_dir.join(file_name).is_file())
}

fn detect_link_lib_name(lib_dir: &Path) -> String {
    if lib_dir.join("blpapi3_64.lib").is_file()
        || lib_dir.join("blpapi3_64.dll").is_file()
        || lib_dir.join("libblpapi3_64.so").is_file()
        || lib_dir.join("libblpapi3_64.dylib").is_file()
        || lib_dir.join("libblpapi3_64.a").is_file()
    {
        return "blpapi3_64".to_string();
    }

    if lib_dir.join("blpapi3_32.lib").is_file()
        || lib_dir.join("blpapi3_32.dll").is_file()
        || lib_dir.join("libblpapi3_32.so").is_file()
        || lib_dir.join("libblpapi3_32.dylib").is_file()
        || lib_dir.join("libblpapi3_32.a").is_file()
    {
        return "blpapi3_32".to_string();
    }

    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
    if target_os == "windows" {
        if target_arch == "x86" {
            "blpapi3_32".to_string()
        } else {
            "blpapi3_64".to_string()
        }
    } else {
        "blpapi3".to_string()
    }
}

fn expected_library_files() -> Vec<&'static str> {
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();

    if target_os == "windows" {
        if target_arch == "x86" {
            vec!["blpapi3_32.lib", "blpapi3_32.dll"]
        } else {
            vec!["blpapi3_64.lib", "blpapi3_64.dll"]
        }
    } else if target_os == "macos" {
        vec![
            "libblpapi3.so",
            "libblpapi3_64.so",
            "libblpapi3.dylib",
            "libblpapi3_64.dylib",
            "libblpapi3.a",
            "libblpapi3_64.a",
        ]
    } else {
        vec![
            "libblpapi3.so",
            "libblpapi3_64.so",
            "libblpapi3.a",
            "libblpapi3_64.a",
        ]
    }
}

fn validate_header_exists(include_dir: &Path) -> Result<(), String> {
    let candidates = [
        "blpapi_session.h",
        "blpapi_defs.h",
        "blpapi_types.h",
        "blpapi_name.h",
    ];
    let ok = candidates.iter().any(|h| include_dir.join(h).is_file());
    if ok {
        Ok(())
    } else {
        Err(format!(
            "Could not find expected Bloomberg headers in {}",
            include_dir.display()
        ))
    }
}

fn generate_wrapper_header(include_dir: &Path) -> Result<String, String> {
    let mut headers: Vec<String> = Vec::new();
    for entry in fs::read_dir(include_dir).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path();
        if let (Some(stem), Some(ext)) = (path.file_name(), path.extension())
            && ext == "h"
        {
            let name = stem.to_string_lossy().to_string();
            if name.starts_with("blpapi_") {
                headers.push(format!("#include <{}>", stem.to_string_lossy()));
            }
        }
    }
    if headers.is_empty() {
        return Err(format!(
            "No blpapi_*.h headers found in {}",
            include_dir.display()
        ));
    }
    headers.sort();
    let mut out = String::new();
    out.push_str("/* auto-generated wrapper for bindgen */\n");
    for line in headers {
        out.push_str(&line);
        out.push('\n');
    }
    Ok(out)
}
