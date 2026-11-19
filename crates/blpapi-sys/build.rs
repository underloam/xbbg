use std::env;
use std::fs;
use std::path::{Path, PathBuf};

mod libclang;

/// The checked-in allowlist of BLPAPI C functions the workspace calls.
const FUNCTIONS_FILE: &str = "functions.txt";

fn main() {
    // Ensure rebuilds when env changes
    println!("cargo:rerun-if-env-changed=BLPAPI_INCLUDE_DIR");
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
    println!("cargo:rerun-if-changed={FUNCTIONS_FILE}");
    println!("cargo:rerun-if-env-changed=DOCS_RS");

    // docs.rs has no Bloomberg SDK and no network access, so header discovery and
    // bindgen cannot run there and this crate cannot be documented on docs.rs.
    //
    // Emitting an empty stub here was tried and rejected: it makes THIS crate build
    // green with an empty API page, while xbbg-core calls the generated function
    // table everywhere and still fails, taking xbbg-async and xbbg-recipes down
    // with it. Fixing docs.rs properly requires committing a pregenerated binding
    // surface, and the Bloomberg SDK license permits distributing copies but not
    // "modifying, adapting", which makes redistributing bindgen-derived
    // declarations a licensing decision rather than a build-script one.
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

    // The headers are the only SDK input: the runtime library is opened by path at
    // run time, so nothing here links it or searches for it.
    let include_dir = resolve_include_dir().unwrap_or_else(|e| panic!("blpapi-sys: {}", e));

    // Consumers use Cargo's resolved SDK rather than maintaining another locator.
    println!(
        "cargo:rustc-env=XBBG_SDK_INCLUDE_DIR={}",
        include_dir
            .canonicalize()
            .expect("resolved SDK include directory must be readable")
            .display()
    );

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));
    let bindings_out = out_dir.join("bindings.rs");

    // The allowlist is also compiled in as `FUNCTION_NAMES`, so a loader can say which
    // functions a runtime lacks: the generated `from_library` only reports that one is missing.
    let functions = read_function_allowlist().unwrap_or_else(|e| panic!("blpapi-sys: {}", e));
    write_function_names(&functions, &out_dir.join("function_names.rs"))
        .unwrap_or_else(|e| panic!("blpapi-sys: {}", e));

    // Track SDK headers so an in-place SDK replacement rebuilds dependent
    // artifacts rather than stamping new provenance onto old bytes.
    println!("cargo:rerun-if-changed={}", include_dir.display());

    if let Some(pregenerated_bindings) = env::var_os("BLPAPI_PREGENERATED_BINDINGS") {
        let pregenerated_bindings = PathBuf::from(pregenerated_bindings);
        if !pregenerated_bindings.is_file() {
            panic!(
                "blpapi-sys: BLPAPI_PREGENERATED_BINDINGS does not point to a file: {}",
                pregenerated_bindings.display()
            );
        }
        println!("cargo:rerun-if-changed={}", pregenerated_bindings.display());

        // Bindings generated for another allowlist would compile until the first call of a
        // function they lack; say so here instead.
        let pregenerated = fs::read_to_string(&pregenerated_bindings).unwrap_or_else(|e| {
            panic!(
                "blpapi-sys: Failed to read {}: {}",
                pregenerated_bindings.display(),
                e
            )
        });
        let missing = functions_missing_from(&pregenerated, &functions);
        if !missing.is_empty() {
            panic!(
                "blpapi-sys: BLPAPI_PREGENERATED_BINDINGS ({}) was generated for another {FUNCTIONS_FILE}; it lacks: {}",
                pregenerated_bindings.display(),
                missing.join(", ")
            );
        }

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

    let mut builder = bindgen::Builder::default()
        .rust_target(rust_target)
        .rust_edition(bindgen::RustEdition::Edition2024)
        .header_contents("wrapper.h", &wrapper)
        .clang_arg(format!("-I{}", include_dir.display()))
        .allowlist_type("^blpapi_.*")
        .allowlist_var("^(BLPAPI_.*|BLPAPI_SDK_VERSION.*|g_blpapi.*)")
        // One struct of function pointers, resolved from a library opened at run
        // time. `require_all` makes the constructor fail at load for a function the
        // library lacks, instead of panicking at the first call; the allowlist
        // below is what keeps that set to the functions actually called.
        .dynamic_library_name("Blpapi")
        .dynamic_link_require_all(true)
        .ctypes_prefix("cty")
        .use_core()
        .layout_tests(false)
        .derive_default(false)
        .generate_comments(false)
        .formatter(bindgen::Formatter::Rustfmt);
    for function in &functions {
        builder = builder.allowlist_function(format!("^{function}$"));
    }

    // Generate and write
    let bindings = builder
        .generate()
        .expect("Unable to generate blpapi bindings via bindgen");

    // bindgen drops an allowlisted name the headers do not declare without a word.
    // Left alone, a typo or a function an SDK removed would only surface as a
    // missing method at the first call site, or never.
    let missing = functions_missing_from(&bindings.to_string(), &functions);
    if !missing.is_empty() {
        panic!(
            "blpapi-sys: {FUNCTIONS_FILE} names functions the SDK headers in {} do not declare: {}",
            include_dir.display(),
            missing.join(", ")
        );
    }

    bindings
        .write_to_file(&bindings_out)
        .unwrap_or_else(|e| panic!("Failed to write bindings: {}", e));

    export_bindings_if_requested(&bindings_out);
}

/// The names in `functions` that `bindings` does not declare as a field of the function table.
///
/// Compared without whitespace: without rustfmt (CI containers may lack it) bindgen writes
/// `pub blpapi_X :` rather than `pub blpapi_X:`.
fn functions_missing_from<'a>(bindings: &str, functions: &'a [String]) -> Vec<&'a str> {
    let generated: String = bindings.split_whitespace().collect();
    functions
        .iter()
        .map(String::as_str)
        .filter(|function| !generated.contains(&format!("pub{function}:")))
        .collect()
}

/// Writes `FUNCTION_NAMES`, the allowlist as C strings, for `src/lib.rs` to include.
fn write_function_names(functions: &[String], path: &Path) -> Result<(), String> {
    let mut source = String::from(
        "/// The BLPAPI C functions the function table [`Blpapi`] resolves, as listed in\n\
         /// `functions.txt`: every one must exist in a runtime for it to load.\n\
         pub const FUNCTION_NAMES: &[&::core::ffi::CStr] = &[\n",
    );
    for function in functions {
        source.push_str(&format!("    c\"{function}\",\n"));
    }
    source.push_str("];\n");
    fs::write(path, source).map_err(|e| format!("Failed to write {}: {}", path.display(), e))
}

/// The function names in [`FUNCTIONS_FILE`]: one per line, blank lines and `#` comments skipped.
fn read_function_allowlist() -> Result<Vec<String>, String> {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let path = Path::new(&manifest_dir).join(FUNCTIONS_FILE);
    let text = fs::read_to_string(&path)
        .map_err(|e| format!("Failed to read {}: {}", path.display(), e))?;

    let mut functions: Vec<String> = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let valid = line.starts_with("blpapi_")
            && line
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
        if !valid {
            return Err(format!(
                "{}:{}: `{}` is not a blpapi_* function name",
                path.display(),
                index + 1,
                line
            ));
        }
        if functions.iter().any(|existing| existing == line) {
            return Err(format!(
                "{}:{}: `{}` is listed twice",
                path.display(),
                index + 1,
                line
            ));
        }
        functions.push(line.to_string());
    }

    if functions.is_empty() {
        return Err(format!("{} lists no functions", path.display()));
    }
    Ok(functions)
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

fn resolve_include_dir() -> Result<PathBuf, String> {
    // 1) Explicit include directory
    if let Some(include) = env::var_os("BLPAPI_INCLUDE_DIR") {
        let include = resolve_env_path(include);
        validate_header_exists(&include)?;
        return Ok(include);
    }

    // 2) Root
    if let Some(root) = env::var_os("BLPAPI_ROOT") {
        let root = resolve_env_path(root);
        // Adding a newer version beneath a container must invalidate discovery.
        println!("cargo:rerun-if-changed={}", root.display());
        return resolve_sdk_include(&root);
    }

    Err("Cannot locate Bloomberg SDK headers. Set BLPAPI_INCLUDE_DIR or BLPAPI_ROOT.".into())
}

fn resolve_sdk_include(root: &Path) -> Result<PathBuf, String> {
    let mut last_error = None;

    for candidate in candidate_sdk_roots(root)? {
        match derive_include(&candidate) {
            Ok(include_dir) => return Ok(include_dir),
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

fn derive_include(root: &Path) -> Result<PathBuf, String> {
    for include_dir in [root.join("include"), root.join("Include")] {
        if include_dir.is_dir() && validate_header_exists(&include_dir).is_ok() {
            return Ok(include_dir);
        }
    }

    Err(format!("Could not find include/ under {}.", root.display()))
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
