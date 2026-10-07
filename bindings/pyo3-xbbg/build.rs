#[path = "../../scripts/rust_build_provenance.rs"]
mod build_provenance;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    build_provenance::emit()?;
    // setup.py exports the setuptools-scm version of the distribution being built.
    // Plain Cargo builds (tests, stub generation) report the crate version instead.
    println!("cargo:rerun-if-env-changed=XBBG_DIST_VERSION");
    let dist_version = match std::env::var("XBBG_DIST_VERSION") {
        Ok(version) if !version.is_empty() => version,
        _ => std::env::var("CARGO_PKG_VERSION")?,
    };
    println!("cargo:rustc-env=XBBG_DIST_VERSION={dist_version}");
    Ok(())
}
