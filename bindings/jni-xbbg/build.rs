#[path = "../../scripts/rust_build_provenance.rs"]
mod build_provenance;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    match std::env::var("CARGO_CFG_TARGET_OS").as_deref() {
        // Leaves room in the Mach-O header for scripts/stage_jvm_natives.py to rewrite
        // install names (`@rpath/libblpapi3_64.so`) with install_name_tool.
        Ok("macos") => println!("cargo:rustc-cdylib-link-arg=-Wl,-headerpad_max_install_names"),
        // The JVM unloads a JNI library when its class loader is collected. Keep the image
        // mapped so thread-local destructors of runtime threads never run unmapped code.
        Ok("linux") => println!("cargo:rustc-cdylib-link-arg=-Wl,-z,nodelete"),
        _ => {}
    }
    build_provenance::emit()
}
