fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    // Room for the staging script to set the install name to @rpath/libxbbg_sdk_jni.dylib.
    if matches!(std::env::var("CARGO_CFG_TARGET_OS").as_deref(), Ok("macos")) {
        println!("cargo:rustc-cdylib-link-arg=-Wl,-headerpad_max_install_names");
    }
}
