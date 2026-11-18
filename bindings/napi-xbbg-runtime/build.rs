fn main() {
    napi_build::setup();
    if matches!(std::env::var("CARGO_CFG_TARGET_OS").as_deref(), Ok("macos")) {
        // The addon is relocated with install_name_tool after the build.
        println!("cargo:rustc-cdylib-link-arg=-Wl,-headerpad_max_install_names");
    }
}
