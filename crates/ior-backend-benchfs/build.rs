use std::env;
use std::path::PathBuf;

fn main() {
    let lib_dir = env::var("BENCHFS_LIB_DIR").unwrap_or_else(|_| {
        let manifest = env::var("CARGO_MANIFEST_DIR").unwrap();
        let workspace_root = PathBuf::from(&manifest)
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        workspace_root
            .join("external/benchfs/target/release")
            .to_string_lossy()
            .into_owned()
    });

    println!("cargo:rustc-link-search=native={}", lib_dir);
    println!("cargo:rustc-link-lib=dylib=benchfs");
    println!("cargo:rerun-if-env-changed=BENCHFS_LIB_DIR");
}
