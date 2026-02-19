use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    let lib_dir = env::var("CHFS_LIB_DIR").unwrap_or_else(|_| {
        let manifest = env::var("CARGO_MANIFEST_DIR").unwrap();
        let workspace_root = PathBuf::from(&manifest)
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        workspace_root
            .join("external/chfs/lib/.libs")
            .to_string_lossy()
            .into_owned()
    });

    println!("cargo:rustc-link-search=native={}", lib_dir);
    println!("cargo:rustc-link-lib=dylib=chfs");

    // libchfs.so depends on margo, mercury, argobots, json-c.
    // Extract RUNPATH from libchfs.so to find these libraries.
    let libchfs_path = format!("{}/libchfs.so", lib_dir);
    if let Ok(output) = Command::new("readelf")
        .args(["-d", &libchfs_path])
        .output()
    {
        let stdout = String::from_utf8_lossy(&output.stdout);
        for line in stdout.lines() {
            if line.contains("RUNPATH") || line.contains("RPATH") {
                // Extract the path list from [...]
                if let Some(start) = line.find('[') {
                    if let Some(end) = line.rfind(']') {
                        let paths = &line[start + 1..end];
                        for path in paths.split(':') {
                            let p = path.trim();
                            if !p.is_empty() {
                                println!("cargo:rustc-link-search=native={}", p);
                            }
                        }
                    }
                }
            }
        }
    }

    // Link margo and its dependencies
    println!("cargo:rustc-link-lib=dylib=margo");
    println!("cargo:rustc-link-lib=dylib=mercury");
    println!("cargo:rustc-link-lib=dylib=abt");

    println!("cargo:rerun-if-env-changed=CHFS_LIB_DIR");
}
