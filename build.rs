use std::path::PathBuf;
use std::process::Command;
use wasm_opt::OptimizationOptions;


fn main() -> Result<(), Box<dyn std::error::Error>> {
    //println!("cargo:rerun-if-changed=./aisdb_web/*.js");
    //println!("cargo:rerun-if-changed=./aisdb_web/*.json");
    //println!("cargo:rerun-if-changed=./aisdb_web/map/*.css");
    //println!("cargo:rerun-if-changed=./aisdb_web/map/*.html");
    //println!("cargo:rerun-if-changed=./aisdb_web/map/*.js");
    //println!("cargo:rerun-if-changed=./aisdb_web/map/*.ts");
    //println!("cargo:rerun-if-changed=./client_webassembly/src/*");

    // only do this for release builds
    // #[cfg(not(debug_assertions))]
    // download web assets from gitlab CD artifacts
    // if OFFLINE_BUILD is not set, it is expected that artifacts will be passed from previous job

    // use current directory as root directory for all commands
    let rootdir = std::env::current_dir().unwrap();
    eprintln!("Root directory: {:?}", rootdir);

    // create aisdb_web/map/pkg if not exists
    let pkg_path = rootdir.join("aisdb_web/map/pkg");
    if !pkg_path.exists() {
        std::fs::create_dir_all(pkg_path).unwrap();
    }

    let wasm_pack_path = Command::new("which")
        .arg("wasm-pack")
        .output()?;

    if !wasm_pack_path.status.success() {
        eprintln!("wasm-pack is not installed or not in PATH.");
        std::process::exit(1);
    } else {
        eprintln!("wasm-pack is installed at: {}", String::from_utf8_lossy(&wasm_pack_path.stdout));
    }

    let pkg_path = rootdir.join("aisdb_web/map/pkg");
    if !pkg_path.exists() {
        eprintln!("Target directory does not exist: {:?}", pkg_path);
        std::process::exit(1);
    } else {
        eprintln!("Target directory exists: {:?}", pkg_path);
    }

    
    let wasm_build = Command::new("wasm-pack")
        .current_dir(format!("{}", rootdir.join("client_webassembly").display()))
        .args([
                "build",
                "--target=web",
                format!("--out-dir={}", rootdir.join("aisdb_web/map/pkg").display()).as_str(),
                #[cfg(not(debug_assertions))]
                "--release",
                #[cfg(debug_assertions)]
                "--dev",
        ])
        .output()
        .expect(
            "Running wasm-pack. Is it installed? https://rustwasm.github.io/wasm-pack/installer/",
        );

    if !wasm_build.status.success() {
        eprintln!("wasm-pack failed with status: {:?}", wasm_build.status);
        eprintln!("stderr: {}", String::from_utf8_lossy(&wasm_build.stderr));
        eprintln!("stdout: {}", String::from_utf8_lossy(&wasm_build.stdout));
        std::process::exit(1);
    }

    // eprintln!("{}", String::from_utf8_lossy(&wasm_build.stderr[..]));
    assert!(wasm_build.status.code() == Some(0));

    // install npm packages
    #[cfg(target_os = "windows")]
    let npm = "npm.cmd";
    #[cfg(not(target_os = "windows"))]
    let npm = "npm";

    let npm_install = Command::new(npm)
        .current_dir(format!("{}", rootdir.join("aisdb_web").display()))
        .arg("install")
        .output()
        .expect("running npm install");
    eprintln!("{}", String::from_utf8_lossy(&npm_install.stderr[..]));
    assert!(npm_install.status.code().unwrap() == 0);

    // bundle html
    let webpath_to_create = PathBuf::from(format!("{}", rootdir.join("aisdb_web/map").display()));
    let webpath = std::path::Path::new(&webpath_to_create);

    #[cfg(target_os = "windows")]
    let npx = "npx.cmd";
    #[cfg(not(target_os = "windows"))]
    let npx = "npx";

    let vite_build_1 = Command::new(npx)
        .current_dir(webpath)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap())
        .env("VITE_DISABLE_SSL_DB", "1")
        .env("VITE_DISABLE_STREAM", "1")
        .env("VITE_AISDBHOST", "localhost")
        .env("VITE_AISDBPORT", "9924")
        .args(["vite", "build", format!("--outDir={}", rootdir.join("aisdb_web/dist_map").display()).as_str()])
        .output()
        .unwrap();
    eprintln!("{}", String::from_utf8_lossy(&vite_build_1.stderr[..]));
    assert!(vite_build_1.status.code().unwrap() == 0);

    let vite_build_2 = Command::new(npx)
        .current_dir(webpath)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap())
        .env("VITE_DISABLE_SSL_DB", "1")
        .env("VITE_DISABLE_STREAM", "1")
        .env("VITE_AISDBHOST", "localhost")
        .env("VITE_AISDBPORT", "9924")
        .env("VITE_BINGMAPTILES", "1")
        .env("VITE_TILESERVER", "dev.virtualearth.net")
        .args([
            "vite",
            "build",
            format!("--outDir={}", rootdir.join("aisdb_web/dist_map_bingmaps").display()).as_str(),
        ])
        .output()
        .unwrap();
    eprintln!("{}", String::from_utf8_lossy(&vite_build_2.stderr[..]));
    assert!(vite_build_2.status.code().unwrap() == 0);

    assert!(PathBuf::from(format!("{}/aisdb_web/map/pkg", env!("CARGO_MANIFEST_DIR"))).exists());

    // compress wasm
    let wasm_opt_file = &PathBuf::from(format!("{}", rootdir.join("aisdb_web/map/pkg/client_bg.wasm").display()));
    OptimizationOptions::new_optimize_for_size()
        .run(wasm_opt_file, wasm_opt_file)
        .expect("running wasm-opt");

    Ok(())
}
