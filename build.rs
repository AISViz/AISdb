use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=./aisdb_web/map/*.js");
    println!("cargo:rerun-if-changed=./aisdb_web/map/*.ts");
    println!("cargo:rerun-if-changed=./aisdb_web/map/*.html");
    println!("cargo:rerun-if-changed=./aisdb_web/map/*.css");
    println!("cargo:rerun-if-changed=./client_webassembly/src/*");

    let wasm_build = Command::new("wasm-pack")
        .current_dir("./client_webassembly/")
        .args([
            "build",
            "--target=web",
            "--out-dir=../aisdb_web/map/pkg",
            "--release",
        ])
        .output()
        .unwrap();
    eprintln!("{}", String::from_utf8_lossy(&wasm_build.stderr[..]));

    let wasm_opt = Command::new("wasm-opt")
        .current_dir("./client_webassembly/")
        .args([
            "-O3",
            "-o=../aisdb_web/map/pkg/client_bg.wasm",
            "../aisdb_web/map/pkg/client_bg.wasm",
        ])
        .output()
        .unwrap();
    eprintln!("{}", String::from_utf8_lossy(&wasm_opt.stderr[..]));

    let npm_install = Command::new("npm")
        .current_dir("./aisdb_web")
        .arg("install")
        .output()
        .unwrap();
    eprintln!("{}", String::from_utf8_lossy(&npm_install.stderr[..]));

    let vite_build_1 = Command::new("npx")
        .current_dir("./aisdb_web/map")
        .env_clear()
        .env("VITE_DISABLE_SSL_DB", "1")
        .env("VITE_DISABLE_STREAM", "1")
        .env("VITE_AISDBHOST", "localhost")
        .env("VITE_AISDBPORT", "9924")
        .args(["vite", "build", "--outDir=../dist_map"])
        .output()
        .unwrap();
    eprintln!("{}", String::from_utf8_lossy(&vite_build_1.stderr[..]));

    let vite_build_2 = Command::new("npx")
        .current_dir("./aisdb_web/map")
        .env_clear()
        .env("VITE_DISABLE_SSL_DB", "1")
        .env("VITE_DISABLE_STREAM", "1")
        .env("VITE_AISDBHOST", "localhost")
        .env("VITE_AISDBPORT", "9924")
        .env("VITE_BINGMAPTILES", "1")
        .env("VITE_TILESERVER", "aisdb.meridian.cs.dal.ca")
        .args(["vite", "build", "--outDir=../dist_map_bingmaps"])
        .output()
        .unwrap();
    eprintln!("{}", String::from_utf8_lossy(&vite_build_2.stderr[..]));
}
