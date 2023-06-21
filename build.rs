use std::fs::{remove_file, File};
use std::io::Write;
use std::process::Command;

use reqwest::blocking::get;
use wasm_opt::OptimizationOptions;

fn main() {
    //println!("cargo:rerun-if-changed=./aisdb_web/*.js");
    //println!("cargo:rerun-if-changed=./aisdb_web/*.json");
    //println!("cargo:rerun-if-changed=./aisdb_web/map/*.css");
    //println!("cargo:rerun-if-changed=./aisdb_web/map/*.html");
    //println!("cargo:rerun-if-changed=./aisdb_web/map/*.js");
    //println!("cargo:rerun-if-changed=./aisdb_web/map/*.ts");
    //println!("cargo:rerun-if-changed=./client_webassembly/src/*");

    // download web assets from gitlab CD artifacts
    // if GITLAB_CI is set, it is expected that artifacts will be passed from previous job
    // if OFFLINE_BUILD is set, see below
    if std::env::var("GITLAB_CI").is_err() && std::env::var("OFFLINE_BUILD").is_err() {
        //let branch = "master";
        let branch = String::from_utf8(
            Command::new("git")
                .args(["rev-parse", "--abbrev-ref", "HEAD"])
                .output()
                .expect("getting current git branch")
                .stdout,
        )
        .unwrap();

        let url = format!(
            "https://git-dev.cs.dal.ca/api/v4/projects/132/jobs/artifacts/{}/download?job=wasm-assets",
            branch
            );
        let mut zipfile = File::create("artifacts.zip").expect("creating empty zipfile");
        let zipfile_bytes = get(url)
            .expect("downloading web asset artifacts")
            .bytes()
            .expect("get asset bytes");
        assert!(zipfile_bytes.len() > 64); // make sure we didnt get error 404
        zipfile
            .write(&zipfile_bytes)
            .expect("writing zipfile bytes");

        // unzip web assets into project
        let unzip = Command::new("unzip")
            .arg("-o")
            .arg("artifacts.zip")
            .output()
            .expect("unzip command");
        eprintln!("{}", String::from_utf8_lossy(&unzip.stderr[..]));
        assert!(unzip.status.code().unwrap() == 0);

        // remove zipfile
        remove_file("artifacts.zip").expect("deleting zip");
    }

    // web assets may also be built locally if OFFLINE_BUILD is set
    if std::env::var("OFFLINE_BUILD").is_ok() {
        // build wasm
        let wasm_build = Command::new("wasm-pack")
            .current_dir("./client_webassembly/")
            .args([
                  "build",
                  "--target=web",
                  "--out-dir=../aisdb_web/map/pkg",
                  #[cfg(not(debug_assertions))]
                  "--release",
                  #[cfg(debug_assertions)]
                  "--dev",
            ])
            .output()
            .expect(
                "Running wasm-pack. Is it installed? https://rustwasm.github.io/wasm-pack/installer/",
                );
        eprintln!("{}", String::from_utf8_lossy(&wasm_build.stderr[..]));
        assert!(wasm_build.status.code() == Some(0));
        if wasm_build.status.code().unwrap() != 0 {
            assert!(std::path::Path::new("./aisdb_web/map/pkg.zip")
                .try_exists()
                .expect("no zip found"));
            let unzip1 = Command::new("unzip")
                .arg("aisdb_web/map/pkg.zip")
                .output()
                .unwrap();
            assert!(unzip1.status.code().unwrap() == 0);
        } else {
            let zip1 = Command::new("zip")
                .arg("-ru9")
                .arg("aisdb_web/map/pkg.zip")
                .arg("aisdb_web/map/pkg/")
                .output()
                .unwrap();
            assert!(zip1.status.code().unwrap() == 0);
        }

        // install npm packages
        #[cfg(target_os = "windows")]
        let npm = "npm.cmd";
        #[cfg(not(target_os = "windows"))]
        let npm = "npm";
        let npm_install = Command::new(npm)
            .current_dir("./aisdb_web")
            .arg("install")
            .output()
            .expect("running npm install");
        eprintln!("{}", String::from_utf8_lossy(&npm_install.stderr[..]));
        assert!(npm_install.status.code().unwrap() == 0);

        // bundle html
        let webpath = std::path::Path::new("./aisdb_web/map");
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
            .args(["vite", "build", "--outDir=../dist_map"])
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
            .env("VITE_TILESERVER", "aisdb.meridian.cs.dal.ca")
            .args(["vite", "build", "--outDir=../dist_map_bingmaps"])
            .output()
            .unwrap();
        eprintln!("{}", String::from_utf8_lossy(&vite_build_2.stderr[..]));
        assert!(vite_build_2.status.code().unwrap() == 0);
    }

    // compress wasm
    let wasm_opt_file = "./aisdb_web/map/pkg/client_bg.wasm";
    OptimizationOptions::new_optimize_for_size()
        .run(wasm_opt_file, wasm_opt_file)
        .expect("running wasm-opt");
}
