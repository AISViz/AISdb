use std::fs::{remove_file, File};
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

use reqwest::blocking::get;
use wasm_opt::OptimizationOptions;

#[derive(PartialEq)]
enum DownloadType {
    SourceCode,
    Build,
}

fn check_source_code_folder() -> bool {
    let path = PathBuf::from("AISdb-Web-main");
    path.exists()
}

fn remove_source_code_folder() {
    let path = PathBuf::from("AISdb-Web-main");
    if path.exists() {
        std::fs::remove_dir_all(path).expect("removing source code folder");
    }
}

fn download_source(version: &str, download_type: &DownloadType) -> Result<PathBuf, String> {
    let (url, type_of_download, file_name) = match download_type {
        DownloadType::SourceCode => ("https://github.com/AISViz/aisdb-web/archive/refs/heads/main.zip".to_string(),
            "Source Code", "main.zip"),
        DownloadType::Build => (format!("https://github.com/AISViz/AISdb-Web/releases/{}/download/aisdb_web.zip", version),
            "Build Project", "aisdb_web.zip"),
    };

    let zipfile_bytes = get(url)
        .expect(format!("downloading {}", type_of_download).as_str())
        .bytes()
        .expect("get asset bytes");
    
    //assert!(zipfile_bytes.len() > 64); // make sure we didnt get error 404
    if zipfile_bytes.len() <= 64 {
        eprintln!("Download: {}, result: {:#?}", type_of_download, zipfile_bytes);
        let (what_to_download, extracted_folder, url) = match download_type {
            DownloadType::SourceCode => {
                ("source code", "AISdb-Web-main", "https://github.com/AISViz/AISdb-Web/")
            },
            DownloadType::Build => {
                ("source code", "aisdb_web", "https://github.com/AISViz/AISdb-Web/releases/")
            }
        };
        eprintln!("Please manually download the {} from the following link, and put the extracted folder {} in the root of the project:\n{}", what_to_download, extracted_folder, url);
        return Err("assert!(zipfile_bytes.len() > 64)".to_string()); // make sure we didnt get error 404s
    }

    let zipfilepath = PathBuf::from(file_name);
    let mut zipfile = File::create(&zipfilepath).expect("creating empty zipfile");
    zipfile
        .write_all(&zipfile_bytes)
        .expect("writing zipfile bytes");
    Ok(zipfilepath)
}

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
    
    let (download_type, file_name, build_type) = match std::env::var("OFFLINE_BUILD") {
        Ok(_) => (DownloadType::SourceCode, "main.zip", "OFFLINE_BUILD"),
        Err(_) => (DownloadType::Build, "aisdb_web.zip", "ONLINE_BUILD"),
    };

    let version = "latest";

    if (build_type == "OFFLINE_BUILD" && !check_source_code_folder()) || build_type == "ONLINE_BUILD"{
        let zipfilepath = download_source(&version, &download_type)?;
        // unzip web assets into project
        let unzip = Command::new("unzip")
            .arg("-o")
            .arg(zipfilepath.display().to_string())
            .output()
            .expect("unzip command");
        eprintln!("{}", String::from_utf8_lossy(&unzip.stderr[..]));
        assert!(unzip.status.code().unwrap() == 0);

        // remove zipfile
        remove_file(file_name).expect("deleting zip");
    }


    // web assets may also be built locally if OFFLINE_BUILD is set
    if build_type == "OFFLINE_BUILD" {
        // build wasm
        let wasm_build = Command::new("wasm-pack")
            .current_dir("AISdb-Web-main/web_assembly")
            .args([
                  "build",
                  "--target=web",
                  "--out-dir=../map/pkg",
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

        // install npm packages
        #[cfg(target_os = "windows")]
        let npm = "npm.cmd";
        #[cfg(not(target_os = "windows"))]
        let npm = "npm";

        let npm_install = Command::new(npm)
            .current_dir("./AISdb-Web-main/")
            .arg("install")
            .output()
            .expect("running npm install");
        eprintln!("{}", String::from_utf8_lossy(&npm_install.stderr[..]));
        assert!(npm_install.status.code().unwrap() == 0);

        // bundle html
        let webpath = std::path::Path::new("./AISdb-Web-main/map");
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
            .args(["vite", "build", "--outDir=../../aisdb_web/dist_map"])
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
            .args(["vite", "build", "--outDir=../../aisdb_web/dist_map_bingmaps"])
            .output()
            .unwrap();
        eprintln!("{}", String::from_utf8_lossy(&vite_build_2.stderr[..]));
        assert!(vite_build_2.status.code().unwrap() == 0);

        let file_move = Command::new("mv")
            .arg("AISdb-Web-main/map/")
            .arg("aisdb_web/map/")
            .output()
            .unwrap();
        assert!(file_move.status.code().unwrap() == 0);
    }

    remove_source_code_folder();

    assert!(PathBuf::from(format!("{}/aisdb_web/map/pkg", env!("CARGO_MANIFEST_DIR"))).exists());

    // compress wasm
    let wasm_opt_file = "./aisdb_web/map/pkg/client_bg.wasm";
    OptimizationOptions::new_optimize_for_size()
        .run(wasm_opt_file, wasm_opt_file)
        .expect("running wasm-opt");

    Ok(())
}