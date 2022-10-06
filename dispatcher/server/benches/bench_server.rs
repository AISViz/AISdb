#![feature(test)]

extern crate test;
use test::Bencher;

use std::fs::File;
use std::path::PathBuf;
use std::str::FromStr;
use std::thread::sleep;
use std::thread::Builder;
use std::time::Duration;
use std::time::Instant;

extern crate client;
use client::client_socket_stream;

extern crate testconfig;
use testconfig::{truncate, TESTINGDIR};

use server::listener;

#[cfg(unix)]
//#[cfg(not(debug_assertions))]
#[bench]
fn test_server_bitrate(_b: &mut Bencher) {
    let pathstr = &[TESTINGDIR, "streamoutput_server_test_largefile.log"].join(&"");
    truncate(PathBuf::from_str(pathstr).unwrap());
    let target_addr = "127.0.0.1:9907".to_string();
    let listen_addr = "0.0.0.0:9907".to_string();

    let _l = listener(listen_addr, PathBuf::from_str(pathstr).unwrap(), false);
    let _c = Builder::new().spawn(move || {
        client_socket_stream(&PathBuf::from("/dev/random"), vec![target_addr], false)
    });
    let bytesize = truncate(PathBuf::from_str(pathstr).unwrap());

    let start = Instant::now();

    while start.elapsed().as_secs() < 2 {
        sleep(Duration::from_millis(25));
    }
    let elapsed = start.elapsed();

    let info = match File::open(&pathstr) {
        Ok(f) => f.metadata().unwrap().len(),
        Err(e) => {
            eprintln!("{}", e);
            0
        }
    };
    println!(
        "log size: {}  elapsed: {:.3}s\tbitrate: {:.1} Mbps",
        info,
        elapsed.as_secs_f32(),
        info / elapsed.as_secs() / 1000000
    );
    truncate(PathBuf::from_str(pathstr).unwrap());
    assert!(bytesize > 0);
}
