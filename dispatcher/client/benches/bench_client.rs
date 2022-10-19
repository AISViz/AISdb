#![feature(test)]

extern crate test;
use test::Bencher;

use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::thread::sleep;
use std::thread::Builder;
use std::time::{Duration, Instant};

extern crate server;
use crate::server::join_unicast;

use client::client_socket_stream;

#[cfg(unix)]
//#[cfg(not(debug_assertions))]
#[bench]
fn test_client_bitrate(_b: &mut Bencher) {
    let target_addr = "127.0.0.1:9917".to_string();
    let listen_addr = "0.0.0.0:9917".to_string();

    let listen_socket =
        join_unicast(listen_addr.to_socket_addrs().unwrap().next().unwrap()).unwrap();

    sleep(Duration::from_millis(15));

    let mut bytecount: i64 = 0;
    let mut buf = [0u8; 32768];

    //b.iter(|| {
    //let target_addr = target_addr.clone();
    let _c = Builder::new().spawn(move || {
        client_socket_stream(&PathBuf::from("/dev/random"), vec![target_addr], false)
    });

    // measure time to send 1Gb of randomized binary data
    let start = Instant::now();
    while bytecount < 1000000000 {
        let (c, _remote) = listen_socket.recv_from(&mut buf[0..32767]).unwrap();
        bytecount += c as i64;
    }
    let elapsed = start.elapsed();

    println!(
        "transferred: {} Mb  elapsed: {:.3}s\tbitrate: {:.1} Mbps",
        bytecount / 1000000,
        elapsed.as_secs_f32(),
        bytecount as f64 / elapsed.as_secs_f64() / 1000000 as f64
    );
    //});
}
