use std::path::PathBuf;
use std::str::FromStr;

extern crate testconfig;
use testconfig::{truncate, TESTDATA, TESTINGDIR};

extern crate server;
use server::listener;

use client::client_socket_stream;

fn test_client(pathstr: &str, listen_addr: String, target_addr: String, tee: bool) {
    let _l = listener(listen_addr, PathBuf::from_str(pathstr).unwrap(), false);
    let _c = client_socket_stream(&PathBuf::from(TESTDATA), vec![target_addr], tee);
    let bytesize = truncate(PathBuf::from_str(pathstr).unwrap());
    println!("log size: {}", bytesize);
    assert!(bytesize > 0);
}

#[test]
fn test_client_socket_stream_unicast_ipv4() {
    let pathstr = &[TESTINGDIR, "streamoutput_client_ipv4_unicast.log"].join(&"");
    let listen_addr = "0.0.0.0:9910".to_string();
    let target_addr = "127.0.0.1:9910".to_string();
    test_client(pathstr, listen_addr, target_addr, false)
}

#[test]
fn test_client_socket_stream_multicast_ipv4() {
    let pathstr = &[TESTINGDIR, "streamoutput_client_ipv4_multicast.log"].join(&"");
    let target_addr = "224.0.0.110:9911".to_string();
    let listen_addr = target_addr.clone();
    test_client(pathstr, listen_addr, target_addr, false)
}

#[test]
fn test_client_socket_stream_unicast_ipv6() {
    let pathstr = &[TESTINGDIR, "streamoutput_client_ipv6_unicast.log"].join(&"");
    let listen_addr = "[::1]:9912".to_string();
    let target_addr = "[::1]:9912".to_string();
    test_client(pathstr, listen_addr, target_addr, false)
}

#[test]
fn test_client_socket_stream_multicast_ipv6() {
    let pathstr = &[TESTINGDIR, "streamoutput_client_ipv6_multicast.log"].join(&"");
    let listen_addr = "[ff02::2]:9913".to_string();
    let target_addr = "[ff02::2]:9913".to_string();
    test_client(pathstr, listen_addr, target_addr, false)
}

#[test]
fn test_client_socket_tee() {
    let pathstr = &[TESTINGDIR, "streamoutput_client_tee.log"].join(&"");
    let target_addr = "127.0.0.1:9914".to_string();
    let listen_addr = "0.0.0.0:9914".to_string();
    test_client(pathstr, listen_addr, target_addr, true)
}

#[test]
fn test_client_multiple_servers() {
    let pathstr_1 = &[TESTINGDIR, "streamoutput_client_ipv6_multiplex_1.log"].join(&"");
    let pathstr_2 = &[TESTINGDIR, "streamoutput_client_ipv6_multiplex_2.log"].join(&"");
    let listen_addr_1 = "[::]:9915".to_string();
    let listen_addr_2 = "[::]:9916".to_string();
    let target_addr_1 = "[::1]:9915".to_string();
    let target_addr_2 = "[::1]:9916".to_string();
    //test_client(pathstr, listen_addr, target_addr, false)

    let bytesize_1 = truncate(PathBuf::from_str(pathstr_1).unwrap());
    let bytesize_2 = truncate(PathBuf::from_str(pathstr_2).unwrap());
    let _l1 = listener(listen_addr_1, PathBuf::from_str(pathstr_1).unwrap(), false);
    let _l2 = listener(listen_addr_2, PathBuf::from_str(pathstr_2).unwrap(), false);
    let _c = client_socket_stream(
        &PathBuf::from(TESTDATA),
        vec![target_addr_1, target_addr_2],
        false,
    );
    println!("log sizes: {}, {}", bytesize_1, bytesize_2);
    assert!(bytesize_1 > 0);
    assert!(bytesize_2 > 0);
    assert!(bytesize_1 == bytesize_2);
}

#[cfg(unix)]
#[cfg(not(debug_assertions))]
#[test]
fn test_client_bitrate() {
    let target_addr = "127.0.0.1:9917".to_string();
    let listen_addr = "0.0.0.0:9917".to_string();

    use dispatcher::server::join_unicast;
    use std::net::ToSocketAddrs;
    use std::thread::Builder;
    use std::time::Instant;

    let listen_socket =
        join_unicast(listen_addr.to_socket_addrs().unwrap().next().unwrap()).unwrap();

    sleep(Duration::from_millis(15));

    let _c = Builder::new().spawn(move || {
        client_socket_stream(&PathBuf::from("/dev/random"), vec![target_addr], false)
    });

    let mut bytecount: i64 = 0;
    let mut buf = [0u8; 32768];

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
}
