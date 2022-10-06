use std::fs::File;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::thread::sleep;
use std::time::Duration;

extern crate client;
use client::{client_check_ipv6_interfaces, client_socket_stream, new_sender};

extern crate testconfig;
use testconfig::{truncate, TESTINGDIR};

use server::listener;

fn test_server_listener(addr: String, logfile: PathBuf) {
    // start server
    listener(addr.clone(), logfile.clone(), false);

    sleep(Duration::from_millis(10));
    let message = b"Hello from client!";
    let addr: SocketAddr = addr.parse().expect("parsing socket address");

    if addr.is_ipv4() {
        let socket = new_sender(&addr).expect("could not create ipv4 sender!");
        socket
            .send_to(message, &addr)
            .expect("could not send to socket!");
    } else {
        let socket = client_check_ipv6_interfaces(&addr).expect("could not create ipv6 sender!");
        socket
            .send_to(message, &addr)
            .expect("could not send to socket!");
    }
    let bytes = truncate(logfile.clone());
    assert!(bytes > 0);
    println!("{:?}: {} bytes", logfile, bytes);
}

#[test]
fn test_server_ipv4_unicast() {
    let ipv4 = "127.0.0.1:9900".to_string();
    let pathstr = &[TESTINGDIR, "streamoutput_ipv4_unicast.log"].join(&"");
    let logfile: PathBuf = PathBuf::from_str(pathstr).unwrap();
    test_server_listener(ipv4, logfile);
}

#[test]
fn test_server_ipv4_multicast() {
    let ipv4 = "224.0.0.2:9901".to_string();
    let pathstr = &[TESTINGDIR, "streamoutput_ipv4_multicast.log"].join(&"");
    let logfile: PathBuf = PathBuf::from_str(pathstr).unwrap();
    test_server_listener(ipv4, logfile);
}

#[test]
fn test_server_ipv6_unicast() {
    let listen = "[::0]:9902".to_string();
    let pathstr = &[TESTINGDIR, "streamoutput_ipv6_unicast.log"].join(&"");
    let logfile: PathBuf = PathBuf::from_str(pathstr).unwrap();
    test_server_listener(listen, logfile);
}

#[test]
fn test_server_ipv6_multicast() {
    let listen = "[ff02::1]:9903".to_string();
    let pathstr = &[TESTINGDIR, "streamoutput_ipv6_multicast.log"].join(&"");
    let logfile: PathBuf = PathBuf::from_str(pathstr).unwrap();
    test_server_listener(listen, logfile);
}

#[test]
fn test_server_multiple_clients_single_channel() {
    //let pathstr_1 = "./testdata/streamoutput_client_ipv6_multiclient_samefile.log";
    let pathstr_1 = &[
        TESTINGDIR,
        "streamoutput_client_ipv6_multiclient_samefile.log",
    ]
    .join(&"");
    File::create(&pathstr_1).expect("truncating file");
    sleep(Duration::from_millis(15));
    let listen_addr_1 = "[::]:9904".to_string();
    let target_addr_1 = "[::1]:9904".to_string();
    let target_addr_2 = "[::1]:9904".to_string();
    let _l = listener(listen_addr_1, PathBuf::from_str(pathstr_1).unwrap(), false);
    let _c1 = client_socket_stream(&PathBuf::from("./Cargo.toml"), vec![target_addr_1], false);
    let _c2 = client_socket_stream(&PathBuf::from("../Cargo.toml"), vec![target_addr_2], false);
}

#[test]
fn test_server_multiple_clients_dual_channel() {
    //let pathstr_1 = "./testdata/streamoutput_client_ipv6_multiclient_different_channels.log";
    let pathstr_1 = &[
        TESTINGDIR,
        "streamoutput_client_ipv6_multiclient_different_channels.log",
    ]
    .join(&"");
    File::create(&pathstr_1).expect("truncating file");
    sleep(Duration::from_millis(15));
    let listen_addr_1 = "[::]:9905".to_string();
    let listen_addr_2 = "[::]:9906".to_string();
    let target_addr_1 = "[::1]:9905".to_string();
    let target_addr_2 = "[::1]:9906".to_string();
    let _l1 = listener(listen_addr_1, PathBuf::from_str(pathstr_1).unwrap(), false);
    let _l2 = listener(listen_addr_2, PathBuf::from_str(pathstr_1).unwrap(), false);
    let _c1 = client_socket_stream(&PathBuf::from("./Cargo.toml"), vec![target_addr_1], false);
    let _c2 = client_socket_stream(&PathBuf::from("../Cargo.toml"), vec![target_addr_2], false);
}
