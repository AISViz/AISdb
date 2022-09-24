use std::path::PathBuf;
use std::thread::sleep;
use std::time::Duration;

extern crate client;
use client::client_socket_stream;

extern crate server;
use server::listener;

extern crate testconfig;
use testconfig::{truncate, TESTDATA, TESTINGDIR};

use proxy::proxy_thread;

#[test]
fn test_proxy_thread_ipv4() {
    let client_target = "127.0.0.1:8890".to_string();
    let proxy_listen = "0.0.0.0:8890".to_string();
    let proxy_target = "127.0.0.1:8891".to_string();
    //let proxy_target = "0.0.0.0:8891".to_string();
    let server_listen = "0.0.0.0:8891".to_string();

    let data = PathBuf::from(TESTDATA);
    let pathstr = &[TESTINGDIR, "streamoutput_proxy_thread_ipv4_output.log"].join(&"");
    let output = PathBuf::from(pathstr);
    assert!(data.is_file());

    let _l = listener(server_listen, output, false);
    sleep(Duration::from_millis(15));

    let targets = vec![proxy_target];
    let _p = proxy_thread(&proxy_listen, &targets, false);
    sleep(Duration::from_millis(15));

    let _c = client_socket_stream(&data, vec![client_target], false);

    let output = PathBuf::from(pathstr);
    let bytesize = truncate(output);
    assert!(bytesize > 0);
}
