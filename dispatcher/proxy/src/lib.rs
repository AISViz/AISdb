use std::io::{stdout, BufWriter, Write};
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::thread::{Builder, JoinHandle};

extern crate client;
use client::{client_check_ipv6_interfaces, new_sender};

extern crate server;
use server::{join_multicast, join_unicast};

pub fn new_listen_socket(listen_addr: &String) -> UdpSocket {
    let listen_addr = listen_addr
        .to_socket_addrs()
        .unwrap()
        .next()
        .expect("parsing socket address");
    match listen_addr.ip().is_multicast() {
        false => join_unicast(listen_addr).expect("failed to create socket listener!"),
        true => {match join_multicast(listen_addr) {
            Ok(s) => s,
            Err(e) => panic!("failed to create multicast listener on address {}! are you sure this is a valid multicast channel?\n{:?}", listen_addr, e),
        }
        },
    }
}
pub fn new_downstream_socket(downstream_addr: &String) -> (SocketAddr, UdpSocket) {
    let addr = downstream_addr
        .to_socket_addrs()
        .unwrap()
        .next()
        .expect("parsing address");
    (
        addr,
        match addr.is_ipv4() {
            true => new_sender(&addr).expect("ipv4 output socket"),
            false => client_check_ipv6_interfaces(&addr).expect("ipv6 output socket"),
        },
    )
}

pub fn proxy_thread(
    listen_addr: &String,
    downstream_addrs: &[String],
    tee: bool,
) -> JoinHandle<()> {
    let listen_socket = new_listen_socket(listen_addr);
    let mut output_buffer = BufWriter::new(stdout());
    let targets: Vec<(SocketAddr, UdpSocket)> =
        downstream_addrs.iter().map(new_downstream_socket).collect();
    let mut buf = [0u8; 32768]; // receive buffer
    Builder::new()
        .name(format!("{:#?}", listen_socket))
        .spawn(move || {
            //listen_socket.read_timeout().unwrap();
            listen_socket.set_broadcast(true).unwrap();
            loop {
                match listen_socket.recv_from(&mut buf[0..]) {
                    Ok((c, _remote_addr)) => {
                        for (target_addr, target_socket) in &targets {
                            target_socket
                                .send_to(&buf[0..c], &target_addr)
                                .expect("sending to server socket");
                        }
                        if tee {
                            let _o = output_buffer
                                .write(&buf[0..c])
                                .expect("writing to output buffer");
                            #[cfg(debug_assertions)]
                            assert!(c == _o);
                        }
                    }
                    Err(err) => {
                        //output_buffer.flush().unwrap();
                        eprintln!("proxy_thread: got an error: {}", err);
                        #[cfg(debug_assertions)]
                        panic!("proxy_thread: got an error: {}", err);
                    }
                }
                output_buffer.flush().unwrap();
            }
        })
        .unwrap()
}

pub fn proxy_gateway(
    downstream_addrs: &[String],
    listen_addrs: &[String],
    tee: bool,
) -> Vec<JoinHandle<()>> {
    let mut threads: Vec<JoinHandle<()>> = vec![];
    for listen_addr in listen_addrs {
        #[cfg(debug_assertions)]
        println!(
            "proxy: forwarding {:?} -> {:?}",
            listen_addr, downstream_addrs
        );
        threads.push(proxy_thread(listen_addr, downstream_addrs, tee));
    }
    threads
}
