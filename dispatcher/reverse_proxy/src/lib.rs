use std::io::{BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::thread::{spawn, JoinHandle};

use client::target_socket_interface;
use server::{join_multicast, upstream_socket_interface};
use socket_dispatch::BUFSIZE;

fn handle_client_tcp(downstream: TcpStream, multicast_addr: String) {
    let multicast_addr = multicast_addr
        .to_socket_addrs()
        .unwrap()
        .next()
        .expect("parsing socket address");
    if !multicast_addr.ip().is_multicast() {
        panic!("not a multicast address {}", multicast_addr);
    }
    let multicast_socket = join_multicast(multicast_addr).unwrap_or_else(|e| {
        panic!("joining multicast socket {}", e);
    });
    //multicast_socket.set_broadcast(true).unwrap();

    let mut buf = [0u8; BUFSIZE];
    let mut tcp_writer = BufWriter::new(downstream);

    loop {
        match multicast_socket.recv_from(&mut buf[0..]) {
            Ok((count_input, _remote_addr)) => {
                let _count_output = tcp_writer.write(&buf[0..count_input]);
            }
            Err(err) => {
                eprintln!("reverse_proxy: got an error: {}", err);
                break;
            }
        }
        if let Err(_e) = tcp_writer.flush() {
            #[cfg(debug_assertions)]
            eprintln!("reverse_proxy: closing {:?} {}", multicast_socket, _e);
            break;
        }
    }
}

/// Forward UDP socket stream to downstream TCP clients
///
/// Spawns a new thread for each client.
/// An additional thread will be spawned to listen for upstream_addr, which
/// is rebroadcasted over the multicast channel. Client handler threads
/// subscribing to this channel will then forward UDP packet information
/// downstream to any clients connected via TCP
pub fn reverse_proxy_udp_tcp(multicast_addr: String, tcp_listen_addr: String) -> JoinHandle<()> {
    // UDP multicast listener -> TCP sender
    // accept new client connections on TCP listening address,
    // and forward messages received over the UDP multicast channel
    spawn(move || {
        let listener = TcpListener::bind(tcp_listen_addr).unwrap();
        for stream in listener.incoming() {
            #[cfg(debug_assertions)]
            println!("new client {:?}", stream);
            let multicast_addr = multicast_addr.clone();
            let _tcp_client = spawn(move || {
                handle_client_tcp(stream.unwrap(), multicast_addr);
            });
        }
    })
}

pub fn reverse_proxy_udp(udp_input_addr: String, udp_output_addr: String) -> JoinHandle<()> {
    spawn(move || {
        let (addr, listen_socket) = upstream_socket_interface(udp_input_addr).unwrap();
        let (outaddr, output_socket) = target_socket_interface(&udp_output_addr).unwrap();
        //let (outaddr, output_socket) = upstream_socket_interface(udp_input_addr).unwrap();
        //let (addr, listen_socket) = target_socket_interface(&udp_output_addr).unwrap();

        let mut buf = [0u8; BUFSIZE];
        loop {
            match listen_socket.recv_from(&mut buf[0..]) {
                Ok((c, _remote_addr)) => {
                    if c == 0 {
                        panic!("{}", outaddr);
                    }
                    let c_out = output_socket
                        .send_to(&buf[0..c], outaddr)
                        .expect("forwarding UDP downstream");
                    assert!(c == c_out);
                }
                Err(err) => {
                    eprintln!("{}:reverse_proxy: error {}", addr, err);
                    break;
                }
            }
        }
    })
}

pub fn reverse_proxy_tcp_udp(upstream_tcp: String, downstream_udp: String) -> JoinHandle<()> {
    //pub fn reverse_proxy_tcp_udp(upstream_tcp: String, downstream_udp: String) {
    spawn(move || {
        let listener = TcpListener::bind(upstream_tcp).expect("binding TCP socket");
        for upstream in listener.incoming() {
            let (target_addr, target_socket) = target_socket_interface(&downstream_udp).unwrap();
            let mut buf = [0u8; BUFSIZE];
            //let mut stream = stream.as_ref().expect("connecting to stream");

            match upstream {
                Ok(mut input) => {
                    spawn(move || loop {
                        match input.read(&mut buf[0..]) {
                            Ok(c) => {
                                target_socket
                                    .send_to(&buf[0..c], target_addr)
                                    .expect("sending to UDP socket");
                            }
                            Err(e) => {
                                eprintln!("err: {}", e);
                                break;
                            }
                        }
                    });
                }
                Err(e) => {
                    eprintln!("dropping client: {}", e);
                }
            }
        }
    })
}
