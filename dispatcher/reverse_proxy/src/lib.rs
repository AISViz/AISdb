//! Multicast Network Dispatcher and Proxy
//!
//! # MPROXY: Reverse Proxy
//! Forward upstream TCP and UDP upstream to downstream listeners.
//! Messages are routed via UDP multicast to downstream sender threads.
//! Spawns one thread per listener.
//!
//!
//! ## Quick Start
//! In `Cargo.toml`
//! ```toml
//! [dependencies]
//! mproxy-reverse = "0.1"
//! ```
//!
//! Example `src/main.rs`
//! ```rust,no_run
//! use mproxy_reverse::{reverse_proxy_tcp_udp, reverse_proxy_udp, reverse_proxy_udp_tcp};
//!
//! pub fn main() {
//!     let udp_listen_addr: Option<String> = Some("0.0.0.0:9920".into());
//!     let tcp_listen_addr: Option<String> = None;
//!     let multicast_addr: String = "[ff02::1]:9918".into();
//!     let tcp_output_addr: Option<String> = Some("[::1]:9921".into());
//!     let udp_output_addr: Option<String> = None;
//!
//!     let mut threads = vec![];
//!
//!     // TCP connection listener -> UDP multicast channel
//!     if let Some(tcpin) = tcp_listen_addr {
//!         let tcp_rproxy = reverse_proxy_tcp_udp(tcpin, multicast_addr.clone());
//!         threads.push(tcp_rproxy);
//!     }
//!
//!     // UDP multicast listener -> TCP sender
//!     if let Some(tcpout) = &tcp_output_addr {
//!         let tcp_proxy = reverse_proxy_udp_tcp(multicast_addr.clone(), tcpout.to_string());
//!         threads.push(tcp_proxy);
//!     }
//!
//!     // UDP multicast listener -> UDP sender
//!     if let Some(udpout) = udp_output_addr {
//!         let udp_proxy = reverse_proxy_udp(multicast_addr, udpout);
//!         threads.push(udp_proxy);
//!     }
//!
//!     for thread in threads {
//!         thread.join().unwrap();
//!     }
//! }
//! ```
//!
//! ## Command Line Interface
//! Install with Cargo
//! ```bash
//! cargo install mproxy-reverse
//! ```
//!
//! ```text
//! MPROXY: Reverse Proxy
//!
//! Forward upstream TCP and UDP upstream to downstream listeners.
//! Messages are routed via UDP multicast to downstream sender threads.
//! Spawns one thread per listener.
//!
//! USAGE:
//!   mproxy-reverse  [FLAGS] [OPTIONS]
//!
//! OPTIONS:
//!   --udp-listen-addr [HOSTNAME:PORT]     Spawn a UDP socket listener, and forward to --multicast-addr
//!   --tcp_listen_addr [HOSTNAME:PORT]     Reverse-proxy accepting TCP connections and forwarding to --multicast-addr
//!   --multicast-addr  [MULTICAST_IP:PORT] Defaults to '[ff02::1]:9918'
//!   --tcp-output-addr [HOSTNAME:PORT]     Forward packets from --multicast-addr to TCP downstream
//!   --udp_output_addr [HOSTNAME:PORT]     Forward packets from --multicast-addr to UDP downstream
//!
//! FLAGS:
//!   -h, --help    Prints help information
//!   -t, --tee     Print UDP input to stdout
//!
//! EXAMPLE:
//!   mproxy-reverse --udp-listen-addr '0.0.0.0:9920' --tcp-output-addr '[::1]:9921' --multicast-addr '224.0.0.1:9922'
//! ```
//!
//! ### See Also
//! - [mproxy-client](https://docs.rs/mproxy-client/)
//! - [mproxy-server](https://docs.rs/mproxy-server/)
//! - [mproxy-forward](https://docs.rs/mproxy-forward/)
//! - [mproxy-reverse](https://docs.rs/mproxy-reverse/)
//!

use std::io::{BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread::{spawn, JoinHandle};

use mproxy_client::target_socket_interface;
use mproxy_server::upstream_socket_interface;
use mproxy_socket_dispatch::BUFSIZE;

fn handle_client_tcp(downstream: TcpStream, multicast_addr: String) {
    #[cfg(debug_assertions)]
    println!(
        "handling downstream client: {} UDP -> {:?} TCP",
        multicast_addr, downstream
    );
    let (_multicast_addr, multicast_socket) =
        if let Ok((addr, socket)) = upstream_socket_interface(multicast_addr) {
            if !addr.ip().is_multicast() {
                panic!("not a multicast address {}", addr);
            }
            (addr, socket)
        } else {
            panic!()
        };

    let mut buf = [0u8; BUFSIZE];
    let mut tcp_writer = BufWriter::new(downstream);

    loop {
        match multicast_socket.recv_from(&mut buf[0..]) {
            Ok((count_input, _remote_addr)) => {
                //println!("{}", String::from_utf8_lossy(&buf[0..count_input]));
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

/// Forward a UDP socket stream (e.g. from a multicast channel) to connected TCP clients.
/// Spawns a listener thread, plus one thread for each incoming TCP connection.
pub fn reverse_proxy_udp_tcp(multicast_addr: String, tcp_listen_addr: String) -> JoinHandle<()> {
    #[cfg(debug_assertions)]
    println!(
        "forwarding: {} UDP -> {} TCP",
        multicast_addr, tcp_listen_addr
    );
    spawn(move || {
        let listener = TcpListener::bind(tcp_listen_addr).expect("binding downstream TCP Listener");
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

/// Forward bytes from UDP upstream socket address to UDP downstream socket address
pub fn reverse_proxy_udp(udp_input_addr: String, udp_output_addr: String) -> JoinHandle<()> {
    #[cfg(debug_assertions)]
    println!(
        "forwarding: {} UDP -> {} UDP",
        udp_input_addr, udp_output_addr
    );
    spawn(move || {
        let (addr, listen_socket) = upstream_socket_interface(udp_input_addr).unwrap();
        let (outaddr, output_socket) = target_socket_interface(&udp_output_addr).unwrap();

        let mut buf = [0u8; BUFSIZE];
        loop {
            match listen_socket.recv_from(&mut buf[0..]) {
                Ok((c, remote_addr)) => {
                    if c == 0 {
                        eprintln!("got message with size 0 from upstream: {}", remote_addr);
                    } else {
                        let c_out = output_socket
                            .send_to(&buf[0..c], outaddr)
                            .expect("forwarding UDP downstream");
                        assert!(c == c_out);
                        //println!("{}", String::from_utf8_lossy(&buf[0..c]));
                    }
                }
                Err(err) => {
                    eprintln!("{}:reverse_proxy: error {}", addr, err);
                    break;
                }
            }
        }
    })
}

/// Listen for incoming TCP connections and forward received bytes to a UDP socket address
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
