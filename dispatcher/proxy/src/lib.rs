//! Multicast Network Dispatcher and Proxy
//!
//! # MPROXY: Forwarding Proxy
//! Forward TLS/TCP, UDP, or Multicast endpoints to a downstream UDP socket address.
//! Use feature `tls` to enable TLS provided by crate `rustls`.
//!
//!
//! ## Quick Start
//! In `Cargo.toml`
//! ```toml
//! [dependencies]
//! mproxy-forward = { version = "0.1", features = ["tls"] }
//! ```
//!
//! Example `src/main.rs`
//! ```rust,no_run
//! use std::thread::JoinHandle;
//!
//! use mproxy_forward::{forward_udp, proxy_tcp_udp};
//!
//! pub fn main() {
//!     let udp_listen_addr: String ="[ff02::1]:9920".into();
//!     let udp_downstream_addrs = vec!["[::1]:9921".into(), "localhost:9922".into()];
//!     let tcp_connect_addr: String = "localhost:9925".into();
//!     let tee = true;  // copy input to stdout
//!     
//!     let mut threads: Vec<JoinHandle<()>> = vec![];
//!     
//!     // spawn UDP socket listener and forward to downstream addresses
//!     threads.push(forward_udp(udp_listen_addr.clone(), &udp_downstream_addrs, tee));
//!     
//!     // connect to TCP upstream, and forward to UDP socket listener
//!     threads.push(proxy_tcp_udp(tcp_connect_addr, udp_listen_addr));
//!
//!     for thread in threads {
//!         thread.join().unwrap();
//!     }
//! }
//! ```
//!
//! ## Command Line Interface
//! Install with cargo
//! ```bash
//! cargo install mproxy-forward
//! ```
//!
//! ```text
//! MPROXY: Forwarding Proxy
//!
//! Forward TLS/TCP, UDP, or Multicast endpoints to a downstream UDP socket address.
//!
//! USAGE:
//!   mproxy-forward  [FLAGS] [OPTIONS]
//!
//! OPTIONS:
//!   --udp-listen-addr     [HOSTNAME:PORT]     UDP listening socket address. May be repeated
//!   --udp-downstream-addr [HOSTNAME:PORT]     UDP downstream socket address. May be repeated
//!   --tcp-connect-addr    [HOSTNAME:PORT]     Connect to TCP host, forwarding stream. May be repeated
//!
//! FLAGS:
//!   -h, --help    Prints help information
//!   -t, --tee     Copy input to stdout
//!
//! EXAMPLE:
//!   mproxy-forward --udp-listen-addr '0.0.0.0:9920' \
//!     --udp-downstream-addr '[::1]:9921' \
//!     --udp-downstream-addr 'localhost:9922' \
//!     --tcp-connect-addr 'localhost:9925' \
//!     --tee
//! ```
//!
//! ### See Also
//! - [mproxy-client](https://docs.rs/mproxy-client/)
//! - [mproxy-server](https://docs.rs/mproxy-server/)
//! - [mproxy-forward](https://docs.rs/mproxy-forward/)
//! - [mproxy-reverse](https://docs.rs/mproxy-reverse/)
//!

use std::io::{stdout, BufWriter, Read, Write};
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::thread::{spawn, Builder, JoinHandle};

use mproxy_client::target_socket_interface;
use mproxy_server::upstream_socket_interface;
use mproxy_socket_dispatch::BUFSIZE;

/// Forward UDP upstream `listen_addr` to downstream UDP socket addresses.
/// `listen_addr` may be a multicast address.
pub fn forward_udp(listen_addr: String, downstream_addrs: &[String], tee: bool) -> JoinHandle<()> {
    let (_addr, listen_socket) = upstream_socket_interface(listen_addr).unwrap();
    let mut output_buffer = BufWriter::new(stdout());
    let targets: Vec<(SocketAddr, UdpSocket)> = downstream_addrs
        .iter()
        .map(|t| target_socket_interface(t).unwrap())
        .collect();
    let mut buf = [0u8; BUFSIZE]; // receive buffer
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
                                .send_to(&buf[0..c], target_addr)
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
                        eprintln!("forward_udp: got an error: {}", err);
                        #[cfg(debug_assertions)]
                        panic!("forward_udp: got an error: {}", err);
                    }
                }
                output_buffer.flush().unwrap();
            }
        })
        .unwrap()
}

/// Wrapper for forward_udp listening on multiple upstream addresses
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
        threads.push(forward_udp(listen_addr.to_string(), downstream_addrs, tee));
    }
    threads
}

/// Connect to TCP upstream server, and forward received bytes to a
/// downstream UDP socket socket address.
/// TLS can be enabled with feature `tls` (provided by crate `rustls`).
pub fn proxy_tcp_udp(upstream_tcp: String, downstream_udp: String) -> JoinHandle<()> {
    let mut buf = [0u8; BUFSIZE];

    let (target_addr, target_socket) =
        target_socket_interface(&downstream_udp).expect("UDP downstream interface");

    #[cfg(debug_assertions)]
    println!(
        "proxy: forwarding TCP {:?} -> UDP {:?}",
        upstream_tcp, downstream_udp
    );

    spawn(move || {
        #[cfg(feature = "tls")]
        let (mut conn, mut stream) = tls_connection(upstream_tcp.clone());
        #[cfg(feature = "tls")]
        let mut stream = TlsStream::new(&mut conn, &mut stream);
        #[cfg(not(feature = "tls"))]
        let mut stream =
            TcpStream::connect(upstream_tcp.clone()).expect("connecting to TCP address");

        loop {
            match stream.read(&mut buf[0..]) {
                Ok(c) => {
                    if c == 0 {
                        panic!("encountered EOF, disconnecting TCP proxy thread...");
                    }
                    target_socket
                        .send_to(&buf[0..c], target_addr)
                        .expect("sending to UDP socket");
                }
                Err(e) => {
                    panic!("err: {}", e);
                }
            }
        }
    })
}

#[cfg(feature = "tls")]
use rustls::client::{ClientConfig, ClientConnection, ServerName};
#[cfg(feature = "tls")]
use rustls::Stream as TlsStream;
#[cfg(feature = "tls")]
use std::sync::Arc;
#[cfg(feature = "tls")]
use webpki_roots::TLS_SERVER_ROOTS;

#[cfg(feature = "tls")]
pub fn tls_connection(tls_connect_addr: String) -> (ClientConnection, TcpStream) {
    let mut root_store = rustls::RootCertStore::empty();
    root_store.add_server_trust_anchors(TLS_SERVER_ROOTS.0.iter().map(|ta| {
        rustls::OwnedTrustAnchor::from_subject_spki_name_constraints(
            ta.subject,
            ta.spki,
            ta.name_constraints,
        )
    }));
    let config = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    let rc_config: Arc<ClientConfig> = Arc::new(config);
    let dns_name: String = tls_connect_addr.split(':').next().unwrap().to_string();
    let server_name = ServerName::try_from(dns_name.as_str()).unwrap();
    let mut conn = rustls::ClientConnection::new(rc_config, server_name).unwrap();
    let sock = TcpStream::connect(tls_connect_addr.clone()).unwrap();
    sock.set_nodelay(true).unwrap();

    // request tls
    let request = format!(
        "GET / HTTP/1.1\r\n\
         Host: {}\r\n\
         Connection: close\r\n\
         Accept-Encoding: identity\r\n\
         \r\n",
        tls_connect_addr
    );
    if let Some(mut early_data) = conn.early_data() {
        early_data.write_all(request.as_bytes()).unwrap();
    }
    (conn, sock)
}
