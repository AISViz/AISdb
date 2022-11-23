use std::io::{stdout, BufWriter, Read, Write};
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::thread::spawn;
use std::thread::{Builder, JoinHandle};

use client::target_socket_interface;
use server::upstream_socket_interface;
use socket_dispatch::BUFSIZE;

pub fn proxy_thread(listen_addr: String, downstream_addrs: &[String], tee: bool) -> JoinHandle<()> {
    //let listen_socket = new_listen_socket(listen_addr);
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
        threads.push(proxy_thread(listen_addr.to_string(), downstream_addrs, tee));
    }
    threads
}

pub fn proxy_tcp_udp(upstream_tcp: String, downstream_udp: String) -> JoinHandle<()> {
    let mut buf = [0u8; BUFSIZE];

    let (target_addr, target_socket) =
        target_socket_interface(&downstream_udp).expect("UDP downstream interface");

    /*
    #[allow(unused_mut)]
    let mut stream = TcpStream::connect(upstream_tcp.clone()).expect("connecting to TCP address");
    #[cfg(feature = "tls")]
    let connector = SslConnector::builder(SslMethod::tls()).unwrap().build();
    #[cfg(feature = "tls")]
    let hostname: String = upstream_tcp.split(':').next().unwrap().to_string();
    #[cfg(feature = "tls")]
    let mut stream = connector.connect(&hostname, stream).unwrap();
    */

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
