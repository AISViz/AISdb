//! Multicast Network Dispatcher and Proxy
//!
//! # MPROXY: Client
//! Stream file or socket data via UDP. Supports multicast routing
//!
//!
//! ## Quick Start
//! In `Cargo.toml`
//! ```toml
//! [dependencies]
//! mproxy-client = "0.1"
//! ```
//!
//! Example `src/main.rs`
//! ```rust,no_run
//! use std::path::PathBuf;
//! use std::thread::spawn;
//!
//! use mproxy_client::client_socket_stream;
//!
//! pub fn main() {
//!     // read input from stdin
//!     let path = PathBuf::from("-");
//!
//!     // downstream UDP socket addresses
//!     let server_addrs =  vec!["127.0.0.1:9919".into(), "localhost:9921".into(), "[ff02::1]:9920".into()];
//!     
//!     // copy input to stdout
//!     let tee = true;
//!
//!     let client_thread = spawn(move || {
//!         client_socket_stream(&path, server_addrs, tee).unwrap();
//!     });
//!
//!     // run client until EOF
//!     client_thread.join().unwrap();
//! }
//! ```
//!
//! ## Command Line Interface
//! Install with cargo
//! ```bash
//! cargo install mproxy-client
//! ```
//!
//! ```text
//! MPROXY: UDP Client
//!
//! Stream local data to logging servers via UDP
//!
//! USAGE:
//!   mproxy-client [FLAGS] [OPTIONS] ...
//!
//! OPTIONS:
//!   --path        [FILE_DESCRIPTOR]   Filepath, descriptor, or handle. Use "-" for stdin
//!   --server-addr [HOSTNAME:PORT]     Downstream UDP server address. May be repeated
//!
//! FLAGS:
//!   -h, --help    Prints help information
//!   -t, --tee     Copy input to stdout
//!
//! EXAMPLE:
//!   mproxy-client --path /dev/random --server-addr '127.0.0.1:9920' --server-addr '[::1]:9921'
//!   mproxy-client --path - --server-addr '224.0.0.1:9922' --server-addr '[ff02::1]:9923' --tee >> logfile.log
//! ```
//!
//! ### See Also
//! - [mproxy-client](https://docs.rs/mproxy-client/)
//! - [mproxy-server](https://docs.rs/mproxy-server/)
//! - [mproxy-forward](https://docs.rs/mproxy-forward/)
//! - [mproxy-reverse](https://docs.rs/mproxy-reverse/)
//!

use std::fs::OpenOptions;
use std::io::{stdin, stdout, BufRead, BufReader, BufWriter, Read, Result as ioResult, Write};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs, UdpSocket};
use std::path::PathBuf;
use std::str::FromStr;

use mproxy_socket_dispatch::{bind_socket, new_socket, BUFSIZE};

/// New upstream socket on any available UDP port.
/// Will allow any downstream IP i.e. 0.0.0.0
fn new_sender(addr: &SocketAddr) -> ioResult<UdpSocket> {
    let socket = new_socket(addr)?;

    if !addr.is_ipv4() {
        panic!("invalid socket address type!")
    }
    if addr.ip().is_multicast() {
        //socket.set_multicast_if_v4(&Ipv4Addr::new(0, 0, 0, 0))?;
        socket.set_multicast_loop_v4(true)?;
    }
    let target_addr = SocketAddr::new(Ipv4Addr::new(0, 0, 0, 0).into(), 0);
    socket.set_reuse_address(true)?;
    bind_socket(&socket, &target_addr)?;

    Ok(socket.into())
}

/// new data output socket to the client IPv6 address
/// socket will allow any downstream IP i.e. ::0
fn new_sender_ipv6(addr: &SocketAddr, ipv6_interface: u32) -> ioResult<UdpSocket> {
    //let target_addr = SocketAddr::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0).into(), addr.port());
    let target_addr = SocketAddr::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0).into(), 0);

    if !addr.is_ipv6() {
        panic!("invalid socket address type!")
    }

    let socket = new_socket(addr)?;
    if addr.ip().is_multicast() {
        if let Err(e) = socket.set_multicast_if_v6(ipv6_interface) {
            panic!("setting multicast ipv6 interface: {} {}", ipv6_interface, e);
        }
        let _b = socket.set_multicast_loop_v6(true);
        let _c = bind_socket(&socket, &target_addr);

        assert!(_b.is_ok());
        if _c.is_err() {
            panic!("error binding socket {:?}", _c);
        }
    }
    socket.set_reuse_address(true)?;
    Ok(socket.into())
}

fn client_check_ipv6_interfaces(addr: &SocketAddr) -> ioResult<UdpSocket> {
    // workaround:
    // find the first suitable interface
    for i in 0..65536 {
        //#[cfg(debug_assertions)]
        //println!("checking interface {}", i);
        let socket = new_sender_ipv6(addr, i)?;
        let result = socket.send_to(b"", addr);
        if let Ok(_r) = result {
            //Ok(_r) => {
            //#[cfg(debug_assertions)]
            //println!("opened interface {}:\t{}", i, _r);
            return Ok(socket);
            //}
            //Err(e) => {
            //    eprintln!("err: could not open interface {}:\t{:?}", i, e)
            //}
        }
    }
    panic!("No suitable network interfaces were found!");
}

/// Binds to a random UDP port for sending to downstream.
/// To bind to a specific port, see mproxy_server::upstream_socket_interface
pub fn target_socket_interface(server_addr: &String) -> ioResult<(SocketAddr, UdpSocket)> {
    let target_addr = server_addr
        .to_socket_addrs()
        .expect(format!("parsing server address from {}", server_addr).as_str())
        .next()
        .expect(format!("parsing server address from {}", server_addr).as_str());
    let target_socket = match target_addr.is_ipv4() {
        true => new_sender(&target_addr).expect("creating ipv4 send socket!"),
        false => client_check_ipv6_interfaces(&target_addr).expect("creating ipv6 send socket!"),
    };
    //if target_addr.ip().is_multicast() {
    target_socket.set_broadcast(true)?;
    //}

    Ok((target_addr, target_socket))
}

/// Read bytes from `path` info a buffer, and forward to downstream UDP server addresses.
/// Optionally copy output to stdout
pub fn client_socket_stream(path: &PathBuf, server_addrs: Vec<String>, tee: bool) -> ioResult<()> {
    let mut targets = vec![];

    for server_addr in server_addrs {
        let (target_addr, target_socket) = target_socket_interface(&server_addr)?;
        targets.push((target_addr, target_socket));
        println!(
            "logging {}: listening for {}",
            &path.as_os_str().to_str().unwrap(),
            server_addr,
        );
    }

    // if path is "-" set read buffer to stdin
    // otherwise, create buffered reader from given file descriptor
    let mut reader: Box<dyn BufRead> = if path == &PathBuf::from_str("-").unwrap() {
        Box::new(BufReader::new(stdin()))
    } else {
        Box::new(BufReader::new(
            OpenOptions::new()
                .create(false)
                .write(false)
                .read(true)
                .open(path)
                .unwrap_or_else(|e| {
                    panic!("opening {}, {}", path.as_os_str().to_str().unwrap(), e)
                }),
        ))
    };

    let mut buf = vec![0u8; BUFSIZE];
    let mut output_buffer = BufWriter::new(stdout());

    while let Ok(c) = reader.read(&mut buf) {
        if c == 0 {
            #[cfg(debug_assertions)]
            println!(
                "\nclient: encountered EOF in {}, exiting...",
                &path.display(),
            );
            break;
        } else if c == 1 && String::from_utf8(buf[0..c].to_vec()).unwrap() == *"\n" {
            // skip empty lines
            continue;
        }

        //#[cfg(debug_assertions)]
        //println!("\nc:{} |{:?}|", c, String::from_utf8(buf[0..c].to_vec()));

        for (target_addr, target_socket) in &targets {
            target_socket
                .send_to(&buf[0..c], target_addr)
                .expect("sending to server socket");
        }
        if tee {
            let _o = output_buffer
                .write(&buf[0..c])
                .expect("writing to output buffer");
            output_buffer.flush().unwrap();
            #[cfg(debug_assertions)]
            assert!(c == _o);
        }
    }
    Ok(())
}
