use std::fs::OpenOptions;
use std::io::{stdin, stdout, BufRead, BufReader, BufWriter, Read, Result as ioResult, Write};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs, UdpSocket};
use std::path::PathBuf;
use std::str::FromStr;

//#[path = "../socket.rs"]
//mod socket;
//use socket::{bind_socket, new_socket};
//use crate::{bind_socket, new_socket};
extern crate dispatch;
use dispatch::{bind_socket, new_socket};

/// new upstream socket
/// socket will allow any downstream IP i.e. 0.0.0.0
pub fn new_sender(addr: &SocketAddr) -> ioResult<UdpSocket> {
    let socket = new_socket(addr)?;

    if !addr.is_ipv4() {
        panic!("invalid socket address type!")
    }
    if addr.ip().is_multicast() {
        //socket.set_multicast_if_v4(&Ipv4Addr::new(0, 0, 0, 0))?;
        socket.set_multicast_loop_v4(true)?;
    }
    let target_addr = SocketAddr::new(Ipv4Addr::new(0, 0, 0, 0).into(), 0);
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
        socket.set_reuse_address(true)?;
        let _c = bind_socket(&socket, &target_addr);

        assert!(_b.is_ok());
        if _c.is_err() {
            panic!("error binding socket {:?}", _c);
        }
    } else {
    }
    Ok(socket.into())
}

pub fn client_check_ipv6_interfaces(addr: &SocketAddr) -> ioResult<UdpSocket> {
    for i in 0..32 {
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

pub fn client_socket_stream(path: &PathBuf, server_addrs: Vec<String>, tee: bool) -> ioResult<()> {
    let mut targets = vec![];

    for server_addr in server_addrs {
        let target_addr = server_addr
            .to_socket_addrs()
            .unwrap()
            .next()
            .expect("parsing server address");
        let target_socket = match target_addr.is_ipv4() {
            true => new_sender(&target_addr).expect("creating ipv4 send socket!"),
            false => {
                client_check_ipv6_interfaces(&target_addr).expect("creating ipv6 send socket!")
            }
        };
        target_socket.set_broadcast(true)?;
        targets.push((target_addr, target_socket));
        println!(
            "logging {}: listening for {}",
            &path.as_os_str().to_str().unwrap(),
            server_addr,
        );
    }

    let mut reader: Box<dyn BufRead> = if path == &PathBuf::from_str("-").unwrap() {
        Box::new(BufReader::new(stdin()))
    } else {
        Box::new(BufReader::new(
            OpenOptions::new()
                .create(false)
                .write(false)
                .read(true)
                .open(&path)
                .unwrap_or_else(|e| {
                    panic!("opening {}, {}", path.as_os_str().to_str().unwrap(), e)
                }),
        ))
    };

    let mut buf = vec![0u8; 32768];
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
                .send_to(&buf[0..c], &target_addr)
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
