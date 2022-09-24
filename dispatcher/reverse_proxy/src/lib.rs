use std::io::{BufWriter, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
//use std::process::exit;
use std::thread::{spawn, JoinHandle};

//use crate::proxy::proxy_thread;
//#[path = "./server.rs"]
//mod server;
extern crate server;
use server::join_multicast;

fn handle_client(downstream: TcpStream, multicast_addr: String) {
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
    // multicast_socket.set_broadcast(true).unwrap();

    let mut buf = [0u8; 32768]; // receive buffer
    let mut tcp_writer = BufWriter::new(downstream);

    loop {
        match multicast_socket.recv_from(&mut buf[0..]) {
            Ok((count_input, _remote_addr)) => {
                let _count_output = tcp_writer.write(&buf[0..count_input]);
            }
            Err(err) => {
                eprintln!("reverse_proxy_client: got an error: {}", err);
                break;
            }
        }
        if let Err(e) = tcp_writer.flush() {
            eprintln!("exiting {:?}: {}", multicast_socket, e);
            break;
        }
    }
}

/// forward UDP socket stream to downstream TCP clients
///
/// Spawns a new thread for each client.
/// An additional thread will be spawned to listen for upstream_addr, which
/// is rebroadcasted over the multicast channel. Client handler threads
/// subscribing to this channel will then forward UDP packet information
/// downstream to any clients connected via TCP
pub fn reverse_proxy_tcp(multicast_addr: String, tcp_listen_addr: String) -> JoinHandle<()> {
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
                handle_client(stream.unwrap(), multicast_addr);
            });
        }
    })
}
