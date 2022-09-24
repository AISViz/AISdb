use std::io::{BufWriter, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::process::exit;
use std::thread::{spawn, JoinHandle};

extern crate pico_args;
use pico_args::Arguments;

#[path = "./server.rs"]
pub mod server;
use server::join_multicast;

#[path = "./proxy.rs"]
pub mod proxy;
use proxy::proxy_thread;

const HELP: &str = r#"
DISPATCH: reverse_proxy 

USAGE:
  reverse_proxy --udp_listen_addr [HOSTNAME:PORT] --tcp_listen_addr [LOCAL_ADDRESS:PORT] --multicast_addr [MULTICAST_IP:PORT] 

  e.g.
  reverse_proxy --udp_listen_addr '0.0.0.0:9920' --tcp_listen_addr '[::1]:9921' --multicast_addr '224.0.0.1:9922'

FLAGS:
  -h, --help    Prints help information
  -t, --tee     Copy input to stdout

"#;

pub struct ReverseProxyArgs {
    udp_listen_addr: String,
    multicast_addr: String,
    tcp_listen_addr: String,
    tee: bool,
}

fn parse_args() -> Result<ReverseProxyArgs, pico_args::Error> {
    let mut pargs = Arguments::from_env();
    if pargs.contains(["-h", "--help"]) || pargs.clone().finish().is_empty() {
        print!("{}", HELP);
        exit(0);
    }
    let tee = pargs.contains(["-t", "--tee"]);
    let args = ReverseProxyArgs {
        udp_listen_addr: pargs.value_from_str("--udp_listen_addr")?,
        multicast_addr: pargs.value_from_str("--multicast_addr")?,
        tcp_listen_addr: pargs.value_from_str("--tcp_listen_addr")?,
        tee,
    };
    let remaining = pargs.finish();
    if !remaining.is_empty() {
        println!("Warning: unused arguments {:?}", remaining)
    }

    Ok(args)
}

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

    let mut buf = [0u8; 16384]; // receive buffer
    let mut tcp_writer = BufWriter::new(downstream);

    loop {
        match multicast_socket.recv_from(&mut buf[0..]) {
            Ok((count_input, _remote_addr)) => {
                let _count_output = tcp_writer.write(&buf[0..count_input]);
            }
            Err(err) => {
                #[cfg(debug_assertions)]
                eprintln!("reverse_proxy_client: got an error: {}", err);
                break;
            }
        }
        if let Err(e) = tcp_writer.flush() {
            #[cfg(debug_assertions)]
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

pub fn main() {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("Error: {}.", e);
            exit(1);
        }
    };

    // UDP listener thread -> UPD multicast sender
    // rebroadcast upstream UDP via multicast to client threads
    let _multicast = proxy_thread(
        &args.udp_listen_addr,
        &[args.multicast_addr.clone()],
        args.tee,
    );

    let r_proxy = reverse_proxy_tcp(args.multicast_addr, args.tcp_listen_addr);
    let _ = r_proxy.join().unwrap();
}
