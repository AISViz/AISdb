use std::io::{stdout, BufWriter, Write};
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::process::exit;
use std::thread::{Builder, JoinHandle};
//use std::thread::sleep;
//use std::time::Duration;

extern crate pico_args;
use pico_args::Arguments;

#[path = "./client.rs"]
pub mod client;
use client::{client_check_ipv6_interfaces, new_sender};

#[path = "./server.rs"]
pub mod server;
use server::{join_multicast, join_unicast};

const HELP: &str = r#"
DISPATCH: proxy 

USAGE:
  proxy --listen_addr [LOCAL_ADDRESS:PORT] --downstream_addr [HOSTNAME:PORT] ...

  either --listen_addr or --downstream_addr may be repeated
  e.g.
  proxy --listen_addr '0.0.0.0:9920' --downstream_addr '[::1]:9921' --downstream_addr 'localhost:9922' --tee


FLAGS:
  -h, --help    Prints help information
  -t, --tee     Copy input to stdout

"#;

pub struct GatewayArgs {
    downstream_addrs: Vec<String>,
    listen_addrs: Vec<String>,
    tee: bool,
}

fn parse_args() -> Result<GatewayArgs, pico_args::Error> {
    let mut pargs = Arguments::from_env();
    if pargs.contains(["-h", "--help"]) || pargs.clone().finish().is_empty() {
        print!("{}", HELP);
        exit(0);
    }
    let tee = pargs.contains(["-t", "--tee"]);

    let args = GatewayArgs {
        listen_addrs: pargs.values_from_str("--listen_addr")?,
        downstream_addrs: pargs.values_from_str("--downstream_addr")?,
        tee,
    };
    let remaining = pargs.finish();
    if !remaining.is_empty() {
        println!("Warning: unused arguments {:?}", remaining)
    }

    Ok(args)
}

pub fn proxy_thread(
    listen_addr: &String,
    downstream_addrs: &[String],
    tee: bool,
) -> JoinHandle<()> {
    let addr = listen_addr
        .to_socket_addrs()
        .unwrap()
        .next()
        .expect("parsing socket address");
    let listen_socket = match addr.ip().is_multicast() {
            false => join_unicast(addr).expect("failed to create socket listener!"),
            true => {match join_multicast(addr) {
                Ok(s) => s,
                Err(e) => panic!("failed to create multicast listener on address {}! are you sure this is a valid multicast channel?\n{:?}", addr, e),
            }
            },
        };
    let mut output_buffer = BufWriter::new(stdout());
    let targets: Vec<(SocketAddr, UdpSocket)> = downstream_addrs
        .iter()
        .map(|a| {
            let addr = a
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
        })
        .collect();
    let mut buf = [0u8; 1024]; // receive buffer
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

pub fn main() {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("Error: {}.", e);
            exit(1);
        }
    };

    for thread in proxy_gateway(&args.downstream_addrs, &args.listen_addrs, args.tee) {
        thread.join().unwrap();
    }
}
