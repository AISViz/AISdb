use std::fs::OpenOptions;
use std::io;
use std::io::{BufWriter, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs, UdpSocket};
use std::path::PathBuf;
use std::process::exit;
use std::str::FromStr;
//use std::sync::{Arc, Barrier};
use std::thread::{Builder, JoinHandle};

extern crate pico_args;
use pico_args::Arguments;

#[path = "../socket.rs"]
pub mod socket;
use socket::{bind_socket, new_socket};

const HELP: &str = r#"
DISPATCH: SERVER

USAGE:
  server --path [OUTPUT_LOGFILE] --listen_addr [SOCKET_ADDR] ...

  e.g.
  server --path logfile.log --listen_addr 127.0.0.1:9920 --listen_addr [::1]:9921


FLAGS:
  -h, --help    Prints help information

"#;

struct ServerArgs {
    listen_addr: Vec<String>,
    path: String,
}

fn parse_args() -> Result<ServerArgs, pico_args::Error> {
    let mut pargs = Arguments::from_env();
    if pargs.contains(["-h", "--help"]) || pargs.clone().finish().is_empty() {
        print!("{}", HELP);
        exit(0);
    }
    let args = ServerArgs {
        path: pargs.value_from_str("--path")?,
        listen_addr: pargs.values_from_str("--listen_addr")?,
    };
    let remaining = pargs.finish();
    if !remaining.is_empty() {
        println!("Warning: unused arguments {:?}", remaining)
    }
    if args.listen_addr.is_empty() {
        eprintln!("Error: the --listen_addr option must be set. Must provide atleast one client IP address");
    };

    Ok(args)
}

/// server: client socket handler
/// binds a new socket connection on the network multicast channel
pub fn join_multicast(addr: SocketAddr) -> io::Result<UdpSocket> {
    // https://bluejekyll.github.io/blog/posts/multicasting-in-rust/
    #[cfg(debug_assertions)]
    println!("server broadcasting to: {}", addr.ip());
    match addr.ip() {
        IpAddr::V4(ref mdns_v4) => {
            let socket = new_socket(&addr)?;
            // join multicast channel on all interfaces
            socket.join_multicast_v4(mdns_v4, &Ipv4Addr::new(0, 0, 0, 0))?;
            let bind_result = bind_socket(&socket, &addr);
            if bind_result.is_err() {
                panic!("binding to {:?}  {:?}", addr, bind_result);
            }

            Ok(socket.into())
        }
        IpAddr::V6(ref mdns_v6) => {
            let socket = match new_socket(&addr) {
                Ok(s) => s,
                Err(e) => panic!("creating new socket {}", e),
            };
            // bind to all interfaces
            //assert!(socket.set_multicast_if_v6(0).is_ok());

            // join multicast channel
            assert!(socket.join_multicast_v6(mdns_v6, 0).is_ok());
            //socket.join_multicast_v6(&Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0), addr.port().into())?;

            // disable ipv4->ipv6 multicast rerouting
            assert!(socket.set_only_v6(true).is_ok());

            let listenaddr = SocketAddr::new(
                IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0)),
                addr.port(),
            );
            let bind_result = bind_socket(&socket, &listenaddr);
            if bind_result.is_err() {
                panic!("binding to {:?}: {:?}", listenaddr, bind_result);
            }

            Ok(socket.into())
        }
    }
}

pub fn join_unicast(addr: SocketAddr) -> io::Result<UdpSocket> {
    let socket = new_socket(&addr)?;
    bind_socket(&socket, &addr)?;
    Ok(socket.into())
}

/// server socket listener
pub fn listener(addr: String, logfile: PathBuf) -> JoinHandle<()> {
    let addr = addr
        .to_socket_addrs()
        .unwrap()
        .next()
        .expect("parsing socket address");

    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(&logfile);
    let mut writer = BufWriter::new(file.unwrap());

    let listen_socket = match addr.ip().is_multicast() {
        false => join_unicast(addr).expect("failed to create socket listener!"),
        true => {match join_multicast(addr) {
            Ok(s) => s,
            Err(e) => panic!("failed to create multicast listener on address {}! are you sure this is a valid multicast channel?\n{:?}", addr, e),
        }},
    };
    let join_handle = Builder::new()
        .name(format!("{}:server", addr))
        .spawn(move || {
            //let mut buf = [0u8; 1024]; // receive buffer
            let mut buf = [0u8; 16384]; // receive buffer
            loop {
                match listen_socket.recv_from(&mut buf[0..]) {
                    Ok((c, _remote_addr)) => {
                        /*
                        #[cfg(debug_assertions)]
                        println!(
                        "{}:server: got {} bytes from {}\t\t{}",
                        addr,
                        c,
                        _remote_addr,
                        String::from_utf8_lossy(&buf[0..c]),
                        );
                        */

                        let _ = writer
                            .write(&buf[0..c])
                            .unwrap_or_else(|_| panic!("writing to {:?}", &logfile));
                        //buf = [0u8; 1024];

                        /*
                        let responder = new_socket(&remote_addr).expect("failed to create responder");
                        let remote_socket = SockAddr::from(remote_addr);
                        responder .send_to(thread_name.as_bytes(), &remote_socket) .expect("failed to respond");
                        #[cfg(debug_assertions)]
                        println!( "{}:server: sent thread_name {} to: {}", thread_name, thread_name, _remote_addr);
                        */
                    }
                    Err(err) => {
                        writer.flush().unwrap();
                        eprintln!("{}:server: got an error: {}", addr, err);
                        #[cfg(debug_assertions)]
                        panic!("{}:server: got an error: {}", addr, err);
                    }
                }
                writer.flush().unwrap();
            }
        })
        .unwrap();

    //downstream_barrier.wait();
    join_handle
}

pub fn main() {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("Error: {}.", e);
            exit(1);
        }
    };

    let mut threads = vec![];

    let append_listen_addr = args.listen_addr.len() > 1;

    for hostname in args.listen_addr {
        // if listening to multiple clients at once, log each client to a
        // separate file, with the client address appended to the filename
        let mut logpath: String = "".to_owned();
        logpath.push_str(&args.path);
        if append_listen_addr {
            for pathsegment in [&args.path, &".".to_string(), &hostname] {
                logpath.push_str(pathsegment);
            }
        }

        println!("logging transmissions from {} to {}", hostname, logpath);
        threads.push(listener(hostname, PathBuf::from_str(&logpath).unwrap()));
    }
    for thread in threads {
        let _ = thread.join().unwrap();
    }
}
