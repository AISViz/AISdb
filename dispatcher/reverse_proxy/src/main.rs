use std::process::exit;

use mproxy_forward::forward_udp;
use mproxy_reverse::{reverse_proxy_tcp_udp, reverse_proxy_udp, reverse_proxy_udp_tcp};

use pico_args::Arguments;

const HELP: &str = r#"
MPROXY: Reverse Proxy

Forward upstream TCP and UDP upstream to downstream listeners.
Messages are routed via UDP multicast to downstream sender threads. 
Spawns one thread per listener.

USAGE:
  mproxy-reverse  [FLAGS] [OPTIONS]

OPTIONS:
  --udp-listen-addr [HOSTNAME:PORT]     Spawn a UDP socket listener, and forward to --multicast-addr
  --tcp-listen-addr [HOSTNAME:PORT]     Reverse-proxy accepting TCP connections and forwarding to --multicast-addr
  --multicast-addr  [MULTICAST_IP:PORT] Defaults to '[ff02::1]:9918'
  --tcp-output-addr [HOSTNAME:PORT]     Forward packets from --multicast-addr to TCP downstream
  --udp-output-addr [HOSTNAME:PORT]     Forward packets from --multicast-addr to UDP downstream

FLAGS:
  -h, --help    Prints help information
  -t, --tee     Print UDP input to stdout

EXAMPLE:
  mproxy-reverse --udp-listen-addr '0.0.0.0:9920' --tcp-output-addr '[::1]:9921' --multicast-addr '224.0.0.1:9922'

"#;

pub struct ReverseProxyArgs {
    pub udp_listen_addr: Option<String>,
    pub tcp_listen_addr: Option<String>,
    pub multicast_addr: Option<String>,
    pub tcp_output_addr: Option<String>,
    pub udp_output_addr: Option<String>,
    pub tee: bool,
}

fn parse_args() -> Result<ReverseProxyArgs, pico_args::Error> {
    let mut pargs = Arguments::from_env();
    if pargs.contains(["-h", "--help"]) || pargs.clone().finish().is_empty() {
        print!("{}", HELP);
        exit(0);
    }
    let tee = pargs.contains(["-t", "--tee"]);
    let args = ReverseProxyArgs {
        udp_listen_addr: pargs.opt_value_from_str("--udp-listen-addr")?,
        tcp_listen_addr: pargs.opt_value_from_str("--tcp-listen-addr")?,
        multicast_addr: pargs.opt_value_from_str("--multicast-addr")?,
        tcp_output_addr: pargs.opt_value_from_str("--tcp-output-addr")?,
        udp_output_addr: pargs.opt_value_from_str("--udp-output-addr")?,
        tee,
    };
    let remaining = pargs.finish();
    if !remaining.is_empty() {
        println!("Warning: unused arguments {:?}", remaining)
    }

    Ok(args)
}

pub fn main() {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("Error: {}.", e);
            exit(1);
        }
    };

    let multicast: String = match args.multicast_addr {
        Some(addr) => addr,
        _ => "[ff02::1]:9918".to_string(),
    };

    let mut threads = vec![];

    // UDP listener thread -> UPD multicast sender
    // rebroadcast upstream UDP via multicast to client threads
    if let Some(udp_listen) = args.udp_listen_addr {
        let multicast = forward_udp(udp_listen, &[multicast.to_string()], args.tee);
        threads.push(multicast);
    }

    // UDP multicast listener -> TCP sender
    if let Some(tcpout) = &args.tcp_output_addr {
        let tcp_proxy = reverse_proxy_udp_tcp(multicast.to_string(), tcpout.to_string());
        threads.push(tcp_proxy);
    }

    // TCP connection listener -> UDP multicast
    if let Some(tcpin) = args.tcp_listen_addr {
        let tcp_rproxy = reverse_proxy_tcp_udp(tcpin, multicast.to_string());
        threads.push(tcp_rproxy);
    }

    // UDP listener -> UDP sender
    if let Some(udpout) = args.udp_output_addr {
        let udp_proxy = reverse_proxy_udp(multicast, udpout);
        threads.push(udp_proxy);
    }

    for thread in threads {
        thread.join().unwrap();
    }
}
