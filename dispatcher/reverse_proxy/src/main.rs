use std::process::exit;

extern crate pico_args;
use pico_args::Arguments;

extern crate proxy;
use proxy::proxy_thread;

use reverse_proxy::reverse_proxy_tcp;

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
    pub udp_listen_addr: String,
    pub multicast_addr: String,
    pub tcp_listen_addr: String,
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
    r_proxy.join().unwrap();
}
