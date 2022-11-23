use std::process::exit;

use proxy::{proxy_gateway, proxy_tcp_udp};

use pico_args::Arguments;

const HELP: &str = r#"
DISPATCH: proxy

USAGE:
  proxy  [FLAGS] [OPTIONS]

  proxy --udp_listen_addr '0.0.0.0:9920' --udp_downstream_addr '[::1]:9921' \
        --udp_downstream_addr 'localhost:9922' --tee

OPTIONS:
  --udp_listen_addr [HOSTNAME:PORT]     UDP listening socket address. May be
                                        repeated for multiple interfaces

  --udp_downstream_addr [HOSTNAME:PORT] UDP downstream socket address. May be
                                        repeated for multiple interfaces

  --tcp_connect_addr [HOSTNAME:PORT]    Connect to TCP host, forwarding stream
                                        data to the first UDP listener address.
                                        May be repeated for multiple connections

FLAGS:
  -h, --help    Prints help information
  -t, --tee     Copy input to stdout

"#;

pub struct GatewayArgs {
    udp_listen_addrs: Vec<String>,
    udp_downstream_addrs: Vec<String>,
    tcp_connect_addrs: Vec<String>,
    tee: bool,
}

fn parse_args() -> Result<GatewayArgs, pico_args::Error> {
    let mut pargs = Arguments::from_env();
    if pargs.contains(["-h", "--help"]) || pargs.clone().finish().is_empty() {
        print!("{}", HELP);
        exit(0);
    }

    let args = GatewayArgs {
        udp_listen_addrs: pargs.values_from_str("--udp_listen_addr")?,
        udp_downstream_addrs: pargs.values_from_str("--udp_downstream_addr")?,
        tcp_connect_addrs: pargs.values_from_str("--tcp_connect_addr")?,
        tee: pargs.contains(["-t", "--tee"]),
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
    let mut threads = vec![];

    for upstream in args.tcp_connect_addrs {
        threads.push(proxy_tcp_udp(upstream, args.udp_listen_addrs[0].clone()));
    }

    for thread in proxy_gateway(&args.udp_downstream_addrs, &args.udp_listen_addrs, args.tee) {
        threads.push(thread);
    }

    for thread in threads {
        thread.join().unwrap();
    }
}
