use std::process::exit;

extern crate pico_args;
use pico_args::Arguments;

use proxy::proxy_gateway;

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
