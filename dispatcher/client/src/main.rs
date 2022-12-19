use std::ffi::OsStr;
use std::path::PathBuf;
use std::process::exit;

use mproxy_client::client_socket_stream;

use pico_args::Arguments;

pub const HELP: &str = r#"
MPROXY: UDP Client

Stream file or socket data via UDP. Supports multicast routing

USAGE:
  mproxy-client [FLAGS] [OPTIONS] ...

OPTIONS:
  --path        [FILE_DESCRIPTOR]   Filepath, descriptor, or handle. Use "-" for stdin
  --server-addr [HOSTNAME:PORT]     Downstream UDP server address. May be repeated

FLAGS:
  -h, --help    Prints help information
  -t, --tee     Copy input to stdout

EXAMPLE:
  mproxy-client --path /dev/random --server-addr '127.0.0.1:9920' --server-addr '[::1]:9921'
  mproxy-client --path - --server-addr '224.0.0.1:9922' --server-addr '[ff02::1]:9923' --tee >> logfile.log

"#;

/// command line arguments
pub struct ClientArgs {
    path: PathBuf,
    server_addrs: Vec<String>,
    tee: bool,
}

/// retrieve command line arguments as ClientArgs struct
fn parse_args() -> Result<ClientArgs, pico_args::Error> {
    let mut pargs = Arguments::from_env();
    if pargs.contains(["-h", "--help"]) || pargs.clone().finish().is_empty() {
        print!("{}", HELP);
        exit(0);
    }
    let tee = pargs.contains(["-t", "--tee"]);

    fn parse_path(s: &OsStr) -> Result<PathBuf, &'static str> {
        Ok(s.into())
    }

    let args = ClientArgs {
        path: pargs.value_from_os_str("--path", parse_path)?,
        server_addrs: pargs.values_from_str("--server-addr")?,
        tee,
    };
    let remaining = pargs.finish();
    if !remaining.is_empty() {
        println!("Warning: unused arguments {:?}", remaining)
    }

    if args.server_addrs.is_empty() && !args.tee {
        println!(
            "At least one server address (or the --tee flag) is required. See --help for more info"
        );
        exit(0);
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
    let _ = client_socket_stream(&args.path, args.server_addrs, args.tee);
}
