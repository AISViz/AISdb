use std::ffi::OsStr;
use std::path::PathBuf;
use std::process::exit;

use client::client_socket_stream;

use pico_args::Arguments;

const HELP: &str = r#"
DISPATCH: CLIENT

USAGE:
  client --path [FILE_DESCRIPTOR]  ...

  Path may be a file descriptor, handle, or socket. 
  To send input via stdin, set the path to "-"

OPTIONS:
  --server_addr [SOCKET_ADDR] downstream UDP server address.
                              can be repeated for multiple servers


  e.g.
  client --path /dev/random --server_addr 127.0.0.1:9920 --server_addr [::1]:9921
  client --path - --server_addr 224.0.0.1:9922 --server_addr [ff02::1]:9923 --tee >> logfile.log

FLAGS:
  -h, --help    Prints help information
  -t, --tee     Copy input to stdout

"#;

/// command line arguments
struct ClientArgs {
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
        server_addrs: pargs.values_from_str("--server_addr")?,
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
    let _ = client_socket_stream(&args.path, args.server_addrs, args.tee);
}
