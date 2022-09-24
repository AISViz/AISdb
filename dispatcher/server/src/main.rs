use std::path::PathBuf;
use std::process::exit;
use std::str::FromStr;

extern crate pico_args;
use pico_args::Arguments;

pub mod lib;
pub use crate::lib::listener;

const HELP: &str = r#"
DISPATCH: SERVER

USAGE:
  server --path [OUTPUT_LOGFILE] --listen_addr [SOCKET_ADDR] ...

  e.g.
  server --path logfile.log --listen_addr 127.0.0.1:9920 --listen_addr [::1]:9921


FLAGS:
  -h, --help    Prints help information
  -t, --tee     Copy input to stdout

"#;

struct ServerArgs {
    listen_addr: Vec<String>,
    path: String,
    tee: bool,
}

fn parse_args() -> Result<ServerArgs, pico_args::Error> {
    let mut pargs = Arguments::from_env();
    if pargs.contains(["-h", "--help"]) || pargs.clone().finish().is_empty() {
        print!("{}", HELP);
        exit(0);
    }
    let tee = pargs.contains(["-t", "--tee"]);
    let args = ServerArgs {
        path: pargs.value_from_str("--path")?,
        listen_addr: pargs.values_from_str("--listen_addr")?,
        tee,
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
        threads.push(listener(
            hostname,
            PathBuf::from_str(&logpath).unwrap(),
            args.tee,
        ));
    }
    for thread in threads {
        thread.join().unwrap();
    }
}
