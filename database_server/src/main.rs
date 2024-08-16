#![feature(generators, generator_trait)]

use std::net::TcpListener;
pub use std::ops::{Generator, GeneratorState, IndexMut};
use std::path::Path;
use std::thread::spawn;

use aisdb_db_server::aisdb_db_server::handle_client;
use aisdb_lib::db::{get_postgresdb_conn, sql_from_file};

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    // database connection config
    let pghost = std::env::var("PGHOST").unwrap_or("[fc00::9]".to_string());
    let pguser = std::env::var("PGUSER").unwrap_or("postgres".to_string());
    let pgport = std::env::var("PGPORT").unwrap_or("5432".to_string());

    // read the database password from secret file
    let default_pgpassfile = Path::join(
        std::env::home_dir()
            .expect("Could not get home directory for .pgpass file")
            .as_path(),
        &Path::new(".pgpass"),
    )
    .to_str()
    .unwrap()
    .to_string();
    let pgpassfile = std::env::var("PGPASSFILE").unwrap_or(default_pgpassfile);
    let mut pgpass = std::fs::read_to_string(Path::new(&pgpassfile))
        .unwrap_or_else(|e| panic!("Reading {}: {}", pgpassfile, e));
    if pgpass.ends_with('\n') {
        pgpass.pop();
        if pgpass.ends_with('\r') {
            pgpass.pop();
        }
    }

    let postgres_connection_string =
        format!("postgresql://{}:{}@{}:{}", pguser, pgpass, pghost, pgport);

    println!(
        "initializing postgres connection to: postgresql://{}:******@{}:{}",
        pguser, pghost, pgport
    );

    // server config
    let allow_clients = std::env::var("AISDBHOSTALLOW").unwrap_or("[::]".to_string());
    let listen_port = std::env::var("AISDBPORT").unwrap_or("9924".to_string());
    let tcp_listen_address = format!("{}:{}", allow_clients, listen_port);
    let listener = TcpListener::bind(tcp_listen_address.clone())
        .unwrap_or_else(|_| panic!("Binding address {}", tcp_listen_address));

    // create database tables if necessary
    let mut pg_main = get_postgresdb_conn(&postgres_connection_string)?;
    pg_main.batch_execute(sql_from_file("createtable_webdata_marinetraffic.sql"))?;
    pg_main.close()?;

    println!(
        "listening on {} for incoming API connections...",
        tcp_listen_address
    );

    // spawn a thread to handle new clients
    for client in listener.incoming() {
        match client {
            Ok(client) => {
                let conn_str = postgres_connection_string.clone();
                spawn(move || {
                    let mut pg = get_postgresdb_conn(&conn_str).unwrap();
                    let handle = handle_client(client, &mut pg);
                    match handle {
                        Err(e) => {
                            eprintln!("error processing client request: {}", e)
                        }
                        Ok(_) => {
                            println!("ended client loop")
                        }
                    }
                    pg.close()
                });
            }
            Err(mut e) => {
                eprintln!("{:#?} {:#?}", e.raw_os_error(), e.get_mut());
            }
        }
    }
    Ok(())
}
