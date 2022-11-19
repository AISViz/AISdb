use std::io::{stdout, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs, UdpSocket};
use std::path::PathBuf;
use std::thread::{spawn, Builder, JoinHandle};
use std::time::{SystemTime, UNIX_EPOCH};

// local path external

extern crate socket_dispatch;
use socket_dispatch::BUFSIZE;

extern crate proxy;
use proxy::new_downstream_socket;
use proxy::new_listen_socket;

extern crate server;
use server::join_multicast;

extern crate aisdb_lib;
use aisdb_lib::db::{get_db_conn, prepare_tx_dynamic, prepare_tx_static};
use aisdb_lib::decode::VesselData;

// external

extern crate nmea_parser;
use nmea_parser::{NmeaParser, ParsedMessage};

extern crate tungstenite;
use tungstenite::{accept, Message};

extern crate serde;
use serde::{Deserialize, Serialize};

extern crate serde_json;
use serde_json::to_string;

#[derive(Serialize, Deserialize)]
struct VesselPositionPing {
    mmsi: u32,
    lon: f64,
    lat: f64,
    time: u64,
    rot: f64,
    sog: f64,
    heading: f64,
}

fn epoch_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Filters incoming UDP messages for vessel dynamic and static messages
/// inserts static and dynamic messages into database,
/// and returns VesselPositionPing for dynamic data
fn filter_insert_vesseldata(
    sentence: &str,
    parser: &mut NmeaParser,
    insert_db: bool,
    dynamic_msgs: &mut Vec<VesselData>,
    static_msgs: &mut Vec<VesselData>,
) -> Option<String> {
    match parser.parse_sentence(sentence).ok()? {
        ParsedMessage::VesselDynamicData(vdd) => {
            if vdd.longitude.is_none()
                || vdd.latitude.is_none()
                || vdd.longitude == Some(0.)
                || vdd.latitude == Some(0.)
            {
                return None;
            }

            let ping = VesselPositionPing {
                mmsi: vdd.mmsi,
                lon: (vdd.longitude.unwrap() * 1000000.0).round() / 1000000.0,
                lat: (vdd.latitude.unwrap() * 1000000.0).round() / 1000000.0,
                time: epoch_time(),
                rot: (vdd.rot.unwrap_or(-1.) * 1000.0).round() / 1000.0,
                sog: (vdd.sog_knots.unwrap_or(-1.) * 1000.0).round() / 1000.0,
                heading: vdd.heading_true.unwrap_or(-1.),
            };

            if insert_db {
                let insert_msg = VesselData {
                    epoch: Some(ping.time as i32),
                    payload: Some(ParsedMessage::VesselDynamicData(vdd)),
                };
                dynamic_msgs.push(insert_msg);
            }

            let msg = to_string(&ping).unwrap();
            Some(msg)
        }
        ParsedMessage::VesselStaticData(vsd) => {
            if insert_db {
                let insert_msg = VesselData {
                    epoch: Some(epoch_time() as i32),
                    payload: Some(ParsedMessage::VesselStaticData(vsd)),
                };
                static_msgs.push(insert_msg);
            }
            //
            None
        }
        _ => None,
    }
}

/// Generate utf-8 strings from buffered bytestream, and split on line breaks.
/// Segmented strings will be parsed as AIS, and filtered to dynamic and
/// static messages
fn process_message(
    buf: &[u8],
    i: usize,
    parser: &mut NmeaParser,
    //dbconn: &mut Option<Connection>,
    insert_db: bool,
    dynamic_msgs: &mut Vec<VesselData>,
    static_msgs: &mut Vec<VesselData>,
) -> Vec<String> {
    let msg_txt = &String::from_utf8(buf[0..i].to_vec()).unwrap();
    let mut msgs = vec![];
    for msg in msg_txt.split("\r\n") {
        if msg.is_empty() {
            continue;
        }
        if let Some(txt) =
            filter_insert_vesseldata(msg, parser, insert_db, dynamic_msgs, static_msgs)
        {
            msgs.push(txt);
        }
    }
    msgs
}

/// UDP multicast server
/// listens for packets on a UDP socket, and forward to
/// local multicast address
fn decode_multicast(
    listen_addr: String,
    multicast_addr: String,
    multicast_rebroadcast: Option<String>,
    tee: bool,
    dbpath: Option<PathBuf>,
    dynamic_msg_buffsize: usize,
    static_msg_bufsize: usize,
) -> JoinHandle<()> {
    let listen_socket = new_listen_socket(&listen_addr);
    let (target_addr, target_socket) = new_downstream_socket(&multicast_addr);
    let mut target_raw: Option<(SocketAddr, UdpSocket)> = None;
    if let Some(rawaddr) = multicast_rebroadcast {
        target_raw = Some(new_downstream_socket(&rawaddr));
    }

    let mut buf = [0u8; BUFSIZE];
    let mut parser = NmeaParser::new();
    let mut output_buffer = BufWriter::new(stdout());

    let mut dbconn = dbpath.map(|dbp| get_db_conn(dbp.as_path()).expect("getting db conn"));

    Builder::new()
        .name(format!("{:#?}", listen_socket))
        .spawn(move || {
            let insert_db = dbconn.is_some();
            let mut dynamic_msgs: Vec<VesselData> = vec![];
            let mut static_msgs: Vec<VesselData> = vec![];
            //listen_socket.read_timeout().unwrap();
            listen_socket.set_broadcast(true).unwrap();
            loop {
                if let Some(ref mut conn) = dbconn {
                    if dynamic_msgs.len() > dynamic_msg_buffsize {
                        prepare_tx_dynamic(conn, "rx", dynamic_msgs)
                            .expect("inserting vessel dynamic data");
                        dynamic_msgs = vec![];
                    }
                    if static_msgs.len() > static_msg_bufsize {
                        prepare_tx_static(conn, "rx", static_msgs)
                            .expect("inserting vessel static data");
                        static_msgs = vec![];
                    }
                }
                match listen_socket.recv_from(&mut buf[0..]) {
                    Ok((c, _remote_addr)) => {
                        for msg in process_message(
                            &buf,
                            c,
                            &mut parser,
                            insert_db,
                            &mut dynamic_msgs,
                            &mut static_msgs,
                        ) {
                            target_socket
                                .send_to(msg.as_bytes(), target_addr)
                                .expect("sending to server socket");

                            if let Some((addr_raw, socket_raw)) = &target_raw {
                                socket_raw
                                    .send_to(&buf[0..c], addr_raw)
                                    .expect("sending to server socket");
                            }
                        }
                        if tee {
                            let _o = output_buffer
                                .write(&buf[0..c])
                                .expect("writing to output buffer");
                            output_buffer.flush().unwrap();
                        }
                    }
                    Err(err) => {
                        eprintln!("decode_multicast: got an error: {}", err);
                        #[cfg(debug_assertions)]
                        panic!("decode_multicast: got an error: {}", err);
                    }
                }
            }
        })
        .unwrap()
}

/// TCP server handler
/// Listens for incoming UDP multicast packets, and reverse-proxy
/// packets downstream to connected TCP clients
fn handle_client(downstream: &TcpStream, multicast_addr: String) {
    let multicast_addr = multicast_addr
        .to_socket_addrs()
        .unwrap()
        .next()
        .expect("parsing socket address");
    if !multicast_addr.ip().is_multicast() {
        panic!("not a multicast address {}", multicast_addr);
    }
    let multicast_socket = join_multicast(multicast_addr).unwrap_or_else(|e| {
        panic!("joining multicast socket {}", e);
    });

    let mut buf = [0u8; 32768];
    let mut websocket = accept(downstream).unwrap();

    loop {
        match multicast_socket.recv_from(&mut buf[0..]) {
            Ok((count_input, _remote_addr)) => {
                if let Err(e) = websocket.write_message(Message::Text(
                    String::from_utf8(buf[0..count_input].to_vec()).unwrap(),
                )) {
                    eprintln!("dropping client: {}", e);
                    return;
                }
            }
            Err(err) => {
                eprintln!("stream_server upstream: got an error: {}", err);
                return;
            }
        }
    }
}

pub fn start_receiver(
    dbpath: Option<&str>,
    udp_listen_addr: &str,
    tcp_listen_addr: &str,
    multicast_addr: &str,
    multicast_rebroadcast: Option<&str>,
    dynamic_msg_bufsize: Option<usize>,
    static_msg_bufsize: Option<usize>,
    tee: bool,
) {
    let dbpath: Option<PathBuf> = dbpath.map(PathBuf::from);
    let multicast_rebroadcast: Option<String> = multicast_rebroadcast.map(|rawaddr| rawaddr.into());

    // spawn multicast rx/tx thread
    let _multicast = decode_multicast(
        udp_listen_addr.into(),
        multicast_addr.into(),
        multicast_rebroadcast,
        tee,
        dbpath,
        dynamic_msg_bufsize.unwrap_or(256),
        static_msg_bufsize.unwrap_or(64),
    );

    // bind TCP listen address
    let listener = TcpListener::bind(tcp_listen_addr).unwrap();

    // handle TCP clients
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let multicast_addr: String = multicast_addr.into();
                spawn(move || {
                    handle_client(&stream, multicast_addr);
                });
            }
            Err(e) => {
                eprintln!("{:?}", e.raw_os_error());
            }
        }
    }
}

#[allow(dead_code)]
fn main() {
    // optionally save to database file
    let dbpath = Some("./ais_rx.db");

    // configure listening socket addresses
    let udp_listen_addr = "[::]:9921";
    let tcp_listen_addr = "[::]:9920";
    let multicast_addr = "224.0.0.20:9919";

    // if this is not None, raw input will be re-broadcasted here
    // can be used to pass input downstream to e.g. reverse proxy
    let multicast_rebroadcast = Some("[ff02::18]:9918");

    // number of messages received before database insert
    let dynamic_msg_bufsize = 128;
    let static_msg_bufsize = 64;

    // copy input to stdout
    let tee = false;

    start_receiver(
        dbpath,
        udp_listen_addr,
        tcp_listen_addr,
        multicast_addr,
        multicast_rebroadcast,
        Some(dynamic_msg_bufsize),
        Some(static_msg_bufsize),
        tee,
    )
}
