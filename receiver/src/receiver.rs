use std::ffi::OsStr;
use std::io::{stdout, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::path::PathBuf;
use std::process::exit;
use std::thread::{spawn, Builder, JoinHandle};
use std::time::{SystemTime, UNIX_EPOCH};

// local
use aisdb_lib::db::{get_db_conn, prepare_tx_dynamic, prepare_tx_static};
use aisdb_lib::decode::VesselData;
use mproxy_client::target_socket_interface;
use mproxy_forward::proxy_tcp_udp;
use mproxy_reverse::{reverse_proxy_tcp_udp, reverse_proxy_udp};
use mproxy_server::upstream_socket_interface;
use mproxy_socket_dispatch::BUFSIZE;

// external
use nmea_parser::{NmeaParser, ParsedMessage};
use pico_args::Arguments;
use serde::{Deserialize, Serialize};
use serde_json::to_string;
use tungstenite::{accept, Message};

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
    let msg_txt = &String::from_utf8_lossy(&buf[0..i]);
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

/// Spawn decoder thread. Accepts input from a UDP socket.
/// Copies raw input to multicast_addr_rawdata (can be any UDP socket address).
/// Decodes AIS messages, then filters for static and dynamic message types
/// Resulting processed data will then optionally be saved to an
/// SQLite database at dbpath, and sent to outgoing websocket
/// clients in JSON format.
fn decode_multicast(
    udp_listen_addr: String,
    multicast_addr_parsed: Option<String>,
    multicast_addr_rawdata: Option<String>,
    tee: bool,
    dbpath: Option<PathBuf>,
    dynamic_msg_buffsize: usize,
    static_msg_bufsize: usize,
) -> JoinHandle<()> {
    let (_udp_listen_addr, listen_socket) = upstream_socket_interface(udp_listen_addr).unwrap();

    // downstream UDP multicast channel for raw data
    let mut target_raw: Option<(SocketAddr, UdpSocket)> = None;
    if let Some(rawaddr) = multicast_addr_rawdata {
        //target_raw = Some(new_downstream_socket(&rawaddr));
        target_raw = Some(target_socket_interface(&rawaddr).unwrap());
    }

    // downstream multicast channel for parsed data
    let target_parsed = multicast_addr_parsed
        .map(|parsedaddr| target_socket_interface(&parsedaddr).expect("binding socket interface"));

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
                // do database insert
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

                // forward raw + parsed downstream via UDP channels
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
                            if let Some((addr_raw, socket_raw)) = &target_raw {
                                socket_raw
                                    .send_to(&buf[0..c], addr_raw)
                                    .expect("sending to UDP listener via multicast");
                            }

                            if let Some((addr_parsed, socket_parsed)) = &target_parsed {
                                socket_parsed
                                    .send_to(msg.as_bytes(), addr_parsed)
                                    .expect("sending to websocket via UDP multicast socket");
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
/// Accepts websocket connections from downstream clients.
/// Listens for incoming UDP multicast packets, and forward
/// packets downstream to connected clients
fn handle_websocket_client(downstream: TcpStream, multicast_addr: String) -> JoinHandle<()> {
    spawn(move || {
        let (_multicast_addr, multicast_socket) =
            target_socket_interface(&multicast_addr).expect("binding socket");

        let mut buf = [0u8; 32768];
        let mut websocket =
            accept(downstream).expect("accepting websocket connection from downstream");

        loop {
            match multicast_socket.recv_from(&mut buf[0..]) {
                Ok((count_input, remote_addr)) => {
                    if let Err(e) = websocket.write_message(Message::Text(
                        String::from_utf8(buf[0..count_input].to_vec()).unwrap(),
                    )) {
                        eprintln!("dropping client: {} {}", remote_addr, e);
                        return;
                    }
                }
                Err(err) => {
                    eprintln!("stream_server upstream: got an error: {}", err);
                    return;
                }
            }
        }
    })
}

/// bind websocket to tcp_output_addr
/// listens on a UDP channel and forwards to connected websocket clients
fn listen_websocket_clients(udp_input_addr: String, tcp_output_addr: String) -> JoinHandle<()> {
    spawn(move || {
        println!("spawning websocket listener on {}/tcp", tcp_output_addr);
        let listener = match TcpListener::bind(tcp_output_addr) {
            Ok(l) => l,
            Err(e) => panic!("{:?}", e.raw_os_error()),
        };
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    handle_websocket_client(stream, udp_input_addr.clone());
                }
                Err(e) => {
                    eprintln!("{:?}", e.raw_os_error());
                }
            }
        }
    })
}

pub fn start_receiver(
    dbpath: Option<PathBuf>,
    tcp_connect_addr: Option<String>,
    tcp_listen_addr: Option<String>,
    udp_listen_addr: String,
    multicast_addr_parsed: Option<String>,
    multicast_addr_rawdata: Option<String>,
    tcp_output_addr: Option<String>,
    udp_output_addr: Option<String>,
    dynamic_msg_bufsize: Option<usize>,
    static_msg_bufsize: Option<usize>,
    tee: bool,
) -> Vec<JoinHandle<()>> {
    let mut threads: Vec<JoinHandle<()>> = vec![];

    // multicast rx/tx thread
    threads.push(decode_multicast(
        udp_listen_addr.clone(),
        multicast_addr_parsed.clone(),
        multicast_addr_rawdata.clone(),
        tee,
        dbpath,
        dynamic_msg_bufsize.unwrap_or(256),
        static_msg_bufsize.unwrap_or(64),
    ));

    // forward upstream TCP to the UDP input channel
    // TODO: SSL
    if let Some(tcpconn) = tcp_connect_addr {
        threads.push(proxy_tcp_udp(tcpconn, udp_listen_addr.clone()));
    }

    // listen for inbound TCP connections and forward to UDP input
    if let Some(tcpaddr) = tcp_listen_addr {
        threads.push(reverse_proxy_tcp_udp(tcpaddr, udp_listen_addr));
    }

    // bind UDP output socket to send raw input from multicast channel
    if let (Some(udpout), Some(multicast_raw)) = (udp_output_addr, multicast_addr_rawdata) {
        threads.push(reverse_proxy_udp(multicast_raw, udpout));
    }

    // parsed JSON output via websocket
    if let (Some(tcpout), Some(multicast_parsed)) = (tcp_output_addr, multicast_addr_parsed) {
        threads.push(listen_websocket_clients(multicast_parsed, tcpout));
    }

    threads
}

struct ReceiverArgs {
    dbpath: Option<PathBuf>,
    tcp_connect_addr: Option<String>,
    tcp_listen_addr: Option<String>,
    udp_listen_addr: String,
    multicast_addr_parsed: Option<String>,
    multicast_addr_rawdata: Option<String>,
    tcp_output_addr: Option<String>,
    udp_output_addr: Option<String>,
    dynamic_msg_bufsize: Option<usize>,
    static_msg_bufsize: Option<usize>,
    tee: bool,
}

fn parse_args() -> Result<ReceiverArgs, pico_args::Error> {
    let mut pargs = Arguments::from_env();
    if pargs.contains(["-h", "--help"]) || pargs.clone().finish().is_empty() {
        //print!("{}", HELP);
        exit(0);
    }
    fn parse_path(s: &OsStr) -> Result<PathBuf, &'static str> {
        Ok(s.into())
    }

    let args = ReceiverArgs {
        dbpath: pargs.opt_value_from_os_str("--path", parse_path)?,
        tcp_connect_addr: pargs.opt_value_from_str("--tcp-connect-addr")?,
        tcp_listen_addr: pargs.opt_value_from_str("--tcp-listen-addr")?,
        udp_listen_addr: pargs.value_from_str("--udp-listen-addr")?,
        multicast_addr_parsed: pargs.opt_value_from_str("--multicast-addr-parsed")?,
        multicast_addr_rawdata: pargs.opt_value_from_str("--multicast-addr-rawdata")?,
        tcp_output_addr: pargs.opt_value_from_str("--tcp-output-addr")?,
        udp_output_addr: pargs.opt_value_from_str("--udp-output-addr")?,
        dynamic_msg_bufsize: pargs
            .opt_value_from_str("--dynamic-msg-bufsize")?
            .map(|s: String| s.parse().unwrap()),
        static_msg_bufsize: pargs
            .opt_value_from_str("--static-msg-bufsize")?
            .map(|s: String| s.parse().unwrap()),
        tee: pargs.contains(["-t", "--tee"]),
    };

    let remaining = pargs.finish();
    if !remaining.is_empty() {
        println!("Warning: unused arguments {:?}", remaining)
    }

    Ok(args)
}

pub fn main() {
    let args = parse_args().unwrap();

    let threads = start_receiver(
        args.dbpath,
        args.tcp_connect_addr,
        args.tcp_listen_addr,
        args.udp_listen_addr,
        args.multicast_addr_parsed,
        args.multicast_addr_rawdata,
        args.tcp_output_addr,
        args.udp_output_addr,
        args.dynamic_msg_bufsize,
        args.static_msg_bufsize,
        args.tee,
    );
    for thread in threads {
        thread.join().unwrap();
    }
}
