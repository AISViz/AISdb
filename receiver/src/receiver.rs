use std::ffi::OsStr;
use std::io::{stdout, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::path::PathBuf;
use std::process::exit;
use std::thread::{spawn, Builder, JoinHandle};
use std::time::{SystemTime, UNIX_EPOCH};

use aisdb_lib::db::{
    get_db_conn, get_postgresdb_conn, postgres_prepare_tx_dynamic, postgres_prepare_tx_static,
    sqlite_prepare_tx_dynamic, sqlite_prepare_tx_static,
};
use aisdb_lib::db::{PGClient, SqliteConnection};
use aisdb_lib::decode::VesselData;

// external
use mproxy_client::target_socket_interface;
use mproxy_forward::proxy_tcp_udp;
use mproxy_reverse::{reverse_proxy_tcp_udp, reverse_proxy_udp};
use mproxy_server::upstream_socket_interface;
use nmea_parser::{NmeaParser, ParsedMessage};
use pico_args::Arguments;
use serde::{Deserialize, Serialize};
use serde_json::to_string;
use tungstenite::{accept, Message};

const BUFSIZE: usize = 8096;

// need to redefine these structs from nmea_parser to allow serde to deserialize them
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

#[derive(Serialize, Deserialize)]
struct VesselStaticPing {
    mmsi: u32,
    imo: u32,
    vessel_name: String,
    ship_type: u8,
    dim_bow: u16,
    dim_stern: u16,
    dim_port: u16,
    dim_star: u16,
    draught: u8,
}

#[derive(Serialize)]
enum VesselPing {
    Dynamic(VesselPositionPing),
    Static(VesselStaticPing),
}

impl From<VesselStaticPing> for VesselPing {
    fn from(p: VesselStaticPing) -> Self {
        VesselPing::Static(p)
    }
}
impl From<VesselPositionPing> for VesselPing {
    fn from(p: VesselPositionPing) -> Self {
        VesselPing::Dynamic(p)
    }
}

impl From<VesselPing> for VesselData {
    fn from(p: VesselPing) -> Self {
        p.into()
    }
}

fn epoch_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn default_udp_listen_addr() -> String {
    "0.0.0.0:9921".to_string()
}

#[derive(Clone, Debug)]
pub struct ReceiverArgs {
    pub sqlite_dbpath: Option<PathBuf>,
    pub postgres_connection_string: Option<String>,
    pub tcp_connect_addr: Option<String>,
    pub tcp_listen_addr: Option<String>,
    pub udp_listen_addr: Option<String>,
    pub multicast_addr_parsed: Option<String>,
    pub multicast_addr_rawdata: Option<String>,
    pub tcp_output_addr: Option<String>,
    pub udp_output_addr: Option<String>,
    pub dynamic_msg_bufsize: Option<usize>,
    pub static_msg_bufsize: Option<usize>,
    pub tee: Option<bool>,
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
        sqlite_dbpath: pargs.opt_value_from_os_str("--path", parse_path)?,
        postgres_connection_string: pargs.opt_value_from_str("--postgres-connect")?,
        tcp_connect_addr: pargs.opt_value_from_str("--tcp-connect-addr")?,
        tcp_listen_addr: pargs.opt_value_from_str("--tcp-listen-addr")?,
        udp_listen_addr: pargs.opt_value_from_str("--udp-listen-addr")?,
        multicast_addr_parsed: pargs.opt_value_from_str("--multicast-addr-parsed")?,
        multicast_addr_rawdata: pargs.opt_value_from_str("--multicast-addr-rawdata")?,
        tcp_output_addr: pargs.opt_value_from_str("--tcp-output-addr")?,
        udp_output_addr: pargs.opt_value_from_str("--udp-output-addr")?,
        dynamic_msg_bufsize: pargs.opt_value_from_str("--dynamic-msg-bufsize")?,
        static_msg_bufsize: pargs.opt_value_from_str("--static-msg-bufsize")?,
        tee: Some(pargs.contains(["-t", "--tee"])),
    };

    let remaining = pargs.finish();
    if !remaining.is_empty() {
        println!("Warning: unused arguments {:?}", remaining)
    }

    Ok(args)
}

/// Filters incoming UDP messages for vessel dynamic and static messages
fn parse_filter_vesseldata(
    sentence: &str,
    parser: &mut NmeaParser,
) -> Option<(VesselPing, VesselData)> {
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
                time: epoch_time(),
                heading: vdd.heading_true.unwrap_or(-1.),
                lon: vdd.longitude.unwrap(),
                lat: vdd.latitude.unwrap(),
                rot: vdd.rot.unwrap_or(-1.),
                sog: vdd.sog_knots.unwrap_or(-1.),
            };

            let insert_msg = VesselData {
                epoch: Some(ping.time as i32),
                payload: Some(ParsedMessage::VesselDynamicData(vdd)),
            };
            Some((VesselPing::from(ping), insert_msg))
        }
        ParsedMessage::VesselStaticData(vsd) => {
            let insert_msg = VesselData {
                epoch: Some(epoch_time() as i32),
                payload: Some(ParsedMessage::VesselStaticData(vsd.clone())),
            };
            let static_ping = VesselStaticPing {
                mmsi: vsd.mmsi,
                imo: vsd.imo_number.unwrap_or_default(),
                vessel_name: vsd.name.unwrap_or("".to_string()),
                ship_type: vsd.ship_type.to_value(),
                dim_bow: vsd.dimension_to_bow.unwrap_or_default(),
                dim_stern: vsd.dimension_to_stern.unwrap_or_default(),
                dim_port: vsd.dimension_to_port.unwrap_or_default(),
                dim_star: vsd.dimension_to_starboard.unwrap_or_default(),
                draught: vsd.draught10.unwrap_or_default(),
            };
            Some((VesselPing::from(static_ping), insert_msg))
        }
        _other => None,
    }
}

/// Generate utf-8 strings from buffered bytestream, and split on line breaks.
/// Segmented strings will be parsed as AIS, and filtered to dynamic and
/// static messages
fn split_parse_filter_msgs(
    buf: &[u8],
    i: usize,
    parser: &mut NmeaParser,
) -> Vec<(VesselPing, VesselData)> {
    let msg_txt = &String::from_utf8(buf[0..i].to_vec()).unwrap();
    let mut msgs = vec![];
    for raw in msg_txt.split("\r\n") {
        if raw.is_empty() {
            continue;
        }
        if let Some((ping, vdata)) = parse_filter_vesseldata(raw, parser) {
            msgs.push((ping, vdata));
        }
    }
    msgs
}

fn serialize_static_buffer(
    sqlite_dbconn: &mut Option<SqliteConnection>,
    postgres_dbconn: &mut Option<PGClient>,
    static_msgs: Vec<VesselData>,
) -> Result<(), Box<dyn std::error::Error>> {
    //#[cfg(debug_assertions)]
    println!("Inserting {} static messages ...", static_msgs.len());
    if let Some(ref mut conn) = sqlite_dbconn {
        if let Err(e) = sqlite_prepare_tx_static(conn, "rx", static_msgs.to_vec()) {
            eprintln!("Error inserting vessel static data: {}", e);
        }
    }
    if let Some(ref mut conn) = postgres_dbconn {
        if let Err(e) = postgres_prepare_tx_static(conn, "rx", static_msgs) {
            eprintln!("Error inserting vessel static data: {}", e);
        }
    }
    Ok(())
}

fn serialize_dynamic_buffer(
    sqlite_dbconn: &mut Option<SqliteConnection>,
    postgres_dbconn: &mut Option<PGClient>,
    dynamic_msgs: Vec<VesselData>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Inserting {} dynamic messages ...", dynamic_msgs.len());
    if let Some(ref mut conn) = sqlite_dbconn {
        if let Err(e) = sqlite_prepare_tx_dynamic(conn, "rx", dynamic_msgs.to_vec()) {
            eprintln!("Error inserting vessel dynamic data: {}", e);
        }
    }
    if let Some(ref mut conn) = postgres_dbconn {
        if let Err(e) = postgres_prepare_tx_dynamic(conn, "rx", dynamic_msgs) {
            eprintln!("Error inserting vessel dynamic data: {}", e);
        }
    }
    Ok(())
}

/// Spawn decoder thread. Accepts input from a UDP socket.
/// Copies raw input to multicast_addr_rawdata (can be any UDP socket address).
/// Decodes AIS messages, then filters for static and dynamic message types
/// Resulting processed data will then optionally be saved to an
/// SQLite database at dbpath, and sent to outgoing websocket
/// clients in JSON format.
fn decode_multicast(args: ReceiverArgs) -> JoinHandle<()> {
    println!(
        "Decoding messages incoming on {}",
        args.udp_listen_addr
            .clone()
            .unwrap_or(default_udp_listen_addr())
    );
    let (_udp_listen_addr, listen_socket) = upstream_socket_interface(
        args.udp_listen_addr
            .clone()
            .unwrap_or(default_udp_listen_addr()),
    )
    .unwrap();

    // downstream UDP multicast channel for raw data
    let mut target_raw: Option<(SocketAddr, UdpSocket)> = None;
    if let Some(rawaddr) = &args.multicast_addr_rawdata {
        println!(
            "copying raw input: {} UDP -> {} UDP",
            _udp_listen_addr, rawaddr,
        );
        //target_raw = Some(new_downstream_socket(&rawaddr));
        target_raw = Some(target_socket_interface(rawaddr).unwrap());
    }

    // downstream multicast channel for parsed data
    let target_parsed = args.multicast_addr_parsed.map(|parsedaddr| {
        println!("copying parsed output to {} UDP", parsedaddr);
        target_socket_interface(&parsedaddr).expect("binding socket interface")
    });

    let mut buf = [0u8; BUFSIZE];
    let mut parser = NmeaParser::new();
    let mut output_buffer = BufWriter::new(stdout());
    let mut sqlite_dbconn = args
        .sqlite_dbpath
        .map(|dbp| get_db_conn(dbp).expect("getting sqlite db connection"));
    let mut postgres_dbconn = args
        .postgres_connection_string
        .as_ref()
        .map(|s| get_postgresdb_conn(s).expect("getting postgres db connection"));
    let max_dynamic = args.dynamic_msg_bufsize.unwrap_or(256);
    let max_static = args.static_msg_bufsize.unwrap_or(32);

    Builder::new()
        .name(format!("{:#?}", listen_socket))
        .spawn(move || {
            let mut dynamic_msgs: Vec<VesselData> = vec![];
            let mut static_msgs: Vec<VesselData> = vec![];
            println!(
                "Spawning receiver. Dynamic msgs buffer: {}\t Static msgs buffer: {}",
                max_dynamic, max_static
            );

            listen_socket.set_broadcast(true).unwrap();
            loop {
                #[cfg(debug_assertions)]
                println!(
                    "dynamic messages: {}\tstatic messages: {}",
                    dynamic_msgs.len(),
                    static_msgs.len()
                );
                if dynamic_msgs.len() >= max_dynamic {
                    serialize_dynamic_buffer(
                        &mut sqlite_dbconn,
                        &mut postgres_dbconn,
                        dynamic_msgs,
                    )
                    .unwrap();
                    dynamic_msgs = vec![];
                } else if static_msgs.len() >= max_static {
                    serialize_static_buffer(&mut sqlite_dbconn, &mut postgres_dbconn, static_msgs)
                        .unwrap();
                    static_msgs = vec![];
                }

                // forward raw + parsed downstream via UDP channels
                match listen_socket.recv_from(&mut buf[0..]) {
                    Ok((c, _remote_addr)) => {
                        for (msg, raw) in split_parse_filter_msgs(&buf, c, &mut parser) {
                            match &msg {
                                VesselPing::Dynamic(_m) => {
                                    dynamic_msgs.push(raw);
                                }
                                VesselPing::Static(_m) => {
                                    static_msgs.push(raw);
                                }
                            }
                            if let Some((addr_raw, socket_raw)) = &target_raw {
                                socket_raw
                                    .send_to(&buf[0..c], addr_raw)
                                    .expect("sending to UDP listener via multicast");
                            }

                            if let Some((addr_parsed, socket_parsed)) = &target_parsed {
                                match msg {
                                    VesselPing::Dynamic(m) => {
                                        let vdata: VesselPositionPing = m;
                                        socket_parsed
                                            .send_to(
                                                to_string(&vdata).unwrap().as_bytes(),
                                                addr_parsed,
                                            )
                                            .expect(
                                                "sending to websocket via UDP multicast socket",
                                            );
                                    }
                                    VesselPing::Static(m) => {
                                        let vdata: VesselStaticPing = m;
                                        socket_parsed
                                            .send_to(
                                                to_string(&vdata).unwrap().as_bytes(),
                                                addr_parsed,
                                            )
                                            .expect(
                                                "sending to websocket via UDP multicast socket",
                                            );
                                    }
                                }
                            }
                        }
                        if args.tee.is_some() && args.tee.unwrap() {
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
fn handle_websocket_client(
    downstream: TcpStream,
    multicast_addr: String,
    tee: bool,
) -> JoinHandle<()> {
    let mut output_buffer = BufWriter::new(stdout());

    spawn(move || {
        let (_multicast_addr, multicast_socket) =
            upstream_socket_interface(multicast_addr).expect("binding socket");

        println!(
            "forwarding: {} UDP -> {:?} WEBSOCKET",
            _multicast_addr,
            downstream.peer_addr().unwrap()
        );

        let mut buf = [0u8; 32768];
        downstream.set_nodelay(true).unwrap();
        let mut websocket =
            accept(downstream).expect("accepting websocket connection from downstream");

        loop {
            match multicast_socket.recv_from(&mut buf[0..]) {
                Ok((count_input, remote_addr)) => {
                    //#[cfg(debug_assertions)]
                    //println!("RX: {}", String::from_utf8_lossy(&buf[0..count_input]));
                    if let Err(e) = websocket.write_message(Message::Text(
                        String::from_utf8(buf[0..count_input].to_vec()).unwrap(),
                    )) {
                        eprintln!("dropping client: {} {}", remote_addr, e);
                        return;
                    }
                    if tee {
                        let _o = output_buffer
                            .write(&buf[0..count_input])
                            .expect("writing to output buffer");
                        output_buffer.flush().unwrap();
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
fn listen_websocket_clients(
    udp_input_addr: String,
    tcp_output_addr: String,
    tee_parsed: bool,
) -> JoinHandle<()> {
    spawn(move || {
        println!("spawning websocket listener on {}/tcp", tcp_output_addr);
        let listener = match TcpListener::bind(tcp_output_addr) {
            Ok(l) => l,
            Err(e) => panic!("{:?}", e.raw_os_error()),
        };
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    handle_websocket_client(stream, udp_input_addr.clone(), tee_parsed);
                }
                Err(e) => {
                    eprintln!("{:?}", e.raw_os_error());
                }
            }
        }
    })
}

pub fn start_receiver(args: ReceiverArgs) -> Vec<JoinHandle<()>> {
    #[cfg(debug_assertions)]
    println!("starting receiver with arguments: {:#?}", args);

    let mut threads: Vec<JoinHandle<()>> = vec![];

    // multicast rx/tx thread
    threads.push(decode_multicast(args.clone()));

    // forward upstream TCP to the UDP input channel
    // TODO: SSL
    if let Some(tcpconn) = args.tcp_connect_addr {
        threads.push(proxy_tcp_udp(
            tcpconn,
            args.udp_listen_addr
                .clone()
                .unwrap_or(default_udp_listen_addr()),
        ));
    }

    // listen for inbound TCP connections and forward to UDP input
    if let Some(tcpaddr) = args.tcp_listen_addr {
        threads.push(reverse_proxy_tcp_udp(
            tcpaddr,
            args.udp_listen_addr.unwrap_or(default_udp_listen_addr()),
        ));
    }

    // bind UDP output socket to send raw input from multicast channel
    if let (Some(udpout), Some(multicast_raw)) = (args.udp_output_addr, args.multicast_addr_rawdata)
    {
        threads.push(reverse_proxy_udp(multicast_raw, udpout));
    }

    // parsed JSON output via websocket
    if let (Some(tcpout), Some(multicast_parsed)) =
        (args.tcp_output_addr, args.multicast_addr_parsed)
    {
        #[cfg(not(debug_assertions))]
        let tee_parsed = false;
        #[cfg(debug_assertions)]
        let tee_parsed = args.tee.is_some() && args.tee.unwrap();

        threads.push(listen_websocket_clients(
            multicast_parsed,
            tcpout,
            tee_parsed,
        ));
    }

    threads
}

pub fn main() {
    let args = parse_args().unwrap();

    let threads = start_receiver(args);

    for thread in threads {
        thread.join().unwrap();
    }
}

#[test]
fn test_receiver() {
    let args: ReceiverArgs = ReceiverArgs {
        dynamic_msg_bufsize: None,
        multicast_addr_parsed: None,
        multicast_addr_rawdata: None,
        postgres_connection_string: None,
        sqlite_dbpath: Some(PathBuf::from("./test_receiver.db")),
        static_msg_bufsize: None,
        tcp_connect_addr: None,
        tcp_listen_addr: None,
        tcp_output_addr: None,
        tee: None,
        udp_listen_addr: Some(default_udp_listen_addr()),
        udp_output_addr: None,
    };
    let threads = start_receiver(args);

    std::thread::sleep(std::time::Duration::from_millis(1000));

    for thread in threads {
        //thread.join().unwrap();
        assert!(!thread.is_finished());
    }

    std::fs::remove_file("./test_receiver.db").unwrap();
}
