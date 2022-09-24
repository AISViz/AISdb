use std::io::{stdout, BufWriter, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::thread::{spawn, Builder, JoinHandle};
use std::time::{SystemTime, UNIX_EPOCH};

extern crate proxy;
use proxy::new_downstream_socket;
use proxy::new_listen_socket;

extern crate server;
use server::join_multicast;

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

struct ReverseProxyArgs {
    udp_listen_addr: String,
    multicast_addr: String,
    tcp_listen_addr: String,
    tee: bool,
}

fn filter_vesseldata(sentence: &str, parser: &mut NmeaParser) -> Option<String> {
    match parser.parse_sentence(sentence).ok()? {
        ParsedMessage::VesselDynamicData(vdd) => {
            let ping = VesselPositionPing {
                mmsi: vdd.mmsi,
                lon: (vdd.longitude.unwrap() * 1000000.0).round() / 1000000.0,
                lat: (vdd.latitude.unwrap() * 1000000.0).round() / 1000000.0,
                time: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                rot: (vdd.rot.unwrap_or(-1.) * 1000.0).round() / 1000.0,
                sog: (vdd.sog_knots.unwrap_or(-1.) * 1000.0).round() / 1000.0,
                heading: vdd.heading_true.unwrap_or(-1.),
            };

            let msg = to_string(&ping).unwrap();
            Some(msg)
        }
        _ => None,
    }
}

fn process_message(buf: &[u8], i: usize, parser: &mut NmeaParser) -> Vec<String> {
    let msg_txt = &String::from_utf8(buf[0..i].to_vec()).unwrap();
    let mut msgs = vec![];
    for msg in msg_txt.split("\r\n") {
        if msg.is_empty() {
            continue;
        }
        if let Some(txt) = filter_vesseldata(msg, parser) {
            msgs.push(txt);
        }
    }
    msgs
}

pub fn decode_multicast(
    listen_addr: &String,
    multicast_addr: &String,
    tee: bool,
) -> JoinHandle<()> {
    let listen_socket = new_listen_socket(listen_addr);
    let (target_addr, target_socket) = new_downstream_socket(multicast_addr);

    let mut buf = [0u8; 32768];
    let mut parser = NmeaParser::new();
    let mut output_buffer = BufWriter::new(stdout());

    Builder::new()
        .name(format!("{:#?}", listen_socket))
        .spawn(move || {
            //listen_socket.read_timeout().unwrap();
            listen_socket.set_broadcast(true).unwrap();
            loop {
                match listen_socket.recv_from(&mut buf[0..]) {
                    Ok((c, _remote_addr)) => {
                        for msg in process_message(&buf, c, &mut parser) {
                            target_socket
                                .send_to(msg.as_bytes(), &target_addr)
                                .expect("sending to server socket");
                            if tee {
                                let _o = output_buffer
                                    .write(&buf[0..c])
                                    .expect("writing to output buffer");
                                output_buffer.flush().unwrap();
                            }
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

pub fn handle_client(downstream: &TcpStream, multicast_addr: String) {
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
    /*
    if msg.is_binary() || msg.is_text() { websocket.write_message(msg).unwrap(); } }
    */

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

fn main() {
    let args = ReverseProxyArgs {
        udp_listen_addr: "[::]:9921".into(),
        tcp_listen_addr: "[::]:9920".into(),
        multicast_addr: "224.0.0.20:9919".into(),
        tee: true,
    };

    let _multicast = decode_multicast(
        &args.udp_listen_addr,
        &args.multicast_addr.clone(),
        args.tee,
    );
    let listener = TcpListener::bind(args.tcp_listen_addr).unwrap();
    for stream in listener.incoming() {
        let multicast_addr = args.multicast_addr.clone();
        spawn(move || {
            handle_client(&stream.unwrap(), multicast_addr);
        });
    }
}
