#![allow(unused_imports)]
#![allow(dead_code)]

use std::{
    fs::File,
    io::{BufRead, BufReader},
};

use nmea_parser::{
    ais::{VesselDynamicData, VesselStaticData},
    NmeaParser, ParsedMessage,
};

#[derive(Debug)]
pub struct VesselData {
    msg: String,
    payload: Option<ParsedMessage>,
    epoch: Option<i32>,
}

fn split_msg(line: String) -> Option<(String, i32)> {
    /* collect base station timestamp and NMEA payload
    as derived from NMEA string with metadata header

    input example:
    \s:43479,c:1635883083,t:1635883172*6C\!AIVDM,1,1,,,144fiV0P00WT:`8POChN4?v4281b,0*64

    returns:
    ("!AIVDM,1,1,,,144fiV0P00WT:`8POChN4?v4281b,0*64", 1635883083)
    */
    //let (meta, payload) = line.rsplit_once("\\").unwrap();
    match line.rsplit_once("\\")? {
        (meta, payload) => match meta.split(",").collect::<Vec<&str>>()[..] {
            [_s, c, _t] => {
                assert_eq!(&c[0..2], "c:");
                Some((payload.to_string(), c[2..].parse::<i32>().ok().unwrap()))
            }
            _ => None,
        },
    }
}

fn parsed(payload: Option<ParsedMessage>) -> Option<ParsedMessage> {
    match payload? {
        ParsedMessage::VesselDynamicData(vdd) => Some(ParsedMessage::VesselDynamicData(vdd)),
        ParsedMessage::VesselStaticData(vsd) => Some(ParsedMessage::VesselStaticData(vsd)),
        _ => None,
    }
}

pub fn decodemsgs(filename: String) -> Vec<VesselData> {
    let reader = BufReader::new(File::open(filename).expect("Cannot open file"));
    let mut parser = NmeaParser::new();
    let mut msgs = <Vec<VesselData>>::new();

    for msg in reader.lines() {
        let mut message = VesselData {
            msg: msg.as_ref().unwrap().to_string(),
            epoch: None,
            payload: None,
        };
        if let Some(splits) = split_msg(msg.as_ref().unwrap().to_string()) {
            if &splits.0 == "!AIVDM,1,1,,,;ie05s`0Kk6UvFiQ`IaUfW3iC8pB,0*02" {
                continue;
            }
            message.epoch = Some(splits.1);

            //println!("{:?}\t{:?} {:?}", splits.1, splits.0, msg.as_ref());
            if let Ok(payload) = parser.parse_sentence(&splits.0) {
                message.payload = parsed(Some(payload));
            }
        }
        //println!("{:?} {:?}", message.epoch, message.payload);
        //println!("{:?}", message);
        msgs.push(message);
    }
    return msgs;
}

/* --------------------------------------------------------------------------------------------- */

#[cfg(test)]
mod tests {
    use super::*;

    fn test_printmsgs(msgs: Vec<VesselData>) {
        for msg in msgs {
            println!("mmsi: {:?}", msg.payload);
        }
    }
    #[test]
    fn test_split_msg() {
        let input = "\\s:43479,c:1635883083,t:1635883172*6C\\!AIVDM,1,1,,,144fiV0P00WT:`8POChN4?v4281b,0*64";
        let result = split_msg(input.to_string()).unwrap();
        let expected = (
            "!AIVDM,1,1,,,144fiV0P00WT:`8POChN4?v4281b,0*64".to_string(),
            1635883083,
        );
        assert_eq!(expected, result);
    }

    #[test]
    fn test_small_dynamicdata_nometadata() {
        let _msgs = decodemsgs("src/aismsgs.nm4".to_string());
        //test_printmsgs(msgs);
    }

    #[test]
    fn test_realdata() {
        let msgs = decodemsgs(
            "testdata/exactEarth_20211102_205717Z_dcb69383-4ad8-4b40-94c4-5a3ae1db1c59.nm4"
                .to_string(),
        );
        //test_printmsgs(msgs);
        println!("{:?}", msgs[msgs.len() - 1])
    }
}
