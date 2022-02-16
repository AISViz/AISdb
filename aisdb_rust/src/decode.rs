pub use std::{
    fs::{create_dir_all, read_dir, File},
    io::{BufRead, BufReader, Error, Write},
    time::{Duration, Instant},
};

use nmea_parser::{
    ais::{VesselDynamicData, VesselStaticData},
    NmeaParser, ParsedMessage,
};

use crate::db::{get_db_conn, prepare_tx_dynamic, prepare_tx_static};

/// collect decoded messages and epoch timestamps
pub struct VesselData {
    pub payload: Option<ParsedMessage>,
    pub epoch: Option<i32>,
}

/// explicit type conversion in decode_msgs fn
impl VesselData {
    pub fn dynamicdata(self) -> (VesselDynamicData, i32) {
        let p = self.payload.unwrap();
        if let ParsedMessage::VesselDynamicData(p) = p {
            (p, self.epoch.unwrap())
        } else {
            panic!("wrong msg type")
        }
    }
    pub fn staticdata(self) -> (VesselStaticData, i32) {
        let p = self.payload.unwrap();
        if let ParsedMessage::VesselStaticData(p) = p {
            (p, self.epoch.unwrap())
        } else {
            panic!("wrong msg type")
        }
    }
}

/// collect base station timestamp and NMEA payload
/// as derived from NMEA string with metadata header
///
/// input example:
/// ``` text
/// \s:43479,c:1635883083,t:1635883172*6C\!AIVDM,1,1,,,144fiV0P00WT:`8POChN4?v4281b,0*64
/// ```
///
/// returns:
/// ``` text
/// ("!AIVDM,1,1,,,144fiV0P00WT:`8POChN4?v4281b,0*64", 1635883083)
/// ```
pub fn parse_headers(line: Result<String, Error>) -> Option<(String, i32)> {
    match line.unwrap().rsplit_once('\\')? {
        (meta, payload) => {
            for tag_outer in meta.split(',') {
                for tag in tag_outer.split('*') {
                    if tag.len() <= 3 || !&tag.contains("c:") {
                        continue;
                    } else if let Ok(i) = tag[2..].parse::<i32>() {
                        return Some((payload.to_string(), i));
                    } else if let Ok(i) = tag[3..].parse::<i32>() {
                        return Some((payload.to_string(), i));
                    } else {
                        return None;
                    }
                }
            }
            None
        }
    }
}

/// workaround for panic from nmea_parser library,
/// caused by malformed base station timestamps / binary application messages?
/// discards all UTC date response and binary application payloads before
/// decoding them
pub fn skipmsg(msg: &str, epoch: &i32) -> Option<(String, i32)> {
    //println!("{:?}", msg);
    if &msg.chars().count() >= &15 && &msg[..12] == "!AIVDM,1,1,," {
        match &msg[12..13] {
            "0" | "1" | "2" | "3" | "A" | "B" => match &msg[14..15] {
                ";" | "I" | "J" => None,
                _ => {
                    //println!("14..15: {:?}", &msg[14..15]);
                    Some((msg.to_string(), *epoch))
                }
            },
            "," => match &msg[13..14] {
                ";" | "I" | "J" => None,
                _ => {
                    //println!("13..14: {:?}", &msg[13..14]);
                    Some((msg.to_string(), *epoch))
                }
            },
            _ => {
                //println!("12..13: {:?}", &msg[12..13]);
                Some((msg.to_string(), *epoch))
            }
        }
    } else {
        Some((msg.to_string(), *epoch))
    }
}

/// discard all other message types, sort filtered categories
pub fn filter_vesseldata(
    sentence: &str,
    epoch: &i32,
    parser: &mut NmeaParser,
) -> Option<(ParsedMessage, i32, bool)> {
    match parser.parse_sentence(sentence).ok()? {
        ParsedMessage::VesselDynamicData(vdd) => {
            Some((ParsedMessage::VesselDynamicData(vdd), *epoch, true))
        }
        ParsedMessage::VesselStaticData(vsd) => {
            Some((ParsedMessage::VesselStaticData(vsd), *epoch, false))
        }
        _ => None,
    }
}

/// open .nm4 file and decode each line, keeping only vessel data.
/// decoded vessel data will be inserted into the SQLite database
/// located at dbpath
pub async fn decode_insert_msgs(
    dbpath: &std::path::Path,
    filename: &std::path::Path,
) -> Result<(), Error> {
    let fstr = &filename.to_str().unwrap();
    assert_eq!(&fstr[&fstr.len() - 4..], ".nm4");
    let start = Instant::now();

    let reader = BufReader::new(
        File::open(filename)
            .unwrap_or_else(|_| panic!("Cannot open .nm4 file {}", filename.to_str().unwrap())),
    );
    let mut parser = NmeaParser::new();
    let mut stat_msgs = <Vec<VesselData>>::new();
    let mut positions = <Vec<VesselData>>::new();
    let mut count = 0;
    //let mut mstr = None;

    let mut c = get_db_conn(dbpath).expect("getting db conn");

    // in 500k batches
    for (payload, epoch, is_dynamic) in reader
        .lines()
        .filter_map(parse_headers)
        .filter_map(|(s, e)| skipmsg(&s, &e))
        .filter_map(|(s, e)| filter_vesseldata(&s, &e, &mut parser))
        .collect::<Vec<(ParsedMessage, i32, bool)>>()
    {
        let message = VesselData {
            epoch: Some(epoch),
            payload: Some(payload),
        };

        if is_dynamic {
            positions.push(message);
            count += 1;
        } else {
            stat_msgs.push(message);
            count += 1;
        }

        if positions.len() >= 500000 {
            let _d = prepare_tx_dynamic(&mut c, positions);
            positions = vec![];
        };
        if stat_msgs.len() >= 500000 {
            let _s = prepare_tx_static(&mut c, stat_msgs);
            stat_msgs = vec![];
        }
    }

    // insert remaining
    if positions.len() > 0 {
        let _d = prepare_tx_dynamic(&mut c, positions);
    }
    if stat_msgs.len() > 0 {
        let _s = prepare_tx_static(&mut c, stat_msgs);
    }

    let elapsed = start.elapsed();

    println!(
        "{}    count:{: >8}    elapsed: {:0.2 }s    rate: {:.0} msgs/s",
        filename
            .to_str()
            .unwrap()
            .rsplit_once(std::path::MAIN_SEPARATOR)
            .unwrap()
            .1,
        count,
        elapsed.as_secs_f32(),
        count as f32 / elapsed.as_secs_f32(),
    );

    Ok(())
}

/* --------------------------------------------------------------------------------------------- */

#[cfg(test)]
pub mod tests {

    use super::{decode_insert_msgs, parse_headers};
    use crate::util::glob_dir;
    use crate::Error;
    use std::fs::create_dir_all;
    use std::fs::File;
    use std::io::Write;

    #[test]
    pub fn testingdata() -> Result<(), &'static str> {
        let c = r#"
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;I=i:8f0D4l>niTdDO`cO3jGqrlQ,0*67
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,another_error!!;wqlirf
\s:42958i,t:1635809521*6F\!AIVDM,1,1,,A,B4eIh>@0<voAFw6HKAi7swf1lH@s,0*61
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,B,14eGdb0001sM5sjIS3C5:qpt0L0G,0*0CFFF
!AIVDM,1,1,,,IjHcmoT=Jk;9uh,4*3E
!AIVDM,1,1,,,;3atgG6bSvJKGpi6=:9Twkk13W:3,0*03
!AIVDM,1,1,,,;3f?`?bDiW2w=Pt3hfnEP6pCJoli,0*4E
!AIVDM,1,1,,,;4=BV5C@NGJfs0ck@oM2gB>6E2hB,0*39
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;5L<FJ3An>wAqn=voHSOHqTv=`GN,0*21
!AIVDM,1,1,,,;7a`OobAVQO<A1nbiBc3rBqih5UB,0*78
\s:42958,c:1635809454\!AIVDM,1,1,,,;9L:cO`CgQ@S:NcT04HENVk@:JR=,0*5F
!AIVDM,1,1,,,;:JvB;MhC4pvK3KB43F60v4bAhuF,0*7B
,t:1635809521*6F!AIVDM,1,1,,,;;d6bbCsM8qH5>?=U0BMdo>>VvmU,0*39
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;;si?Qj0:wNL4tDTd`BN41nL0D11,0*0D
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;=K;HJ:wsf0Bg8IDJ2MQ7PISJ;jJ,0*23
\c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;=OeV7HR4n8tM3grUTk1Cs9glLGE,0*5A
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;@Ha?G0t<>ekGDOI:>sE<2BnWHNr,0*33
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;AHld`?P<wLu6<T:L6TVm0QqcQWl,0*48
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;BI:gwCuqWqP7Wr8JVKTwAIDRiWl,0*5E
\s:42958,t:1635809521*6F\!AIVDM,1,1,,,;EH8O`wtiWs;0MmE@F;U2:srnf?E,0*75
\s:42958,c:,t:1635809521*6F\!AIVDM,1,1,,,;I=i:8f0D4l>niTdDO`cO3jGqrlQ,0*67
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;KaC759LogaaW=4r:nn>VEc<m2qs,0*73
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;KpNTNJNbq9wMffET:P<C35Pmo`1,0*26
\g:23412341234kj\!AIVDM,1,1,,,;M`m7tluWVNmIBnh5NoiARj<spps,0*51
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;OJvuN<7esIlAgPIus4NJa:UlqDP,0*22
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;RIRt:dp:3qqmr67hRoGGJ>e7uTi,0*63
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;U>f0=vU6au?gW@E8UuVoI=P07H=,0*20
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;cvCpIRTKPSqU9kT0dOLKWuC2KE2,0*2C
\s:42958,c:-12134,t:1635809521*6F\!AIVDM,1,1,,,;cw3<IPo<pHlaoEPuT9PqcAn5fnM,0*47
\s:42958,c:asbhdjf,t:1635809521*6F\!AIVDM,1,1,,,;eeA1PBssU1OQwN8orvatv97;@tm,0*21
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;g9tT:6O@0Ujsr<mCJCwnAG83cv?,0*0A
\s:42958,c1635809454,t:1635809521*6F\!AIVDM,1,1,,,;h3woll47wk?0<tSF0l4Q@000P00,0*4B
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;i<rac5sg@;huMi4QhiWacTLEQj<,0*71
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,2,1,4,,54sc8041SAu`uDPr220dU<56222222222222221618@247?m07SUEBp1,0*0C
\c:1617253215*5A\!AIVDM,1,1,,,13nWPR0003K7<OsQsrGW1K>L0881,0*68
\c:1617284347*56\!AIVDM,1,1,,,13n7aN0wQnsN4lfE8nEUgDf:0<00,0*18
\c:1617289692*56\!AIVDM,1,1,,,C4N6S1005=6h:aw8::=9CwTHL:`<BVAWWWKQa1111110CP81110W,0*7A"#;

        if create_dir_all("testdata/").is_ok() {
            let mut output = File::create("testdata/testingdata.nm4").unwrap();
            let _ = write!(output, "{}", c);
            Ok(())
        } else {
            Err("cant create testing data dir!")
        }
    }

    #[test]
    pub fn test_parse_headers() {
        let input = r#"\s:43479,c:1635883083,t:1635883172*6C\!AIVDM,1,1,,,144fiV0P00WT:`8POChN4?v4281b,0*64"#;
        let result = parse_headers(Ok(input.to_string())).unwrap();
        let expected = (
            "!AIVDM,1,1,,,144fiV0P00WT:`8POChN4?v4281b,0*64".to_string(),
            1635883083,
        );
        assert_eq!(expected, result);
    }

    pub fn test_decode_insert_msgs() -> Result<(), Error> {
        let fpaths = glob_dir(std::path::PathBuf::from("testdata/"), "nm4").expect("globbing");
        for filepath in fpaths {
            let _ = decode_insert_msgs(
                &std::path::Path::new("testdata/test.db").to_path_buf(),
                &std::path::Path::new(&filepath).to_path_buf(),
            );
        }

        Ok(())
    }
}
