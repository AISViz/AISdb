pub use std::{
    fs::{create_dir_all, read_dir, File},
    io::{BufRead, BufReader, Error, Write},
    time::{Duration, Instant},
};

use nmea_parser::{
    ais::{VesselDynamicData, VesselStaticData},
    NmeaParser, ParsedMessage,
};

pub struct VesselData {
    pub payload: Option<ParsedMessage>,
    pub epoch: Option<i32>,
}

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

/// discard all other message types, sort filtered categories
pub fn filter_vesseldata(
    sentence: &str,
    epoch: &i32,
    parser: &mut NmeaParser,
) -> Option<(ParsedMessage, i32, bool)> {
    match parser.parse_sentence(&sentence).ok()? {
        ParsedMessage::VesselDynamicData(vdd) => {
            Some((ParsedMessage::VesselDynamicData(vdd), *epoch, true))
        }
        ParsedMessage::VesselStaticData(vsd) => {
            Some((ParsedMessage::VesselStaticData(vsd), *epoch, false))
        }
        _ => None,
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
    match line.unwrap().rsplit_once("\\")? {
        (meta, payload) => {
            for tag in meta.split(",") {
                if &tag[0..2] != "c:" {
                    continue;
                } else if let Ok(i) = tag[2..].parse::<i32>() {
                    return Some((payload.to_string(), i));
                }
            }
            return None;
        }
    }
}

/// work around panic bug in chrono library / indexing bug in nmea_parser library ???
/// <https://github.com/zaari/nmea-parser/issues/25>
pub fn skipmsg(msg: &str, epoch: &i32) -> Option<(String, i32)> {
    /* true for base station reports and binary application data */
    if msg.contains("!AIVDM,1,1,,,;")
        || msg.contains("!AIVDM,1,1,,,I")
        || msg.contains("!AIVDM,1,1,,,J")
    {
        None
    } else {
        Some((msg.to_string(), *epoch))
    }
}

/// open .nm4 file and decode each line, keeping only vessel data
/// returns vector of static and dynamic reports
pub fn decodemsgs(filename: &String) -> (Vec<VesselData>, Vec<VesselData>) {
    assert_eq!(&filename[&filename.len() - 4..], ".nm4");
    //println!("opening file {}...", filename);

    let reader = BufReader::new(File::open(filename).expect("Cannot open file"));
    let mut parser = NmeaParser::new();
    let mut stat_msgs = <Vec<VesselData>>::new();
    let mut positions = <Vec<VesselData>>::new();

    for (payload, epoch, is_dynamic) in reader
        .lines()
        .filter_map(|l| parse_headers(l))
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
        } else {
            stat_msgs.push(message);
        }
    }

    println!(
        "{}    dynamic: {}    static: {}",
        filename.rsplit_once("/").unwrap().1,
        positions.len(),
        stat_msgs.len(),
    );
    return (positions, stat_msgs);
}

/* --------------------------------------------------------------------------------------------- */

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::util::glob_dir;

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
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,2,1,4,,54sc8041SAu`uDPr220dU<56222222222222221618@247?m07SUEBp1,0*0C"#;

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

    #[test]
    pub fn test_files() -> Result<(), Error> {
        let _ = testingdata();
        let fpaths = glob_dir("testdata/", "nm4", 0).unwrap();

        let mut n = 0;
        for filepath in fpaths {
            if n > 10 {
                break;
            }
            let start = Instant::now();
            let (positions, stat_msgs) = decodemsgs(&filepath);
            //assert_ne!(positions.len(), 0);
            //assert_ne!(stat_msgs.len(), 0);
            let elapsed = start.elapsed();
            println!(
                "{}\tdecoded {} msgs/s",
                n,
                ((positions.len() + stat_msgs.len()) as f64 / elapsed.as_secs_f64())
            );
            n += 1;
        }

        Ok(())
    }
}