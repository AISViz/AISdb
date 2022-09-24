use std::fs::File;
use std::path::PathBuf;
use std::thread::sleep;
use std::time::Duration;

pub const TESTDATA: &str = "../readme.md";
pub const TESTINGDIR: &str = "../testconfig/";

pub fn truncate(path: PathBuf) -> i32 {
    sleep(Duration::from_millis(15));
    let info = match File::open(&path) {
        Ok(f) => f.metadata().unwrap().len(),
        Err(e) => {
            eprintln!("{}", e);
            0
        }
    };

    let i = info as i32;
    File::create(&path).expect("creating file");
    i
}
