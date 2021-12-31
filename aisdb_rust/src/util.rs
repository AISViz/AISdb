use std::fs::read_dir;

/// yields sorted vector of files in dirname with a matching file extension.
/// Optionally, skip the first N results
pub fn glob_dir(dirname: &str, matching: &str, skip: usize) -> Option<Vec<String>> {
    //    let mut fpaths = read_dir("testdata/")
    //        .unwrap()
    //        .map(|f| f.unwrap().path().display().to_string())
    //        .collect::<Vec<String>>();
    //    fpaths.sort();
    let files = read_dir(dirname).unwrap();
    let mut fnames: Vec<String> = [].to_vec();
    for f in files {
        //let path_str = f.ok()?.path().to_str().unwrap();
        //match path_str.rsplit_once(".") {
        match f.ok()?.path().to_str().unwrap().rsplit_once(".") {
            Some((pattern, "nm4")) | Some((pattern, "NM4")) => {
                let fname = format!("{}.{}", pattern, matching);
                fnames.push(fname);
            }
            q => {
                println!("skipping path {}", q.unwrap().0);
                continue;
            }
        }
    }
    fnames.sort();
    Some(fnames[skip..].to_vec())
}
