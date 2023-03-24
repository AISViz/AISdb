/// links new content from aisdb/aisdb_sql/ at compile time
fn main() {
    println!("cargo:rerun-if-changed=../aisdb/aisdb_sql/*");
}
