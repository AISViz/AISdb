benchmarking rust for SQLite DB inserts 

compiles a CLI executable at ./target/release/aisdb


AISDB
  convert AIS data in .nm4 format to an SQLite database containing
  vessel position reports and static data reports
  (message types 1, 2, 3, 5, 18, 24, 27)

USAGE:
  aisdb --dbpath [DBPATH] [OPTIONS]

FLAGS:
  -h, --help      Prints this message
  --dbpath        SQLite database path

OPTIONS:
  --file          Path to .nm4 file. Can be repeated
  --rawdata_dir   Path to .nm4 data files               [default=./]
  --start         Optionally skip the first N files     [default=0]
  --end           Optionally skip files after index N   [default=usize::MAX]

