benchmarking rust for SQLite DB inserts

compiles to ./target/release/aisdb


AISDB
  convert AIS data in .nm4 format to an SQLite database containing
  vessel position reports and static data reports
  (message types 1, 2, 3, 5, 18, 24, 27)

USAGE:
  aisdb --dbpath DBPATH ... [OPTIONS]

ARGS:
  --dbpath        SQLite database path

OPTIONS:
  -h, --help      Prints this message
  --file          Path to .nm4 file. Can be repeated multiple times
  --rawdata_dir   Path to .nm4 data directory

