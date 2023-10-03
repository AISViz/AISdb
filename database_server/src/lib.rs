#![feature(generators, generator_trait)]

pub mod aisdb_db_server;
pub use aisdb_db_server::{track_generator, GeneratorIteratorAdapter};
