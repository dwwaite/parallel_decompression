mod compression;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FrameMeta {
    position: u64,
    length: usize,
    order: usize,
}

impl FrameMeta {
    pub fn new(position: u64, length: usize, order: usize) -> FrameMeta {
        FrameMeta {
            position,
            length,
            order,
        }
    }
}

pub fn perform_compression(
    input_file: &str,
    output_file: &str,
    index_file: &str,
    block_size: usize,
    zstd_level: i32,
) -> Result<()> {
    let input_handle = OpenOptions::new().read(true).open(input_file).unwrap();

    let output_handle = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(output_file)
        .unwrap();

    let index_handle = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(index_file)
        .unwrap();

    let input_reader: BufReader<File> = BufReader::new(input_handle);
    let idx_writer: BufWriter<File> = BufWriter::new(index_handle);

    compression::write_indexed_zstd(
        input_reader,
        output_handle,
        idx_writer,
        block_size,
        zstd_level,
    )
}
