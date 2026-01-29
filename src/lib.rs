mod compression;
mod decompression;
use ahash::AHashMap;
use anyhow::{bail, Result};
use byte_unit::Byte;
use clap::ValueEnum;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};

#[derive(ValueEnum, Clone, Debug)]
pub enum Mode {
    DashMap,
    Vector,
    Merge,
}

pub enum EitherMap<K, V> {
    Dash(DashMap<K, V>),
    AHash(AHashMap<K, V>),
}

impl<K: Eq + std::hash::Hash, V> EitherMap<K, V> {
    pub fn len(&self) -> usize {
        match self {
            EitherMap::Dash(m) => m.len(),
            EitherMap::AHash(m) => m.len(),
        }
    }

    pub fn into_ahash(self) -> Option<ahash::AHashMap<K, V>> {
        match self {
            EitherMap::AHash(m) => Some(m),
            _ => None,
        }
    }

    pub fn into_dash(self) -> Option<dashmap::DashMap<K, V>> {
        match self {
            EitherMap::Dash(m) => Some(m),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FrameMeta {
    position: u64,
    length: u64,
    order: u64,
}

impl FrameMeta {
    pub fn new(position: u64, length: u64, order: u64) -> FrameMeta {
        FrameMeta {
            position,
            length,
            order,
        }
    }

    pub fn parse_length(&self) -> Result<usize> {
        let u: usize = match self.length.try_into() {
            Ok(u) => u,
            Err(_) => bail!(
                "The frame at position {} could not be parsed correctly!",
                self.position,
            ),
        };
        Ok(u)
    }
}

fn parse_block_input(block_size: &str) -> Result<usize> {
    let block_value: u64 = match Byte::parse_str(block_size, true) {
        Ok(b) => b.as_u64(),
        Err(_) => bail!("Unable to parse user-specified block size to numeric value!"),
    };

    let parsed_block: usize = match usize::try_from(block_value) {
        Ok(u) => u,
        Err(_) => bail!("Value to large for block specification!"),
    };

    Ok(parsed_block)
}

pub fn perform_compression(
    input_file: &str,
    output_file: &str,
    index_file: &str,
    block_size: &str,
    zstd_level: i32,
) -> Result<()> {
    let block_usize: usize = parse_block_input(block_size)?;
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

    let operation_result = compression::write_indexed_zstd(
        input_reader,
        output_handle,
        idx_writer,
        block_usize,
        zstd_level,
    );

    if operation_result.is_ok() {
        println!("Success!");
        println!("  Input file:  {}", input_file);
        println!("  Output file: {}", output_file);
        println!("  Index file:  {}", index_file);
    }
    operation_result
}

pub fn perform_decompression(
    zstd_file: &str,
    idx_file: &str,
    mode: &Mode,
    num_threads: usize,
) -> Result<()> {
    let idx_handle = OpenOptions::new().read(true).open(idx_file)?;
    let idx_reader: BufReader<File> = BufReader::new(idx_handle);

    let operation_result = match mode {
        Mode::DashMap => {
            decompression::read_indexed_zstd_dashmap(zstd_file, idx_reader, num_threads)
        }
        Mode::Vector => decompression::read_indexed_zstd_vector(zstd_file, idx_reader, num_threads),
        Mode::Merge => decompression::read_indexed_zstd_merge(zstd_file, idx_reader, num_threads),
    };

    match &operation_result {
        Ok(map) => {
            println!("Success!");
            println!("  Input file:  {}", zstd_file);
            println!("  Index file:  {}", idx_file);
            println!("  Total records processed: {}", map.len());
        }
        Err(e) => bail!(e.to_string()),
    }

    Ok(())
}
