use crate::FrameMeta;
use anyhow::{bail, Result};
use dashmap::DashMap;
use rayon::prelude::*;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Cursor};
use std::os::unix::fs::FileExt;

//region: Private functions

fn load_frame_index(index_file: &mut BufReader<File>) -> Result<Vec<FrameMeta>> {
    let frame_vector: Vec<FrameMeta> = match serde_json::from_reader(index_file) {
        Ok(v) => v,
        Err(_) => bail!("Unable to load the zstd index!"),
    };

    Ok(frame_vector)
}

fn parse_bytes_to_numeric(bytes: &[u8]) -> Result<u64> {
    let s = match str::from_utf8(bytes) {
        Ok(v) => v,
        Err(_) => bail!("Unable to parse record content. Taxid will be reported as '0'!"),
    };

    let taxid: u64 = match s.trim().parse() {
        Ok(t) => t,
        Err(_) => bail!("Unable to convert value to numeric. Taxid will be reported as '0'!"),
    };

    Ok(taxid)
}

fn parse_lines_to_map(buf: &[u8]) -> Vec<(String, u64)> {
    let mut unpacked_data: Vec<(String, u64)> = Vec::new();

    for line_repr in buf.split(|&b| b == b'\n') {
        if let Some(tab_position) = line_repr.iter().position(|&b| b == b'\t') {
            let accession = String::from_utf8_lossy(&line_repr[..tab_position]).to_string();

            let taxid = match parse_bytes_to_numeric(&line_repr[tab_position + 1..]) {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("Error parsing record '{}'. {}", accession, e);
                    0
                }
            };

            unpacked_data.push((accession, taxid));
        }
    }
    unpacked_data
}

fn map_zstd_frame(zstd_file: &str, idx_frame: FrameMeta) -> Result<Vec<(String, u64)>> {
    let payload_length = idx_frame.parse_length()?;
    let mut frame_payload = vec![0u8; payload_length];

    let zstd_reader = OpenOptions::new().read(true).open(zstd_file)?;
    zstd_reader.read_exact_at(&mut frame_payload, idx_frame.position)?;

    let payload = zstd::decode_all(Cursor::new(frame_payload))?;
    let payload_data = parse_lines_to_map(&payload);

    Ok(payload_data)
}

//endregion:

pub fn read_indexed_zstd(
    zstd_file: &str,
    mut idx_reader: BufReader<File>,
    num_threads: usize,
) -> Result<DashMap<String, u64>> {
    let idx_buffer: Vec<FrameMeta> = load_frame_index(&mut idx_reader)?;
    let record_map: DashMap<String, u64> = DashMap::new();

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .thread_name(|i| format!("decompression-worker-{i}"))
        .build()
        .unwrap();

    pool.install(|| {
        idx_buffer.into_iter().par_bridge().for_each(|idx_frame| {
            match map_zstd_frame(zstd_file, idx_frame) {
                Ok(payload_data) => {
                    for (k, v) in payload_data {
                        record_map.insert(k, v);
                    }
                }
                Err(e) => eprintln!("{:#?}", e),
            }
        })
    });

    Ok(record_map)
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::collections::HashMap;
    use std::fs::OpenOptions;
    use std::io::BufRead;

    fn open_file_read(file_path: &str) -> File {
        OpenOptions::new().read(true).open(file_path).unwrap()
    }

    #[test]
    fn test_load_frame_index_() {
        let file_name = "test/example.zstd.idx";
        let mut json_handle = BufReader::new(open_file_read(file_name));

        let exp_content: Vec<FrameMeta> =
            serde_json::from_reader(open_file_read(file_name)).unwrap();

        let obs_result = load_frame_index(&mut json_handle);
        assert!(obs_result.is_ok());

        let obs_content = obs_result.unwrap();
        assert_eq!(exp_content, obs_content);
    }

    #[test]
    fn test_parse_bytes_to_numeric() {
        let exp_value: u64 = 123;
        let bytes_slice: &[u8] = "123".as_bytes();

        let obs_result = parse_bytes_to_numeric(bytes_slice);
        assert!(obs_result.is_ok());

        let obs_value = obs_result.unwrap();
        assert_eq!(exp_value, obs_value);
    }

    #[test]
    fn test_parse_lines_to_map_success() {
        let input_bytes = "a\t1\nb\t2\nc\t3\n".as_bytes();

        let exp_vector: Vec<(String, u64)> =
            vec![("a".into(), 1), ("b".into(), 2), ("c".into(), 3)];

        let obs_vector = parse_lines_to_map(&input_bytes);
        assert_eq!(exp_vector, obs_vector);
    }

    #[test]
    fn test_parse_lines_to_map_fail() {
        let input_bytes = "a\t1\nb\t2\nc\tq\n".as_bytes();

        let exp_vector: Vec<(String, u64)> =
            vec![("a".into(), 1), ("b".into(), 2), ("c".into(), 0)];

        let obs_vector = parse_lines_to_map(&input_bytes);
        assert_eq!(exp_vector, obs_vector);
    }

    #[test]
    fn test_map_zstd_frame() {
        // Take from the final block of the test data
        let idx_frame = FrameMeta::new(301, 120, 2);
        let input_file = "test/example.zstd";

        let exp_vector: Vec<(String, u64)> = vec![
            ("MDY3279706.1".into(), 2831996),
            ("PYI97175.1".into(), 2026799),
            ("PYJ33862.1".into(), 2026799),
            ("WP_137987990.1".into(), 492670),
            ("TKZ18939.1".into(), 492670),
            ("WP_372757791.1".into(), 1979402),
            ("KLA26572.1".into(), 1396),
            ("GAA1911923.1".into(), 433649),
        ];

        let obs_result = map_zstd_frame(input_file, idx_frame);
        assert!(obs_result.is_ok());

        let obs_vector = obs_result.unwrap();

        assert_eq!(exp_vector, obs_vector);
    }

    #[test]
    fn test_read_indexed_zstd() {
        let input_file = "test/example.zstd";
        let idx_reader = BufReader::new(open_file_read("test/example.zstd.idx"));

        let exp_map: HashMap<String, u64> = BufReader::new(open_file_read("test/data.txt"))
            .lines()
            .map(|line| {
                let line_content = line.unwrap();

                let (acc, rest) = line_content.trim().split_once('\t').unwrap();
                let u: u64 = rest.parse().unwrap();

                (acc.to_string(), u)
            })
            .collect();

        let obs_result = read_indexed_zstd(input_file, idx_reader, 2);
        assert!(obs_result.is_ok());

        // DashMap does not implement PartialEq, so cast to HashMap for easy comparison.
        let obs_map: HashMap<String, u64> = obs_result.unwrap().into_iter().collect();
        assert_eq!(exp_map, obs_map);
    }
}
