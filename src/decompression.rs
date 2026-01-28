use crate::FrameMeta;
use ahash::AHashMap;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use anyhow::{bail, Result};
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

fn parse_lines_to_map(buf: &[u8]) -> AHashMap<String, u64> {
    let mut taxid_map: AHashMap<String, u64> = AHashMap::new();

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

            taxid_map.insert(accession, taxid);
        }
    }

    taxid_map
}

fn combine_hashmaps(partial_maps: Vec<AHashMap<String, u64>>) -> AHashMap<String, u64> {
    let total_capacity: usize = partial_maps.iter().map(|m| m.len()).sum();
    let mut final_map: AHashMap<String, u64> = AHashMap::with_capacity(total_capacity);

    for mut partial_map in partial_maps {
        for (k, v) in partial_map.drain() {
            final_map.insert(k, v);
        }
    }

    final_map
}

//endregion:

fn map_zstd_frame(zstd_file: &str, idx_frame: FrameMeta) -> Result<AHashMap<String, u64>> {
    let zstd_reader = OpenOptions::new().read(true).open(zstd_file)?;

    let payload_length = idx_frame.parse_length()?;
    let mut frame_payload = vec![0u8; payload_length];

    zstd_reader.read_exact_at(&mut frame_payload, idx_frame.position)?;
    let payload = zstd::decode_all(Cursor::new(frame_payload))?;

    let taxid_map = parse_lines_to_map(&payload);
    Ok(taxid_map)
}

pub fn read_indexed_zstd_st(
    zstd_file: &str,
    mut idx_reader: BufReader<File>,
) -> Result<AHashMap<String, u64>> {
    let idx_buffer: Vec<FrameMeta> = load_frame_index(&mut idx_reader)?;
    let mut frame_buffer: Vec<AHashMap<String, u64>> = Vec::with_capacity(idx_buffer.len());

    for idx_frame in idx_buffer {
        let taxid_map = map_zstd_frame(zstd_file, idx_frame)?;
        frame_buffer.push(taxid_map);
    }

    // Merge all partial hashmaps.
    let final_map = combine_hashmaps(frame_buffer);
    Ok(final_map)
}

pub fn read_indexed_zstd_mt(
    zstd_file: &str,
    mut idx_reader: BufReader<File>,
    num_threads: usize,
) -> Result<AHashMap<String, u64>> {
    let idx_buffer: Vec<FrameMeta> = load_frame_index(&mut idx_reader)?;

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .unwrap();

    let result_buffer: Vec<Result<AHashMap<String, u64>>> = pool.install(|| {
        idx_buffer.into_par_iter().map(|idx_frame| {
            let taxid_map = map_zstd_frame(zstd_file, idx_frame);
            taxid_map
        })
        .collect()
    });

    // Merge all partial hashmaps.
    let frame_buffer: Vec<AHashMap<String, u64>> = result_buffer.into_iter().filter_map(Result::ok).collect();
    let final_map = combine_hashmaps(frame_buffer);
    Ok(final_map)
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::fs::OpenOptions;

    fn open_file_read(file_path: &str) -> File {
        OpenOptions::new().read(true).open(file_path).unwrap()
    }

    #[test]
    fn load_frame_index_pass() {
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
    fn parse_bytes_to_numeric_pass() {
        let exp_value: u64 = 123;
        let bytes_slice: &[u8] = "123".as_bytes();

        let obs_result = parse_bytes_to_numeric(bytes_slice);
        assert!(obs_result.is_ok());

        let obs_value = obs_result.unwrap();
        assert_eq!(exp_value, obs_value);
    }

    #[test]
    fn parse_lines_to_map_success() {
        let input_bytes = "a\t1\nb\t2\nc\t3\n".as_bytes();

        let exp_map: AHashMap<String, u64> = vec![
            (String::from("a"), 1),
            (String::from("b"), 2),
            (String::from("c"), 3),
        ]
        .into_iter()
        .collect();

        let obs_map = parse_lines_to_map(&input_bytes);
        assert_eq!(exp_map, obs_map);
    }

    #[test]
    fn parse_lines_to_map_fail() {
        let input_bytes = "a\t1\nb\t2\nc\tq\n".as_bytes();

        let exp_map: AHashMap<String, u64> = vec![
            (String::from("a"), 1),
            (String::from("b"), 2),
            (String::from("c"), 0),
        ]
        .into_iter()
        .collect();

        let obs_map = parse_lines_to_map(&input_bytes);
        assert_eq!(exp_map, obs_map);
    }

    #[test]
    fn combine_hashmaps_merge() {
        let partial_maps: Vec<AHashMap<String, u64>> = vec![
            vec![(String::from("a"), 1), (String::from("b"), 2)]
                .into_iter()
                .collect(),
            vec![(String::from("c"), 3), (String::from("d"), 4)]
                .into_iter()
                .collect(),
        ];

        let exp_map: AHashMap<String, u64> = vec![
            (String::from("a"), 1),
            (String::from("b"), 2),
            (String::from("c"), 3),
            (String::from("d"), 4),
        ]
        .into_iter()
        .collect();

        let obs_map = combine_hashmaps(partial_maps);
        assert_eq!(exp_map, obs_map);
    }
}
