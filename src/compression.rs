use crate::FrameMeta;
use anyhow::{bail, Result};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Seek, Write};

//region: Private functions

fn read_chunk(
    file_reader: &mut BufReader<File>,
    read_buffer: &mut String,
    block_size: usize,
) -> Result<Option<u64>> {
    // TODO: check that block_size is > 0
    let mut total_bytes_read: usize = 0;

    loop {
        let bytes_read = file_reader.read_line(read_buffer)?;

        // Terminate early on an EOF
        if bytes_read == 0 {
            return if total_bytes_read == 0 {
                Ok(None)
            } else {
                Ok(Some(total_bytes_read as u64))
            };
        }

        total_bytes_read += bytes_read;

        // Terminate if block_size is met
        if total_bytes_read >= block_size {
            return Ok(Some(total_bytes_read as u64));
        }
    }
}

fn encode_zstd_block(
    mut zstd_writer: &File,
    content_bytes: &[u8],
    zstd_level: i32,
) -> Result<(u64, u64)> {
    // Find the offset for the writing stream before zstd write
    let start_offset = match zstd_writer.stream_position() {
        Ok(u) => u,
        Err(_) => bail!("Unable to find current location of zstd stream!"),
    };

    // Create an encoder and compress the block
    let mut encoder = zstd::stream::Encoder::new(zstd_writer, zstd_level).unwrap();
    encoder.include_checksum(true).unwrap();

    let mut af_encoder = encoder.auto_finish();

    match &af_encoder.write_all(content_bytes) {
        Ok(_) => (),
        Err(_) => bail!("Unable to write to zstd stream!"),
    };

    drop(af_encoder);

    // Find the offset for the writing stream after zstd write
    let end_offset = match zstd_writer.stream_position() {
        Ok(u) => u,
        Err(_) => bail!("Unable to find current location of zstd stream!"),
    };

    Ok((start_offset, end_offset))
}

//endregion:

pub fn write_indexed_zstd(
    mut input_reader: BufReader<File>,
    zstd_writer: File,
    mut idx_writer: BufWriter<File>,
    block_size: usize,
    zstd_level: i32,
) -> Result<()> {
    let mut idx_records: Vec<FrameMeta> = Vec::new();
    let mut seq_position = 0;

    let mut read_buffer = String::new();

    while let Ok(Some(_)) = read_chunk(&mut input_reader, &mut read_buffer, block_size) {
        let content = std::mem::take(&mut read_buffer);
        let content_bytes = content.as_bytes();

        let (start_pos, end_pos) = encode_zstd_block(&zstd_writer, content_bytes, zstd_level)?;

        let length = end_pos - start_pos;
        let frame_record = FrameMeta::new(start_pos, length, seq_position);

        idx_records.push(frame_record);
        seq_position += 1;
    }

    // Write out the index file
    serde_json::to_writer_pretty(&mut idx_writer, &idx_records)?;
    idx_writer.flush()?;

    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::fs::OpenOptions;
    use std::io::{BufReader, BufWriter, Read};

    fn open_file_read(file_path: &str) -> File {
        OpenOptions::new().read(true).open(file_path).unwrap()
    }

    fn open_file_write(file_path: &str) -> File {
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(file_path)
            .unwrap()
    }

    #[test]
    fn test_read_chunk_single() {
        // Read with a block too small for a single line to ensure that reading does
        // proceed until the end of the line.
        let input_handle = open_file_read("test/data.txt");
        let mut input_reader: BufReader<File> = BufReader::new(input_handle);

        let mut read_buffer = String::new();
        let result = read_chunk(&mut input_reader, &mut read_buffer, 5);

        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
        // Evaluate the data read into `read_buffer`
        assert_eq!("WP_413685322.1\t584\n", read_buffer);
    }

    #[test]
    fn test_read_chunk_multi() {
        // Read with a block too small for the whole file, but to cover several lines, to
        // confirm expected use case of multi-line reading.
        let input_handle = open_file_read("test/data.txt");
        let mut input_reader: BufReader<File> = BufReader::new(input_handle);

        let exp_content = concat!(
            "WP_413685322.1\t584\nXNR99298.1\t584\nMEX9938374.1\t587\nKJX92028.1\t1047168\n",
            "EFG1759503.1\t562\nEGJ4377881.1\t562\nEJZ1046351.1\t562\nEOA4653345.1\t562\n",
            "EOP3024222.1\t562\nWP_198835266.1\t2779367\nMBJ2149627.1\t2779367\n",
            "MBD3193859.1\t2053489\n"
        );

        let mut read_buffer = String::new();
        let result = read_chunk(&mut input_reader, &mut read_buffer, 200);

        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
        assert_eq!(exp_content, read_buffer);
    }

    #[test]
    fn test_read_chunk_file() {
        // Test the behaviour of the function over the complete file.
        let input_handle = open_file_read("test/data.txt");
        let mut input_reader: BufReader<File> = BufReader::new(input_handle);

        let exp_content: Vec<String> = vec!(
            "WP_413685322.1\t584\nXNR99298.1\t584\nMEX9938374.1\t587\nKJX92028.1\t1047168\n".into(),
            "EFG1759503.1\t562\nEGJ4377881.1\t562\nEJZ1046351.1\t562\nEOA4653345.1\t562\nEOP3024222.1\t562\n".into(),
            "WP_198835266.1\t2779367\nMBJ2149627.1\t2779367\nMBD3193859.1\t2053489\nMBD3198741.1\t2053489\n".into(),
            "MBR5368159.1\t1898203\nMCL6526161.1\t2614257\nUXB85809.1\t2697049\nMDO5780201.1\t1506\n".into(),
            "MDP1794720.1\t2201156\nMDE2592313.1\t1911520\nUMM52736.1\t2922427\nMDB4345056.1\t1869227\n".into(),
            "XP_035011836.2\t195615\nMDY3279706.1\t2831996\nPYI97175.1\t2026799\nPYJ33862.1\t2026799\n".into(),
            "WP_137987990.1\t492670\nTKZ18939.1\t492670\nWP_372757791.1\t1979402\nKLA26572.1\t1396\n".into(),
            "GAA1911923.1\t433649\n".into(),
        );

        let mut read_buffer = String::new();
        let mut obs_results: Vec<String> = Vec::new();

        while let Ok(Some(_)) = read_chunk(&mut input_reader, &mut read_buffer, 70) {
            let content = std::mem::take(&mut read_buffer);
            obs_results.push(content);
        }

        assert_eq!(exp_content, obs_results);
    }

    #[test]
    fn test_encode_zstd_block_single() {
        let target_file = "encode_zstd_block_single.zstd";
        let mut target_handle = open_file_write(target_file);

        let content = "test string for compression!";

        let obs_result = encode_zstd_block(&mut target_handle, content.as_bytes(), 0);
        assert!(obs_result.is_ok());

        let (start, stop) = obs_result.unwrap();
        assert_eq!((0, 41), (start, stop));

        drop(target_handle);
        let _ = std::fs::remove_file(target_file);
    }

    #[test]
    fn test_encode_zstd_block_multiple() {
        let target_file = "encode_zstd_block_multiple.zstd";
        let mut target_handle = open_file_write(target_file);

        let full_content: Vec<(String, (u64, u64))> = vec![
            ("first entry!".into(), (0, 25)),
            ("second entry!".into(), (25, 51)),
            ("third entry!".into(), (51, 76)),
        ];

        for (content, (exp_start, exp_stop)) in &full_content {
            let obs_result = encode_zstd_block(&mut target_handle, content.as_bytes(), 0);
            assert!(obs_result.is_ok());

            let exp_values = (*exp_start, *exp_stop);
            assert_eq!(exp_values, obs_result.unwrap());
        }

        drop(target_handle);
        let _ = std::fs::remove_file(target_file);
    }

    #[test]
    fn test_write_indexed_zstd_single_frame() {
        // Set up in the input reader/writers for the function arguments
        let input_handle = open_file_read("test/data.txt");
        let input_reader: BufReader<File> = BufReader::new(input_handle);

        let zstd_file = "write_indexed_zstd_single_frame.zstd";
        let zstd_handle = open_file_write(zstd_file);

        let index_file = "write_indexed_zstd_single_frame.zstd.idx";
        let index_handle = open_file_write(index_file);
        let index_writer: BufWriter<File> = BufWriter::new(index_handle);

        // Execute the command
        let obs_result = write_indexed_zstd(input_reader, zstd_handle, index_writer, 200, 0);
        assert!(obs_result.is_ok());

        // Decompress only the first block in the zstd file to check that the blocks
        // are being created correctly.
        let exp_zstd_frame = concat!(
            "WP_413685322.1\t584\nXNR99298.1\t584\nMEX9938374.1\t587\nKJX92028.1\t1047168\n",
            "EFG1759503.1\t562\nEGJ4377881.1\t562\nEJZ1046351.1\t562\nEOA4653345.1\t562\n",
            "EOP3024222.1\t562\nWP_198835266.1\t2779367\nMBJ2149627.1\t2779367\n",
            "MBD3193859.1\t2053489\n",
        );

        let mut obs_zstd = String::new();
        let mut decoder = zstd::stream::Decoder::new(open_file_read(zstd_file))
            .unwrap()
            .single_frame();
        let _ = decoder.read_to_string(&mut obs_zstd);

        assert_eq!(exp_zstd_frame, obs_zstd);

        // Clean up
        let _ = std::fs::remove_file(zstd_file);
        let _ = std::fs::remove_file(index_file);
    }

    #[test]
    fn test_write_indexed_zstd_complete() {
        // Set up in the input reader/writers for the function arguments
        let input_handle = open_file_read("test/data.txt");
        let input_reader: BufReader<File> = BufReader::new(input_handle);

        let zstd_file = "write_indexed_zstd_complete.zstd";
        let zstd_handle = open_file_write(zstd_file);

        let index_file = "write_indexed_zstd_complete.zstd.idx";
        let index_handle = open_file_write(index_file);
        let index_writer: BufWriter<File> = BufWriter::new(index_handle);

        // Execute the command
        let obs_result = write_indexed_zstd(input_reader, zstd_handle, index_writer, 200, 0);
        assert!(obs_result.is_ok());

        // Decompress the zstd file and compare against the expected payload
        // This just checks that compression worked
        let exp_zstd = std::fs::read_to_string("test/data.txt").unwrap();

        let mut obs_zstd = String::new();
        let mut decoder = zstd::stream::Decoder::new(open_file_read(zstd_file)).unwrap();
        let _ = decoder.read_to_string(&mut obs_zstd);

        assert_eq!(exp_zstd, obs_zstd);

        // Compare the contents of the JSON file against the expected payload
        // This checks that compression was performed in the expected blocks
        let exp_json: Vec<FrameMeta> =
            serde_json::from_reader(open_file_read("test/example.zstd.idx")).unwrap();
        let obs_json: Vec<FrameMeta> = serde_json::from_reader(open_file_read(index_file)).unwrap();

        assert_eq!(exp_json, obs_json);

        // Clean up
        let _ = std::fs::remove_file(zstd_file);
        let _ = std::fs::remove_file(index_file);
    }
}
