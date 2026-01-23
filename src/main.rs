fn main() {
    // Compress input file into zstd with blocks
    let input_file: String = "test/data.txt".into();
    let output_file: String = "example.zstd".into();
    let index_file: String = "example.zstd.idx".into();

    // Test the reader
    let block_size: usize = 200;
    let zstd_level: i32 = 3;

    let zstd_result = parallel_decompression::perform_compression(
        &input_file,
        &output_file,
        &index_file,
        block_size,
        zstd_level,
    );

    match zstd_result {
        Ok(_) => {
            println!("Success!");
            println!("  Input file:  {}", input_file);
            println!("  Output file: {}", output_file);
            println!("  Index file:  {}", index_file);
        }
        Err(e) => eprintln!("{}", e),
    }
}
