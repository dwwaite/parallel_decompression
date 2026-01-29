use anyhow::Result;
use clap::Parser;
use parallel_decompression::Mode;

fn main() {
    let user_inputs = ArgumentParser::parse();

    let operation_results: Result<()> = match &user_inputs.command {
        Workflow::Compress {
            input,
            output,
            zindex,
            block_size,
            level,
        } => parallel_decompression::perform_compression(input, output, zindex, block_size, *level),
        Workflow::Decompress {
            input,
            zindex,
            mode,
            num_threads,
        } => parallel_decompression::perform_decompression(input, zindex, mode, *num_threads),
    };

    match operation_results {
        Ok(_) => println!("\nCompleted!"),
        Err(e) => {
            eprintln!("Operation failed!\n");
            eprintln!("{}", e);
        }
    }
}

#[derive(Parser)]
#[clap(author="David Waite", version, about, long_about=None)]
struct ArgumentParser {
    #[command(subcommand)]
    command: Workflow,
}

#[derive(clap::Subcommand)]
enum Workflow {
    /// Create an indexed zstd compression of the target input file
    Compress {
        /// The input file to which taxonomic information is appended (REQUIRED)
        #[clap(short, long, value_parser, value_name = "INPUT")]
        input: String,

        /// Target file to store the blocked zstd payload (REQUIRED)
        #[clap(short, long, value_parser, value_name = "OUTPUT")]
        output: String,

        /// Target file to store the blocked zstd index (REQUIRED)
        #[clap(short, long, value_parser, value_name = "INDEX")]
        zindex: String,

        /// The block size for compression (supports human-readable formats e.g. '64KiB, 128MiB, 2GB')
        #[clap(short, long, default_value_t = String::from("64KiB"), value_name = "BLOCK_SIZE")]
        block_size: String,

        /// Compression level for zstd
        #[clap(short, long, default_value_t = 3, value_name = "COMPRESSION")]
        level: i32,
    },

    /// Read an indexed zstd compression and parse results to a HashMap
    Decompress {
        /// The zstd file to be decompressed and parsed (REQUIRED)
        #[clap(short, long, value_parser, value_name = "INPUT")]
        input: String,

        /// The zstd index file to be decompressed and parsed (REQUIRED)
        #[clap(short, long, value_parser, value_name = "INDEX")]
        zindex: String,

        /// Number of threads to use for parallel file parsing
        #[clap(short, long, default_value_t = 1, value_name = "THREADS")]
        num_threads: usize,

        /// Method for gathering zstd frame results
        #[clap(long, default_value_t = Mode::DashMap, value_name = "MODE", value_enum)]
        mode: Mode,
    },
}
