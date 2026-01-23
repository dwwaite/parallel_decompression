# parallel_decompression

A tool to created indexed, compressed records of large text files using zstd. Ensures that each block within the zstd archive terminates at the end of a line record, allowing for embarassingly parallel decompression after the fact.

In this instance, using data obtained from the NCBI nr database with two columns representing the accession number and the taxid value for records ([mock data here](./test/data.txt)).

Minimal working example:

```bash
./target/debug/parallel_decompression

zstd -f -d example.zstd -o example.txt

md5sum test/data.txt example.txt
# a9fad2ab133b27077914647dee98b38b  test/data.txt
# a9fad2ab133b27077914647dee98b38b  example.txt
```

TO DO:

1. [ ] Add clippy front end
1. [ ] Compression:
   1. [ ] Allow reading from file or stdin
1. [ ] Decompression
   1. [ ] Full implementation - single thread
   1. [ ] Full implementation - multithreaded

