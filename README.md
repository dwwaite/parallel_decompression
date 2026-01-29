# parallel_decompression

A tool to created indexed, compressed records of large text files using zstd. Ensures that each block within the zstd archive terminates at the end of a line record, allowing for embarassingly parallel decompression after the fact.

In this instance, using data obtained from the NCBI nr database with two columns representing the accession number and the taxid value for records ([mock data here](./test/data.txt)).

Minimal working example:

```bash
./target/debug/parallel_decompression compress -i test/data.txt -o example.zstd -z example.zstd.idx -b 200

zstd -f -d example.zstd -o example.txt

md5sum test/data.txt example.txt
# a9fad2ab133b27077914647dee98b38b  test/data.txt
# a9fad2ab133b27077914647dee98b38b  example.txt
```

---

# Benchmarking

Reference file `nr_202601.accessions.master.tsv` contains a total of 1,687,587,711 records. Cut this down to a third for first performance checks.

```bash
head -n 500000000 nr_202601.accessions.master.tsv > nr_202601.accessions.tsv

time ./target/release/parallel_decompression compress \
   -i nr_202601.accessions.tsv \
   -o outputs/nr_202601.accessions.128MiB.zstd \
   -z outputs/nr_202601.accessions.128MiB.zstd.idx \
   -b 128MiB

time ./target/release/parallel_decompression compress \
   -i nr_202601.accessions.tsv \
   -o outputs/nr_202601.accessions.2GiB.zstd \
   -z outputs/nr_202601.accessions.2GiB.zstd.idx \
   -b 2GiB
```

|Block size|Blocks|Runtime|
|:---:|:---:|:---:|
|128MiB|75|`2m34.195s`|
|2GiB|5|`2m36.868s`|

## v0.2.0

Each individual thread creates a partial HashMap, then they are sequentially drained into a master HashMap on fan-in.

|Threads|128MiB (s)|2GiB (s)|
|:---:|:---:|:---:|
|1|1020.6|1213.5|
|2|919.9|1143.4|
|3|837.1|1570.6|
|4|1362.5|918.4|
|5|923.3|904.7|

## v0.3.0

Redesign with a shared HashMap (`DashMap`) which is accessed by each thread, avoiding the need for a final gathering stage.

|Threads|128MiB (s)|2GiB (s)|
|:---:|:---:|:---:|
|1|800.8|756.0|
|2|849.5|810.6|
|3|740.9|816.2|
|4|681.8|813.1|
|5|624.4|656.0|

>General improvement, but doesn't scale well with more workers as more waiting occurs for `DashMap` insertions.

## 0.3.1

Added two additional different modes for how the data can be combined. Now have three methods:

1. `DashMap` implementation (default)
1. Fan-in to a vector of results, then convert to HashMap using an iter/collect statement
1. Each thread collects a HashMap of results, then HashMaps go through a distribute merge via the rayon `reduce` statement.

In case 2 and 3, `AHashMap` is used for a drop-in faster replacement to `HashMap`.

```bash
parallel -j 2 'time (./target/release/parallel_decompression decompress -n {1} --mode {2} -i {4}.{3}.zstd -z {4}.{3}.zstd.idx) 2> {1}.{2}.{3}.txt' \
   ::: {1..5} \
   ::: dash-map vector merge \
   ::: 128MiB 2GiB \
   ::: "outputs/nr_202601.accessions"
```

|Threads|128MiB<br />dash-map|<br />vector|<br />merge|2GiB<br />dash-map|<br />vector|<br />merge|
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
|1|820.5|686.3|811.1|758.9|803.6|894.8|
|2|659.0|636.6|862.4|651.0|677.1|811.1|
|3|594.3|655.9|944.7|611.0|674.7|870.2|
|4|553.5|630.4|844.0|603.2|705.7|832.5|
|5|553.9|639.4|817.3|587.7|724.7|775.1|

---
