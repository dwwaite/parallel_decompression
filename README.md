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

Proof of concept for multi-threaded approach:

```bash
for b in 128MiB 2GiB;
do
    for i in {1..5};
    do
       time ./target/release/parallel_decompression decompress -n ${i} -i outputs/nr_202601.accessions.${b}.zstd  -z outputs/nr_202601.accessions.${b}.zstd.idx
    done
done
```

## v0.2.0

|Threads|Runtime (128MiB)|Runtime (2GiB)|
|:---:|:---:|:---:|
|1|`17m0.573s`|`20m13.515s`|
|2|`15m19.865s`|`19m3.426s`|
|3|`13m57.065s`|`26m10.589s`|
|4|`22m42.528s`|`15m18.410s`|
|5|`15m23.290s`|`15m4.741s`|

>Problem: Far too much time is spent at the end, draining each partial `AHashMap` into the final map. Redesign with a shared HashMap (`DashMap`) and measure performance change.

## v0.3.0

|Threads|Runtime (128MiB)|Runtime (2GiB)|
|:---:|:---:|:---:|
|1|`13m20.762s`|`12m35.962s`|
|2|`14m9.498s`|`13m30.579s`|
|3|`12m20.938s`|`13m36.170s`|
|4|`11m21.766s`|`13m33.092s`|
|5|`10m24.358s`|`10m55.957s`|

---
