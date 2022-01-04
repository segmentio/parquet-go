# segmentio/parquet

This was an initial experiment from Thomas Pelletier in fall 2020 and gained
steam in fall 2021 as a replacement for JSON storage for trace data in TraceDB.

The goal is to open source this project soon so maybe keep the documentation
focused on usage for public consumption and not necessarily on how Segment is
using this library. That said, we have seen significant storage performance
improvements by replacing protobuf storage on disk with Parquet using the zstd
compression algorithm.

## Specification

Columnar storage allows Parquet to store data more efficiently than, say,
Protobuf. For more information [see the Encoding documentation for
Parquet][encoding-docs].

For details on the parquet format, please [see the Apache
documentation][compression-docs].

[compression-docs]: https://github.com/apache/parquet-format/blob/master/Compression.md
[encoding-docs]: https://github.com/apache/parquet-format/blob/master/Encodings.md

## Usage

As of January 2022 nothing is importing this library though we expect that
TraceDB will import it soon.

## CI

We are using Github Actions for CI.
