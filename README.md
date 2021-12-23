# segmentio/parquet

High-performance Go library to manipulate parquet files.

## Motivations

Parquet has been established as a powerful solution to representing columnar
data on persistent storage mediums, achieving levels of compression and query
performance that enable managing data sets at scales that reach the petabytes.
More importantly, having intensive data application sharing a common format
creates opportunities for interperability and composability in our tool kits,
providing greater leverage and value to engineers maintaining and operating
those systems.

The creation and evolution of large scale data management system, combined with
realtime requirements come with the need for both powerful APIs and challenging
performance constraints; requirements that existing solutions to use parquet
with Go were not addressing unfortunately.

The `segmentio/parquet` package was designed and developed to respond to those
challenges, offering high level APIs to read and write parquet files, while
keeping a low compute and memory footprint in order to be used in environments
where data volumes and cost constraints require software to achieve high levels
of efficiency.

## Installation

The package is distributed as a standard Go module that programs can take a
dependency on and install with the following command:

```
$ go get github.com/segmentio/parquet
```

Note that Go 1.17 or later is required to use the package.

## Usage

### Writing Parquet Files: [parquet.Writer]()

A parquet file is a collection of rows sharing the same schema, arranged in
columns to support faster scan operations on subsets of the data set.

The `parquet.Schema` type is a in-memory representation of the schema of parquet
rows, and is translated from the type of Go values.

The `parquet.Writer` type denormalizes rows into columns, then encodes the
columns into a parquet file, generating row groups, column chunks, and pages
based on configurable heuristics.

```go
schema := parquet.SchemaOf(rows[0])
writer := parquet.NewWriter(file, schema)

for _, row := range rows {
    if err := writer.WriteRow(row); err != nil {
        ...
    }
}

if err := writer.Close(); err != nil {
    ...
}
```

### Reading Parquet Files: [parquet.Reader]()

The `parquet.Reader` type supports reading rows from parquet files into Go
values. When reading rows, the schema is already determined by metadata within
the file; the reader knows how to leverage this information so the application
does not need to explicitly declare the schema of values that will be read.
However, the reader will validate that the schemas of the file and Go value
are compatible.

This example shows how a `parquet.Reader` is typically used:

```go
reader := parquet.NewReader(file)

for {
    row := new(RowType)
    err := reader.ReadRow(row)
    if err != nil {
        if err == io.EOF {
            break
        }
        ...
    }
    ...
}
```

### Inspecting Parquet Files: [parquet.File]()

Sometimes, lower-level APIs can be useful to leverage the columnar layout of
parquet files. The `parquet.File` type is intended to provide such features to
Go applications, by exposing APIs to iterate over the various parts of a
parquet file.

```go
f, err := parquet.OpenFile(file, size)
if err != nil {
    ...
}

// TODO

```
