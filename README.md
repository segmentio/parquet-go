# segmentio/parquet-go ![build status](https://github.com/segmentio/parquet-go/actions/workflows/test.yml/badge.svg) [![GoDoc](https://pkg.go.dev/github.com/segmentio/parquet-go?status.svg)](https://pkg.go.dev/github.com/segmentio/parquet-go)

High-performance Go library to manipulate parquet files.

## Motivation

Parquet has been established as a powerful solution to represent columnar data
on persistent storage mediums, achieving levels of compression and query
performance that enable managing data sets at scales that reach the petabytes.
In addition, having intensive data applications sharing a common format creates
opportunities for interperation and composition in our tool kits, providing
greater leverage and value to engineers maintaining and operating those systems.

The creation and evolution of large scale data management systems, combined with
realtime expectations come with challenging maintenance and performance
requirements, that existing solutions to use parquet with Go were not addressing.

The `segmentio/parquet-go` package was designed and developed to respond to those
challenges, offering high level APIs to read and write parquet files, while
keeping a low compute and memory footprint in order to be used in environments
where data volumes and cost constraints require software to achieve high levels
of efficiency.

## Specification

Columnar storage allows Parquet to store data more efficiently than, say,
using JSON or Protobuf. For more information, refer to the [Parquet Format Specification](https://github.com/apache/parquet-format).

## Installation

The package is distributed as a standard Go module that programs can take a
dependency on and install with the following command:

```
$ go get github.com/segmentio/parquet-go
```

Go 1.17 or later is required to use the package.

## Usage

### Writing Parquet Files: [parquet.Writer](https://pkg.go.dev/github.com/segmentio/parquet-go#Writer)

A parquet file is a collection of rows sharing the same schema, arranged in
columns to support faster scan operations on subsets of the data set.

The `parquet.Writer` type denormalizes rows into columns, then encodes the
columns into a parquet file, generating row groups, column chunks, and pages
based on configurable heuristics.

```go
writer := parquet.NewWriter(file)

for _, row := range rows {
    if err := writer.Write(row); err != nil {
        ...
    }
}

// Closing the writer is necessary to flush buffers and write the file footer.
if err := writer.Close(); err != nil {
    ...
}
```

By default, the writer will lazily determine the schema of rows by introspecting
the struct types of row values.

Explicit declaration of the parquet schema on a writer is useful when the
application needs to ensure that data written to a file adheres to a predefined
schema and the rows come from dynamic or user input. The `parquet.Schema` type
is a in-memory representation of the schema of parquet rows, translated from the
type of Go values, and can be used for this purpose.

```go
schema := parquet.SchemaOf(rows[0])
writer := parquet.NewWriter(file, schema)
...
```

### Reading Parquet Files: [parquet.Reader](https://pkg.go.dev/github.com/segmentio/parquet-go#Reader)

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
    err := reader.Read(row)
    if err != nil {
        if err == io.EOF {
            break
        }
        ...
    }
    ...
}
```

The expected schema of rows can be explicitly declared when the reader is
constructed, which is useful to ensure that the program receives rows matching
an specific format; for example, when dealing with files from remote storage
sources that applications cannot trust to have used an expected schema.

Configuring the schema of a reader is done by passing a `parquet.Schema`
instance as argument when constructing a reader. When the schema is declared,
conversion rules implemented by the package are applied to ensure that rows
read by the application match the desired format (see **Evolving Parquet Schemas**).

```go
schema := parquet.SchemaOf(new(RowType))
reader := parquet.NewReader(file, schema)
...
```

### Inspecting Parquet Files: [parquet.File](https://pkg.go.dev/github.com/segmentio/parquet-go#File)

Sometimes, lower-level APIs can be useful to leverage the columnar layout of
parquet files. The `parquet.File` type is intended to provide such features to
Go applications, by exposing APIs to iterate over the various parts of a
parquet file.

```go
f, err := parquet.OpenFile(file, size)
if err != nil {
    ...
}

numRowGroups := f.NumRowGroups()
for i := 0; i < numRowGroups; i++ {
    rowGroup := f.RowGroup(i)

    numColumns := rowGroup.NumColumns()
    for j := 0; j < numColumns; j++ {
        columnChunk := rowGroup.Column(j)
        ...
    }
}
```

### Evolving Parquet Schemas: [parquet.Convert](https://pkg.go.dev/github.com/segmentio/parquet-go#Convert)

Parquet files embed all the metadata necessary to interpret their content,
including a description of the schema of the tables represented by the rows and
columns they contain.

Parquet files are also immutable; once written, there is not mechanism for
_updating_ a file. If their contents need to be changed, rows must be read,
modified, and written to a new file.

Because applications evolve, the schema written to parquet files also tend to
evolve over time. Those requirements creating challenges when applications need
to operate on parquet files with heterogenous schemas: algorithms that expect
new columns to exist may have issues dealing with rows that come from files with
mismatching schema versions.

To help build applications that can handle evolving schemas, `segmentio/parquet-go`
implements conversion rules that create views of row groups to translate between
schema versions.

The `parquet.Convert` function is the low-level routine constructing conversion
rules from a source to a target schema. The function is used to build converted
views of `parquet.RowReader` or `parquet.RowGroup`, for example:

```go
source := parquet.NewSchema(&RowTypeV1{})
target := parquet.NewSchema(&RowTypeV2{})

conversion, err := parquet.Convert(target, source)
if err != nil {
    ...
}

targetRowGroup := parquet.ConvertRowGroup(sourceRowGroup, conversion)
...
```

Conversion rules are automatically applied by the `parquet.CopyRows` function
when the reader and writers passed to the function also implement the
`parquet.RowReaderWithSchema` and `parquet.RowWriterWithSchema` interfaces.
The copy determines whether the reader and writer schemas can be converted from
one to the other, and automatically applies the conversion rules to facilitate
the translation between schemas.

At this time, conversion rules only supports adding or removing columns from
the schemas, there are no type conversions performed, nor ways to rename
columns, etc... More advanced conversion rules may be added in the future.

### Sorting Row Groups: [parquet.Buffer](https://pkg.go.dev/github.com/segmentio/parquet-go#Buffer)

The `parquet.Writer` type is optimized for minimal memory usage, keeping the
order rows unchanged and flushing pages as soon as they are filled.

Parquet supports expressing columns by which rows are sorted through the
declaration of _sorting columns_ on row groups. Sorting row groups requires
buffering all rows before ordering and writing them to a parquet file.

To help with those use cases, the `segmentio/parquet-go` package exposes the
`parquet.Buffer` type which acts as a buffer of rows and implements
`sort.Interface` to allow applications to sort rows prior to writing them
to a file.

The columns that rows are ordered by are configured when creating
`parquet.Buffer` instances using the `parquet.SortingColumns` function to
construct row group options configuring the buffer. The type of parquet columns
defines how values are compared, see [Parquet Logical Types](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md)
for details.

When written to a file, the buffer is materialized into a single row group with
the declared sorting columns. After being written, buffers can be reused by
calling their `Reset` method.

The following example shows how to use a `parquet.Buffer` to order rows written
to a parquet file:

```go
buffer := parquet.NewBuffer(
    parquet.SortingColumns(
        parquet.Ascending("LastName"),
        parquet.Ascending("FistName"),
    ),
)

buffer.Write(&Character{FirstName: "Luke", LastName: "Skywalker"})
buffer.Write(&Character{FirstName: "Han", LastName: "Solo"})
buffer.Write(&Character{FirstName: "Anakin", LastName: "Skywalker"})

sort.Sort(buffer)

writer := parquet.NewWriter(output)
_, err := parquet.CopyRows(writer, buffer)
if err != nil {
    ...
}
if err := writer.Close(); err != nil {
    ...
}
```

### Merging Row Groups: [parquet.MergeRowGroups](http://pkg.go.dev/github.com/segmentio/parquet-go#MergeRowGroups)

Parquet files are often used as part of the underlying engine for data
processing or storage layers, in which cases merging multiple row groups
into one that contains more rows can be a useful operation to improve query
performance; for example, bloom filters in parquet files are stored for each
row group, the larger the row group, the fewer filters need to be stored and
the more effective they become.

The `segmentio/parquet-go` package supports creating merged views of row groups,
where the view contains all the rows of the merged groups, maintaining the order
defined by the sorting columns of the groups.

There are a few constraints when merging row groups:

* The sorting columns of all the row groups must be the same, or the merge
  operation must be explicitly configured a set of sorting columns which are
  a prefix of the sorting columns of all merged row groups.

* The schemas of row groups must all be equal, or the merge operation must
  be explicitly configured with a schema that all row groups can be converted
  to, in which case the limitations of schema conversions apply.

Once a merged view is created, it may be written to a new parquet file or buffer
in order to create a larger row group:

```go
merge, err := parquet.MergeRowGroups(rowGroups)
if err != nil {
    ...
}

writer := parquet.NewWriter(output)
_, err := parquet.CopyRows(writer, merge)
if err != nil {
    ...
}
if err := writer.Close(); err != nil {
    ...
}
```

## Maintenance

The project is hosted and maintained by Twilio; we welcome external contributors
to participate in the form of discussions or code changes. Please review to the
[Contribution](./CONTRIBUTING.md) guidelines as well as the [Code of Condution](./CODE_OF_CONDUCT.md)
before submitting contributions.

### Continuous Integration

The project uses [Github Actions](https://github.com/segmentio/parquet-go/actions) for CI.
