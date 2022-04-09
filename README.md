# segmentio/parquet-go [![build status](https://github.com/segmentio/parquet-go/actions/workflows/test.yml/badge.svg)](https://github.com/segmentio/parquet-go/actions) [![Go Report Card](https://goreportcard.com/badge/github.com/segmentio/parquet-go)](https://goreportcard.com/report/github.com/segmentio/parquet-go) [![Go Reference](https://pkg.go.dev/badge/github.com/segmentio/parquet-go.svg)](https://pkg.go.dev/github.com/segmentio/parquet-go)

High-performance Go library to manipulate parquet files.

## Motivation

Parquet has been established as a powerful solution to represent columnar data
on persistent storage mediums, achieving levels of compression and query
performance that enable managing data sets at scales that reach the petabytes.
In addition, having intensive data applications sharing a common format creates
opportunities for interoperation in our tool kits, providing greater leverage
and value to engineers maintaining and operating those systems.

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
go get github.com/segmentio/parquet-go
```

Go 1.17 or later is required to use the package.

### Compatibility Guarantees

The package is currently released as a pre-v1 version, which gives maintainers
the freedom to break backward compatibility to help improve the APIs as we learn
which initial design decisions would need to be revisited to better support the
use cases that the library solves for. These occurrences are expected to be rare
in frequency and documentation will be produce to guide users on how to adapt
their programs to breaking changes.

One planned breaking change will be to support generics, which were released in
Go 1.18 and will simplify the code and provide better type safety.
The targetted timeline for this change is end of 2022; programs depending on
parquet-go will be expected to compile with Go 1.18 by then, at which point
we will drop compatibility with Go 1.17.

## Usage

The following sections describe how to use APIs exposed by the library,
highlighting the use cases with code examples to demonstrate how they are used
in practice.

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
_, err := parquet.CopyRows(writer, buffer.Rows())
if err != nil {
    ...
}
if err := writer.Close(); err != nil {
    ...
}
```

### Merging Row Groups: [parquet.MergeRowGroups](https://pkg.go.dev/github.com/segmentio/parquet-go#MergeRowGroups)

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

### Using Bloom Filters: [parquet.BloomFilter](https://pkg.go.dev/github.com/segmentio/parquet-go#BloomFilter)

Parquet files can embed bloom filters to help improve the performance of point
lookups in the files. The format of parquet bloom filters is documented in
the parquet specification: [Parquet Bloom Filter](https://github.com/apache/parquet-format/blob/master/BloomFilter.md)

By default, no bloom filters are created in parquet files, but applications can
configure the list of columns to create filters for using the `parquet.BloomFilters`
option when instantiating writers; for example:

```go
writer := parquet.NewWriter(output,
    parquet.BloomFilters(
        // Configures the write to generate split-block bloom filters for the
        // "first_name" and "last_name" columns of the parquet schema of rows
        // witten by the application.
        parquet.SplitBlockFilter("first_name"),
        parquet.SplitBlockFilter("last_name"),
    ),
)
...
```

Generating bloom filters requires to know how many values exist in a column
chunk in order to properly size the filter, which requires buffering all the
values written to the column in memory. Because of it, the memory footprint
of `parquet.Writer` increases linearly with the number of columns that the
writer needs to generate filters for. This extra cost is optimized away when
rows are copied from a `parquet.Buffer` to a writer, since in this case the
number of values per column in known since the buffer already holds all the
values in memory.

When reading parquet files, column chunks expose the generated bloom filters
with the `parquet.ColumnChunk.BloomFilter` method, returning a
`parquet.BloomFilter` instance if a filter was available, or `nil` when there
were no filters.

Using bloom filters in parquet files is useful when performing point-lookups in
parquet files; searching for column rows matching a given value. Programs can
quickly eliminate column chunks that they know does not contain the value they
search for by checking the filter first, which is often multiple orders of
magnitude faster than scanning the column.

The following code snippet hilights how filters are typically used:

```go
var candidateChunks []parquet.ColumnChunk

for i, n := 0, file.NumRowGroups(); i < n; i++ {
    columnChunk := file.RowGroup(i).Column(columnIndex)
    bloomFilter := columnChunk.BloomFilter()

    if bloomFilter != nil {
        if ok, err := bloomFilter.Check(value); err != nil {
            ...
        } else if !ok {
            // Bloom filters may return false positives, but never return false
            // negatives, we know this column chunk does not contain the value.
            continue
        }
    }

    candidateChunks = append(candidateChunks, columnChunk)
}
```

## Optimizations

The following sections describe common optimization techniques supported by the
library.

### Optimizing Reads

Lower level APIs used to read parquet files offer more efficient ways to access
column values. Consecutive sequences of values are grouped into pages which are
represented by the `parquet.Page` interface.

A column chunk may contain multiple pages, each holding a section of the column
values. Applications can retrieve the column values either by reading them into
buffers of `parquet.Value`, or type asserting the pages to read arrays of
primitive Go values. The following example demonstrates how to use both
mechanisms to read column values:

```go
pages := column.Pages()

for {
    p, err := pages.ReadPage()
    if err != nil {
        ... // io.EOF when there are no more pages
    }

    switch page := p.Values().(type) {
    case parquet.Int32Page:
        values := make([]int32, page.NumValues())
        _, err := page.ReadInt32s(values)
        ...
    case parquet.Int64Page:
        values := make([]int64, page.NumValues())
        _, err := page.ReadInt64s(values)
        ...
    default:
        values := make([]parquet.Value, page.NumValues())
        _, err := page.ReadValues(values)
        ...
    }
}
```

Reading arrays of typed values is often preferable when performing aggregations
on the values as this model offers a more compact representation of the values
in memory, and pairs well with the use of optimizations like SIMD vectorization.

### Optimizing Writes

Applications that deal with columnar storage are sometimes designed to work with
columnar data throughout the abstraction layers; it then becomes possible to
write columns of values directly instead of reconstructing rows from the column
values. The package offers two main mechanisms to satisfy those use cases:

#### A. Writing Columns of Typed Arrays

The first solution assumes that the program works with in-memory arrays of typed
values, for example slices of primitive Go types like `[]float32`; this would be
the case if the application is built on top of a framework like
[Apache Arrow](https://pkg.go.dev/github.com/apache/arrow/go/arrow).

`parquet.Buffer` is an implementation of the `parquet.RowGroup` interface which
maintains in-memory buffers of column values. Rows can be written by either
boxing primitive values into arrays of `parquet.Value`, or type asserting the
columns to a access specialized versions of the write methods accepting arrays
of Go primitive types.

When using either of these models, the application is responsible for ensuring
that the same number of rows are written to each column or the resulting parquet
file will be malformed.

The following examples demonstrate how to use these two models to write columns
of Go values:

```go
func writeColumns(buffer *parquet.Buffer, columns [3][]interface{}) error {
    values := make([]parquet.Value, len(columns[0]))
    for i := range columns {
        c := buffer.ColumnBuffer(i)
        for j, v := range columns[i] {
            values[j] = parquet.ValueOf(v)
        }
        if _, err := c.WriteValues(values); err != nil {
            return err
        }
    }
    return nil
}
```

```go
func writeColumns(buffer *parquet.Buffer, ids []int64, values []float32) error {
    if len(ids) != len(values) {
        return fmt.Errorf("number of ids and values mismatch: ids=%d values=%d", len(ids), len(values))
    }
    col0 := buffer.ColumnBuffer(0)
    col1 := buffer.ColumnBuffer(1)
    if err := col0.(parquet.Int64Writer).WriteInt64s(ids); err != nil {
        return err
    }
    if err := col1.(parquet.FloatWriter).WriteFloats(values); err != nil {
        return err
    }
    return nil
}
```

The latter is more efficient as it does not require boxing the input into an
intermediary array of `parquet.Value`. However, it may not always be the right
model depending on the situation, sometimes the generic abstraction can be a
more expressive model.

#### B. Implementing parquet.RowGroup

Programs that need full control over the construction of row groups can choose
to provide their own implementation of the `parquet.RowGroup` interface, which
includes defining implementations of `parquet.ColumnChunk` and `parquet.Page`
to expose column values of the row group.

This model can be preferable when the underlying storage or in-memory
representation of the data needs to be optimized further than what can be
achieved by using an intermediary buffering layer with `parquet.Buffer`.

See [parquet.RowGroup](https://pkg.go.dev/github.com/segmentio/parquet-go#RowGroup)
for the full interface documentation.

#### C. Using on-disk page buffers

When generating parquet files, the writer needs to buffer all pages before it
can create the row group. This may require significant amounts of memory as the
entire file content must be buffered prior to generating it. In some cases, the
files might even be larger than the amount of memory available to the program.

The `parquet.Writer` can be configured to use disk storage instead as a scratch
buffer when generating files, by configuring a different page buffer pool using
the `parquet.ColumnPageBuffers` option and `parquet.PageBufferPool` interface.

The parquet-go package provides an implementation of the interface which uses
temporary files to store pages while a file is generated, allowing programs to
use local storage as swap space to hold pages and keep memory utilization to a
minimum. The following example demonstrates how to configure a parquet writer
to use on-disk page buffers:

```go
writer := parquet.NewWriter(output,
    parquet.ColumnPageBuffers(
        parquet.NewFileBufferPool("", "buffers.*"),
    ),
)
```

When a row group is complete, pages buffered to disk need to be copied back to
the output file. This results in doubling I/O operations and storage space
requirements (the system needs to have enough free disk space to hold two copies
of the file). The resulting write amplification can often be optimized away by
the kernel if the file system supports copy-on-write of disk pages since copies
between `os.File` instances are optimized using `copy_file_range(2)` (on linux).

See [parquet.PageBufferPool](https://pkg.go.dev/github.com/segmentio/parquet-go#PageBufferPool)
for the full interface documentation.

## Maintenance

The project is hosted and maintained by Twilio; we welcome external contributors
to participate in the form of discussions or code changes. Please review to the
[Contribution](./CONTRIBUTING.md) guidelines as well as the [Code of Condution](./CODE_OF_CONDUCT.md)
before submitting contributions.

### Continuous Integration

The project uses [Github Actions](https://github.com/segmentio/parquet-go/actions) for CI.
