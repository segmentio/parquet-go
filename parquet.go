/*
Package parquet is a re-implementation of the Parquet format specification.

It intends to be efficient, and provide multiple level of abstractions for
reading and writing Parquet files.

Reading

The high-level interface for reading Parquet files record-by-record is
RowReader.

Tooling

This package additionally provides tooling, similar to parquet-tools. The
program is available at ./cmd/ptools.
*/
package parquet
