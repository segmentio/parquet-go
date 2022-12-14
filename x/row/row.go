// Package row contains experimental extensions to the core parquet package
// which are oriented towards working with parquet rows.
//
// In particular, this package contains the impplementation of an alternative
// row-oriented file format designed to efficiently store and load sequences
// of full parquet rows. This format is NOT COMPATIBLE with parquet and NOT
// PART of the parquet specification. The use of this format is only intended
// to support use cases where applications intend to generate temporary files
// accessed in a row format rather than as columns.
package row
