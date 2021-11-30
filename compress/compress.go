// Package compress provides the generic APIs implemented by parquet compression
// codecs.
//
// https://github.com/apache/parquet-format/blob/master/Compression.md
package compress

import (
	"io"

	"github.com/segmentio/parquet/format"
)

// The Codec interface represents parquet compression codecs implemented by the
// compress sub-packages.
//
// Codec instances must be safe to use concurrently from multiple goroutines.
type Codec interface {
	// Returns the code of the compression codec in the parquet format.
	CompressionCodec() format.CompressionCodec

	// Creates a new Reader decompressing data from the io.Reader passed as
	// argument.
	NewReader(io.Reader) (Reader, error)

	// Creates a new Writer compressing data to the io.Writer passed as
	// argument.
	NewWriter(io.Writer) (Writer, error)
}

// Reader is an extension of io.Reader implemented by all decompressors.
type Reader interface {
	io.Reader
	io.Closer
	Reset(io.Reader) error
}

// Writer is an extension of io.Writer implemented by all compressors.
type Writer interface {
	io.Writer
	io.Closer
	Reset(io.Writer) error
}
