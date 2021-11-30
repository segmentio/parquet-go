package compress

import (
	"io"

	"github.com/segmentio/parquet/format"
)

type Reader interface {
	io.Reader
	io.Closer
	Reset(io.Reader) error
}

type Writer interface {
	io.Writer
	io.Closer
	Reset(io.Writer) error
}

type Codec interface {
	CompressionCodec() format.CompressionCodec
	NewReader(io.Reader) (Reader, error)
	NewWriter(io.Writer) (Writer, error)
}
