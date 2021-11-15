package uncompressed

import (
	"io"

	"github.com/segmentio/parquet/compress"
	"github.com/segmentio/parquet/format"
)

type Codec struct {
}

func (c *Codec) CompressionCodec() format.CompressionCodec {
	return format.Uncompressed
}

func (c *Codec) NewReader(r io.Reader) (compress.Reader, error) {
	return &reader{r}, nil
}

func (c *Codec) NewWriter(w io.Writer) (compress.Writer, error) {
	return &writer{w}, nil
}

type reader struct{ io.Reader }

func (r *reader) Close() error             { return nil }
func (r *reader) Reset(rr io.Reader) error { r.Reader = rr; return nil }

type writer struct{ io.Writer }

func (w *writer) Close() error             { return nil }
func (w *writer) Reset(ww io.Writer) error { w.Writer = ww; return nil }
