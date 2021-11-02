package lz4

import (
	"io"

	"github.com/pierrec/lz4/v4"
	"github.com/segmentio/parquet/compress"
)

type Codec struct {
}

func (c *Codec) NewReader(r io.Reader) (compress.Reader, error) {
	return reader{lz4.NewReader(r)}, nil
}

func (c *Codec) NewWriter(w io.Writer) (compress.Writer, error) {
	return writer{lz4.NewWriter(w)}, nil
}

type reader struct{ *lz4.Reader }

func (r reader) Close() error             { return nil }
func (r reader) Reset(rr io.Reader) error { r.Reader.Reset(rr); return nil }

type writer struct{ *lz4.Writer }

func (w writer) Reset(ww io.Writer) error { w.Writer.Reset(ww); return nil }
