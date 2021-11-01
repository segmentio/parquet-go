package snappy

import (
	"io"

	"github.com/klauspost/compress/snappy"
	"github.com/segmentio/parquet/compress"
)

type Codec struct {
}

func (c *Codec) NewReader(r io.Reader) (compress.Reader, error) {
	return reader{snappy.NewReader(r)}, nil
}

func (c *Codec) NewWriter(w io.Writer) (compress.Writer, error) {
	return writer{snappy.NewWriter(w)}, nil
}

type reader struct{ *snappy.Reader }

func (r reader) Close() error             { return nil }
func (r reader) Reset(rr io.Reader) error { r.Reader.Reset(rr); return nil }

type writer struct{ *snappy.Writer }

func (w writer) Reset(ww io.Writer) error { w.Writer.Reset(ww); return nil }
