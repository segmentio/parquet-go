package brotli

import (
	"io"

	"github.com/andybalholm/brotli"
	"github.com/segmentio/parquet/compress"
)

type Codec struct {
}

func (c *Codec) NewReader(r io.Reader) (compress.Reader, error) {
	return reader{brotli.NewReader(r)}, nil
}

func (c *Codec) NewWriter(w io.Writer) (compress.Writer, error) {
	return writer{brotli.NewWriter(w)}, nil
}

type reader struct{ *brotli.Reader }

func (r reader) Close() error { return nil }

type writer struct{ *brotli.Writer }

func (w writer) Reset(ww io.Writer) error { w.Writer.Reset(ww); return nil }
