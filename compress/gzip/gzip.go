// Package gzip implements the GZIP parquet compression codec.
package gzip

import (
	"io"
	"strings"

	"github.com/klauspost/compress/gzip"
	"github.com/segmentio/parquet/compress"
	"github.com/segmentio/parquet/format"
)

const (
	emptyGzip = "\x1f\x8b\b\x00\x00\x00\x00\x00\x02\xff\x01\x00\x00\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00"
)

const (
	NoCompression      = gzip.NoCompression
	BestSpeed          = gzip.BestSpeed
	BestCompression    = gzip.BestCompression
	DefaultCompression = gzip.DefaultCompression
	HuffmanOnly        = gzip.HuffmanOnly
)

type Codec struct {
	Level int
}

func (c *Codec) String() string {
	return "GZIP"
}

func (c *Codec) CompressionCodec() format.CompressionCodec {
	return format.Gzip
}

func (c *Codec) NewReader(r io.Reader) (compress.Reader, error) {
	if r == nil {
		r = strings.NewReader(emptyGzip)
	}
	z, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	return reader{z}, nil
}

func (c *Codec) NewWriter(w io.Writer) (compress.Writer, error) {
	if w == nil {
		w = io.Discard
	}
	z, err := gzip.NewWriterLevel(w, c.Level)
	if err != nil {
		return nil, err
	}
	return writer{z}, nil
}

type reader struct{ *gzip.Reader }

func (r reader) Reset(rr io.Reader) error {
	if rr == nil {
		rr = strings.NewReader(emptyGzip)
	}
	return r.Reader.Reset(rr)
}

type writer struct{ *gzip.Writer }

func (w writer) Reset(ww io.Writer) error {
	if ww == nil {
		ww = io.Discard
	}
	w.Writer.Reset(ww)
	return nil
}
