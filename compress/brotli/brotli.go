// Package brotli implements the BROTLI parquet compression codec.
package brotli

import (
	"io"

	"github.com/andybalholm/brotli"
	"github.com/segmentio/parquet-go/compress"
	"github.com/segmentio/parquet-go/format"
)

const (
	DefaultQuality = 0
	DefaultLGWin   = 0
)

type Codec struct {
	// Quality controls the compression-speed vs compression-density trade-offs.
	// The higher the quality, the slower the compression. Range is 0 to 11.
	Quality int
	// LGWin is the base 2 logarithm of the sliding window size.
	// Range is 10 to 24. 0 indicates automatic configuration based on Quality.
	LGWin int
}

func (c *Codec) String() string {
	return "BROTLI"
}

func (c *Codec) CompressionCodec() format.CompressionCodec {
	return format.Brotli
}

func (c *Codec) NewReader(r io.Reader) (compress.Reader, error) {
	return reader{brotli.NewReader(r)}, nil
}

func (c *Codec) NewWriter(w io.Writer) (compress.Writer, error) {
	opts := brotli.WriterOptions{
		Quality: c.Quality,
		LGWin:   c.LGWin,
	}
	return writer{brotli.NewWriterOptions(w, opts)}, nil
}

type reader struct{ *brotli.Reader }

func (r reader) Close() error { return nil }

type writer struct{ *brotli.Writer }

func (w writer) Reset(ww io.Writer) error { w.Writer.Reset(ww); return nil }
