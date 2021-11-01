package compress

import (
	"fmt"
	"io"
	"sync/atomic"

	"github.com/segmentio/parquet/schema"
)

type Reader interface {
	io.Reader
	io.Closer
	Reset(io.Reader) error
}

type Writer interface {
	io.Writer
	io.Closer
	Flush() error
	Reset(io.Writer) error
}

type Codec interface {
	NewReader(io.Reader) (Reader, error)
	NewWriter(io.Writer) (Writer, error)
}

type Registry struct {
	// The compression codes go from 0 to 7 (see parquet/schema).
	codecs [8]atomic.Value
}

func (r *Registry) Register(c schema.CompressionCodec, x Codec) {
	r.codecs[c].Store(x)
}

func (r *Registry) Lookup(c schema.CompressionCodec) Codec {
	if c >= 0 && int(c) < len(r.codecs) {
		x, _ := r.codecs[c].Load().(Codec)
		if x != nil {
			return x
		}
	}
	return &unsupported{c}
}

type unsupported struct{ codec schema.CompressionCodec }

func (u *unsupported) NewReader(r io.Reader) (Reader, error) { return unsupportedReader{u}, nil }
func (u *unsupported) NewWriter(w io.Writer) (Writer, error) { return unsupportedWriter{u}, nil }
func (u *unsupported) error() error                          { return fmt.Errorf("unsupported compression codec: %s", u.codec) }

type unsupportedReader struct{ *unsupported }

func (r unsupportedReader) Close() error               { return nil }
func (r unsupportedReader) Reset(io.Reader) error      { return nil }
func (r unsupportedReader) Read(b []byte) (int, error) { return 0, r.error() }

type unsupportedWriter struct{ *unsupported }

func (w unsupportedWriter) Close() error                { return nil }
func (w unsupportedWriter) Flush() error                { return nil }
func (w unsupportedWriter) Reset(io.Writer) error       { return nil }
func (w unsupportedWriter) Write(b []byte) (int, error) { return 0, w.error() }
