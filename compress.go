package parquet

import (
	"fmt"
	"io"
	"runtime"
	"sync"

	"github.com/segmentio/parquet/compress"
	"github.com/segmentio/parquet/compress/brotli"
	"github.com/segmentio/parquet/compress/gzip"
	"github.com/segmentio/parquet/compress/lz4"
	"github.com/segmentio/parquet/compress/snappy"
	"github.com/segmentio/parquet/compress/uncompressed"
	"github.com/segmentio/parquet/compress/zstd"
	"github.com/segmentio/parquet/schema"
)

var (
	compressedPageReaders = [8]sync.Pool{}
	compressionCodecs     = [8]compress.Codec{
		schema.Uncompressed: new(uncompressed.Codec),
		schema.Gzip:         new(gzip.Codec),
		schema.Snappy:       new(snappy.Codec),
		schema.Brotli:       new(brotli.Codec),
		schema.Zstd:         new(zstd.Codec),
		schema.Lz4Raw:       new(lz4.Codec),
	}
)

func lookupCompressionCodec(codec schema.CompressionCodec) compress.Codec {
	if codec >= 0 && int(codec) < len(compressionCodecs) {
		if c := compressionCodecs[codec]; c != nil {
			return c
		}
	}
	return &unsupported{codec}
}

func acquireCompressedPageReader(codec schema.CompressionCodec, page io.Reader) *compressedPageReader {
	r, _ := compressedPageReaders[codec].Get().(*compressedPageReader)
	if r == nil {
		r = &compressedPageReader{codec: codec}
		r.reader, r.err = lookupCompressionCodec(codec).NewReader(page)
		runtime.SetFinalizer(r, func(r *compressedPageReader) { r.Close() })
	} else {
		r.Reset(page)
	}
	return r
}

func releaseCompressedPageReader(r *compressedPageReader) {
	r.Reset(nil)
	compressedPageReaders[r.codec].Put(r)
}

type compressedPageReader struct {
	codec  schema.CompressionCodec
	reader compress.Reader
	err    error
}

func (r *compressedPageReader) Close() error {
	return r.reader.Close()
}

func (r *compressedPageReader) Read(b []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	return r.reader.Read(b)
}

func (r *compressedPageReader) Reset(page io.Reader) {
	r.err = r.reader.Reset(page)
}

type unsupported struct{ codec schema.CompressionCodec }

func (u *unsupported) NewReader(r io.Reader) (compress.Reader, error) {
	return unsupportedReader{u}, nil
}

func (u *unsupported) NewWriter(w io.Writer) (compress.Writer, error) {
	return unsupportedWriter{u}, nil
}

func (u *unsupported) error() error {
	return fmt.Errorf("unsupported compression codec: %s", u.codec)
}

type unsupportedReader struct{ *unsupported }

func (r unsupportedReader) Close() error               { return nil }
func (r unsupportedReader) Reset(io.Reader) error      { return nil }
func (r unsupportedReader) Read(b []byte) (int, error) { return 0, r.error() }

type unsupportedWriter struct{ *unsupported }

func (w unsupportedWriter) Close() error                { return nil }
func (w unsupportedWriter) Flush() error                { return nil }
func (w unsupportedWriter) Reset(io.Writer) error       { return nil }
func (w unsupportedWriter) Write(b []byte) (int, error) { return 0, w.error() }
