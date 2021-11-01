package parquet

import (
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
	compressionCodecs     compress.Registry
	compressedPageReaders [8]sync.Pool
)

func init() {
	compressionCodecs.Register(schema.Uncompressed, new(uncompressed.Codec))
	compressionCodecs.Register(schema.Gzip, new(gzip.Codec))
	compressionCodecs.Register(schema.Snappy, new(snappy.Codec))
	compressionCodecs.Register(schema.Brotli, new(brotli.Codec))
	compressionCodecs.Register(schema.Zstd, new(zstd.Codec))
	compressionCodecs.Register(schema.Lz4Raw, new(lz4.Codec))
}

func acquireCompressedPageReader(codec schema.CompressionCodec, page io.Reader) *compressedPageReader {
	r, _ := compressedPageReaders[codec].Get().(*compressedPageReader)
	if r == nil {
		r = &compressedPageReader{codec: codec}
		r.reader, r.err = compressionCodecs.Lookup(codec).NewReader(page)
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
