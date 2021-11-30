package parquet

import (
	"fmt"
	"io"
	"runtime"
	"sort"
	"sync"

	"github.com/segmentio/parquet/compress"
	"github.com/segmentio/parquet/compress/brotli"
	"github.com/segmentio/parquet/compress/gzip"
	"github.com/segmentio/parquet/compress/lz4"
	"github.com/segmentio/parquet/compress/snappy"
	"github.com/segmentio/parquet/compress/uncompressed"
	"github.com/segmentio/parquet/compress/zstd"
	"github.com/segmentio/parquet/format"
)

var (
	// Uncompressed is a parquet compression codec representing uncompressed
	// pages.
	Uncompressed uncompressed.Codec

	// Snappy is the SNAPPY parquet compression codec.
	Snappy snappy.Codec

	// Gzip is the GZIP parquet compression codec.
	Gzip = gzip.Codec{
		Level: gzip.DefaultCompression,
	}

	// Brotli is the BROTLI parquet compression codec.
	Brotli = brotli.Codec{
		Quality: brotli.DefaultQuality,
		LGWin:   brotli.DefaultLGWin,
	}

	// Zstd is the ZSTD parquet compression codec.
	Zstd = zstd.Codec{
		Level:       zstd.DefaultLevel,
		Concurrency: zstd.DefaultConcurrency,
	}

	// Lz4Raw is the LZ4_RAW parquet compression codec.
	Lz4Raw = lz4.Codec{
		BlockSize:   lz4.DefaultBlockSize,
		Level:       lz4.DefaultLevel,
		Concurrency: lz4.DefaultConcurrency,
	}

	// Table of compression codecs indexed by their code in the parquet format.
	compressionCodecs = [...]compress.Codec{
		format.Uncompressed: &Uncompressed,
		format.Snappy:       &Snappy,
		format.Gzip:         &Gzip,
		format.Brotli:       &Brotli,
		format.Zstd:         &Zstd,
		format.Lz4Raw:       &Lz4Raw,
	}

	// Pools of compressed page readers used to retain compression codecs across
	// page reads to reduce the compute and memory footprint of creating new
	// decompressors for every new page read in a parquet file.
	compressedPageReaders = [len(compressionCodecs)]sync.Pool{}
)

func lookupCompressionCodec(codec format.CompressionCodec) compress.Codec {
	if codec >= 0 && int(codec) < len(compressionCodecs) {
		if c := compressionCodecs[codec]; c != nil {
			return c
		}
	}
	return &unsupported{codec}
}

func acquireCompressedPageReader(codec format.CompressionCodec, page io.Reader) *compressedPageReader {
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
	codec  format.CompressionCodec
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

type unsupported struct{ codec format.CompressionCodec }

func (u *unsupported) CompressionCodec() format.CompressionCodec {
	return u.codec
}

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

func sortCodecs(codecs []compress.Codec) {
	if len(codecs) > 1 {
		sort.Slice(codecs, func(i, j int) bool {
			return codecs[i].CompressionCodec() < codecs[j].CompressionCodec()
		})
	}
}

func dedupeSortedCodecs(codecs []compress.Codec) []compress.Codec {
	if len(codecs) > 1 {
		i := 0

		for _, c := range codecs[1:] {
			if c.CompressionCodec() != codecs[i].CompressionCodec() {
				i++
				codecs[i] = c
			}
		}

		codecs = codecs[:i+1]
	}
	return codecs
}
