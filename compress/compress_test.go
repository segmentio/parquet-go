package compress_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/segmentio/parquet-go/compress"
	"github.com/segmentio/parquet-go/compress/brotli"
	"github.com/segmentio/parquet-go/compress/gzip"
	"github.com/segmentio/parquet-go/compress/lz4"
	"github.com/segmentio/parquet-go/compress/snappy"
	"github.com/segmentio/parquet-go/compress/uncompressed"
	"github.com/segmentio/parquet-go/compress/zstd"
)

var tests = [...]struct {
	scenario string
	codec    compress.Codec
}{
	{
		scenario: "uncompressed",
		codec:    new(uncompressed.Codec),
	},

	{
		scenario: "snappy",
		codec:    new(snappy.Codec),
	},

	{
		scenario: "gzip",
		codec:    new(gzip.Codec),
	},

	{
		scenario: "brotli",
		codec:    new(brotli.Codec),
	},

	{
		scenario: "zstd",
		codec:    new(zstd.Codec),
	},

	{
		scenario: "lz4",
		codec:    new(lz4.Codec),
	},
}

var testdata = bytes.Repeat([]byte("1234567890qwertyuiopasdfghjklzxcvbnm"), 10e3)

func TestCompressionCodec(t *testing.T) {
	buffer := make([]byte, 0, len(testdata))
	output := make([]byte, 0, len(testdata))

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			const N = 10
			// Run the test multiple times to exercise codecs that maintain
			// state across compression/decompression.
			for i := 0; i < N; i++ {
				var err error

				buffer, err = test.codec.Encode(buffer[:0], testdata)
				if err != nil {
					t.Fatal(err)
				}

				output, err = test.codec.Decode(output[:0], buffer)
				if err != nil {
					t.Fatal(err)
				}

				if !bytes.Equal(testdata, output) {
					t.Errorf("content mismatch after compressing and decompressing (attempt %d/%d)", i+1, N)
				}
			}
		})
	}
}

func BenchmarkEncode(b *testing.B) {
	buffer := make([]byte, 0, len(testdata))

	for _, test := range tests {
		b.Run(test.scenario, func(b *testing.B) {
			b.SetBytes(int64(len(testdata)))
			benchmarkZeroAllocsPerRun(b, func() {
				buffer, _ = test.codec.Encode(buffer[:0], testdata)
			})
		})
	}
}

func BenchmarkDecode(b *testing.B) {
	buffer := make([]byte, 0, len(testdata))
	output := make([]byte, 0, len(testdata))

	for _, test := range tests {
		b.Run(test.scenario, func(b *testing.B) {
			buffer, _ = test.codec.Encode(buffer[:0], testdata)
			b.SetBytes(int64(len(testdata)))
			benchmarkZeroAllocsPerRun(b, func() {
				output, _ = test.codec.Encode(output[:0], buffer)
			})
		})
	}
}

type simpleReader struct{ io.Reader }

func (s *simpleReader) Close() error            { return nil }
func (s *simpleReader) Reset(r io.Reader) error { s.Reader = r; return nil }

type simpleWriter struct{ io.Writer }

func (s *simpleWriter) Close() error      { return nil }
func (s *simpleWriter) Reset(w io.Writer) { s.Writer = w }

func BenchmarkCompressor(b *testing.B) {
	compressor := compress.Compressor{}
	src := make([]byte, 1000)
	dst := make([]byte, 1000)

	benchmarkZeroAllocsPerRun(b, func() {
		dst, _ = compressor.Encode(dst, src, func(w io.Writer) (compress.Writer, error) {
			return &simpleWriter{Writer: w}, nil
		})
	})
}

func BenchmarkDecompressor(b *testing.B) {
	decompressor := compress.Decompressor{}
	src := make([]byte, 1000)
	dst := make([]byte, 1000)

	benchmarkZeroAllocsPerRun(b, func() {
		dst, _ = decompressor.Decode(dst, src, func(r io.Reader) (compress.Reader, error) {
			return &simpleReader{Reader: r}, nil
		})
	})
}

func benchmarkZeroAllocsPerRun(b *testing.B, f func()) {
	if allocs := testing.AllocsPerRun(b.N, f); allocs != 0 && !testing.Short() {
		b.Errorf("too many memory allocations: %g > 0", allocs)
	}
}
