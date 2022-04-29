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

func TestCompressionCodec(t *testing.T) {
	tests := []struct {
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

	random := bytes.Repeat([]byte("1234567890qwertyuiopasdfghjklzxcvbnm"), 1000)
	buffer := make([]byte, 0, len(random))
	output := make([]byte, 0, len(random))

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			const N = 10
			// Run the test multiple times to exercise codecs that maintain
			// state across compression/decompression.
			for i := 0; i < N; i++ {
				var err error

				buffer, err = test.codec.Encode(buffer[:0], random)
				if err != nil {
					t.Fatal(err)
				}

				output, err = test.codec.Decode(output[:0], buffer)
				if err != nil {
					t.Fatal(err)
				}

				if !bytes.Equal(random, output) {
					t.Errorf("content mismatch after compressing and decompressing (attempt %d/%d)", i+1, N)
				}
			}
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

	allocs := testing.AllocsPerRun(b.N, func() {
		var err error
		dst, err = compressor.Encode(dst, src, func(w io.Writer) (compress.Writer, error) {
			return &simpleWriter{Writer: w}, nil
		})
		if err != nil {
			b.Fatal(err)
		}
	})

	if allocs != 0 {
		b.Errorf("too many memory allocations: %g > 0", allocs)
	}
}

func BenchmarkDecompressor(b *testing.B) {
	decompressor := compress.Decompressor{}
	src := make([]byte, 1000)
	dst := make([]byte, 1000)

	allocs := testing.AllocsPerRun(b.N, func() {
		var err error
		dst, err = decompressor.Decode(dst, src, func(r io.Reader) (compress.Reader, error) {
			return &simpleReader{Reader: r}, nil
		})
		if err != nil {
			b.Fatal(err)
		}
	})

	if allocs != 0 {
		b.Errorf("too many memory allocations: %g > 0", allocs)
	}
}
