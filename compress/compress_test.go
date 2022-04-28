package compress_test

import (
	"bytes"
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
