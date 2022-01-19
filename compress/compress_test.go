package compress_test

import (
	"bytes"
	"io"
	"testing"
	"testing/iotest"

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

	buffer := new(bytes.Buffer)
	output := new(bytes.Buffer)
	random := bytes.Repeat([]byte("1234567890qwertyuiopasdfghjklzxcvbnm"), 1000)

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			w, err := test.codec.NewWriter(nil)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()

			r, err := test.codec.NewReader(nil)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()

			for i := 0; i < 10; i++ {
				buffer.Reset()
				output.Reset()

				if err := w.Reset(buffer); err != nil {
					t.Fatal(err)
				}
				if _, err := io.Copy(w, iotest.OneByteReader(bytes.NewReader(random))); err != nil {
					t.Fatal(err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}

				if err := r.Reset(buffer); err != nil {
					t.Fatal(err)
				}
				if _, err := io.Copy(output, iotest.OneByteReader(r)); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(random, output.Bytes()) {
					t.Errorf("content mismatch after compressing and decompressing:\n%q\n%q", random, output.Bytes())
				}

				if err := w.Reset(nil); err != nil {
					t.Fatal(err)
				}
				if err := r.Reset(nil); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
