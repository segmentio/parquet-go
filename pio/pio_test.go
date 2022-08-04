package pio_test

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/segmentio/parquet-go/pio/piotest"
)

func TestBytesReader(t *testing.T) {
	piotest.TestReaderAt(t, func(data []byte) (io.ReaderAt, func(), error) {
		return bytes.NewReader(data), func() {}, nil
	})
}

func TestFile(t *testing.T) {
	piotest.TestReaderAt(t, func(data []byte) (io.ReaderAt, func(), error) {
		f, err := os.CreateTemp("", "piotest.*")
		if err != nil {
			return nil, nil, err
		}
		if _, err := f.Write(data); err != nil {
			f.Close()
			return nil, nil, err
		}
		return f, func() { os.Remove(f.Name()); f.Close() }, nil
	})
}
