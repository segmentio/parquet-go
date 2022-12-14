package ioext_test

import (
	"bytes"
	"io"
	"testing"
	"testing/iotest"

	"github.com/segmentio/parquet-go/internal/ioext"
	"github.com/segmentio/parquet-go/internal/quick"
)

func TestReaderAt(t *testing.T) {
	err := quick.Check(func(data []byte) bool {
		b := bytes.NewReader(data)
		r := ioext.NewReaderAt(struct{ io.ReadSeeker }{b})
		s := io.NewSectionReader(r, 0, int64(len(data)))

		if err := iotest.TestReader(s, data); err != nil {
			t.Error(err)
			return false
		}

		return true
	})
	if err != nil {
		t.Error(err)
	}
}
