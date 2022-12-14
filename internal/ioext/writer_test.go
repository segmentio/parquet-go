package ioext_test

import (
	"io"
	"testing"

	"github.com/segmentio/parquet-go/internal/ioext"
	"github.com/segmentio/parquet-go/internal/quick"
)

func TestOffsetTrackingWriter(t *testing.T) {
	w := new(ioext.OffsetTrackingWriter)

	t.Run("single call to Write", func(t *testing.T) {
		err := quick.Check(func(data []byte) bool {
			w.Reset(io.Discard)

			n, err := w.Write(data)
			if err != nil {
				t.Error(err)
				return false
			}
			if n != len(data) {
				t.Errorf("wrong number of bytes written: %d != %d", n, len(data))
				return false
			}
			if w.Offset() != int64(len(data)) {
				t.Errorf("wrong byte offset: %d != %d", w.Offset(), len(data))
				return false
			}
			return true
		})
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("write bytes one by one", func(t *testing.T) {
		err := quick.Check(func(data []byte) bool {
			w.Reset(io.Discard)

			for i := range data {
				n, err := w.Write(data[i : i+1])
				if err != nil {
					t.Error(err)
					return false
				}
				if n != 1 {
					t.Errorf("wrong number of bytes written: %d != %d", n, 1)
					return false
				}
				if w.Offset() != int64(i+1) {
					t.Errorf("wrong byte offset: %d != %d", w.Offset(), i+1)
					return false
				}
			}
			return true
		})
		if err != nil {
			t.Error(err)
		}
	})
}
