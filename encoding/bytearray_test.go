package encoding_test

import (
	"bytes"
	"testing"

	"github.com/segmentio/parquet-go/encoding"
)

func TestByteArrayPage(t *testing.T) {
	values := [][]byte{
		[]byte("Hello"),
		[]byte("World"),
		[]byte("!!!"),
	}

	offsets := []int32{
		5, 10, 13,
	}

	data := []byte("HelloWorld!!!")
	page := encoding.EncodeByteArrayPage(nil, offsets, data)

	i := 0
	page.Range(func(v []byte) bool {
		if !bytes.Equal(v, values[i]) {
			t.Errorf("wrong value at index %d: want=%q got=%q", i, values[i], v)
		}
		i++
		return true
	})

	for i, want := range values {
		got := page.Index(i)
		if !bytes.Equal(want, got) {
			t.Errorf("wrong value at index %d: want=%q got=%q", i, want, got)
		}
	}
}
