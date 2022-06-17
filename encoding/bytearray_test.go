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
		0, 5, 10, 13,
	}

	data := []byte("HelloWorld!!!")
	page := encoding.EncodeByteArrayPage(nil, offsets, data)

	i := 0
	page.Range(func(value []byte) bool {
		if !bytes.Equal(value, values[i]) {
			t.Errorf("wrong value at index %d: want=%q got=%q", i, values[i], value)
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

func TestByteArraySlice(t *testing.T) {
	values := [][]byte{
		[]byte("Hello"),
		[]byte("World"),
		[]byte("!!!"),
	}

	offsets := []int32{
		0, 5, 10, 13,
	}

	data := []byte("HelloWorld!!!")
	page := encoding.EncodeByteArrayPage(nil, offsets, data)

	slice := page.Slice(0, page.Len())

	i := 0
	slice.Range(func(value []byte) bool {
		if !bytes.Equal(value, values[i]) {
			t.Errorf("wrong value at index %d: want=%q got=%q", i, values[i], value)
		}
		i++
		return true
	})

	for i, want := range values {
		got := slice.Index(i)
		if !bytes.Equal(want, got) {
			t.Errorf("wrong value at index %d: want=%q got=%q", i, want, got)
		}
	}
}
