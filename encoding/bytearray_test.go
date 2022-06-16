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

func TestConvertBetweenLengthsAndOffsets(t *testing.T) {
	lengths := make([]int32, 1000)
	offsets := make([]int32, 1000)
	for i := range lengths {
		lengths[i] = int32(i)
	}

	encoding.ConvertLengthsToOffsets(offsets, lengths)
	wantOffset := int32(0)
	for i, length := range lengths {
		if wantOffset += length; wantOffset != offsets[i] {
			t.Errorf("wrong offset at index %d: want=%d got=%d", i, wantOffset, offsets[i])
		}
	}

	encoding.ConvertOffsetsToLengths(offsets, offsets)
	for i, length := range lengths {
		if length != offsets[i] {
			t.Errorf("wrong length at index %d: want=%d got=%d", i, length, offsets[i])
		}
	}
}

func BenchmarkConvertLengthsToOffsets(b *testing.B) {
	dst := make([]int32, 1000)
	src := make([]int32, 1000)
	for i := 0; i < b.N; i++ {
		encoding.ConvertLengthsToOffsets(dst, src)
	}
	b.SetBytes(4 * int64(len(dst)))
}

func BenchmarkConvertOffsetsToLengths(b *testing.B) {
	dst := make([]int32, 1000)
	src := make([]int32, 1000)
	for i := 0; i < b.N; i++ {
		encoding.ConvertOffsetsToLengths(dst, src)
	}
	b.SetBytes(4 * int64(len(dst)))
}
