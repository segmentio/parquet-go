//go:build go1.18
// +build go1.18

package rle

import (
	"testing"

	"github.com/segmentio/parquet-go/encoding/fuzz"
)

func FuzzEncodeBoolean(f *testing.F) {
	fuzz.EncodeBoolean(f, &Encoding{BitWidth: 1})
}

func FuzzEncodeLevels(f *testing.F) {
	fuzz.EncodeLevels(f, &Encoding{BitWidth: 8})
}

func FuzzEncodeInt32(f *testing.F) {
	fuzz.EncodeInt32(f, &Encoding{BitWidth: 32})
}

func TestIsZero(t *testing.T) {
	data := make([]byte, 1234)
	if !isZero(data) {
		t.Fatal("data is zero but got isZero=false")
	}
	data[len(data)-1] = 1
	if isZero(data) {
		t.Fatal("data is not zero but got isZero=true")
	}
}

func TestIsOnes(t *testing.T) {
	data := make([]byte, 1234)
	for i := range data {
		data[i] = 0xFF
	}
	if !isOnes(data) {
		t.Fatal("data is ones but got isOnes=false")
	}
	data[len(data)-1] = 0
	if isOnes(data) {
		t.Fatal("data is not ones but got isOnes=true")
	}
}

func BenchmarkIsZero(b *testing.B) {
	data := make([]byte, 1000)
	for i := 0; i < b.N; i++ {
		if !isZero(data) {
			b.Fatal("isZero=false")
		}
	}
	b.SetBytes(int64(len(data)))
}

func BenchmarkIsOnes(b *testing.B) {
	data := make([]byte, 1000)
	for i := range data {
		data[i] = 0xFF
	}
	for i := 0; i < b.N; i++ {
		if !isOnes(data) {
			b.Fatal("isOnes=false")
		}
	}
	b.SetBytes(int64(len(data)))
}
