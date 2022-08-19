//go:build go1.18
// +build go1.18

package rle

import (
	"testing"

	"github.com/1712n/parquet-go/encoding/fuzz"
	"github.com/1712n/parquet-go/internal/quick"
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

func TestEncodeInt32IndexEqual8Contiguous(t *testing.T) {
	testEncodeInt32IndexEqual8Contiguous(t, encodeInt32IndexEqual8Contiguous)
}

func testEncodeInt32IndexEqual8Contiguous(t *testing.T, f func([][8]int32) int) {
	t.Helper()

	err := quick.Check(func(words [][8]int32) bool {
		want := 0

		for want < len(words) && words[want] != broadcast8x4(words[want][0]) {
			want++
		}

		if got := f(words); got != want {
			t.Errorf("want=%d got=%d", want, got)
			return false
		}

		return true
	})
	if err != nil {
		t.Error(err)
	}
}

func BenchmarkEncodeInt32IndexEqual8Contiguous(b *testing.B) {
	benchmarkEncodeInt32IndexEqual8Contiguous(b, encodeInt32IndexEqual8Contiguous)
}

func benchmarkEncodeInt32IndexEqual8Contiguous(b *testing.B, f func([][8]int32) int) {
	words := make([][8]int32, 1000)
	for i := range words {
		words[i][0] = 1
	}
	for i := 0; i < b.N; i++ {
		_ = f(words)
	}
	b.SetBytes(32 * int64(len(words)))
}
