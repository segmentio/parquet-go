//go:build go1.18 && !purego && amd64
// +build go1.18,!purego,amd64

package rle

import "testing"

func TestEncodeInt32IndexEqual8ContiguousAVX2(t *testing.T) {
	testEncodeInt32IndexEqual8Contiguous(t, encodeInt32IndexEqual8ContiguousAVX2)
}

func TestEncodeInt32IndexEqual8ContiguousSSE(t *testing.T) {
	testEncodeInt32IndexEqual8Contiguous(t, encodeInt32IndexEqual8ContiguousSSE)
}

func BenchmarkEncodeInt32IndexEqual8ContiguousAVX2(b *testing.B) {
	benchmarkEncodeInt32IndexEqual8Contiguous(b, encodeInt32IndexEqual8ContiguousAVX2)
}

func BenchmarkEncodeInt32IndexEqual8ContiguousSSE(b *testing.B) {
	benchmarkEncodeInt32IndexEqual8Contiguous(b, encodeInt32IndexEqual8ContiguousSSE)
}
