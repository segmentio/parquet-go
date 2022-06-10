package parquet

import "testing"

func BenchmarkCompareBE128(b *testing.B) {
	v1 := [16]byte{}
	v2 := [16]byte{}

	for i := 0; i < b.N; i++ {
		compareBE128(&v1, &v2)
	}
}

func BenchmarkLessBE128(b *testing.B) {
	v1 := [16]byte{}
	v2 := [16]byte{}

	for i := 0; i < b.N; i++ {
		lessBE128(&v1, &v2)
	}
}
