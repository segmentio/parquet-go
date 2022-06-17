package parquet

import "testing"

func TestBroadcastValueInt32(t *testing.T) {
	buf := make([]int32, 123)
	broadcastValueInt32(buf, 0x0A)

	for i, v := range buf {
		if v != 0x0A0A0A0A {
			t.Fatalf("wrong value at index %d: %v", i, v)
		}
	}
}

func TestBroadcastRangeInt32(t *testing.T) {
	buf := make([]int32, 123)
	broadcastRangeInt32(buf, 1)

	for i, v := range buf {
		if v != int32(1+i) {
			t.Fatalf("wrong value at index %d: %v", i, v)
		}
	}
}

func BenchmarkBroadcastValueInt32(b *testing.B) {
	buf := make([]int32, 1000)
	for i := 0; i < b.N; i++ {
		broadcastValueInt32(buf, -1)
	}
	b.SetBytes(4 * int64(len(buf)))
}

func BenchmarkBroadcastRangeInt32(b *testing.B) {
	buf := make([]int32, 1000)
	for i := 0; i < b.N; i++ {
		broadcastRangeInt32(buf, 0)
	}
	b.SetBytes(4 * int64(len(buf)))
}
