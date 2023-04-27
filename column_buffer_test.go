package parquet

import (
	"testing"
)

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

// https://github.com/segmentio/parquet-go/issues/501
func TestIssue501(t *testing.T) {
	col := newBooleanColumnBuffer(BooleanType, 0, 2055208)

	// write all trues and then flush the buffer
	_, err := col.WriteBooleans([]bool{true, true, true, true, true, true, true, true})
	if err != nil {
		t.Fatal(err)
	}
	col.Reset()

	// write a single false, we are trying to trip a certain line of code in WriteBooleans
	_, err = col.WriteBooleans([]bool{false})
	if err != nil {
		t.Fatal(err)
	}
	// now write 7 booleans at once, this will cause WriteBooleans to attempt its "alignment" logic
	_, err = col.WriteBooleans([]bool{false, false, false, false, false, false, false})
	if err != nil {
		panic(err)
	}

	for i := 0; i < 8; i++ {
		read := make([]Value, 1)
		_, err = col.ReadValuesAt(read, int64(i))
		if err != nil {
			t.Fatal(err)
		}
		if read[0].Boolean() {
			t.Fatalf("expected false at index %d", i)
		}
	}
}
