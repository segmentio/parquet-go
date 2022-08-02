package delta

import "testing"

func TestDecodeByteArrayLengths(t *testing.T) {
	lengths := make([]int32, 999)
	offsets := make([]uint32, len(lengths)+1)

	totalLength := uint32(0)
	for i := range lengths {
		lengths[i] = int32(i)
		totalLength += uint32(i)
	}

	lastOffset, invalidLength := decodeByteArrayLengths(offsets, lengths)
	if invalidLength != 0 {
		t.Fatal("wrong invalid length:", invalidLength)
	}
	if lastOffset != totalLength {
		t.Fatalf("wrong last offset: want=%d got=%d", lastOffset, totalLength)
	}

	expectOffset := uint32(0)
	for i, offset := range offsets[:len(lengths)] {
		if offset != expectOffset {
			t.Fatalf("wrong offset at index %d: want=%d got=%d", i, expectOffset, offset)
		}
		expectOffset += uint32(lengths[i])
	}

	if offsets[len(lengths)] != lastOffset {
		t.Fatalf("wrong last offset: want=%d got=%d", lastOffset, offsets[len(lengths)])
	}
}
