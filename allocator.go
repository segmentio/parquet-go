package parquet

import "github.com/segmentio/parquet-go/internal/unsafecast"

type allocator struct{ buffer []byte }

func (a *allocator) makeBytes(n int) []byte {
	if free := cap(a.buffer) - len(a.buffer); free < n {
		newCap := 2 * cap(a.buffer)
		if newCap == 0 {
			newCap = 4096
		}
		for newCap < n {
			newCap *= 2
		}
		a.buffer = make([]byte, 0, newCap)
	}

	i := len(a.buffer)
	j := len(a.buffer) + n
	a.buffer = a.buffer[:j]
	return a.buffer[i:j:j]
}

func (a *allocator) copyBytes(v []byte) []byte {
	b := a.makeBytes(len(v))
	copy(b, v)
	return b
}

func (a *allocator) copyString(v string) string {
	b := a.makeBytes(len(v))
	copy(b, v)
	return unsafecast.BytesToString(b)
}

func (a *allocator) reset() {
	a.buffer = a.buffer[:0]
}

// rowAllocator is a memory allocator used to make copy of rows referencing
// memory buffers that parquet-go does not have ownership of.
//
// This type is used in the implementation of a various readers and writers that
// need to capture rows passed to the ReadRows/WriteRows methods.
//
// See: RowBuffer, DedupeRowReader, DedupeRowWriter
type rowAllocator struct{ allocator }

func (a *rowAllocator) capture(row Row) {
	for i, v := range row {
		switch kind := v.Kind(); kind {
		case ByteArray, FixedLenByteArray:
			row[i].ptr = unsafecast.AddressOfBytes(a.copyBytes(v.byteArray()))
		}
	}
}
