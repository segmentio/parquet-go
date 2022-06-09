package delta

import (
	"sync"
	"unsafe"

	"github.com/segmentio/parquet-go/internal/unsafecast"
)

type int32Buffer struct {
	values []int32
}

func (buf *int32Buffer) decode(src []byte) ([]byte, error) {
	values, remain, err := decodeInt32(unsafecast.Int32ToBytes(buf.values[:0]), src)
	buf.values = unsafecast.BytesToInt32(values)
	return remain, err
}

var (
	int32BufferPool sync.Pool // *int32Buffer
)

func getInt32Buffer() *int32Buffer {
	b, _ := int32BufferPool.Get().(*int32Buffer)
	if b != nil {
		b.values = b.values[:0]
	} else {
		b = &int32Buffer{
			values: make([]int32, 0, 1024),
		}
	}
	return b
}

func putInt32Buffer(b *int32Buffer) {
	int32BufferPool.Put(b)
}

func bytesToInt32(b []byte) []int32 {
	return unsafe.Slice(*(**int32)(unsafe.Pointer(&b)), len(b)/4)
}

func bytesToInt64(b []byte) []int64 {
	return unsafe.Slice(*(**int64)(unsafe.Pointer(&b)), len(b)/8)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
