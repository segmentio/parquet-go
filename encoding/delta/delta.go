package delta

import (
	"bytes"
	"sync"
)

const (
	defaultBufferSize = 4096
)

type int32Buffer struct {
	values []int32
}

var (
	int32BufferPool sync.Pool // *int32Buffer
	bytesBufferPool sync.Pool // *bytes.Buffer
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

func getBytesBuffer() *bytes.Buffer {
	b, _ := bytesBufferPool.Get().(*bytes.Buffer)
	if b != nil {
		b.Reset()
	} else {
		b = new(bytes.Buffer)
	}
	return b
}

func putBytesBuffer(b *bytes.Buffer) {
	bytesBufferPool.Put(b)
}
