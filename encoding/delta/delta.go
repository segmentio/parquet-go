package delta

import (
	"bytes"
	"io"
	"sync"

	"github.com/segmentio/parquet-go/encoding"
)

const (
	defaultBufferSize = 4096
)

func appendDecodeInt32(d encoding.Decoder, data []int32) ([]int32, error) {
	for {
		if len(data) == cap(data) {
			if cap(data) == 0 {
				data = make([]int32, 0, blockSize32)
			} else {
				newData := make([]int32, len(data), 2*cap(data))
				copy(newData, data)
				data = newData
			}
		}

		n, err := d.DecodeInt32(data[len(data):cap(data)])
		data = data[:len(data)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return data, err
		}
	}
}

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
