package parquet

import (
	"sync"

	"github.com/segmentio/parquet-go/internal/unsafecast"
)

type Allocator interface {
	Allocate(size int) []byte
}

// DefaultAllocator is the default memory allocator used by parquet-go.
// It is initialized to use the standard Go memory allocator.
var DefaultAllocator Allocator = goheap{}

type arena struct {
	mutex sync.Mutex
	block []byte
}

func (a *arena) Allocate(size int) []byte {
	if size < 0 {
		panic("invalid negative memory allocation size")
	}

	a.mutex.Lock()
	defer a.mutex.Unlock()

	for {
		i := len(a.block)
		j := len(a.block) + size

		if j <= cap(a.block) {
			b := a.block[i:j:j]
			a.block = a.block[:j]
			return b
		}

		const pageSize = 65536
		blockSize := ((size + (pageSize - 1)) / pageSize) * pageSize
		minSize := 2 * cap(a.block)
		if blockSize < minSize {
			blockSize = minSize
		}
		a.block = make([]byte, 0, blockSize)
	}
}

func (a *arena) reset() {
	a.mutex.Lock()
	a.block = a.block[:0]
	a.mutex.Unlock()
}

type goheap struct{}

func (goheap) Allocate(size int) []byte { return make([]byte, size) }

func makeSliceInt32(a Allocator, size int) []int32 {
	return unsafecast.BytesToInt32(a.Allocate(4 * size))
}

func makeSliceInt64(a Allocator, size int) []int64 {
	return unsafecast.BytesToInt64(a.Allocate(8 * size))
}

func makeSliceFloat32(a Allocator, size int) []float32 {
	return unsafecast.BytesToFloat32(a.Allocate(4 * size))
}

func makeSliceFloat64(a Allocator, size int) []float64 {
	return unsafecast.BytesToFloat64(a.Allocate(8 * size))
}
