package bloom

import (
	"io"
	"unsafe"

	"github.com/segmentio/parquet-go/internal/bits"
)

// Filter is an interface representing read-only bloom filters where programs
// can probe for the possible presence of a hash key.
type Filter interface {
	Check(uint64) bool
}

// SplitBlockFilter is an in-memory implementation of the parquet bloom filters.
//
// This type is useful to construct bloom filters that are later serialized
// to a storage medium.
type SplitBlockFilter []Block

// NumSplitBlocksOf returns the number of blocks in a filter intended to hold
// the given nuumber of values and bits of filter per value.
//
// This function is useful to determine the number of blocks when creating bloom
// filters in memory, for example:
//
//	f := make(bloom.SplitBlockFilter, bloom.NumSplitBlocksOf(n, 10))
//
func NumSplitBlocksOf(numValues, bitsPerValue int) int {
	numBytes := bits.ByteCount(uint(numValues) * uint(bitsPerValue))
	numBlocks := (numBytes + (BlockSize - 1)) / BlockSize
	return numBlocks
}

// Reset clears the content of the filter f.
func (f SplitBlockFilter) Reset() {
	for i := range f {
		f[i] = Block{}
	}
}

// Block returns a pointer to the block that the given value hashes to in the
// bloom filter.
func (f SplitBlockFilter) Block(x uint64) *Block {
	// Note: the call to blockIndex causes the function cost to exceed the
	// inlining budget in SplitBlockFilter.Insert and SplitBlockFilter.Check,
	// preventing those functions from being inlined. It is technically possible
	// to allow inlining by manually copying the code of blockIndex in
	// SplitBlockFilter.Insert and SplitBlockFilter.Check, at the expense of
	// readability and maintainability, which is why we don't do it.
	//
	//	$ go build -gcflags '-m -m'
	//	...
	//	can inline blockIndex with cost 8 as: func(uint64, uint64) uint64 { return x >> 32 * n >> 32 }
	//	can inline SplitBlockFilter.Block with cost 18 as: method(SplitBlockFilter) func(uint64) *Block { return &f[blockIndex(x, uint64(len(f)))] }
	//	cannot inline SplitBlockFilter.Insert: function too complex: cost 87 exceeds budget 80
	//	cannot inline SplitBlockFilter.Check: function too complex: cost 89 exceeds budget 80
	//
	// If inlining is important, the program should be compiled with
	// -gcflags=-l=4, in which case the compiler is able to inline the methods:
	//
	//	$ go build -gcflags '-m -m -l=4'
	//	...
	//	can inline blockIndex with cost 8 as: func(uint64, uint64) uint64 { return x >> 32 * n >> 32 }
	//	can inline SplitBlockFilter.Block with cost 18 as: method(SplitBlockFilter) func(uint64) *Block { return &f[blockIndex(x, uint64(len(f)))] }
	//	can inline SplitBlockFilter.Insert with cost 31 as: method(SplitBlockFilter) func(uint64) { f.Block(x).Insert(uint32(x)) }
	//	can inline SplitBlockFilter.Check with cost 33 as: method(SplitBlockFilter) func(uint64) bool { return f.Block(x).Check(uint32(x)) }
	//
	return &f[blockIndex(x, uint64(len(f)))]
}

// Insert adds x to f.
func (f SplitBlockFilter) Insert(x uint64) {
	f.Block(x).Insert(uint32(x))
}

// Check tests whether x is in f.
func (f SplitBlockFilter) Check(x uint64) bool {
	return f.Block(x).Check(uint32(x))
}

// Bytes converts f to a byte slice.
//
// The returned slice shares the memory of f. The method is intended to be used
// to serialize the bloom filter to a storage medium.
func (f SplitBlockFilter) Bytes() []byte {
	return unsafe.Slice(*(**byte)(unsafe.Pointer(&f)), len(f)*BlockSize)
}

// CheckSplitBlock is similar to bloom.SplitBlockFilter.Check but reads the
// bloom filter of n bytes from r, using b as buffer to load the block in which
// to check for the existance of x.
//
// The size n of the bloom filter is assumed to be a multiple of the block size.
func CheckSplitBlock(r io.ReaderAt, n int64, b *Block, x uint64) (bool, error) {
	offset := BlockSize * blockIndex(x, uint64(n)/BlockSize)
	_, err := r.ReadAt(b.Bytes(), int64(offset))
	return b.Check(uint32(x)), err
}

func blockIndex(x, n uint64) uint64 {
	return ((x >> 32) * n) >> 32
}
