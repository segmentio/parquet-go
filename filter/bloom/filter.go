package bloom

import (
	"io"
	"unsafe"

	"github.com/segmentio/parquet-go/internal/bits"
)

// Filter is an in-memory implementation of the parquet bloom filters.
//
// This type is useful to construct bloom filters that are later serialized
// to a storage medium.
type Filter []Block

// NumBlocksOf returns the number of blocks in a filter intended to hold the
// given nuumber of values and bits of filter per value.
//
// This function is useful to determine the number of blocks when creating bloom
// filters in memory, for example:
//
//	f := make(bloom.Filter, bloom.NumBlocksOf(n, 10))
//
func NumBlocksOf(numValues, bitsPerValue int) int {
	return (bits.ByteCount(uint(numValues)*uint(bitsPerValue)) + (BlockSize - 1)) / BlockSize
}

// FilterFromBytes constructs a bloom filter from a byte slice.
//
// The returned filter shares the memory of the byte slice, if the program
// needs the to make a copy of the slice.
func FilterFromBytes(b []byte) Filter {
	return Filter(unsafe.Slice(*(**Block)(unsafe.Pointer(&b)), len(b)/BlockSize))
}

// Block returns a pointer to the block that the given value hashes to in the
// bloom filter.
func (f Filter) Block(x uint64) *Block {
	// Note: the call to blockIndex causes the function cost to exceed the
	// inlining budget in Filter.Insert and Filter.Check, preventing those
	// functions from being inlined. It is technically possible to allow
	// inlining by manually copying the code of blockIndex in Filter.Insert
	// and Filter.Check, at the expense of readability and maintainability,
	// which is why we don't do it.
	//
	//	$ go build -gcflags '-m -m'
	//	...
	//	can inline blockIndex with cost 8 as: func(uint64, uint64) uint64 { return x >> 32 * n >> 32 }
	//	can inline Filter.Block with cost 18 as: method(Filter) func(uint64) *Block { return &f[blockIndex(x, uint64(len(f)))] }
	//	cannot inline Filter.Insert: function too complex: cost 87 exceeds budget 80
	//	cannot inline Filter.Check: function too complex: cost 89 exceeds budget 80
	//
	// If inlining is important, the program should be compiled with
	// -gcflags=-l=4, in which case the compiler is able to inline the methods:
	//
	//	$ go build -gcflags '-m -m -l=4'
	//	...
	//	can inline blockIndex with cost 8 as: func(uint64, uint64) uint64 { return x >> 32 * n >> 32 }
	//	can inline Filter.Block with cost 18 as: method(Filter) func(uint64) *Block { return &f[blockIndex(x, uint64(len(f)))] }
	//	can inline Filter.Insert with cost 31 as: method(Filter) func(uint64) { f.Block(x).Insert(uint32(x)) }
	//	can inline Filter.Check with cost 33 as: method(Filter) func(uint64) bool { return f.Block(x).Check(uint32(x)) }
	//
	return &f[blockIndex(x, uint64(len(f)))]
}

// Insert adds x to f.
func (f Filter) Insert(x uint64) {
	f.Block(x).Insert(uint32(x))
}

// Check tests whether x is in f.
func (f Filter) Check(x uint64) bool {
	return f.Block(x).Check(uint32(x))
}

// Bytes converts f to a byte slice.
//
// The returned slice shares the memory of f. The method is intended to be used
// to serialize the bloom filter to a storage medium.
func (f Filter) Bytes() []byte {
	return unsafe.Slice(*(**byte)(unsafe.Pointer(&f)), len(f)*BlockSize)
}

// Check is similar to bloom.Filter.Check but reads the bloom filter of n bytes
// from r, using b as buffer to load the block in which to check for the
// existance of x.
//
// The size n of the bloom filter is assumed to be a multiple of the block size.
func Check(r io.ReaderAt, n int64, b *Block, x uint64) (bool, error) {
	offset := BlockSize * blockIndex(x, uint64(n)/BlockSize)
	_, err := r.ReadAt(b.Bytes(), int64(offset))
	return b.Check(uint32(x)), err
}

func blockIndex(x, n uint64) uint64 {
	return ((x >> 32) * n) >> 32
}
