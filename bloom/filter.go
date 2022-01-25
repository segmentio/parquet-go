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
	return &f[fasthash1x64(x, int32(len(f)))]
}

// InsertBulk adds all values from x into f.
func (f SplitBlockFilter) InsertBulk(x []uint64) { filterInsertBulk(f, x) }

// Insert adds x to f.
func (f SplitBlockFilter) Insert(x uint64) { filterInsert(f, x) }

// Check tests whether x is in f.
func (f SplitBlockFilter) Check(x uint64) bool { return filterCheck(f, x) }

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
	offset := BlockSize * fasthash1x64(x, int32(n/BlockSize))
	_, err := r.ReadAt(b.Bytes(), int64(offset))
	return b.Check(uint32(x)), err
}
