package parquet

import (
	"github.com/segmentio/parquet-go/bloom"
	"github.com/segmentio/parquet-go/format"
)

const (
	// DefaultBitsPerValue is the default number of bits per value used by bloom
	// filter.
	DefaultBitsPerValue = 10
)

// The BloomFilter interface is a declarative representation of bloom filters
// used when configuring filters on a parquet writer.
type BloomFilter interface {
	// Returns the path of the column that the filter applies to.
	Path() []string

	// Returns the same filter but configured to use the given number of bits
	// per value.
	BitsPerValue(bitsPerValue int) BloomFilter

	// NewEncoder constructs an encoder of bloom filter configured to hold the
	// given number of values and bits of filter per value.
	NewEncoder(numValues int64) bloom.Encoder
}

// SplitBlockFilter constructs a split block bloom filter object for the column
// at the given path.
func SplitBlockFilter(path ...string) BloomFilter {
	return &splitBlockFilter{
		path:         path,
		bitsPerValue: DefaultBitsPerValue,
	}
}

type splitBlockFilter struct {
	path         columnPath
	bitsPerValue int
}

func (f *splitBlockFilter) Path() []string {
	return f.path
}

func (f *splitBlockFilter) BitsPerValue(bitsPerValue int) BloomFilter {
	return &splitBlockFilter{
		path:         f.path,
		bitsPerValue: bitsPerValue,
	}
}

func (f *splitBlockFilter) NewEncoder(numValues int64) bloom.Encoder {
	return bloom.NewSplitBlockEncoder(
		make(bloom.SplitBlockFilter, bloom.NumSplitBlocksOf(int(numValues), f.bitsPerValue)),
	)
}

// Creates a header from the given bloom filter.
//
// For now there is only one type of filter supported, but we provide this
// function to suggest a model for extending the implementation if new filters
// are added to the parquet specs.
func bloomFilterHeader(filter BloomFilter) (header format.BloomFilterHeader) {
	switch filter.(type) {
	case *splitBlockFilter:
		header.Algorithm.Block = &format.SplitBlockAlgorithm{}
	}
	header.Hash.XxHash = &format.XxHash{}
	header.Compression.Uncompressed = &format.BloomFilterUncompressed{}
	return header
}

func searchBloomFilter(filters []BloomFilter, path columnPath) BloomFilter {
	for _, f := range filters {
		if path.equal(f.Path()) {
			return f
		}
	}
	return nil
}
