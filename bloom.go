package parquet

import (
	"io"
	"unsafe"

	"github.com/segmentio/parquet-go/bloom"
	"github.com/segmentio/parquet-go/bloom/xxhash"
	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/format"
)

// BloomFilter is an interface allowing applications to test whether a key
// exists in a bloom filter.
type BloomFilter interface {
	Check(key []byte) bool
}

// The BloomFilterColumn interface is a declarative representation of bloom filters
// used when configuring filters on a parquet writer.
type BloomFilterColumn interface {
	// Returns the path of the column that the filter applies to.
	Path() []string
	// Returns the hashing algorithm used when inserting keys into a bloom
	// filter.
	Hash() BloomFilterHash
	// NewFilter constructs a new bloom filter configured to hold the given
	// number of values and bits of filter per value.
	NewFilter(numValues int64, bitsPerValue uint) bloom.MutableFilter
}

// SplitBlockFilter constructs a split block bloom filter object for the column
// at the given path.
func SplitBlockFilter(path ...string) BloomFilterColumn { return splitBlockFilter(path) }

type splitBlockFilter []string

func (f splitBlockFilter) Path() []string        { return f }
func (f splitBlockFilter) Hash() BloomFilterHash { return bloomFilterXXH64{} }
func (f splitBlockFilter) NewFilter(numValues int64, bitsPerValue uint) bloom.MutableFilter {
	return make(bloom.SplitBlockFilter, bloom.NumSplitBlocksOf(numValues, bitsPerValue))
}

// Creates a header from the given bloom filter.
//
// For now there is only one type of filter supported, but we provide this
// function to suggest a model for extending the implementation if new filters
// are added to the parquet specs.
func bloomFilterHeader(filter BloomFilterColumn) (header format.BloomFilterHeader) {
	switch filter.(type) {
	case *splitBlockFilter:
		header.Algorithm.Block = &format.SplitBlockAlgorithm{}
	}
	switch filter.Hash().(type) {
	case bloomFilterXXH64:
		header.Hash.XxHash = &format.XxHash{}
	}
	header.Compression.Uncompressed = &format.BloomFilterUncompressed{}
	return header
}

func searchBloomFilterColumn(filters []BloomFilterColumn, path columnPath) BloomFilterColumn {
	for _, f := range filters {
		if path.equal(f.Path()) {
			return f
		}
	}
	return nil
}

// BloomFilterHash is an interface abstracting the hashing algorithm used in
// bloom filters.
//
// BloomFilterHash instances must be safe to use concurrently from multiple
// goroutines.
type BloomFilterHash interface {
	// Returns the 64 bit hash of the value passed as argument.
	Sum64(value []byte) uint64
	// Compute hashes of the array of fixed size values passed as arguments,
	// returning the number of hashes written to the destination buffer.
	MultiSum64Uint8(dst []uint64, src []uint8) int
	MultiSum64Uint16(dst []uint64, src []uint16) int
	MultiSum64Uint32(dst []uint64, src []uint32) int
	MultiSum64Uint64(dst []uint64, src []uint64) int
	MultiSum64Uint128(dst []uint64, src [][16]byte) int
}

type bloomFilterXXH64 struct{}

func (bloomFilterXXH64) Sum64(b []byte) uint64 {
	return xxhash.Sum64(b)
}

func (bloomFilterXXH64) MultiSum64Uint8(h []uint64, v []uint8) int {
	return xxhash.MultiSum64Uint8(h, v)
}

func (bloomFilterXXH64) MultiSum64Uint16(h []uint64, v []uint16) int {
	return xxhash.MultiSum64Uint16(h, v)
}

func (bloomFilterXXH64) MultiSum64Uint32(h []uint64, v []uint32) int {
	return xxhash.MultiSum64Uint32(h, v)
}

func (bloomFilterXXH64) MultiSum64Uint64(h []uint64, v []uint64) int {
	return xxhash.MultiSum64Uint64(h, v)
}

func (bloomFilterXXH64) MultiSum64Uint128(h []uint64, v [][16]byte) int {
	return xxhash.MultiSum64Uint128(h, v)
}

// bloomFilterEncoder is an adapter type which implements the encoding.Encoder
// interface on top of a bloom filter.
type bloomFilterEncoder struct {
	filter bloom.MutableFilter
	hash   BloomFilterHash
	keys   [128]uint64
}

func newBloomFilterEncoder(filter bloom.MutableFilter, hash BloomFilterHash) *bloomFilterEncoder {
	return &bloomFilterEncoder{filter: filter, hash: hash}
}

func (e *bloomFilterEncoder) Check(value []byte) bool {
	return e.filter.Check(xxhash.Sum64(value))
}

func (e *bloomFilterEncoder) Bytes() []byte {
	return e.filter.Bytes()
}

func (e *bloomFilterEncoder) Reset(io.Writer) {
	e.filter.Reset()
}

func (e *bloomFilterEncoder) SetBitWidth(int) {
}

func (e *bloomFilterEncoder) EncodeBoolean(data []bool) error {
	return e.insert8(*(*[]uint8)(unsafe.Pointer(&data)))
}

func (e *bloomFilterEncoder) EncodeInt8(data []int8) error {
	return e.insert8(*(*[]uint8)(unsafe.Pointer(&data)))
}

func (e *bloomFilterEncoder) EncodeInt16(data []int16) error {
	return e.insert16(*(*[]uint16)(unsafe.Pointer(&data)))
}

func (e *bloomFilterEncoder) EncodeInt32(data []int32) error {
	return e.insert32(*(*[]uint32)(unsafe.Pointer(&data)))
}

func (e *bloomFilterEncoder) EncodeInt64(data []int64) error {
	return e.insert64(*(*[]uint64)(unsafe.Pointer(&data)))
}

func (e *bloomFilterEncoder) EncodeInt96(data []deprecated.Int96) error {
	return e.EncodeFixedLenByteArray(12, deprecated.Int96ToBytes(data))
}

func (e *bloomFilterEncoder) EncodeFloat(data []float32) error {
	return e.insert32(*(*[]uint32)(unsafe.Pointer(&data)))
}

func (e *bloomFilterEncoder) EncodeDouble(data []float64) error {
	return e.insert64(*(*[]uint64)(unsafe.Pointer(&data)))
}

func (e *bloomFilterEncoder) EncodeByteArray(data encoding.ByteArrayList) error {
	data.Range(func(v []byte) bool { e.insert(v); return true })
	return nil
}

func (e *bloomFilterEncoder) EncodeFixedLenByteArray(size int, data []byte) error {
	if size == 16 {
		return e.insert128(*(*[][16]byte)(unsafe.Pointer(&data)))
	}
	for i, j := 0, size; j <= len(data); {
		e.insert(data[i:j])
		i += size
		j += size
	}
	return nil
}

func (e *bloomFilterEncoder) Encode(value []byte) error {
	e.insert(value)
	return nil
}

func (e *bloomFilterEncoder) insert(value []byte) {
	e.filter.Insert(e.hash.Sum64(value))
}

func (e *bloomFilterEncoder) insert8(data []uint8) error {
	k := e.keys[:]
	for i := 0; i < len(data); {
		n := e.hash.MultiSum64Uint8(k, data[i:])
		e.filter.InsertBulk(k[:n:n])
		i += n
	}
	return nil
}

func (e *bloomFilterEncoder) insert16(data []uint16) error {
	k := e.keys[:]
	for i := 0; i < len(data); {
		n := e.hash.MultiSum64Uint16(k, data[i:])
		e.filter.InsertBulk(k[:n:n])
		i += n
	}
	return nil
}

func (e *bloomFilterEncoder) insert32(data []uint32) error {
	k := e.keys[:]
	for i := 0; i < len(data); {
		n := e.hash.MultiSum64Uint32(k, data[i:])
		e.filter.InsertBulk(k[:n:n])
		i += n
	}
	return nil
}

func (e *bloomFilterEncoder) insert64(data []uint64) error {
	k := e.keys[:]
	for i := 0; i < len(data); {
		n := e.hash.MultiSum64Uint64(k, data[i:])
		e.filter.InsertBulk(k[:n:n])
		i += n
	}
	return nil
}

func (e *bloomFilterEncoder) insert128(data [][16]byte) error {
	k := e.keys[:]
	for i := 0; i < len(data); {
		n := e.hash.MultiSum64Uint128(k, data[i:])
		e.filter.InsertBulk(k[:n:n])
		i += n
	}
	return nil
}
