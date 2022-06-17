package encoding

import (
	"encoding/binary"
	"fmt"

	"github.com/segmentio/parquet-go/internal/unsafecast"
)

// ByteArrayPage is a read-only type implementing the layout of byte array pages
// in memory.
//
// The type uses the following memory layout:
//
//		[length][offsets][values]
//
// - The length is a 32 bits little-endian number of values held in the page.
//
// - length+1 offsets are the absolute positions of values in the values
//   section, encoded as 32 bits little-endian integers. The length of each byte
//   array value is the difference between two consecutive offsets.
//
// - The page suffix contains the concatenated byte array values.
//
// The zero-value is not a valid page, an empty page is at least 8 bytes long,
// 4 bytes for the length prefix and 4 bytes for the zero offset (both set to
// zero).
//
// Because offsets are expressed as signed 32 bit integers, the maximum length
// of the values section is 2 GiB.
type ByteArrayPage []byte

func (b ByteArrayPage) Len() int {
	return int(binary.LittleEndian.Uint32(b[:4]))
}

func (b ByteArrayPage) Offsets() []int32 {
	n := 8 + 4*b.Len()
	return unsafecast.BytesToInt32(b[4:n:n])
}

func (b ByteArrayPage) Values() []byte {
	return b[8+4*b.Len():]
}

func (b ByteArrayPage) Region(i int) (baseOffset, endOffset int) {
	region := binary.LittleEndian.Uint64(b[4+4*i:])
	return int(region & 0xFFFFFFFF), int(region >> 32)
}

func (b ByteArrayPage) Index(i int) []byte {
	baseOffset, endOffset := b.Region(i)
	return b.Values()[baseOffset:endOffset:endOffset]
}

func (b ByteArrayPage) Range(f func([]byte) bool) {
	byteArrayRange(b.Offsets(), b.Values(), f)
}

func (b ByteArrayPage) Slice(i, j int) ByteArraySlice {
	return MakeByteArraySlice(b.Offsets()[i:j+1:j+1], b.Values())
}

func (b ByteArrayPage) Validate() error {
	if len(b) < 8 {
		return fmt.Errorf("input is too short to be a byte array page: %d < 8", len(b))
	}
	length := b.Len()
	if minSize := 8 + 4*length; len(b) < minSize {
		return fmt.Errorf("input is too short ot be a byte array page of length %d: %d < %d", length, len(b), minSize)
	}
	offsets, values := b.Offsets(), b.Values()
	if int(offsets[length]) != len(values) {
		return fmt.Errorf("last offset is not equal to the length of values in byte array page: %d != %d", offsets[length], len(values))
	}
	// TODO: validate all offsets
	return nil
}

func MakeByteArrayPage(page ByteArrayPage, numValues, valuesLength int) ByteArrayPage {
	size := 8 + 4*numValues + valuesLength
	if cap(page) < size {
		page = make([]byte, size)
	} else {
		page = page[:size]
	}
	binary.LittleEndian.PutUint32(page, uint32(numValues))
	return page
}

func JoinByteArrayPage(page ByteArrayPage, data [][]byte) ByteArrayPage {
	valuesLength := 0
	for i := range data {
		valuesLength += len(data[i])
	}
	page = MakeByteArrayPage(page, len(data), valuesLength)
	offsets, values := page.Offsets(), page.Values()
	n := 0
	for i := range data {
		offsets[i+1] = offsets[i] + int32(len(data[i]))
		n += copy(values[n:], data[i])
	}
	return page
}

// EncodeByteArrayPage encodes the values made of the offsets and buffer into
// a ByteArray represetnation.
func EncodeByteArrayPage(page ByteArrayPage, offsets []int32, values []byte) ByteArrayPage {
	page = MakeByteArrayPage(page, len(offsets)-1, len(values))
	copy(page[4:], unsafecast.Int32ToBytes(offsets))
	copy(page[4+4*len(offsets):], values)
	return page
}

// ByteArraySlice is a view of data held in a ByteArrayPage.
type ByteArraySlice struct {
	offsets []int32
	values  []byte
}

func (s ByteArraySlice) Len() int {
	return len(s.offsets) - 1
}

func (s ByteArraySlice) Offsets() []int32 {
	return s.offsets
}

func (s ByteArraySlice) Values() []byte {
	return s.values
}

func (s ByteArraySlice) Region(i int) (baseOffset, endOffset int) {
	return int(s.offsets[i]), int(s.offsets[i+1])
}

func (s ByteArraySlice) Index(i int) []byte {
	baseOffset, endOffset := s.Region(i)
	return s.values[baseOffset:endOffset:endOffset]
}

func (s ByteArraySlice) Range(f func([]byte) bool) {
	byteArrayRange(s.offsets, s.values, f)
}

func (s ByteArraySlice) Slice(i, j int) ByteArraySlice {
	return MakeByteArraySlice(s.offsets[i:j+1:j+1], s.values)
}

func MakeByteArraySlice(offsets []int32, values []byte) ByteArraySlice {
	return ByteArraySlice{offsets: offsets, values: values}
}

func byteArrayRange(offsets []int32, values []byte, f func([]byte) bool) {
	base := offsets[0]

	for _, end := range offsets[1:] {
		if !f(values[base:end:end]) {
			break
		}
		base = end
	}
}
