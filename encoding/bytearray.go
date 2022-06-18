package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/segmentio/parquet-go/internal/unsafecast"
)

var (
	// Backing array for offsets of empty byte arrray slices; we use a global
	// to avoid the heap allocation that would otherwise occur if we used make
	// to create a new slice.
	emptyOffsets [1]int32
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

func (p ByteArrayPage) Clone() ByteArrayPage {
	c := make(ByteArrayPage, len(p))
	copy(c, p)
	return c
}

func (p ByteArrayPage) Size() int64 {
	return int64(len(p))
}

func (p ByteArrayPage) Len() int {
	return int(binary.LittleEndian.Uint32(p[:4]))
}

func (p ByteArrayPage) Offsets() []int32 {
	n := 8 + 4*p.Len()
	return unsafecast.BytesToInt32(p[4:n:n])
}

func (p ByteArrayPage) Values() []byte {
	return p[8+4*p.Len():]
}

func (p ByteArrayPage) Region(i int) (baseOffset, endOffset int) {
	region := binary.LittleEndian.Uint64(p[4+4*i:])
	return int(region & 0xFFFFFFFF), int(region >> 32)
}

func (p ByteArrayPage) Index(i int) []byte {
	baseOffset, endOffset := p.Region(i)
	return p.Values()[baseOffset:endOffset:endOffset]
}

func (p ByteArrayPage) Slice(i, j int) ByteArraySlice {
	s := MakeByteArraySlice(p.Offsets()[i:j+1:j+1], p.Values())
	if i == 0 && j == p.Len() {
		s.page = p
	}
	return s
}

func (p ByteArrayPage) Range(f func([]byte) bool) {
	byteArrayRange(p.Offsets(), p.Values(), f)
}

func (p ByteArrayPage) Min() (min []byte) {
	return byteArrayMin(p.Offsets(), p.Values())
}

func (p ByteArrayPage) Max() (max []byte) {
	return byteArrayMax(p.Offsets(), p.Values())
}

func (p ByteArrayPage) Bounds() (min, max []byte) {
	return byteArrayBounds(p.Offsets(), p.Values())
}

func (p ByteArrayPage) Validate() error {
	if len(p) < 8 {
		return fmt.Errorf("input is too short to be a byte array page: %d < 8", len(p))
	}
	length := p.Len()
	if minSize := 8 + 4*length; len(p) < minSize {
		return fmt.Errorf("input is too short ot be a byte array page of length %d: %d < %d", length, len(p), minSize)
	}
	return byteArrayValidate(p.Offsets(), p.Values())
}

func MakeByteArrayPage(page ByteArrayPage, numValues, valuesLength int) ByteArrayPage {
	size := 8 + 4*numValues + valuesLength
	if cap(page) < size {
		page = make([]byte, size)
	} else {
		page = page[:size]
		for i := range page {
			page[i] = 0
		}
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
	// Original page that the slice was derived from; this field is nil on sub
	// slices, it is used as an optimization to avoid rematerializing the page
	// on slices that start at index zero and span across the whole page length.
	page ByteArrayPage
	// TODO:
	//
	// This type has a large memory footprint relative to its purpose, it could
	// be simplified to:
	//
	//	type ByteArraySlice struct {
	//		page ByteArrayPage
	//		i, j int32
	//	}
	//
	// The trade off might be on increased compute time due to having to
	// recalculate the offsets and values slices when calling methods of
	// ByteArraySlice, which may also prevent inlining.
	//
	// It could be a worthy experiment to evaluate whether the trade off
	// is worth it.
}

func (s ByteArraySlice) Page() ByteArrayPage {
	if s.page != nil {
		return s.page
	}
	return EncodeByteArrayPage(nil, s.offsets, s.values)
}

func (s ByteArraySlice) Clone() ByteArraySlice {
	return s.Page().Slice(0, s.Len())
}

func (s ByteArraySlice) Size() int64 {
	return int64(4*len(s.offsets) + len(s.values))
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

func (s ByteArraySlice) Slice(i, j int) ByteArraySlice {
	if i == 0 && j == s.Len() {
		return s // to preserve the page field
	}
	return MakeByteArraySlice(s.offsets[i:j+1:j+1], s.values)
}

func (s ByteArraySlice) Range(f func([]byte) bool) {
	byteArrayRange(s.offsets, s.values, f)
}

func (s ByteArraySlice) Validate() error {
	return byteArrayValidate(s.offsets, s.values)
}

func (s ByteArraySlice) Min() (min []byte) {
	return byteArrayMin(s.offsets, s.values)
}

func (s ByteArraySlice) Max() (max []byte) {
	return byteArrayMax(s.offsets, s.values)
}

func (s ByteArraySlice) Bounds() (min, max []byte) {
	return byteArrayBounds(s.offsets, s.values)
}

func EmptyByteArraySlice() ByteArraySlice {
	return ByteArraySlice{offsets: emptyOffsets[:]}
}

func MakeByteArraySlice(offsets []int32, values []byte) ByteArraySlice {
	_ = offsets[:1] // need at least one offset to be valid
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

func byteArrayMin(offsets []int32, values []byte) (min []byte) {
	if len(offsets) > 1 {
		base := offsets[1]
		min = values[offsets[0]:base:base]

		for _, end := range offsets[2:] {
			value := values[base:end:end]
			if bytes.Compare(value, min) < 0 {
				min = value
			}
			base = end
		}
	}
	return min
}

func byteArrayMax(offsets []int32, values []byte) (max []byte) {
	if len(offsets) > 1 {
		base := offsets[1]
		max = values[offsets[0]:base:base]

		for _, end := range offsets[2:] {
			value := values[base:end:end]
			if bytes.Compare(value, max) > 0 {
				max = value
			}
			base = end
		}
	}
	return max
}

func byteArrayBounds(offsets []int32, values []byte) (min, max []byte) {
	if len(offsets) > 1 {
		base := offsets[1]
		min = values[offsets[0]:base:base]
		max = min

		for _, end := range offsets[2:] {
			value := values[base:end:end]
			switch {
			case bytes.Compare(value, min) < 0:
				min = value
			case bytes.Compare(value, max) > 0:
				max = value
			}
			base = end
		}
	}
	return min, max
}

func byteArrayValidate(offsets []int32, values []byte) error {
	length := len(offsets) - 1
	if int(offsets[length]) != len(values) {
		return fmt.Errorf("last offset is not equal to the length of values in byte array page: %d != %d", offsets[length], len(values))
	}
	// TODO: validate all offsets
	return nil
}
