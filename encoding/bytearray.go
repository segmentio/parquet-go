package encoding

import (
	"encoding/binary"

	"github.com/segmentio/parquet-go/internal/unsafecast"
)

// ByteArrayPage is a read-only type implementing the layout of byte array pages
// in memory.
//
// The type uses the following memory layout:
//
//		[length][offsets][data]
//
// - The length is a 32 bits little-endian number of values held in the page.
//
// - length+1 offsets are the absolute positions of values in the data section,
//   encoded as 32 bits little-endian integers. The length of each byte array
//   value is the difference between two consecutive offsets.
//
// - The page suffix contains the concatenated byte array data.
//
type ByteArrayPage []byte

func (b ByteArrayPage) Len() int {
	if len(b) > 0 {
		return int(binary.LittleEndian.Uint32(b[:4]))
	}
	return 0
}

func (b ByteArrayPage) Offsets() []int32 {
	if n := b.Len(); n > 0 {
		return unsafecast.BytesToInt32(b[8 : 8+4*n])
	}
	return nil
}

func (b ByteArrayPage) Data() []byte {
	if n := b.Len(); n > 0 {
		return b[8+4*n:]
	}
	return nil
}

func (b ByteArrayPage) Region(i int) (baseOffset, endOffset int) {
	if len(b) > 0 {
		region := binary.LittleEndian.Uint64(b[4+4*i:])
		baseOffset = int(region & 0xFFFFFFFF)
		endOffset = int(region >> 32)
	}
	return baseOffset, endOffset
}

func (b ByteArrayPage) Index(i int) []byte {
	baseOffset, endOffset := b.Region(i)
	return b.Data()[baseOffset:endOffset:endOffset]
}

func (b ByteArrayPage) Range(f func([]byte) bool) {
	base := int32(0)
	data := b.Data()
	for _, offset := range b.Offsets() {
		if !f(data[base:offset:offset]) {
			break
		}
		base = offset
	}
}

func EncodeByteArrayPage(page ByteArrayPage, offsets []int32, values []byte) ByteArrayPage {
	size := 8 + 4*len(offsets) + len(values)

	if cap(page) < size {
		page = make([]byte, size)
	} else {
		page = page[:size]
	}

	binary.LittleEndian.PutUint32(page[0:], uint32(len(offsets)))
	binary.LittleEndian.PutUint32(page[4:], uint32(0))
	copy(page[8:], unsafecast.Int32ToBytes(offsets))
	copy(page[8+4*len(offsets):], values)
	return page
}

func ConvertLengthsToOffsets(offsets, lengths []int32) {
	copy(offsets, lengths)

	for i := 1; i < len(offsets); i++ {
		offsets[i] += offsets[i-1]
	}
}

func ConvertOffsetsToLengths(lengths, offsets []int32) {
	copy(lengths, offsets)

	for i := len(lengths) - 1; i > 0; i-- {
		lengths[i] -= lengths[i-1]
	}
}
