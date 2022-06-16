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
// The zero-value is not a valid page, an empty page is at least 8 bytes long,
// 4 bytes for the length prefix and 4 bytes for the zero offset.
//
// Because offsets are expressed as signed 32 bit integers, the maximum length
// of the data section is 2 GiB.
type ByteArrayPage []byte

func (b ByteArrayPage) Len() int {
	return int(binary.LittleEndian.Uint32(b[:4]))
}

func (b ByteArrayPage) Offsets() []int32 {
	n := 8 + 4*b.Len()
	return unsafecast.BytesToInt32(b[8:n:n])
}

func (b ByteArrayPage) Data() []byte {
	return b[8+4*b.Len():]
}

func (b ByteArrayPage) Region(i int) (baseOffset, endOffset int) {
	region := binary.LittleEndian.Uint64(b[4+4*i:])
	return int(region & 0xFFFFFFFF), int(region >> 32)
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

// EncodeByteArrayPage encodes the values made of the offsets and buffer into
// a ByteArray represetnation.
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
