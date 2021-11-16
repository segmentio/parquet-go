package compact

import (
	"encoding/binary"
	"math/bits"

	"github.com/segmentio/parquet/encoding"
)

// FixedIntArray is an array of integer values, compacted to use fewer bits to
// represent values in memory than if a the program had used a slice of
// fixed-size integers.
//
// The type supports representing integers with up to 64 bits of precision.
type FixedIntArray struct {
	class intArrayClass
	bits  []byte
}

func NewFixedIntArray(bitWidth int) *FixedIntArray {
	class := intArrayClassOf(bitWidth)
	return &FixedIntArray{
		class: class,
		bits:  make([]byte, 0, (256*8)/class.bitWidth()),
	}
}

func (a *FixedIntArray) BitWidth() int {
	if a != nil {
		return a.class.bitWidth()
	}
	return 0
}

func (a *FixedIntArray) Append(v int64) {
	a.bits = a.class.append(a.bits, v)
}

func (a *FixedIntArray) Cap() int {
	if a != nil {
		return (8 * cap(a.bits)) / a.class.bitWidth()
	}
	return 0
}

func (a *FixedIntArray) Len() int {
	if a != nil {
		return (8 * len(a.bits)) / a.class.bitWidth()
	}
	return 0
}

func (a *FixedIntArray) Index(index int) int64 {
	return a.class.index(a.bits, index)
}

func (a *FixedIntArray) Bits() []byte {
	if a != nil {
		return a.bits
	}
	return nil
}

func (a *FixedIntArray) Reset() {
	if a != nil {
		a.bits = a.bits[:0]
	}
}

type DynamicIntArray struct {
	class    intArrayClass
	bits     []byte
	bitWidth int // cached to avoid an indirect method call in Append
}

func NewDynamicIntArray() *DynamicIntArray {
	class := intArrayClass8{}
	return &DynamicIntArray{
		class:    class,
		bits:     make([]byte, 0, 256),
		bitWidth: class.bitWidth(),
	}
}

func (a *DynamicIntArray) reclass(bitWidth int) {
	numItems := a.Len()
	newArray := &FixedIntArray{
		class: intArrayClassOf(bitWidth),
		bits:  make([]byte, 0, a.Cap()),
	}

	for i := 0; i < numItems; i++ { // TODO: optimize
		newArray.Append(a.Index(i))
	}

	*a = DynamicIntArray{
		class:    newArray.class,
		bits:     newArray.bits,
		bitWidth: newArray.class.bitWidth(),
	}
}

func (a *DynamicIntArray) BitWidth() int {
	if a != nil {
		return a.bitWidth
	}
	return 0
}

func (a *DynamicIntArray) Append(v int64) {
	if bitWidth := bits.Len64(uint64(v)); bitWidth > a.bitWidth {
		a.reclass(bitWidth)
	}
	a.bits = a.class.append(a.bits, v)
}

func (a *DynamicIntArray) Cap() int {
	if a != nil {
		return (8 * cap(a.bits)) / a.bitWidth
	}
	return 0
}

func (a *DynamicIntArray) Len() int {
	if a != nil {
		return (8 * len(a.bits)) / a.bitWidth
	}
	return 0
}

func (a *DynamicIntArray) Index(index int) int64 {
	return a.class.index(a.bits, index)
}

func (a *DynamicIntArray) Bits() []byte {
	if a != nil {
		return a.bits
	}
	return nil
}

func (a *DynamicIntArray) Reset() {
	if a != nil {
		a.class = intArrayClass8{}
		a.bits = a.bits[:0]
		a.bitWidth = a.class.bitWidth()
	}
}

type intArrayClass interface {
	bitWidth() int
	append([]byte, int64) []byte
	index([]byte, int) int64
}

func intArrayClassOf(bitWidth int) intArrayClass {
	// TODO: support representing bit widths smaller than 8: 1, 2, 4 would be
	// useful for parquet repetition and definition levels. This is blocked on
	// improving the RLE encoder to be able to offset at partial bytes in the
	// array buffer.
	switch {
	case bitWidth <= 8:
		return intArrayClass8{}
	case bitWidth <= 16:
		return intArrayClass16{}
	case bitWidth <= 24:
		return intArrayClass24{}
	case bitWidth <= 32:
		return intArrayClass32{}
	default:
		return intArrayClass64{}
	}
}

type intArrayClass8 struct{}

func (intArrayClass8) bitWidth() int { return 8 }

func (intArrayClass8) append(bits []byte, value int64) []byte { return append(bits, byte(value)) }

func (intArrayClass8) index(bits []byte, index int) int64 { return int64(int8(bits[index])) }

type intArrayClass16 struct{}

func (intArrayClass16) bitWidth() int { return 16 }

func (intArrayClass16) append(bits []byte, value int64) []byte {
	buf := [2]byte{}
	binary.LittleEndian.PutUint16(buf[:], uint16(value))
	return append(bits, buf[:]...)
}

func (intArrayClass16) index(bits []byte, index int) int64 {
	return int64(int16(binary.LittleEndian.Uint16(bits[2*index:])))
}

type intArrayClass24 struct{}

func (intArrayClass24) bitWidth() int { return 24 }

func (intArrayClass24) append(bits []byte, value int64) []byte {
	buf := [4]byte{}
	binary.LittleEndian.PutUint32(buf[:], uint32(value))
	return append(bits, buf[:3]...)
}

func (intArrayClass24) index(bits []byte, index int) int64 {
	buf := [4]byte{}
	copy(buf[:3], bits[3*index:])
	u64 := uint64(binary.LittleEndian.Uint32(buf[:]))
	// When the most-significant bit is set the value is negative and we must
	// expand the one bits in the highest byte.
	if ((u64 >> 23) & 1) != 0 {
		u64 |= 0xFFFFFFFFFF000000
	}
	return int64(u64)
}

type intArrayClass32 struct{}

func (intArrayClass32) bitWidth() int { return 32 }

func (intArrayClass32) append(bits []byte, value int64) []byte {
	buf := [4]byte{}
	binary.LittleEndian.PutUint32(buf[:], uint32(value))
	return append(bits, buf[:]...)
}

func (intArrayClass32) index(bits []byte, index int) int64 {
	return int64(int32(binary.LittleEndian.Uint32(bits[4*index:])))
}

type intArrayClass64 struct{}

func (intArrayClass64) bitWidth() int { return 64 }

func (intArrayClass64) append(bits []byte, value int64) []byte {
	buf := [8]byte{}
	binary.LittleEndian.PutUint64(buf[:], uint64(value))
	return append(bits, buf[:]...)
}

func (intArrayClass64) index(bits []byte, index int) int64 {
	return int64(binary.LittleEndian.Uint64(bits[8*index:]))
}

var (
	_ encoding.IntArray = (*FixedIntArray)(nil)
	_ encoding.IntArray = (*DynamicIntArray)(nil)
)
