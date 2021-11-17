package encoding

import (
	"github.com/segmentio/parquet/internal/bits"
)

// IntArray is an interface implemented by arrays of integers.
type IntArray interface {
	IntArrayBuffer
	IntArrayView
	Reset()
}

// IntArrayBuffer is an interface presenting the methods of IntArray which
// support writing to the array.
type IntArrayBuffer interface {
	// Appends a value to the array.
	Append(int64)

	// Appends a range of bit-packed integers to the array.
	AppendBits(data []byte, bitWidth int)
}

// IntArrayView is an interface presenting the methods of IntArray which support
// reading values from the array.
type IntArrayView interface {
	// Returns the number of items in the array.
	Len() int

	// Returns the value at the given index in the array.
	Index(int) int64

	// Returns the underlying backing array as a byte slice.
	//
	// The returned slice shares the memory are of the array, the program must
	// treat it as an immutable value or the behavior is undefined.
	Bits() []byte

	// Returns the bit width of items in the array.
	BitWidth() int
}

func NewIntArray() IntArray {
	return &dynamicIntArray{new(fixedIntArray8)}
}

func NewFixedIntArray(bitWidth int) IntArray {
	return newFixedIntArray(bitWidth, 256)
}

func newFixedIntArray(bitWidth, bufferSize int) IntArray {
	// TODO: support representing bit widths smaller than 8: 1, 2, 4 would be
	// useful for parquet repetition and definition levels. This is blocked on
	// improving the RLE encoder to be able to offset at partial bytes in the
	// array buffer.
	switch {
	case bitWidth <= 8:
		return &fixedIntArray8{values: make([]int8, 0, bufferSize)}
	case bitWidth <= 16:
		return &fixedIntArray16{values: make([]int16, 0, bufferSize/2)}
	case bitWidth <= 32:
		return &fixedIntArray32{values: make([]int32, 0, bufferSize/4)}
	default:
		return &fixedIntArray64{values: make([]int64, 0, bufferSize/8)}
	}
}

type fixedIntArray8 struct{ values []int8 }

func (a *fixedIntArray8) BitWidth() int     { return 8 }
func (a *fixedIntArray8) Len() int          { return len(a.values) }
func (a *fixedIntArray8) Index(i int) int64 { return int64(a.values[i]) }
func (a *fixedIntArray8) Bits() []byte      { return bits.Int8ToBytes(a.values) }
func (a *fixedIntArray8) Reset()            { a.values = a.values[:0] }
func (a *fixedIntArray8) Append(v int64)    { a.values = append(a.values, int8(v)) }
func (a *fixedIntArray8) AppendBits(data []byte, bitWidth int) {
	values := appendBits(bits.Int8ToBytes(a.values[:cap(a.values)])[:len(a.values)], 8, data, bitWidth)
	a.values = bits.BytesToInt8(values[:cap(values)])[:len(values)]
}

type fixedIntArray16 struct{ values []int16 }

func (a *fixedIntArray16) BitWidth() int     { return 16 }
func (a *fixedIntArray16) Len() int          { return len(a.values) }
func (a *fixedIntArray16) Index(i int) int64 { return int64(a.values[i]) }
func (a *fixedIntArray16) Bits() []byte      { return bits.Int16ToBytes(a.values) }
func (a *fixedIntArray16) Reset()            { a.values = a.values[:0] }
func (a *fixedIntArray16) Append(v int64)    { a.values = append(a.values, int16(v)) }
func (a *fixedIntArray16) AppendBits(data []byte, bitWidth int) {
	values := appendBits(bits.Int16ToBytes(a.values[:cap(a.values)])[:len(a.values)*2], 16, data, bitWidth)
	a.values = bits.BytesToInt16(values[:cap(values)])[:len(values)/2]
}

type fixedIntArray32 struct{ values []int32 }

func (a *fixedIntArray32) BitWidth() int     { return 32 }
func (a *fixedIntArray32) Len() int          { return len(a.values) }
func (a *fixedIntArray32) Index(i int) int64 { return int64(a.values[i]) }
func (a *fixedIntArray32) Bits() []byte      { return bits.Int32ToBytes(a.values) }
func (a *fixedIntArray32) Reset()            { a.values = a.values[:0] }
func (a *fixedIntArray32) Append(v int64)    { a.values = append(a.values, int32(v)) }
func (a *fixedIntArray32) AppendBits(data []byte, bitWidth int) {
	values := appendBits(bits.Int32ToBytes(a.values[:cap(a.values)])[:len(a.values)*4], 32, data, bitWidth)
	a.values = bits.BytesToInt32(values[:cap(values)])[:len(values)/4]
}

type fixedIntArray64 struct{ values []int64 }

func (a *fixedIntArray64) BitWidth() int     { return 64 }
func (a *fixedIntArray64) Len() int          { return len(a.values) }
func (a *fixedIntArray64) Index(i int) int64 { return int64(a.values[i]) }
func (a *fixedIntArray64) Bits() []byte      { return bits.Int64ToBytes(a.values) }
func (a *fixedIntArray64) Reset()            { a.values = a.values[:0] }
func (a *fixedIntArray64) Append(v int64)    { a.values = append(a.values, int64(v)) }
func (a *fixedIntArray64) AppendBits(data []byte, bitWidth int) {
	values := appendBits(bits.Int64ToBytes(a.values[:cap(a.values)])[:len(a.values)*8], 64, data, bitWidth)
	a.values = bits.BytesToInt64(values[:cap(values)])[:len(values)/8]
}

type dynamicIntArray struct{ IntArray }

func (d *dynamicIntArray) reclass(dstWidth, srcWidth int) {
	a := newFixedIntArray(dstWidth, 0)
	a.AppendBits(d.Bits(), srcWidth)
	d.IntArray = a
}

func (d *dynamicIntArray) reclassIfNeeded(dstWidth int) {
	if srcWidth := d.BitWidth(); dstWidth > srcWidth {
		d.reclass(dstWidth, srcWidth)
	}
}

func (d *dynamicIntArray) Append(value int64) {
	d.reclassIfNeeded(bits.Len64(value))
	d.IntArray.Append(value)
}

func (d *dynamicIntArray) AppendBits(data []byte, bitWidth int) {
	d.reclassIfNeeded(bitWidth)
	d.IntArray.AppendBits(data, bitWidth)
}

func appendBits(dst []byte, dstWidth int, src []byte, srcWidth int) []byte {
	dstCap := (8 * cap(dst)) / dstWidth
	dstLen := (8 * len(dst)) / dstWidth
	srcLen := (8 * len(src)) / srcWidth

	if avail := dstCap - dstLen; avail < srcLen {
		newCap := 2 * dstCap
		if newCap == 0 {
			newCap = srcLen
		}
		for (newCap - dstLen) < srcLen {
			newCap *= 2
		}
		tmp := make([]byte, len(dst), (newCap*dstWidth)/8)
		copy(tmp, dst)
		dst = tmp
	}

	offset := len(dst)
	dst = dst[:offset+bits.ByteCount(uint(dstWidth*srcLen))]

	bits.Pack(dst[offset:], uint(dstWidth), src, uint(srcWidth))
	return dst
}
