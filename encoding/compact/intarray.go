package compact

import "encoding/binary"

// IntArray is an interface implemented by arrays of integers provided by this
// package.
type IntArray interface {
	IntArrayBuffer
	IntArrayView
	Reset()
}

// IntArrayBuffer is an interface presenting the methods of IntArray which
// support writing to the array.
type IntArrayBuffer interface {
	Append(int64)
}

// IntArrayView is an interface presenting the methods of IntArray which support
// reading values from the array.
type IntArrayView interface {
	BitWidth() int

	Len() int

	Index(int) int64

	Bytes() []byte

	EqualFunc() func([]byte, []byte) bool
}

// FixedIntArray is an array of integer values, compacted to use fewer bits to
// represent values in memory than if a the program had used a slice of
// fixed-size integers.
//
// The type supports representing integers with up to 64 bits of precision.
type FixedIntArray struct {
	class intArrayClass
	data  []byte
}

func NewFixedIntArray(bitWidth, capacity int) *FixedIntArray {
	var class intArrayClass

	// TODO: support representing bit widths smaller than 8: 1, 2, 4 would be
	// useful for parquet repetition and definition levels. This is blocked on
	// improving the RLE encoder to be able to offset at partial bytes in the
	// array buffer.
	switch {
	case bitWidth <= 8:
		class = intArrayClass8{}
	case bitWidth <= 16:
		class = intArrayClass16{}
	case bitWidth <= 24:
		class = intArrayClass24{}
	case bitWidth <= 32:
		class = intArrayClass32{}
	default:
		class = intArrayClass64{}
	}

	return &FixedIntArray{
		class: class,
		data:  make([]byte, 0, capacity*class.size()),
	}
}

func (a *FixedIntArray) BitWidth() int {
	if a != nil {
		return 8 * a.class.size()
	}
	return 0
}

func (a *FixedIntArray) Append(v int64) {
	a.data = a.class.append(a.data, v)
}

func (a *FixedIntArray) Len() int {
	if a != nil {
		return len(a.data) / a.class.size()
	}
	return 0
}

func (a *FixedIntArray) Index(index int) int64 {
	if a != nil {
		return a.class.index(a.data, index)
	}
	return 0
}

func (a *FixedIntArray) Bytes() []byte {
	if a != nil {
		return a.data
	}
	return nil
}

func (a *FixedIntArray) Reset() {
	if a != nil {
		a.data = a.data[:0]
	}
}

func (a *FixedIntArray) EqualFunc() func([]byte, []byte) bool {
	if a != nil {
		return a.class.equalFunc()
	}
	return nil
}

type equalFunc = func([]byte, []byte) bool

type intArrayClass interface {
	size() int

	append([]byte, int64) []byte

	index([]byte, int) int64

	equalFunc() equalFunc
}

type intArrayClass8 struct{}

func (intArrayClass8) size() int { return 1 }

func (intArrayClass8) append(data []byte, value int64) []byte { return append(data, byte(value)) }

func (intArrayClass8) index(data []byte, index int) int64 { return int64(data[index]) }

func (intArrayClass8) equalFunc() equalFunc { return equalInt8 }

type intArrayClass16 struct{}

func (intArrayClass16) size() int { return 2 }

func (intArrayClass16) append(data []byte, value int64) []byte {
	buf := [2]byte{}
	binary.LittleEndian.PutUint16(buf[:], uint16(value))
	return append(data, buf[:]...)
}

func (intArrayClass16) index(data []byte, index int) int64 {
	return int64(int16(binary.LittleEndian.Uint16(data[2*index:])))
}

func (intArrayClass16) equalFunc() equalFunc { return equalInt16 }

type intArrayClass24 struct{}

func (intArrayClass24) size() int { return 3 }

func (intArrayClass24) append(data []byte, value int64) []byte {
	buf := [4]byte{}
	binary.LittleEndian.PutUint32(buf[:], uint32(value))
	return append(data, buf[:3]...)
}

func (intArrayClass24) index(data []byte, index int) int64 {
	buf := [4]byte{}
	copy(buf[:3], data[3*index:])
	return int64(int32(binary.LittleEndian.Uint32(buf[:])))
}

func (intArrayClass24) equalFunc() equalFunc { return equalInt24 }

type intArrayClass32 struct{}

func (intArrayClass32) size() int { return 4 }

func (intArrayClass32) append(data []byte, value int64) []byte {
	buf := [4]byte{}
	binary.LittleEndian.PutUint32(buf[:], uint32(value))
	return append(data, buf[:]...)
}

func (intArrayClass32) index(data []byte, index int) int64 {
	return int64(int32(binary.LittleEndian.Uint32(data[4*index:])))
}

func (intArrayClass32) equalFunc() equalFunc { return equalInt32 }

type intArrayClass64 struct{}

func (intArrayClass64) size() int { return 8 }

func (intArrayClass64) append(data []byte, value int64) []byte {
	buf := [8]byte{}
	binary.LittleEndian.PutUint64(buf[:], uint64(value))
	return append(data, buf[:]...)
}

func (intArrayClass64) index(data []byte, index int) int64 {
	return int64(binary.LittleEndian.Uint64(data[8*index:]))
}

func (intArrayClass64) equalFunc() equalFunc { return equalInt64 }

func equalInt8(a, b []byte) bool  { return a[0] == b[0] }
func equalInt16(a, b []byte) bool { return *(*[2]byte)(a) == *(*[2]byte)(b) }
func equalInt24(a, b []byte) bool { return *(*[3]byte)(a) == *(*[3]byte)(b) }
func equalInt32(a, b []byte) bool { return *(*[4]byte)(a) == *(*[4]byte)(b) }
func equalInt64(a, b []byte) bool { return *(*[8]byte)(a) == *(*[8]byte)(b) }

var (
	_ IntArray = (*FixedIntArray)(nil)
)
