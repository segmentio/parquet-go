package parquet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"unsafe"

	"github.com/google/uuid"
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/internal/bits"
)

var (
	ErrBufferFull = errors.New("page buffer is full")
)

type PageBuffer interface {
	Reset()

	Bounds() (min, max []byte)

	WriteValue(reflect.Value) error

	WriteTo(encoding.Encoder) error
}

type booleanPageBuffer struct {
	typ    Type
	values []bool
}

func newBooleanPageBuffer(typ Type, bufferSize int) *booleanPageBuffer {
	return &booleanPageBuffer{
		typ:    typ,
		values: make([]bool, 0, bufferSize),
	}
}

func (buf *booleanPageBuffer) Reset() {
	buf.values = buf.values[:0]
}

func (buf *booleanPageBuffer) Bounds() (min, max []byte) {
	return nil, nil // TODO: do boolean columns have min/max?
}

func (buf *booleanPageBuffer) WriteValue(v reflect.Value) error {
	if len(buf.values) == cap(buf.values) {
		return ErrBufferFull
	}
	buf.values = append(buf.values, v.Bool())
	return nil
}

func (buf *booleanPageBuffer) WriteTo(enc encoding.Encoder) error {
	enc.SetBitWidth(1)
	return enc.EncodeBoolean(buf.values)
}

type int32PageBuffer struct {
	typ    Type
	min    [4]byte
	max    [4]byte
	values []int32
}

func newInt32PageBuffer(typ Type, bufferSize int) *int32PageBuffer {
	return &int32PageBuffer{
		typ:    typ,
		values: make([]int32, 0, bufferSize/4),
	}
}

func (buf *int32PageBuffer) Reset() {
	buf.values = buf.values[:0]
}

func (buf *int32PageBuffer) Bounds() (min, max []byte) {
	min32, max32 := bits.MinMaxInt32(buf.values)
	binary.LittleEndian.PutUint32(buf.min[:], uint32(min32))
	binary.LittleEndian.PutUint32(buf.max[:], uint32(max32))
	return buf.min[:], buf.max[:]
}

func (buf *int32PageBuffer) WriteValue(v reflect.Value) error {
	if len(buf.values) == cap(buf.values) {
		return ErrBufferFull
	}
	buf.values = append(buf.values, int32(v.Int()))
	return nil
}

func (buf *int32PageBuffer) WriteTo(enc encoding.Encoder) error {
	enc.SetBitWidth(bits.MaxLen32(buf.values))
	return enc.EncodeInt32(buf.values)
}

type int64PageBuffer struct {
	typ    Type
	min    [8]byte
	max    [8]byte
	values []int64
}

func newInt64PageBuffer(typ Type, bufferSize int) *int64PageBuffer {
	return &int64PageBuffer{
		typ:    typ,
		values: make([]int64, 0, bufferSize/8),
	}
}

func (buf *int64PageBuffer) Reset() {
	buf.values = buf.values[:0]
}

func (buf *int64PageBuffer) Bounds() (min, max []byte) {
	min64, max64 := bits.MinMaxInt64(buf.values)
	binary.LittleEndian.PutUint64(buf.min[:], uint64(min64))
	binary.LittleEndian.PutUint64(buf.max[:], uint64(max64))
	return buf.min[:], buf.max[:]
}

func (buf *int64PageBuffer) WriteTo(enc encoding.Encoder) error {
	enc.SetBitWidth(bits.MaxLen64(buf.values))
	return enc.EncodeInt64(buf.values)
}

func (buf *int64PageBuffer) WriteValue(v reflect.Value) error {
	if len(buf.values) == cap(buf.values) {
		return ErrBufferFull
	}
	buf.values = append(buf.values, v.Int())
	return nil
}

type int96PageBuffer struct {
	typ    Type
	min    [12]byte
	max    [12]byte
	values [][12]byte
}

func newInt96PageBuffer(typ Type, bufferSize int) *int96PageBuffer {
	return &int96PageBuffer{
		typ:    typ,
		values: make([][12]byte, 0, bufferSize/12),
	}
}

func (buf *int96PageBuffer) Reset() {
	buf.values = buf.values[:0]
}

func (buf *int96PageBuffer) Bounds() (min, max []byte) {
	buf.min, buf.max = bits.MinMaxInt96(buf.values)
	return buf.min[:], buf.max[:]
}

func (buf *int96PageBuffer) WriteValue(v reflect.Value) error {
	if len(buf.values) == cap(buf.values) {
		return ErrBufferFull
	}
	buf.values = append(buf.values, v.Interface().([12]byte)) // TODO: can we optimize this?
	return nil
}

func (buf *int96PageBuffer) WriteTo(enc encoding.Encoder) error {
	enc.SetBitWidth(bits.MaxLen96(buf.values))
	return enc.EncodeInt96(buf.values)
}

type floatPageBuffer struct {
	typ    Type
	min    [4]byte
	max    [4]byte
	values []float32
}

func newFloatPageBuffer(typ Type, bufferSize int) *floatPageBuffer {
	return &floatPageBuffer{
		typ:    typ,
		values: make([]float32, 0, bufferSize/4),
	}
}

func (buf *floatPageBuffer) Reset() {
	buf.values = buf.values[:0]
}

func (buf *floatPageBuffer) Bounds() (min, max []byte) {
	min32, max32 := bits.MinMaxFloat32(buf.values)
	binary.LittleEndian.PutUint32(buf.min[:], math.Float32bits(min32))
	binary.LittleEndian.PutUint32(buf.max[:], math.Float32bits(max32))
	return buf.min[:], buf.max[:]
}

func (buf *floatPageBuffer) WriteValue(v reflect.Value) error {
	if len(buf.values) == cap(buf.values) {
		return ErrBufferFull
	}
	buf.values = append(buf.values, float32(v.Float()))
	return nil
}

func (buf *floatPageBuffer) WriteTo(enc encoding.Encoder) error {
	enc.SetBitWidth(32)
	return enc.EncodeFloat(buf.values)
}

type doublePageBuffer struct {
	typ    Type
	min    [8]byte
	max    [8]byte
	values []float64
}

func newDoublePageBuffer(typ Type, bufferSize int) *doublePageBuffer {
	return &doublePageBuffer{
		typ:    typ,
		values: make([]float64, 0, bufferSize/8),
	}
}

func (buf *doublePageBuffer) Bounds() (min, max []byte) {
	min64, max64 := bits.MinMaxFloat64(buf.values)
	binary.LittleEndian.PutUint64(buf.min[:], math.Float64bits(min64))
	binary.LittleEndian.PutUint64(buf.max[:], math.Float64bits(max64))
	return buf.min[:], buf.max[:]
}

func (buf *doublePageBuffer) Reset() {
	buf.values = buf.values[:0]
}

func (buf *doublePageBuffer) WriteValue(v reflect.Value) error {
	if len(buf.values) == cap(buf.values) {
		return ErrBufferFull
	}
	buf.values = append(buf.values, v.Float())
	return nil
}

func (buf *doublePageBuffer) WriteTo(enc encoding.Encoder) error {
	enc.SetBitWidth(64)
	return enc.EncodeDouble(buf.values)
}

type byteArrayPageBuffer struct {
	typ    Type
	buffer []byte
	values [][]byte
}

func newByteArrayPageBuffer(typ Type, bufferSize int) *byteArrayPageBuffer {
	return &byteArrayPageBuffer{
		buffer: make([]byte, 0, bufferSize/2),
		values: make([][]byte, 0, bufferSize/8),
	}
}

func (buf *byteArrayPageBuffer) Reset() {
	buf.buffer = buf.buffer[:0]
	buf.values = buf.values[:0]
}

func (buf *byteArrayPageBuffer) Bounds() (min, max []byte) {
	return bits.MinMaxByteArray(buf.values)
}

func (buf *byteArrayPageBuffer) WriteValue(v reflect.Value) error {
	return buf.write(v.Bytes())
}

func (buf *byteArrayPageBuffer) write(value []byte) error {
	if len(value) > cap(buf.buffer) {
		if len(buf.buffer) != 0 {
			return ErrBufferFull
		}
		buf.buffer = make([]byte, len(value))
		buf.values = [][]byte{buf.buffer}
		copy(buf.buffer, value)
		return nil
	}

	if (cap(buf.buffer) - len(buf.buffer)) < len(value) {
		return ErrBufferFull
	}

	i := len(buf.buffer)
	j := len(buf.buffer) + len(value)
	buf.buffer = append(buf.buffer, value...)
	buf.values = append(buf.values, buf.buffer[i:j:j])
	return nil
}

func (buf *byteArrayPageBuffer) WriteTo(enc encoding.Encoder) error {
	enc.SetBitWidth(0)
	return enc.EncodeByteArray(buf.values)
}

type fixedLenByteArrayPageBuffer struct {
	typ  Type
	size int
	data []byte
}

func newFixedLenByteArrayPageBuffer(typ Type, bufferSize int) *fixedLenByteArrayPageBuffer {
	return &fixedLenByteArrayPageBuffer{
		typ:  typ,
		size: typ.Length(),
		data: make([]byte, 0, bufferSize),
	}
}

func (buf *fixedLenByteArrayPageBuffer) Reset() {
	buf.data = buf.data[:0]
}

func (buf *fixedLenByteArrayPageBuffer) Bounds() (min, max []byte) {
	return bits.MinMaxFixedLenByteArray(buf.size, buf.data)
}

func (buf *fixedLenByteArrayPageBuffer) WriteValue(v reflect.Value) error {
	if v.Kind() != reflect.Array {
		panic(fmt.Sprintf("cannot write value of type %s to fixed length byte array buffer of size %d", v.Type(), buf.size))
	}
	if v.Len() != buf.size {
		panic(fmt.Sprintf("cannot write value of size %d to fixed length byte array buffer of size %d", v.Len(), buf.size))
	}
	iface := (*[2]unsafe.Pointer)(unsafe.Pointer(&v))
	return buf.write(unsafe.Slice((*byte)(iface[1]), buf.size))
}

func (buf *fixedLenByteArrayPageBuffer) write(value []byte) error {
	if (cap(buf.data) - len(buf.data)) < len(value) {
		return ErrBufferFull
	}
	buf.data = append(buf.data, value...)
	return nil
}

func (buf *fixedLenByteArrayPageBuffer) WriteTo(enc encoding.Encoder) error {
	enc.SetBitWidth(0)
	return enc.EncodeFixedLenByteArray(buf.size, buf.data)
}

type uint32PageBuffer struct{ *int32PageBuffer }

func (buf uint32PageBuffer) Bounds() (min, max []byte) {
	min32, max32 := bits.MinMaxUint32(bits.Int32ToUint32(buf.values))
	binary.LittleEndian.PutUint32(buf.min[:], uint32(min32))
	binary.LittleEndian.PutUint32(buf.max[:], uint32(max32))
	return buf.min[:], buf.max[:]
}

func (buf uint32PageBuffer) WriteValue(v reflect.Value) error {
	if len(buf.values) == cap(buf.values) {
		return ErrBufferFull
	}
	buf.values = append(buf.values, int32(v.Uint()))
	return nil
}

type uint64PageBuffer struct{ *int64PageBuffer }

func (buf uint64PageBuffer) Bounds() (min, max []byte) {
	min64, max64 := bits.MinMaxUint64(bits.Int64ToUint64(buf.values))
	binary.LittleEndian.PutUint64(buf.min[:], uint64(min64))
	binary.LittleEndian.PutUint64(buf.max[:], uint64(max64))
	return buf.min[:], buf.max[:]
}

func (buf uint64PageBuffer) WriteValue(v reflect.Value) error {
	if len(buf.values) == cap(buf.values) {
		return ErrBufferFull
	}
	buf.values = append(buf.values, int64(v.Uint()))
	return nil
}

type stringPageBuffer struct{ *byteArrayPageBuffer }

func (buf stringPageBuffer) WriteValue(v reflect.Value) error {
	return buf.write(unsafeStringToBytes(v.String()))
}

type uuidPageBuffer struct{ *fixedLenByteArrayPageBuffer }

func (buf uuidPageBuffer) WriteValue(v reflect.Value) error {
	u, err := uuid.Parse(v.String())
	if err != nil {
		return err
	}
	return buf.write(u[:])
}

func unsafeStringToBytes(s string) []byte {
	return unsafe.Slice(*(**byte)(unsafe.Pointer(&s)), len(s))
}
