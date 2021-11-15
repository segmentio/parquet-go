package parquet

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"unsafe"

	"github.com/google/uuid"
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/internal/bits"
)

var (
	ErrBufferFull = errors.New("page buffer is full")
)

type Buffer interface {
	io.Reader
	io.Writer
}

type BufferPool interface {
	GetBuffer() Buffer
	PutBuffer(Buffer)
}

func NewBufferPool() BufferPool { return new(bufferPool) }

type bufferPool struct{ sync.Pool }

func (pool *bufferPool) GetBuffer() Buffer {
	b, _ := pool.Get().(*buffer)
	if b == nil {
		b = new(buffer)
	} else {
		b.Reset()
	}
	return b
}

func (pool *bufferPool) PutBuffer(buf Buffer) {
	if b, _ := buf.(*buffer); b != nil {
		pool.Put(b)
	}
}

type buffer struct{ bytes.Buffer }

func (b *buffer) Close() error {
	b.Reset()
	return nil
}

type fileBufferPool struct {
	err     error
	tempdir string
	pattern string
}

func NewFileBufferPool(tempdir, pattern string) BufferPool {
	pool := &fileBufferPool{
		tempdir: tempdir,
		pattern: pattern,
	}
	pool.tempdir, pool.err = filepath.Abs(pool.tempdir)
	return pool
}

func (pool *fileBufferPool) GetBuffer() Buffer {
	if pool.err != nil {
		return &errorBuffer{err: pool.err}
	}
	f, err := os.CreateTemp(pool.tempdir, pool.pattern)
	if err != nil {
		return &errorBuffer{err: err}
	}
	return f
}

func (pool *fileBufferPool) PutBuffer(buf Buffer) {
	if f, _ := buf.(*os.File); f != nil {
		defer f.Close()
		os.Remove(f.Name())
	}
}

type errorBuffer struct{ err error }

func (errbuf *errorBuffer) Close() error                      { return nil }
func (errbuf *errorBuffer) Read([]byte) (int, error)          { return 0, errbuf.err }
func (errbuf *errorBuffer) Write([]byte) (int, error)         { return 0, errbuf.err }
func (errbuf *errorBuffer) ReadFrom(io.Reader) (int64, error) { return 0, errbuf.err }
func (errbuf *errorBuffer) WriteTo(io.Writer) (int64, error)  { return 0, errbuf.err }

var (
	_ io.ReaderFrom = (*errorBuffer)(nil)
	_ io.WriterTo   = (*errorBuffer)(nil)
)

type PageBuffer interface {
	Reset()

	Bounds() (min, max []byte)

	Less(v1, v2 []byte) bool

	WriteValue(Value) error

	WriteTo(encoding.Encoder) (numValues, distinctCount int, err error)
}

type booleanPageBuffer struct {
	typ    Type
	min    [1]byte
	max    [1]byte
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

func (buf *booleanPageBuffer) scan() (hasTrue, hasFalse bool) {
	for _, value := range buf.values {
		if value {
			hasTrue = true
		} else {
			hasFalse = true
		}
		if hasTrue && hasFalse {
			break
		}
	}
	return hasTrue, hasFalse
}

func (buf *booleanPageBuffer) Bounds() (min, max []byte) {
	buf.min[0] = 0
	buf.max[0] = 0

	if len(buf.values) > 0 {
		hasTrue, hasFalse := buf.scan()
		if !hasFalse {
			buf.min[0] = 1
		}
		if hasTrue {
			buf.max[0] = 1
		}
	}

	return buf.min[:], buf.max[:]
}

func (buf *booleanPageBuffer) Less(v1, v2 []byte) bool {
	return v1[0] < v2[0]
}

func (buf *booleanPageBuffer) WriteValue(v Value) error {
	if kind := v.Kind(); kind != Boolean {
		panic("cannot write " + kind.String() + " value to parquet column of type BOOLEAN")
	}
	if len(buf.values) == cap(buf.values) {
		return ErrBufferFull
	}
	buf.values = append(buf.values, v.Boolean())
	return nil
}

func (buf *booleanPageBuffer) WriteTo(enc encoding.Encoder) (int, int, error) {
	defer buf.Reset()

	hasTrue, hasFalse := buf.scan()
	distinctCount := 0
	if hasTrue {
		distinctCount++
	}
	if hasFalse {
		distinctCount++
	}

	values := buf.values
	enc.SetBitWidth(1)

	if err := enc.EncodeBoolean(values); err != nil {
		return 0, 0, err
	}

	return len(values), distinctCount, nil
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

func (buf *int32PageBuffer) Less(v1, v2 []byte) bool {
	i1 := int32(binary.LittleEndian.Uint32(v1))
	i2 := int32(binary.LittleEndian.Uint32(v2))
	return i1 < i2
}

func (buf *int32PageBuffer) WriteValue(v Value) error {
	if kind := v.Kind(); kind != Int32 {
		panic("cannot write " + kind.String() + " value to parquet column of type INT32")
	}
	if len(buf.values) == cap(buf.values) {
		return ErrBufferFull
	}
	buf.values = append(buf.values, v.Int32())
	return nil
}

func (buf *int32PageBuffer) WriteTo(enc encoding.Encoder) (int, int, error) {
	defer buf.Reset()
	values := buf.values
	enc.SetBitWidth(bits.MaxLen32(values))

	if err := enc.EncodeInt32(values); err != nil {
		return 0, 0, err
	}

	bits.SortInt32(values)
	return len(values), bits.CountDistinctInt32(values), nil
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

func (buf *int64PageBuffer) Less(v1, v2 []byte) bool {
	i1 := int64(binary.LittleEndian.Uint64(v1))
	i2 := int64(binary.LittleEndian.Uint64(v2))
	return i1 < i2
}

func (buf *int64PageBuffer) WriteTo(enc encoding.Encoder) (int, int, error) {
	defer buf.Reset()
	values := buf.values
	enc.SetBitWidth(bits.MaxLen64(values))

	if err := enc.EncodeInt64(values); err != nil {
		return 0, 0, err
	}

	bits.SortInt64(values)
	return len(values), bits.CountDistinctInt64(values), nil
}

func (buf *int64PageBuffer) WriteValue(v Value) error {
	if kind := v.Kind(); kind != Int64 {
		panic("cannot write " + kind.String() + " value to parquet column of type INT64")
	}
	if len(buf.values) == cap(buf.values) {
		return ErrBufferFull
	}
	buf.values = append(buf.values, v.Int64())
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

func (buf *int96PageBuffer) Less(v1, v2 []byte) bool {
	return bits.CompareInt96(*(*[12]byte)(v1), *(*[12]byte)(v2)) < 0
}

func (buf *int96PageBuffer) WriteValue(v Value) error {
	if kind := v.Kind(); kind != Int96 {
		panic("cannot write " + kind.String() + " value to parquet column of type INT96")
	}
	if len(buf.values) == cap(buf.values) {
		return ErrBufferFull
	}
	buf.values = append(buf.values, v.Int96())
	return nil
}

func (buf *int96PageBuffer) WriteTo(enc encoding.Encoder) (int, int, error) {
	defer buf.Reset()
	values := buf.values
	enc.SetBitWidth(bits.MaxLen96(values))

	if err := enc.EncodeInt96(values); err != nil {
		return 0, 0, err
	}

	bits.SortInt96(values)
	return len(values), bits.CountDistinctInt96(values), nil
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

func (buf *floatPageBuffer) Less(v1, v2 []byte) bool {
	f1 := math.Float32frombits(binary.LittleEndian.Uint32(v1))
	f2 := math.Float32frombits(binary.LittleEndian.Uint32(v2))
	return f1 < f2
}

func (buf *floatPageBuffer) WriteValue(v Value) error {
	if kind := v.Kind(); kind != Float {
		panic("cannot write " + kind.String() + " value to parquet column of type FLOAT")
	}
	if len(buf.values) == cap(buf.values) {
		return ErrBufferFull
	}
	buf.values = append(buf.values, v.Float())
	return nil
}

func (buf *floatPageBuffer) WriteTo(enc encoding.Encoder) (int, int, error) {
	defer buf.Reset()
	values := buf.values
	enc.SetBitWidth(32)

	if err := enc.EncodeFloat(values); err != nil {
		return 0, 0, err
	}

	bits.SortFloat32(values)
	return len(values), bits.CountDistinctFloat32(values), nil
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

func (buf *doublePageBuffer) Reset() {
	buf.values = buf.values[:0]
}

func (buf *doublePageBuffer) Bounds() (min, max []byte) {
	min64, max64 := bits.MinMaxFloat64(buf.values)
	binary.LittleEndian.PutUint64(buf.min[:], math.Float64bits(min64))
	binary.LittleEndian.PutUint64(buf.max[:], math.Float64bits(max64))
	return buf.min[:], buf.max[:]
}

func (buf *doublePageBuffer) Less(v1, v2 []byte) bool {
	d1 := math.Float64frombits(binary.LittleEndian.Uint64(v1))
	d2 := math.Float64frombits(binary.LittleEndian.Uint64(v2))
	return d1 < d2
}

func (buf *doublePageBuffer) WriteValue(v Value) error {
	if kind := v.Kind(); kind != Double {
		panic("cannot write " + kind.String() + " value to parquet column of type DOUBLE")
	}
	if len(buf.values) == cap(buf.values) {
		return ErrBufferFull
	}
	buf.values = append(buf.values, v.Double())
	return nil
}

func (buf *doublePageBuffer) WriteTo(enc encoding.Encoder) (int, int, error) {
	defer buf.Reset()
	values := buf.values
	enc.SetBitWidth(64)

	if err := enc.EncodeDouble(values); err != nil {
		return 0, 0, err
	}

	bits.SortFloat64(values)
	return len(values), bits.CountDistinctFloat64(values), nil
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

func (buf *byteArrayPageBuffer) Less(v1, v2 []byte) bool {
	return bytes.Compare(v1, v2) < 0
}

func (buf *byteArrayPageBuffer) WriteValue(v Value) error {
	if kind := v.Kind(); kind != ByteArray {
		panic("cannot write " + kind.String() + " value to parquet column of type BYTE_ARRAY")
	}
	return buf.write(v.ByteArray())
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

func (buf *byteArrayPageBuffer) WriteTo(enc encoding.Encoder) (int, int, error) {
	defer buf.Reset()
	values := buf.values
	enc.SetBitWidth(0)

	if err := enc.EncodeByteArray(values); err != nil {
		return 0, 0, err
	}

	bits.SortByteArray(values)
	return len(values), bits.CountDistinctByteArray(values), nil
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

func (buf *fixedLenByteArrayPageBuffer) Less(v1, v2 []byte) bool {
	return bytes.Compare(v1, v2) < 0
}

func (buf *fixedLenByteArrayPageBuffer) WriteValue(v Value) error {
	if kind := v.Kind(); kind != FixedLenByteArray {
		panic("cannot write " + kind.String() + " value to parquet column of type FIXED_LEN_BYTE_ARRAY")
	}
	b := v.ByteArray()
	if len(b) != buf.size {
		panic("cannot write " + v.Kind().String() + " value to parquet column with different fixed length")
	}
	return buf.write(b)
}

func (buf *fixedLenByteArrayPageBuffer) write(value []byte) error {
	if (cap(buf.data) - len(buf.data)) < len(value) {
		return ErrBufferFull
	}
	buf.data = append(buf.data, value...)
	return nil
}

func (buf *fixedLenByteArrayPageBuffer) WriteTo(enc encoding.Encoder) (int, int, error) {
	defer buf.Reset()
	size := buf.size
	data := buf.data
	enc.SetBitWidth(0)

	if err := enc.EncodeFixedLenByteArray(size, data); err != nil {
		return 0, 0, err
	}

	bits.SortFixedLenByteArray(size, data)
	return len(data) / size, bits.CountDistinctFixedLenByteArray(size, data), nil
}

type uint32PageBuffer struct{ *int32PageBuffer }

func (buf uint32PageBuffer) Bounds() (min, max []byte) {
	min32, max32 := bits.MinMaxUint32(bits.Int32ToUint32(buf.values))
	binary.LittleEndian.PutUint32(buf.min[:], min32)
	binary.LittleEndian.PutUint32(buf.max[:], max32)
	return buf.min[:], buf.max[:]
}

func (buf uint32PageBuffer) Less(v1, v2 []byte) bool {
	u1 := binary.LittleEndian.Uint32(v1)
	u2 := binary.LittleEndian.Uint32(v2)
	return u1 < u2
}

type uint64PageBuffer struct{ *int64PageBuffer }

func (buf uint64PageBuffer) Bounds() (min, max []byte) {
	min64, max64 := bits.MinMaxUint64(bits.Int64ToUint64(buf.values))
	binary.LittleEndian.PutUint64(buf.min[:], min64)
	binary.LittleEndian.PutUint64(buf.max[:], max64)
	return buf.min[:], buf.max[:]
}

func (buf uint64PageBuffer) Less(v1, v2 []byte) bool {
	u1 := binary.LittleEndian.Uint64(v1)
	u2 := binary.LittleEndian.Uint64(v2)
	return u1 < u2
}

type uuidPageBuffer struct{ *fixedLenByteArrayPageBuffer }

func (buf uuidPageBuffer) WriteValue(v Value) error {
	if kind := v.Kind(); kind != FixedLenByteArray {
		panic("cannot write " + kind.String() + " value to parquet column of type UUID")
	}
	b := v.ByteArray()
	if len(b) != 16 {
		u, err := uuid.ParseBytes(b)
		if err != nil {
			return err
		}
		b = u[:]
	}
	return buf.write(b)
}

func unsafeStringToBytes(s string) []byte {
	return unsafe.Slice(*(**byte)(unsafe.Pointer(&s)), len(s))
}
