package parquet

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/internal/bits"
)

var (
	ErrBufferFull = errors.New("page buffer is full")
)

var (
	defaultBufferPool bufferPool
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
	ValueWriter

	Type() Type

	Reset()

	NumValues() int

	DistinctCount() int

	Bounds() (min, max Value)

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

func (buf *booleanPageBuffer) Type() Type { return buf.typ }

func (buf *booleanPageBuffer) Reset() { buf.values = buf.values[:0] }

func (buf *booleanPageBuffer) NumValues() int { return len(buf.values) }

func (buf *booleanPageBuffer) DistinctCount() int {
	hasTrue, hasFalse := buf.scan()
	distinctCount := 0
	if hasTrue {
		distinctCount++
	}
	if hasFalse {
		distinctCount++
	}
	return distinctCount
}

func (buf *booleanPageBuffer) Bounds() (Value, Value) {
	min := makeValueBoolean(false)
	max := makeValueBoolean(false)

	if len(buf.values) > 0 {
		hasTrue, hasFalse := buf.scan()
		if !hasFalse {
			min = makeValueBoolean(true)
		}
		if hasTrue {
			max = makeValueBoolean(true)
		}
	}

	return min, max
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

func (buf *booleanPageBuffer) WriteValue(v Value) error {
	if len(buf.values) == cap(buf.values) {
		return ErrBufferFull
	}
	buf.values = append(buf.values, v.Boolean())
	return nil
}

func (buf *booleanPageBuffer) WriteTo(enc encoding.Encoder) error {
	enc.SetBitWidth(1)
	return enc.EncodeBoolean(buf.values)
}

type int32PageBuffer struct {
	typ    Type
	values []int32
}

func newInt32PageBuffer(typ Type, bufferSize int) *int32PageBuffer {
	return &int32PageBuffer{
		typ:    typ,
		values: make([]int32, 0, bufferSize/4),
	}
}

func (buf *int32PageBuffer) Type() Type { return buf.typ }

func (buf *int32PageBuffer) Reset() { buf.values = buf.values[:0] }

func (buf *int32PageBuffer) NumValues() int { return len(buf.values) }

func (buf *int32PageBuffer) DistinctCount() int { return 0 }

func (buf *int32PageBuffer) Bounds() (Value, Value) {
	min, max := bits.MinMaxInt32(buf.values)
	return makeValueInt32(min), makeValueInt32(max)
}

func (buf *int32PageBuffer) WriteValue(v Value) error {
	if len(buf.values) == cap(buf.values) {
		return ErrBufferFull
	}
	buf.values = append(buf.values, v.Int32())
	return nil
}

func (buf *int32PageBuffer) WriteTo(enc encoding.Encoder) error {
	enc.SetBitWidth(bits.MaxLen32(buf.values))
	return enc.EncodeInt32(buf.values)
}

type int64PageBuffer struct {
	typ    Type
	values []int64
}

func newInt64PageBuffer(typ Type, bufferSize int) *int64PageBuffer {
	return &int64PageBuffer{
		typ:    typ,
		values: make([]int64, 0, bufferSize/8),
	}
}

func (buf *int64PageBuffer) Type() Type { return buf.typ }

func (buf *int64PageBuffer) Reset() { buf.values = buf.values[:0] }

func (buf *int64PageBuffer) NumValues() int { return len(buf.values) }

func (buf *int64PageBuffer) DistinctCount() int { return 0 }

func (buf *int64PageBuffer) Bounds() (Value, Value) {
	min, max := bits.MinMaxInt64(buf.values)
	return makeValueInt64(min), makeValueInt64(max)
}

func (buf *int64PageBuffer) WriteValue(v Value) error {
	if len(buf.values) == cap(buf.values) {
		return ErrBufferFull
	}
	buf.values = append(buf.values, v.Int64())
	return nil
}

func (buf *int64PageBuffer) WriteTo(enc encoding.Encoder) error {
	enc.SetBitWidth(bits.MaxLen64(buf.values))
	return enc.EncodeInt64(buf.values)
}

type int96PageBuffer struct {
	typ    Type
	values [][12]byte
}

func newInt96PageBuffer(typ Type, bufferSize int) *int96PageBuffer {
	return &int96PageBuffer{
		typ:    typ,
		values: make([][12]byte, 0, bufferSize/12),
	}
}

func (buf *int96PageBuffer) Type() Type { return buf.typ }

func (buf *int96PageBuffer) Reset() { buf.values = buf.values[:0] }

func (buf *int96PageBuffer) NumValues() int { return len(buf.values) }

func (buf *int96PageBuffer) DistinctCount() int { return 0 }

func (buf *int96PageBuffer) Bounds() (Value, Value) {
	min, max := bits.MinMaxInt96(buf.values)
	return makeValueInt96(min), makeValueInt96(max)
}

func (buf *int96PageBuffer) WriteValue(v Value) error {
	if len(buf.values) == cap(buf.values) {
		return ErrBufferFull
	}
	buf.values = append(buf.values, v.Int96())
	return nil
}

func (buf *int96PageBuffer) WriteTo(enc encoding.Encoder) error {
	enc.SetBitWidth(bits.MaxLen96(buf.values))
	return enc.EncodeInt96(buf.values)
}

type floatPageBuffer struct {
	typ    Type
	values []float32
}

func newFloatPageBuffer(typ Type, bufferSize int) *floatPageBuffer {
	return &floatPageBuffer{
		typ:    typ,
		values: make([]float32, 0, bufferSize/4),
	}
}

func (buf *floatPageBuffer) Type() Type { return buf.typ }

func (buf *floatPageBuffer) Reset() { buf.values = buf.values[:0] }

func (buf *floatPageBuffer) NumValues() int { return len(buf.values) }

func (buf *floatPageBuffer) DistinctCount() int { return 0 }

func (buf *floatPageBuffer) Bounds() (Value, Value) {
	min, max := bits.MinMaxFloat32(buf.values)
	return makeValueFloat(min), makeValueFloat(max)
}

func (buf *floatPageBuffer) WriteValue(v Value) error {
	if len(buf.values) == cap(buf.values) {
		return ErrBufferFull
	}
	buf.values = append(buf.values, v.Float())
	return nil
}

func (buf *floatPageBuffer) WriteTo(enc encoding.Encoder) error {
	enc.SetBitWidth(32)
	return enc.EncodeFloat(buf.values)
}

type doublePageBuffer struct {
	typ    Type
	values []float64
}

func newDoublePageBuffer(typ Type, bufferSize int) *doublePageBuffer {
	return &doublePageBuffer{
		typ:    typ,
		values: make([]float64, 0, bufferSize/8),
	}
}

func (buf *doublePageBuffer) Type() Type { return buf.typ }

func (buf *doublePageBuffer) Reset() { buf.values = buf.values[:0] }

func (buf *doublePageBuffer) NumValues() int { return len(buf.values) }

func (buf *doublePageBuffer) DistinctCount() int { return 0 }

func (buf *doublePageBuffer) Bounds() (Value, Value) {
	min, max := bits.MinMaxFloat64(buf.values)
	return makeValueDouble(min), makeValueDouble(max)
}

func (buf *doublePageBuffer) WriteValue(v Value) error {
	if len(buf.values) == cap(buf.values) {
		return ErrBufferFull
	}
	buf.values = append(buf.values, v.Double())
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
	bufferSize /= 2
	return &byteArrayPageBuffer{
		typ:    typ,
		buffer: make([]byte, 0, bufferSize),
		values: make([][]byte, 0, bufferSize/24),
	}
}

func (buf *byteArrayPageBuffer) Type() Type { return buf.typ }

func (buf *byteArrayPageBuffer) Reset() {
	buf.buffer = buf.buffer[:0]
	buf.values = buf.values[:0]
}

func (buf *byteArrayPageBuffer) NumValues() int { return len(buf.values) }

func (buf *byteArrayPageBuffer) DistinctCount() int { return 0 }

func (buf *byteArrayPageBuffer) Bounds() (Value, Value) {
	min, max := bits.MinMaxByteArray(buf.values)
	min = copyBytes(min)
	max = copyBytes(max)
	return makeValueBytes(ByteArray, min), makeValueBytes(ByteArray, max)
}

func (buf *byteArrayPageBuffer) WriteValue(v Value) error {
	if len(buf.values) == cap(buf.values) {
		return ErrBufferFull
	}

	value := v.ByteArray()

	if len(value) > cap(buf.buffer) {
		if len(buf.buffer) != 0 {
			return ErrBufferFull
		}
		buf.buffer = make([]byte, len(value))
		buf.values = append(buf.values[:0], buf.buffer)
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
	size := typ.Length()
	return &fixedLenByteArrayPageBuffer{
		typ:  typ,
		size: size,
		data: make([]byte, 0, (bufferSize/size)*size),
	}
}

func (buf *fixedLenByteArrayPageBuffer) Type() Type { return buf.typ }

func (buf *fixedLenByteArrayPageBuffer) Reset() { buf.data = buf.data[:0] }

func (buf *fixedLenByteArrayPageBuffer) NumValues() int { return len(buf.data) / buf.size }

func (buf *fixedLenByteArrayPageBuffer) DistinctCount() int { return 0 }

func (buf *fixedLenByteArrayPageBuffer) Bounds() (Value, Value) {
	min, max := bits.MinMaxFixedLenByteArray(buf.size, buf.data)
	min = copyBytes(min)
	max = copyBytes(max)
	return makeValueBytes(FixedLenByteArray, min), makeValueBytes(FixedLenByteArray, max)
}

func (buf *fixedLenByteArrayPageBuffer) WriteValue(v Value) error {
	return buf.write(v.ByteArray())
}

func (buf *fixedLenByteArrayPageBuffer) write(value []byte) error {
	if (cap(buf.data) - len(buf.data)) < len(value) {
		return ErrBufferFull
	}
	buf.data = append(buf.data, value...)
	return nil
}

func (buf *fixedLenByteArrayPageBuffer) WriteTo(enc encoding.Encoder) error {
	enc.SetBitWidth(8 * buf.size)
	return enc.EncodeFixedLenByteArray(buf.size, buf.data)
}

type uint32PageBuffer struct{ *int32PageBuffer }

func (buf uint32PageBuffer) Bounds() (Value, Value) {
	min, max := bits.MinMaxUint32(bits.Int32ToUint32(buf.values))
	return makeValueInt32(int32(min)), makeValueInt32(int32(max))
}

type uint64PageBuffer struct{ *int64PageBuffer }

func (buf uint64PageBuffer) Bounds() (Value, Value) {
	min, max := bits.MinMaxUint64(bits.Int64ToUint64(buf.values))
	return makeValueInt64(int64(min)), makeValueInt64(int64(max))
}

type uuidPageBuffer struct{ *fixedLenByteArrayPageBuffer }

func (buf uuidPageBuffer) WriteValue(v Value) error {
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

func copyBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
