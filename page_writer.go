package parquet

import (
	"bytes"
	"fmt"
	"math"

	"github.com/google/uuid"
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/encoding/plain"
	"github.com/segmentio/parquet/internal/bits"
)

// PageWriter is an interface implemented by types which support writing valuees
// to a buffer and flushing it to an underlying encoder.
type PageWriter interface {
	ValueWriter

	// Returns the type values written to the underlying page.
	Type() Type

	// Returns the number of values currently buffered in the writer.
	NumValues() int

	// Returns the min and max values currently bufffered in the writter.
	//
	// When no values have been written, or the writer was flushed, the min and
	// max values are both null.
	Bounds() (min, max Value)

	// Flushes all buffered values to the underlying encoder.
	//
	// Flush must be called explicitly when calling WriteValue returns
	// ErrBufferFull, or when no more values will be written.
	//
	// The design decision of requiring explicit flushes was made to given
	// applications the opportunity to gather the number and bounds of values
	// before flushing the buffers.
	Flush() error

	// Resets the encoder used to write values to the parquet page. This method
	// is useful to allow reusing writers. Calling this method drops all values
	// previously buffered by the writer.
	Reset(encoding.Encoder)
}

type booleanPageWriter struct {
	typ     Type
	encoder encoding.Encoder
	values  []bool
}

func newBooleanPageWriter(typ Type, encoder encoding.Encoder, bufferSize int) *booleanPageWriter {
	return &booleanPageWriter{
		typ:     typ,
		encoder: encoder,
		values:  make([]bool, 0, atLeastOne(bufferSize)),
	}
}

func (w *booleanPageWriter) Type() Type { return w.typ }

func (w *booleanPageWriter) NumValues() int { return len(w.values) }

func (w *booleanPageWriter) Bounds() (min, max Value) {
	if len(w.values) > 0 {
		min = makeValueBoolean(false)
		max = makeValueBoolean(false)
		hasFalse, hasTrue := false, false

		for _, value := range w.values {
			if value {
				hasTrue = true
			} else {
				hasFalse = true
			}
			if hasTrue && hasFalse {
				break
			}
		}

		if !hasFalse {
			min = makeValueBoolean(true)
		}
		if hasTrue {
			max = makeValueBoolean(true)
		}
	}
	return min, max
}

func (w *booleanPageWriter) WriteValue(v Value) error {
	if len(w.values) == cap(w.values) {
		return ErrBufferFull
	}
	w.values = append(w.values, v.Boolean())
	return nil
}

func (w *booleanPageWriter) Flush() error {
	defer func() { w.values = w.values[:0] }()
	return w.encoder.EncodeBoolean(w.values)
}

func (w *booleanPageWriter) Reset(encoder encoding.Encoder) {
	w.encoder, w.values = encoder, w.values[:0]
}

type int32PageWriter struct {
	typ     Type
	encoder encoding.Encoder
	values  []int32
}

func newInt32PageWriter(typ Type, encoder encoding.Encoder, bufferSize int) *int32PageWriter {
	return &int32PageWriter{
		typ:     typ,
		encoder: encoder,
		values:  make([]int32, 0, atLeastOne(bufferSize/4)),
	}
}

func (w *int32PageWriter) Type() Type { return w.typ }

func (w *int32PageWriter) NumValues() int { return len(w.values) }

func (w *int32PageWriter) Bounds() (min, max Value) {
	if len(w.values) > 0 {
		minInt32, maxInt32 := bits.MinMaxInt32(w.values)
		min = makeValueInt32(minInt32)
		max = makeValueInt32(maxInt32)
	}
	return min, max
}

func (w *int32PageWriter) WriteValue(v Value) error {
	if len(w.values) == cap(w.values) {
		return ErrBufferFull
	}
	w.values = append(w.values, v.Int32())
	return nil
}

func (w *int32PageWriter) Flush() error {
	defer func() { w.values = w.values[:0] }()
	return w.encoder.EncodeInt32(w.values)
}

func (w *int32PageWriter) Reset(encoder encoding.Encoder) {
	w.encoder, w.values = encoder, w.values[:0]
}

type int64PageWriter struct {
	typ     Type
	encoder encoding.Encoder
	values  []int64
}

func newInt64PageWriter(typ Type, encoder encoding.Encoder, bufferSize int) *int64PageWriter {
	return &int64PageWriter{
		typ:     typ,
		encoder: encoder,
		values:  make([]int64, 0, atLeastOne(bufferSize/8)),
	}
}

func (w *int64PageWriter) Type() Type { return w.typ }

func (w *int64PageWriter) NumValues() int { return len(w.values) }

func (w *int64PageWriter) Bounds() (min, max Value) {
	if len(w.values) > 0 {
		minInt64, maxInt64 := bits.MinMaxInt64(w.values)
		min = makeValueInt64(minInt64)
		max = makeValueInt64(maxInt64)
	}
	return min, max
}

func (w *int64PageWriter) WriteValue(v Value) error {
	if len(w.values) == cap(w.values) {
		return ErrBufferFull
	}
	w.values = append(w.values, v.Int64())
	return nil
}

func (w *int64PageWriter) Flush() error {
	defer func() { w.values = w.values[:0] }()
	return w.encoder.EncodeInt64(w.values)
}

func (w *int64PageWriter) Reset(encoder encoding.Encoder) {
	w.encoder, w.values = encoder, w.values[:0]
}

type int96PageWriter struct {
	typ     Type
	encoder encoding.Encoder
	values  []int96
}

func newInt96PageWriter(typ Type, encoder encoding.Encoder, bufferSize int) *int96PageWriter {
	return &int96PageWriter{
		typ:     typ,
		encoder: encoder,
		values:  make([]int96, 0, atLeastOne(bufferSize/12)),
	}
}

func (w *int96PageWriter) Type() Type { return w.typ }

func (w *int96PageWriter) NumValues() int { return len(w.values) }

func (w *int96PageWriter) Bounds() (min, max Value) {
	if len(w.values) > 0 {
		minInt96, maxInt96 := bits.MinMaxInt96(w.values)
		min = makeValueInt96(minInt96)
		max = makeValueInt96(maxInt96)
	}
	return min, max
}

func (w *int96PageWriter) WriteValue(v Value) error {
	if len(w.values) == cap(w.values) {
		return ErrBufferFull
	}
	w.values = append(w.values, v.Int96())
	return nil
}

func (w *int96PageWriter) Flush() error {
	defer func() { w.values = w.values[:0] }()
	return w.encoder.EncodeInt96(w.values)
}

func (w *int96PageWriter) Reset(encoder encoding.Encoder) {
	w.encoder, w.values = encoder, w.values[:0]
}

type floatPageWriter struct {
	typ     Type
	encoder encoding.Encoder
	values  []float32
}

func newFloatPageWriter(typ Type, encoder encoding.Encoder, bufferSize int) *floatPageWriter {
	return &floatPageWriter{
		typ:     typ,
		encoder: encoder,
		values:  make([]float32, 0, atLeastOne(bufferSize/4)),
	}
}

func (w *floatPageWriter) Type() Type { return w.typ }

func (w *floatPageWriter) NumValues() int { return len(w.values) }

func (w *floatPageWriter) Bounds() (min, max Value) {
	if len(w.values) > 0 {
		minFloat, maxFloat := bits.MinMaxFloat32(w.values)
		min = makeValueFloat(minFloat)
		max = makeValueFloat(maxFloat)
	}
	return min, max
}

func (w *floatPageWriter) WriteValue(v Value) error {
	if len(w.values) == cap(w.values) {
		return ErrBufferFull
	}
	w.values = append(w.values, v.Float())
	return nil
}

func (w *floatPageWriter) Flush() error {
	defer func() { w.values = w.values[:0] }()
	return w.encoder.EncodeFloat(w.values)
}

func (w *floatPageWriter) Reset(encoder encoding.Encoder) {
	w.encoder, w.values = encoder, w.values[:0]
}

type doublePageWriter struct {
	typ     Type
	encoder encoding.Encoder
	values  []float64
}

func newDoublePageWriter(typ Type, encoder encoding.Encoder, bufferSize int) *doublePageWriter {
	return &doublePageWriter{
		typ:     typ,
		encoder: encoder,
		values:  make([]float64, 0, atLeastOne(bufferSize/8)),
	}
}

func (w *doublePageWriter) Type() Type { return w.typ }

func (w *doublePageWriter) NumValues() int { return len(w.values) }

func (w *doublePageWriter) Bounds() (min, max Value) {
	if len(w.values) > 0 {
		minDouble, maxDouble := bits.MinMaxFloat64(w.values)
		min = makeValueDouble(minDouble)
		max = makeValueDouble(maxDouble)
	}
	return min, max
}

func (w *doublePageWriter) WriteValue(v Value) error {
	if len(w.values) == cap(w.values) {
		return ErrBufferFull
	}
	w.values = append(w.values, v.Double())
	return nil
}

func (w *doublePageWriter) Flush() error {
	defer func() { w.values = w.values[:0] }()
	return w.encoder.EncodeDouble(w.values)
}

func (w *doublePageWriter) Reset(encoder encoding.Encoder) {
	w.encoder, w.values = encoder, w.values[:0]
}

type byteArrayPageWriter struct {
	typ     Type
	encoder encoding.Encoder
	values  []byte
	min     []byte
	max     []byte
	count   int
}

func newByteArrayPageWriter(typ Type, encoder encoding.Encoder, bufferSize int) *byteArrayPageWriter {
	return &byteArrayPageWriter{
		typ:     typ,
		encoder: encoder,
		values:  make([]byte, 0, atLeast(bufferSize, 4)),
	}
}

func (w *byteArrayPageWriter) Type() Type { return w.typ }

func (w *byteArrayPageWriter) NumValues() int { return w.count }

func (w *byteArrayPageWriter) Bounds() (min, max Value) {
	if w.count > 0 {
		min = makeValueBytes(ByteArray, w.min)
		max = makeValueBytes(ByteArray, w.max)
	}
	return min, max
}

func (w *byteArrayPageWriter) WriteValue(v Value) error {
	value := v.ByteArray()

	if len(value) > (math.MaxInt32 - 4) {
		return fmt.Errorf("cannot write value of length %d to parquet byte array page", len(value))
	}

	if (cap(w.values) - len(w.values)) < (4 + len(value)) {
		if len(w.values) > 0 {
			return ErrBufferFull
		}
	}

	w.values = plain.AppendByteArray(w.values, value)

	if w.count == 0 {
		w.setMin(value)
		w.setMax(value)
	} else {
		if bytes.Compare(value, w.min) < 0 {
			w.setMin(value)
		}
		if bytes.Compare(value, w.max) > 0 {
			w.setMax(value)
		}
	}

	w.count++
	return nil
}

func (w *byteArrayPageWriter) setMin(min []byte) {
	w.min = append(w.min[:0], min...)
}

func (w *byteArrayPageWriter) setMax(max []byte) {
	w.max = append(w.max[:0], max...)
}

func (w *byteArrayPageWriter) Flush() error {
	defer func() { w.values = w.values[:0] }()
	return w.encoder.EncodeByteArray(w.values)
}

func (w *byteArrayPageWriter) Reset(encoder encoding.Encoder) {
	w.encoder = encoder
	w.values = w.values[:0]
	w.min = w.min[:0]
	w.max = w.max[:0]
	w.count = 0
}

type fixedLenByteArrayPageWriter struct {
	typ     Type
	encoder encoding.Encoder
	size    int
	data    []byte
	min     []byte
	max     []byte
}

func newFixedLenByteArrayPageWriter(typ Type, encoder encoding.Encoder, bufferSize int) *fixedLenByteArrayPageWriter {
	size := typ.Length()
	return &fixedLenByteArrayPageWriter{
		typ:     typ,
		encoder: encoder,
		size:    size,
		data:    make([]byte, 0, atLeast((bufferSize/size)*size, size)),
		min:     make([]byte, size),
		max:     make([]byte, size),
	}
}

func (w *fixedLenByteArrayPageWriter) Type() Type { return w.typ }

func (w *fixedLenByteArrayPageWriter) NumValues() int { return len(w.data) / w.size }

func (w *fixedLenByteArrayPageWriter) Bounds() (min, max Value) {
	if len(w.data) > 0 {
		minValue, maxValue := bits.MinMaxFixedLenByteArray(w.size, w.data)
		min = makeValueBytes(FixedLenByteArray, minValue)
		max = makeValueBytes(FixedLenByteArray, maxValue)
	}
	return min, max
}

func (w *fixedLenByteArrayPageWriter) WriteValue(v Value) error {
	return w.write(v.ByteArray())
}

func (w *fixedLenByteArrayPageWriter) write(value []byte) error {
	if len(value) != w.size {
		return fmt.Errorf("cannot write value of size %d to fixed length parquet page of size %d", len(value), w.size)
	}

	if (cap(w.data) - len(w.data)) < len(value) {
		return ErrBufferFull
	}

	w.data = append(w.data, value...)
	return nil
}

func (w *fixedLenByteArrayPageWriter) Flush() error {
	defer func() { w.data = w.data[:0] }()
	return w.encoder.EncodeFixedLenByteArray(w.size, w.data)
}

func (w *fixedLenByteArrayPageWriter) Reset(encoder encoding.Encoder) {
	w.encoder, w.data = encoder, w.data[:0]
}

// The following two specializations for unsigned integer types are needed to
// apply an unsigned comparison when looking up the min and max page values.

type uint32PageWriter struct{ *int32PageWriter }

func newUint32PageWriter(typ Type, encoder encoding.Encoder, bufferSize int) uint32PageWriter {
	return uint32PageWriter{newInt32PageWriter(typ, encoder, bufferSize)}
}

func (w uint32PageWriter) Bounds() (min, max Value) {
	if len(w.values) > 0 {
		minUint32, maxUint32 := bits.MinMaxUint32(bits.Int32ToUint32(w.values))
		min = makeValueInt32(int32(minUint32))
		max = makeValueInt32(int32(maxUint32))
	}
	return min, max
}

type uint64PageWriter struct{ *int64PageWriter }

func newUint64PageWriter(typ Type, encoder encoding.Encoder, bufferSize int) uint64PageWriter {
	return uint64PageWriter{newInt64PageWriter(typ, encoder, bufferSize)}
}

func (w uint64PageWriter) Bounds() (min, max Value) {
	if len(w.values) > 0 {
		minUint64, maxUint64 := bits.MinMaxUint64(bits.Int64ToUint64(w.values))
		min = makeValueInt64(int64(minUint64))
		max = makeValueInt64(int64(maxUint64))
	}
	return min, max
}

// uuidPageWriter is a specialization for nodes of the logical UUID type which
// supports parsing string representations of UUIDs into their fixed length 16
// bytes array.
//
// The specialization is implemented as an adapter on top of the generic writer
// for pages of fixed length byte arrays.
type uuidPageWriter struct{ *fixedLenByteArrayPageWriter }

func (w uuidPageWriter) WriteValue(v Value) error {
	b := v.ByteArray()
	if len(b) != 16 {
		u, err := uuid.ParseBytes(b)
		if err != nil {
			return err
		}
		b = u[:]
	}
	return w.write(b)
}

var (
	_ PageWriter = (*booleanPageWriter)(nil)
	_ PageWriter = (*int32PageWriter)(nil)
	_ PageWriter = (*int64PageWriter)(nil)
	_ PageWriter = (*int96PageWriter)(nil)
	_ PageWriter = (*floatPageWriter)(nil)
	_ PageWriter = (*doublePageWriter)(nil)
	_ PageWriter = (*byteArrayPageWriter)(nil)
	_ PageWriter = (*fixedLenByteArrayPageWriter)(nil)
	_ PageWriter = uint32PageWriter{}
	_ PageWriter = uint64PageWriter{}
	_ PageWriter = uuidPageWriter{}
)
