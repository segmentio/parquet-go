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

type PageWriter interface {
	ValueWriter

	Type() Type

	NumValues() int

	DistinctCount() int

	Bounds() (min, max Value)

	Flush() error

	Reset(encoding.Encoder)
}

type booleanPageWriter struct {
	typ     Type
	encoder encoding.Encoder
	values  []bool
	min     bool
	max     bool
	count   int
}

func newBooleanPageWriter(typ Type, encoder encoding.Encoder, bufferSize int) *booleanPageWriter {
	return &booleanPageWriter{
		typ:     typ,
		encoder: encoder,
		values:  make([]bool, 0, bufferSize),
	}
}

func (w *booleanPageWriter) Type() Type { return w.typ }

func (w *booleanPageWriter) NumValues() int { return w.count }

func (w *booleanPageWriter) DistinctCount() int { return 0 }

func (w *booleanPageWriter) Bounds() (min, max Value) {
	if w.count > 0 {
		min = makeValueBoolean(w.min)
		max = makeValueBoolean(w.max)
	}
	return min, max
}

func (w *booleanPageWriter) WriteValue(v Value) error {
	if len(w.values) == cap(w.values) {
		if err := w.Flush(); err != nil {
			return err
		}
	}

	value := v.Boolean()
	w.values = append(w.values, value)

	if w.count == 0 {
		w.min = value
		w.max = value
	} else {
		if value {
			w.max = value
		} else {
			w.min = value
		}
	}

	w.count++
	return nil
}

func (w *booleanPageWriter) Flush() error {
	defer func() { w.values = w.values[:0] }()
	return w.encoder.EncodeBoolean(w.values)
}

func (w *booleanPageWriter) Reset(encoder encoding.Encoder) {
	w.encoder = encoder
	w.values = w.values[:0]
	w.min = false
	w.max = false
	w.count = 0
}

type int32PageWriter struct {
	typ     Type
	encoder encoding.Encoder
	values  []int32
	min     int32
	max     int32
	count   int
	flush   int
}

func newInt32PageWriter(typ Type, encoder encoding.Encoder, bufferSize int) *int32PageWriter {
	return &int32PageWriter{
		typ:     typ,
		encoder: encoder,
		values:  make([]int32, 0, bufferSize/4),
	}
}

func (w *int32PageWriter) Type() Type { return w.typ }

func (w *int32PageWriter) NumValues() int { return w.count }

func (w *int32PageWriter) DistinctCount() int { return 0 }

func (w *int32PageWriter) Bounds() (min, max Value) {
	if w.count > 0 {
		minInt32, maxInt32 := w.bounds()
		min = makeValueInt32(minInt32)
		max = makeValueInt32(maxInt32)
	}
	return min, max
}

func (w *int32PageWriter) WriteValue(v Value) error {
	if len(w.values) == cap(w.values) {
		if err := w.Flush(); err != nil {
			return err
		}
	}
	w.values = append(w.values, v.Int32())
	w.count++
	return nil
}

func (w *int32PageWriter) Flush() error {
	defer func() { w.values = w.values[:0] }()
	w.min, w.max = w.bounds()
	w.flush++
	return w.encoder.EncodeInt32(w.values)
}

func (w *int32PageWriter) Reset(encoder encoding.Encoder) {
	w.encoder = encoder
	w.values = w.values[:0]
	w.min = 0
	w.max = 0
	w.count = 0
	w.flush = 0
}

func (w *int32PageWriter) bounds() (min, max int32) {
	min = w.min
	max = w.max
	if len(w.values) > 0 {
		minValue, maxValue := bits.MinMaxInt32(w.values)
		if w.flush == 0 {
			min = minValue
			max = maxValue
		} else {
			if minValue < min {
				min = minValue
			}
			if maxValue > max {
				max = maxValue
			}
		}
	}
	return min, max
}

type int64PageWriter struct {
	typ     Type
	encoder encoding.Encoder
	values  []int64
	min     int64
	max     int64
	count   int
	flush   int
}

func newInt64PageWriter(typ Type, encoder encoding.Encoder, bufferSize int) *int64PageWriter {
	return &int64PageWriter{
		typ:     typ,
		encoder: encoder,
		values:  make([]int64, 0, bufferSize/8),
	}
}

func (w *int64PageWriter) Type() Type { return w.typ }

func (w *int64PageWriter) NumValues() int { return w.count }

func (w *int64PageWriter) DistinctCount() int { return 0 }

func (w *int64PageWriter) Bounds() (min, max Value) {
	if w.count > 0 {
		minInt64, maxInt64 := w.bounds()
		min = makeValueInt64(minInt64)
		max = makeValueInt64(maxInt64)
	}
	return min, max
}

func (w *int64PageWriter) WriteValue(v Value) error {
	if len(w.values) == cap(w.values) {
		if err := w.Flush(); err != nil {
			return err
		}
	}
	w.values = append(w.values, v.Int64())
	w.count++
	return nil
}

func (w *int64PageWriter) Flush() error {
	defer func() { w.values = w.values[:0] }()
	w.min, w.max = w.bounds()
	w.flush++
	return w.encoder.EncodeInt64(w.values)
}

func (w *int64PageWriter) Reset(encoder encoding.Encoder) {
	w.encoder = encoder
	w.values = w.values[:0]
	w.min = 0
	w.max = 0
	w.count = 0
	w.flush = 0
}

func (w *int64PageWriter) bounds() (min, max int64) {
	min = w.min
	max = w.max
	if len(w.values) > 0 {
		minValue, maxValue := bits.MinMaxInt64(w.values)
		if w.flush == 0 {
			min = minValue
			max = maxValue
		} else {
			if minValue < min {
				min = minValue
			}
			if maxValue > max {
				max = maxValue
			}
		}
	}
	return min, max
}

type int96PageWriter struct {
	typ     Type
	encoder encoding.Encoder
	values  []int96
	min     int96
	max     int96
	count   int
	flush   int
}

func newInt96PageWriter(typ Type, encoder encoding.Encoder, bufferSize int) *int96PageWriter {
	return &int96PageWriter{
		typ:     typ,
		encoder: encoder,
		values:  make([]int96, 0, bufferSize/12),
	}
}

func (w *int96PageWriter) Type() Type { return w.typ }

func (w *int96PageWriter) NumValues() int { return w.count }

func (w *int96PageWriter) DistinctCount() int { return 0 }

func (w *int96PageWriter) Bounds() (min, max Value) {
	if w.count > 0 {
		minInt96, maxInt96 := w.bounds()
		min = makeValueInt96(minInt96)
		max = makeValueInt96(maxInt96)
	}
	return min, max
}

func (w *int96PageWriter) WriteValue(v Value) error {
	if len(w.values) == cap(w.values) {
		if err := w.Flush(); err != nil {
			return err
		}
	}
	w.values = append(w.values, v.Int96())
	w.count++
	return nil
}

func (w *int96PageWriter) Flush() error {
	defer func() { w.values = w.values[:0] }()
	w.min, w.max = w.bounds()
	w.flush++
	return w.encoder.EncodeInt96(w.values)
}

func (w *int96PageWriter) Reset(encoder encoding.Encoder) {
	w.encoder = encoder
	w.values = w.values[:0]
	w.min = int96{}
	w.max = int96{}
	w.count = 0
	w.flush = 0
}

func (w *int96PageWriter) bounds() (min, max int96) {
	min = w.min
	max = w.max
	if len(w.values) > 0 {
		minValue, maxValue := bits.MinMaxInt96(w.values)
		if w.flush == 0 {
			min = minValue
			max = maxValue
		} else {
			if bits.CompareInt96(minValue, min) < 0 {
				min = minValue
			}
			if bits.CompareInt96(maxValue, max) < 0 {
				max = maxValue
			}
		}
	}
	return min, max
}

type floatPageWriter struct {
	typ     Type
	encoder encoding.Encoder
	values  []float32
	min     float32
	max     float32
	count   int
	flush   int
}

func newFloatPageWriter(typ Type, encoder encoding.Encoder, bufferSize int) *floatPageWriter {
	return &floatPageWriter{
		typ:     typ,
		encoder: encoder,
		values:  make([]float32, 0, bufferSize/4),
	}
}

func (w *floatPageWriter) Type() Type { return w.typ }

func (w *floatPageWriter) NumValues() int { return w.count }

func (w *floatPageWriter) DistinctCount() int { return 0 }

func (w *floatPageWriter) Bounds() (min, max Value) {
	if w.count > 0 {
		minFloat, maxFloat := w.bounds()
		min = makeValueFloat(minFloat)
		max = makeValueFloat(maxFloat)
	}
	return min, max
}

func (w *floatPageWriter) WriteValue(v Value) error {
	if len(w.values) == cap(w.values) {
		if err := w.Flush(); err != nil {
			return err
		}
	}
	w.values = append(w.values, v.Float())
	w.count++
	return nil
}

func (w *floatPageWriter) Flush() error {
	defer func() { w.values = w.values[:0] }()
	w.min, w.max = w.bounds()
	w.flush++
	return w.encoder.EncodeFloat(w.values)
}

func (w *floatPageWriter) Reset(encoder encoding.Encoder) {
	w.encoder = encoder
	w.values = w.values[:0]
	w.min = 0
	w.max = 0
	w.count = 0
	w.flush = 0
}

func (w *floatPageWriter) bounds() (min, max float32) {
	min = w.min
	max = w.max
	if len(w.values) > 0 {
		minValue, maxValue := bits.MinMaxFloat32(w.values)
		if w.flush == 0 {
			min = minValue
			max = maxValue
		} else {
			if minValue < min {
				min = minValue
			}
			if maxValue > max {
				max = maxValue
			}
		}
	}
	return min, max
}

type doublePageWriter struct {
	typ     Type
	encoder encoding.Encoder
	values  []float64
	min     float64
	max     float64
	count   int
	flush   int
}

func newDoublePageWriter(typ Type, encoder encoding.Encoder, bufferSize int) *doublePageWriter {
	return &doublePageWriter{
		typ:     typ,
		encoder: encoder,
		values:  make([]float64, 0, bufferSize/8),
	}
}

func (w *doublePageWriter) Type() Type { return w.typ }

func (w *doublePageWriter) NumValues() int { return w.count }

func (w *doublePageWriter) DistinctCount() int { return 0 }

func (w *doublePageWriter) Bounds() (min, max Value) {
	if w.count > 0 {
		minDouble, maxDouble := w.bounds()
		min = makeValueDouble(minDouble)
		max = makeValueDouble(maxDouble)
	}
	return min, max
}

func (w *doublePageWriter) WriteValue(v Value) error {
	if len(w.values) == cap(w.values) {
		if err := w.Flush(); err != nil {
			return err
		}
	}
	w.values = append(w.values, v.Double())
	w.count++
	return nil
}

func (w *doublePageWriter) Flush() error {
	defer func() { w.values = w.values[:0] }()
	w.min, w.max = w.bounds()
	w.flush++
	return w.encoder.EncodeDouble(w.values)
}

func (w *doublePageWriter) Reset(encoder encoding.Encoder) {
	w.encoder = encoder
	w.values = w.values[:0]
	w.min = 0
	w.max = 0
	w.count = 0
	w.flush = 0
}

func (w *doublePageWriter) bounds() (min, max float64) {
	min = w.min
	max = w.max
	if len(w.values) > 0 {
		minValue, maxValue := bits.MinMaxFloat64(w.values)
		if w.flush == 0 {
			min = minValue
			max = maxValue
		} else {
			if minValue < min {
				min = minValue
			}
			if maxValue > max {
				max = maxValue
			}
		}
	}
	return min, max
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
		values:  make([]byte, 0, bufferSize),
	}
}

func (w *byteArrayPageWriter) Type() Type { return w.typ }

func (w *byteArrayPageWriter) NumValues() int { return w.count }

func (w *byteArrayPageWriter) DistinctCount() int { return 0 }

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

	if (4 + len(value)) > cap(w.values) {
		if len(w.values) != 0 {
			if err := w.Flush(); err != nil {
				return err
			}
		}
		w.values = plain.ByteArray(value)
		return w.Flush() // flush large values immediately
	}

	if (cap(w.values) - len(w.values)) < (4 + len(value)) {
		if err := w.Flush(); err != nil {
			return err
		}
	}

	w.values = plain.AppendByteArray(w.values, value)

	if w.count == 0 {
		w.setMin(value)
		w.setMax(value)
	} else {
		if string(value) < string(w.min) {
			w.setMin(value)
		}
		if string(value) > string(w.max) {
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
	count   int
	flush   int
}

func newFixedLenByteArrayPageWriter(typ Type, encoder encoding.Encoder, bufferSize int) *fixedLenByteArrayPageWriter {
	size := typ.Length()
	return &fixedLenByteArrayPageWriter{
		typ:     typ,
		encoder: encoder,
		size:    size,
		data:    make([]byte, 0, (bufferSize/size)*size),
		min:     make([]byte, size),
		max:     make([]byte, size),
	}
}

func (w *fixedLenByteArrayPageWriter) Type() Type { return w.typ }

func (w *fixedLenByteArrayPageWriter) NumValues() int { return w.count }

func (w *fixedLenByteArrayPageWriter) DistinctCount() int { return 0 }

func (w *fixedLenByteArrayPageWriter) Bounds() (min, max Value) {
	if w.count > 0 {
		minValue, maxValue := w.bounds()
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
		if err := w.Flush(); err != nil {
			return err
		}
	}

	w.data = append(w.data, value...)
	w.count++
	return nil
}

func (w *fixedLenByteArrayPageWriter) Flush() error {
	defer func() { w.data = w.data[:0] }()
	min, max := w.bounds()
	copy(w.min, min)
	copy(w.max, max)
	w.flush++
	return w.encoder.EncodeFixedLenByteArray(w.size, w.data)
}

func (w *fixedLenByteArrayPageWriter) Reset(encoder encoding.Encoder) {
	w.encoder = encoder
	w.data = w.data[:0]
	w.count = 0
	w.flush = 0
}

func (w *fixedLenByteArrayPageWriter) bounds() (min, max []byte) {
	min = w.min
	max = w.max
	if len(w.data) > 0 {
		minValue, maxValue := bits.MinMaxFixedLenByteArray(w.size, w.data)
		if w.flush == 0 {
			min = minValue
			max = maxValue
		} else {
			if bytes.Compare(minValue, min) < 0 {
				min = minValue
			}
			if bytes.Compare(maxValue, max) > 0 {
				max = maxValue
			}
		}
	}
	return min, max
}

type uint32PageWriter struct {
	typ     Type
	encoder encoding.Encoder
	values  []uint32
	min     uint32
	max     uint32
	count   int
	flush   int
}

func newUint32PageWriter(typ Type, encoder encoding.Encoder, bufferSize int) *uint32PageWriter {
	return &uint32PageWriter{
		typ:     typ,
		encoder: encoder,
		values:  make([]uint32, 0, bufferSize/4),
	}
}

func (w *uint32PageWriter) Type() Type { return w.typ }

func (w *uint32PageWriter) NumValues() int { return w.count }

func (w *uint32PageWriter) DistinctCount() int { return 0 }

func (w *uint32PageWriter) Bounds() (min, max Value) {
	if w.count > 0 {
		minUint32, maxUint32 := w.bounds()
		min = makeValueInt32(int32(minUint32))
		max = makeValueInt32(int32(maxUint32))
	}
	return min, max
}

func (w *uint32PageWriter) WriteValue(v Value) error {
	if len(w.values) == cap(w.values) {
		if err := w.Flush(); err != nil {
			return err
		}
	}
	w.values = append(w.values, uint32(v.Int32()))
	w.count++
	return nil
}

func (w *uint32PageWriter) Flush() error {
	defer func() { w.values = w.values[:0] }()
	w.min, w.max = w.bounds()
	w.flush++
	return w.encoder.EncodeInt32(bits.Uint32ToInt32(w.values))
}

func (w *uint32PageWriter) Reset(encoder encoding.Encoder) {
	w.encoder = encoder
	w.values = w.values[:0]
	w.min = 0
	w.max = 0
	w.count = 0
	w.flush = 0
}

func (w *uint32PageWriter) bounds() (min, max uint32) {
	min = w.min
	max = w.max
	if len(w.values) > 0 {
		minValue, maxValue := bits.MinMaxUint32(w.values)
		if w.flush == 0 {
			min = minValue
			max = maxValue
		} else {
			if minValue < min {
				min = minValue
			}
			if maxValue > max {
				max = maxValue
			}
		}
	}
	return min, max
}

type uint64PageWriter struct {
	typ     Type
	encoder encoding.Encoder
	values  []uint64
	min     uint64
	max     uint64
	count   int
	flush   int
}

func newUint64PageWriter(typ Type, encoder encoding.Encoder, bufferSize int) *uint64PageWriter {
	return &uint64PageWriter{
		typ:     typ,
		encoder: encoder,
		values:  make([]uint64, 0, bufferSize/8),
	}
}

func (w *uint64PageWriter) Type() Type { return w.typ }

func (w *uint64PageWriter) NumValues() int { return w.count }

func (w *uint64PageWriter) DistinctCount() int { return 0 }

func (w *uint64PageWriter) Bounds() (min, max Value) {
	if w.count > 0 {
		minUint64, maxUint64 := w.bounds()
		min = makeValueInt64(int64(minUint64))
		max = makeValueInt64(int64(maxUint64))
	}
	return min, max
}

func (w *uint64PageWriter) WriteValue(v Value) error {
	if len(w.values) == cap(w.values) {
		if err := w.Flush(); err != nil {
			return err
		}
	}
	w.values = append(w.values, uint64(v.Int64()))
	w.count++
	return nil
}

func (w *uint64PageWriter) Flush() error {
	defer func() { w.values = w.values[:0] }()
	w.min, w.max = w.bounds()
	w.flush++
	return w.encoder.EncodeInt64(bits.Uint64ToInt64(w.values))
}

func (w *uint64PageWriter) Reset(encoder encoding.Encoder) {
	w.encoder = encoder
	w.values = w.values[:0]
	w.min = 0
	w.max = 0
	w.count = 0
	w.flush = 0
}

func (w *uint64PageWriter) bounds() (min, max uint64) {
	min = w.min
	max = w.max
	if len(w.values) > 0 {
		minValue, maxValue := bits.MinMaxUint64(w.values)
		if w.flush == 0 {
			min = minValue
			max = maxValue
		} else {
			if minValue < min {
				min = minValue
			}
			if maxValue > max {
				max = maxValue
			}
		}
	}
	return min, max
}

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
	_ PageWriter = (*uint32PageWriter)(nil)
	_ PageWriter = (*uint64PageWriter)(nil)
	_ PageWriter = uuidPageWriter{}
)
