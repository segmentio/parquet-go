package parquet

import (
	"fmt"
	"math"

	"github.com/segmentio/parquet/deprecated"
	"github.com/segmentio/parquet/encoding/plain"
)

// PageWriter is an interface implemented by types which support writing valuees
// to a buffer and flushing it to an underlying encoder.
//
// PageWriter implements both ValueWriter and ValueBatchWriter as a mechanism to
// append values to the buffer; both methods are atomic operations, the values
// are either all written or none are and ErrBufferFull is returned.
type PageWriter interface {
	// Returns the type values written to the underlying page.
	Type() Type

	// Page returns a Page value containing the values have have been written to
	// the writer.
	//
	// The returned page shares the writer's underlying buffer, it remains valid
	// to use until the next call to Reset.
	Page() Page

	// Resets the encoder used to write values to the parquet page. This method
	// is useful to allow reusing writers. Calling this method drops all values
	// previously buffered by the writer.
	Reset()

	ValueWriter

	ValueBatchWriter
}

type booleanPageWriter struct{ booleanPage }

func newBooleanPageWriter(typ Type, bufferSize int) *booleanPageWriter {
	return &booleanPageWriter{
		booleanPage: booleanPage{
			typ:    typ,
			values: make([]bool, 0, atLeastOne(bufferSize)),
		},
	}
}

func (w *booleanPageWriter) Page() Page { return &w.booleanPage }

func (w *booleanPageWriter) Reset() { w.values = w.values[:0] }

func (w *booleanPageWriter) WriteValue(value Value) error {
	if len(w.values) == cap(w.values) {
		return ErrBufferFull
	}
	w.values = append(w.values, value.Boolean())
	return nil
}

func (w *booleanPageWriter) WriteValueBatch(values []Value) (int, error) {
	if len(w.values) > 0 && (cap(w.values)-len(w.values)) < len(values) {
		return 0, ErrBufferFull
	}
	for _, value := range values {
		w.values = append(w.values, value.Boolean())
	}
	return len(values), nil
}

type int32PageWriter struct{ int32Page }

func newInt32PageWriter(typ Type, bufferSize int) *int32PageWriter {
	return &int32PageWriter{
		int32Page: int32Page{
			typ:    typ,
			values: make([]int32, 0, atLeastOne(bufferSize/4)),
		},
	}
}

func (w *int32PageWriter) Page() Page { return &w.int32Page }

func (w *int32PageWriter) Reset() { w.values = w.values[:0] }

func (w *int32PageWriter) WriteValue(value Value) error {
	if len(w.values) == cap(w.values) {
		return ErrBufferFull
	}
	w.values = append(w.values, value.Int32())
	return nil
}

func (w *int32PageWriter) WriteValueBatch(values []Value) (int, error) {
	if len(w.values) > 0 && (cap(w.values)-len(w.values)) < len(values) {
		return 0, ErrBufferFull
	}
	for _, value := range values {
		w.values = append(w.values, value.Int32())
	}
	return len(values), nil
}

type int64PageWriter struct{ int64Page }

func newInt64PageWriter(typ Type, bufferSize int) *int64PageWriter {
	return &int64PageWriter{
		int64Page: int64Page{
			typ:    typ,
			values: make([]int64, 0, atLeastOne(bufferSize/8)),
		},
	}
}

func (w *int64PageWriter) Page() Page { return &w.int64Page }

func (w *int64PageWriter) Reset() { w.values = w.values[:0] }

func (w *int64PageWriter) WriteValue(value Value) error {
	if len(w.values) == cap(w.values) {
		return ErrBufferFull
	}
	w.values = append(w.values, value.Int64())
	return nil
}

func (w *int64PageWriter) WriteValueBatch(values []Value) (int, error) {
	if len(w.values) > 0 && (cap(w.values)-len(w.values)) < len(values) {
		return 0, ErrBufferFull
	}
	for _, value := range values {
		w.values = append(w.values, value.Int64())
	}
	return len(values), nil
}

type int96PageWriter struct{ int96Page }

func newInt96PageWriter(typ Type, bufferSize int) *int96PageWriter {
	return &int96PageWriter{
		int96Page: int96Page{
			typ:    typ,
			values: make([]deprecated.Int96, 0, atLeastOne(bufferSize/12)),
		},
	}
}

func (w *int96PageWriter) Page() Page { return &w.int96Page }

func (w *int96PageWriter) Reset() { w.values = w.values[:0] }

func (w *int96PageWriter) WriteValue(value Value) error {
	if len(w.values) == cap(w.values) {
		return ErrBufferFull
	}
	w.values = append(w.values, value.Int96())
	return nil
}

func (w *int96PageWriter) WriteValueBatch(values []Value) (int, error) {
	if len(w.values) > 0 && (cap(w.values)-len(w.values)) < len(values) {
		return 0, ErrBufferFull
	}
	for _, value := range values {
		w.values = append(w.values, value.Int96())
	}
	return len(values), nil
}

type floatPageWriter struct{ floatPage }

func newFloatPageWriter(typ Type, bufferSize int) *floatPageWriter {
	return &floatPageWriter{
		floatPage: floatPage{
			typ:    typ,
			values: make([]float32, 0, atLeastOne(bufferSize/4)),
		},
	}
}

func (w *floatPageWriter) Page() Page { return &w.floatPage }

func (w *floatPageWriter) Reset() { w.values = w.values[:0] }

func (w *floatPageWriter) WriteValue(value Value) error {
	if len(w.values) == cap(w.values) {
		return ErrBufferFull
	}
	w.values = append(w.values, value.Float())
	return nil
}

func (w *floatPageWriter) WriteValueBatch(values []Value) (int, error) {
	if len(w.values) > 0 && (cap(w.values)-len(w.values)) < len(values) {
		return 0, ErrBufferFull
	}
	for _, value := range values {
		w.values = append(w.values, value.Float())
	}
	return len(values), nil
}

type doublePageWriter struct{ doublePage }

func newDoublePageWriter(typ Type, bufferSize int) *doublePageWriter {
	return &doublePageWriter{
		doublePage: doublePage{
			typ:    typ,
			values: make([]float64, 0, atLeastOne(bufferSize/8)),
		},
	}
}

func (w *doublePageWriter) Page() Page { return &w.doublePage }

func (w *doublePageWriter) Reset() { w.values = w.values[:0] }

func (w *doublePageWriter) WriteValue(value Value) error {
	if len(w.values) == cap(w.values) {
		return ErrBufferFull
	}
	w.values = append(w.values, value.Double())
	return nil
}

func (w *doublePageWriter) WriteValueBatch(values []Value) (int, error) {
	if len(w.values) > 0 && (cap(w.values)-len(w.values)) < len(values) {
		return 0, ErrBufferFull
	}
	for _, value := range values {
		w.values = append(w.values, value.Double())
	}
	return len(values), nil
}

type byteArrayPageWriter struct{ byteArrayPage }

func newByteArrayPageWriter(typ Type, bufferSize int) *byteArrayPageWriter {
	return &byteArrayPageWriter{
		byteArrayPage: byteArrayPage{
			typ:    typ,
			values: make([]byte, 0, atLeast(bufferSize, 4)),
		},
	}
}

func (w *byteArrayPageWriter) Page() Page { return &w.byteArrayPage }

func (w *byteArrayPageWriter) Reset() { w.values, w.count = w.values[:0], 0 }

func (w *byteArrayPageWriter) WriteValue(value Value) error {
	b := value.ByteArray()

	if len(b) > (math.MaxInt32 - 4) {
		return fmt.Errorf("cannot write value of length %d to parquet byte array page", len(b))
	}

	if (cap(w.values) - len(w.values)) < (4 + len(b)) {
		if len(w.values) > 0 {
			return ErrBufferFull
		}
	}

	w.values = plain.AppendByteArray(w.values, b)
	w.count++
	return nil
}

func (w *byteArrayPageWriter) WriteValueBatch(values []Value) (int, error) {
	if len(values) == 0 {
		return 0, nil
	}

	totalSize := 0

	for _, value := range values {
		b := value.ByteArray()

		if len(b) > (math.MaxInt32 - 4) {
			return 0, fmt.Errorf("cannot write value of length %d to parquet byte array page", len(b))
		}

		totalSize += 4 + len(b)
	}

	if w.count > 0 && (cap(w.values)-len(w.values)) < totalSize {
		return 0, ErrBufferFull
	}

	for _, value := range values {
		w.values = plain.AppendByteArray(w.values, value.ByteArray())
	}

	w.count += len(values)
	return len(values), nil
}

type fixedLenByteArrayPageWriter struct{ fixedLenByteArrayPage }

func newFixedLenByteArrayPageWriter(typ Type, bufferSize int) *fixedLenByteArrayPageWriter {
	size := typ.Length()
	return &fixedLenByteArrayPageWriter{
		fixedLenByteArrayPage: fixedLenByteArrayPage{
			typ:  typ,
			size: size,
			data: make([]byte, 0, atLeast((bufferSize/size)*size, size)),
		},
	}
}

func (w *fixedLenByteArrayPageWriter) Page() Page { return &w.fixedLenByteArrayPage }

func (w *fixedLenByteArrayPageWriter) Reset() { w.data = w.data[:0] }

func (w *fixedLenByteArrayPageWriter) WriteValue(value Value) error {
	b := value.ByteArray()

	if len(b) != w.size {
		return fmt.Errorf("cannot write value of size %d to fixed length parquet page of size %d", len(b), w.size)
	}

	if (cap(w.data) - len(w.data)) < len(b) {
		return ErrBufferFull
	}

	w.data = append(w.data, b...)
	return nil
}

func (w *fixedLenByteArrayPageWriter) WriteValueBatch(values []Value) (int, error) {
	for _, value := range values {
		if b := value.ByteArray(); len(b) != w.size {
			return 0, fmt.Errorf("cannot write value of size %d to fixed length parquet page of size %d", len(b), w.size)
		}
	}

	n := len(w.data) / w.size
	c := cap(w.data) / w.size
	if n > 0 && (c-n) < len(values) {
		return 0, ErrBufferFull
	}

	for _, value := range values {
		w.data = append(w.data, value.ByteArray()...)
	}

	return len(values), nil
}

// The following two specializations for unsigned integer types are needed to
// apply an unsigned comparison when looking up the min and max page values.

type uint32PageWriter struct{ *int32PageWriter }

func newUint32PageWriter(typ Type, bufferSize int) uint32PageWriter {
	return uint32PageWriter{newInt32PageWriter(typ, bufferSize)}
}

func (w uint32PageWriter) Page() Page { return uint32Page{&w.int32Page} }

type uint64PageWriter struct{ *int64PageWriter }

func newUint64PageWriter(typ Type, bufferSize int) uint64PageWriter {
	return uint64PageWriter{newInt64PageWriter(typ, bufferSize)}
}

func (w uint64PageWriter) Page() Page { return uint64Page{&w.int64Page} }

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
)
