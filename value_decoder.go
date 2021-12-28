package parquet

import (
	"io"

	"github.com/segmentio/parquet/deprecated"
	"github.com/segmentio/parquet/encoding"
)

// ValueDecoder is an extension of the ValueReader interface for types that
// support decoding values from an underlying decoder.
type ValueDecoder interface {
	ValueReader
	Reset(encoding.Decoder)
}

type booleanValueDecoder struct {
	decoder encoding.Decoder
	values  []bool
	offset  uint
}

func newBooleanValueDecoder(bufferSize int) *booleanValueDecoder {
	return &booleanValueDecoder{
		values: make([]bool, 0, atLeastOne(bufferSize)),
	}
}

func (r *booleanValueDecoder) ReadValues(values []Value) (int, error) {
	i := 0
	for {
		for r.offset < uint(len(r.values)) && i < len(values) {
			values[i] = makeValueBoolean(r.values[r.offset])
			r.offset++
			i++
		}

		if i == len(values) {
			return i, nil
		}
		if r.decoder == nil {
			return i, io.EOF
		}

		n, err := r.decoder.DecodeBoolean(r.values[:cap(r.values)])
		if n == 0 {
			return i, err
		}

		r.values = r.values[:n]
		r.offset = 0
	}
}

func (r *booleanValueDecoder) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.values = r.values[:0]
	r.offset = 0
}

type int32ValueDecoder struct {
	decoder encoding.Decoder
	values  []int32
	offset  uint
}

func newInt32ValueDecoder(bufferSize int) *int32ValueDecoder {
	return &int32ValueDecoder{
		values: make([]int32, 0, atLeastOne(bufferSize/4)),
	}
}

func (r *int32ValueDecoder) ReadValues(values []Value) (int, error) {
	i := 0
	for {
		for r.offset < uint(len(r.values)) && i < len(values) {
			values[i] = makeValueInt32(r.values[r.offset])
			r.offset++
			i++
		}

		if i == len(values) {
			return i, nil
		}
		if r.decoder == nil {
			return i, io.EOF
		}

		n, err := r.decoder.DecodeInt32(r.values[:cap(r.values)])
		if n == 0 {
			return i, err
		}

		r.values = r.values[:n]
		r.offset = 0
	}
}

func (r *int32ValueDecoder) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.values = r.values[:0]
	r.offset = 0
}

type int64ValueDecoder struct {
	decoder encoding.Decoder
	values  []int64
	offset  uint
}

func newInt64ValueDecoder(bufferSize int) *int64ValueDecoder {
	return &int64ValueDecoder{
		values: make([]int64, 0, atLeastOne(bufferSize/8)),
	}
}

func (r *int64ValueDecoder) ReadValues(values []Value) (int, error) {
	i := 0
	for {
		for r.offset < uint(len(r.values)) && i < len(values) {
			values[i] = makeValueInt64(r.values[r.offset])
			r.offset++
			i++
		}

		if i == len(values) {
			return i, nil
		}
		if r.decoder == nil {
			return i, io.EOF
		}

		n, err := r.decoder.DecodeInt64(r.values[:cap(r.values)])
		if n == 0 {
			return i, err
		}

		r.values = r.values[:n]
		r.offset = 0
	}
}

func (r *int64ValueDecoder) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.values = r.values[:0]
	r.offset = 0
}

type int96ValueDecoder struct {
	decoder encoding.Decoder
	values  []deprecated.Int96
	offset  uint
}

func newInt96ValueDecoder(bufferSize int) *int96ValueDecoder {
	return &int96ValueDecoder{
		values: make([]deprecated.Int96, 0, atLeastOne(bufferSize/12)),
	}
}

func (r *int96ValueDecoder) ReadValues(values []Value) (int, error) {
	i := 0
	for {
		for r.offset < uint(len(r.values)) && i < len(values) {
			values[i] = makeValueInt96(r.values[r.offset])
			r.offset++
			i++
		}

		if i == len(values) {
			return i, nil
		}
		if r.decoder == nil {
			return i, io.EOF
		}

		n, err := r.decoder.DecodeInt96(r.values[:cap(r.values)])
		if n == 0 {
			return i, err
		}

		r.values = r.values[:n]
		r.offset = 0
	}
}

func (r *int96ValueDecoder) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.values = r.values[:0]
	r.offset = 0
}

type floatValueDecoder struct {
	decoder encoding.Decoder
	values  []float32
	offset  uint
}

func newFloatValueDecoder(bufferSize int) *floatValueDecoder {
	return &floatValueDecoder{
		values: make([]float32, 0, atLeastOne(bufferSize/4)),
	}
}

func (r *floatValueDecoder) ReadValues(values []Value) (int, error) {
	i := 0
	for {
		for r.offset < uint(len(r.values)) && i < len(values) {
			values[i] = makeValueFloat(r.values[r.offset])
			r.offset++
			i++
		}

		if i == len(values) {
			return i, nil
		}
		if r.decoder == nil {
			return i, io.EOF
		}

		n, err := r.decoder.DecodeFloat(r.values[:cap(r.values)])
		if n == 0 {
			return i, err
		}

		r.values = r.values[:n]
		r.offset = 0
	}
}

func (r *floatValueDecoder) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.values = r.values[:0]
	r.offset = 0
}

type doubleValueDecoder struct {
	decoder encoding.Decoder
	values  []float64
	offset  uint
}

func newDoubleValueDecoder(bufferSize int) *doubleValueDecoder {
	return &doubleValueDecoder{
		values: make([]float64, 0, atLeastOne(bufferSize/8)),
	}
}

func (r *doubleValueDecoder) ReadValues(values []Value) (int, error) {
	i := 0
	for {
		for r.offset < uint(len(r.values)) && i < len(values) {
			values[i] = makeValueDouble(r.values[r.offset])
			r.offset++
			i++
		}

		if i == len(values) {
			return i, nil
		}
		if r.decoder == nil {
			return i, io.EOF
		}

		n, err := r.decoder.DecodeDouble(r.values[:cap(r.values)])
		if n == 0 {
			return i, err
		}

		r.values = r.values[:n]
		r.offset = 0
	}
}

func (r *doubleValueDecoder) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.values = r.values[:0]
	r.offset = 0
}

type byteArrayValueDecoder struct {
	decoder encoding.Decoder
	values  encoding.ByteArrayList
	index   int
}

func newByteArrayValueDecoder(bufferSize int) *byteArrayValueDecoder {
	return &byteArrayValueDecoder{
		values: encoding.MakeByteArrayList(atLeastOne(bufferSize / 16)),
	}
}

func (r *byteArrayValueDecoder) ReadValues(values []Value) (int, error) {
	i := 0
	for {
		for r.index < r.values.Len() && i < len(values) {
			values[i] = makeValueBytes(ByteArray, copyBytes(r.values.Index(r.index)))
			r.index++
			i++
		}

		if i == len(values) {
			return i, nil
		}
		if r.decoder == nil {
			return i, io.EOF
		}

		r.values.Reset()
		n, err := r.decoder.DecodeByteArray(&r.values)
		if err != nil && n == 0 {
			return i, err
		}

		r.index = 0
	}
}

func (r *byteArrayValueDecoder) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.values.Reset()
	r.index = 0
}

type fixedLenByteArrayValueDecoder struct {
	decoder encoding.Decoder
	values  []byte
	offset  uint
	size    uint
}

func newFixedLenByteArrayValueDecoder(size, bufferSize int) *fixedLenByteArrayValueDecoder {
	return &fixedLenByteArrayValueDecoder{
		size:   uint(size),
		values: make([]byte, 0, atLeast((bufferSize/size)*size, size)),
	}
}

func (r *fixedLenByteArrayValueDecoder) ReadValues(values []Value) (int, error) {
	i := 0
	for {
		for (r.offset+r.size) <= uint(len(r.values)) && i < len(values) {
			values[i] = makeValueBytes(FixedLenByteArray, copyBytes(r.values[r.offset:r.offset+r.size]))
			r.offset += r.size
			i++
		}

		if i == len(values) {
			return i, nil
		}
		if r.decoder == nil {
			return i, io.EOF
		}

		n, err := r.decoder.DecodeFixedLenByteArray(int(r.size), r.values[:cap(r.values)])
		if n == 0 {
			return i, err
		}

		r.values = r.values[:uint(n)*r.size]
		r.offset = 0
	}
}

func (r *fixedLenByteArrayValueDecoder) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.values = r.values[:0]
	r.offset = 0
}

func atLeastOne(size int) int {
	return atLeast(size, 1)
}

func atLeast(size, least int) int {
	if size < least {
		return least
	}
	return size
}
