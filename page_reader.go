package parquet

import (
	"fmt"
	"io"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/encoding/plain"
	"github.com/segmentio/parquet/internal/bits"
)

// PageReader is an interface implemented by types that support reading values
// from pages of parquet files.
//
// The values read from the page do not have repetition or definition levels
// set, use a DataPageReader to decode values with levels.
type PageReader interface {
	ValueReader

	// Returns the type of values read from the underlying page.
	Type() Type

	// Resets the decoder used to read values from the parquet page. This method
	// is useful to allow reusing readers. Calling this method drops all values
	// previously buffered by the reader.
	Reset(encoding.Decoder)
}

// DataPageReader reads values from a data page.
//
// DataPageReader implements the ValueReader interface; when they exist,
// the reader decodes repetition and definition levels in order to assign
// levels to values returned to the application, which includes producing
// null values when needed.
type DataPageReader struct {
	page               PageReader
	remain             int
	maxRepetitionLevel int8
	maxDefinitionLevel int8
	repetition         levelReader
	definition         levelReader
}

func NewDataPageReader(repetition, definition encoding.Decoder, numValues int, page PageReader, maxRepetitionLevel, maxDefinitionLevel int8, bufferSize int) *DataPageReader {
	repetitionBufferSize := 0
	definitionBufferSize := 0

	switch {
	case maxRepetitionLevel > 0 && maxDefinitionLevel > 0:
		repetitionBufferSize = bufferSize / 2
		definitionBufferSize = bufferSize / 2

	case maxRepetitionLevel > 0:
		repetitionBufferSize = bufferSize

	case maxDefinitionLevel > 0:
		definitionBufferSize = bufferSize
	}

	repetition.SetBitWidth(bits.Len8(maxRepetitionLevel))
	definition.SetBitWidth(bits.Len8(maxDefinitionLevel))
	return &DataPageReader{
		page:               page,
		remain:             numValues,
		maxRepetitionLevel: maxRepetitionLevel,
		maxDefinitionLevel: maxDefinitionLevel,
		repetition:         makeLevelReader(repetition, repetitionBufferSize),
		definition:         makeLevelReader(definition, definitionBufferSize),
	}
}

func (r *DataPageReader) ReadValue() (Value, error) {
	if r.remain == 0 {
		return Value{}, io.EOF
	}

	var val Value
	var err error
	var repetitionLevel int8
	var definitionLevel int8

	switch {
	case r.maxRepetitionLevel > 0:
		repetitionLevel, err = r.repetition.ReadLevel()
		if err != nil {
			return val, fmt.Errorf("reading parquet repetition level: %w", err)
		}
		fallthrough

	case r.maxDefinitionLevel > 0:
		definitionLevel, err = r.definition.ReadLevel()
		if err != nil {
			return val, fmt.Errorf("reading parquet definition level: %w", err)
		}
	}

	if definitionLevel == r.maxDefinitionLevel {
		val, err = r.page.ReadValue()
	}

	val.repetitionLevel = repetitionLevel
	val.definitionLevel = definitionLevel
	r.remain--
	return val, err
}

func (r *DataPageReader) Reset(repetition, definition encoding.Decoder, numValues int, page PageReader) {
	repetition.SetBitWidth(bits.Len8(r.maxRepetitionLevel))
	definition.SetBitWidth(bits.Len8(r.maxDefinitionLevel))
	r.page = page
	r.remain = numValues
	r.repetition.Reset(repetition)
	r.definition.Reset(definition)
}

type levelReader struct {
	decoder encoding.Decoder
	levels  []int8
	offset  uint
}

func makeLevelReader(decoder encoding.Decoder, bufferSize int) levelReader {
	return levelReader{
		decoder: decoder,
		levels:  make([]int8, 0, bufferSize),
	}
}

func (r *levelReader) ReadLevel() (int8, error) {
	for {
		if r.offset < uint(len(r.levels)) {
			lvl := r.levels[r.offset]
			r.offset++
			return lvl, nil
		}

		n, err := r.decoder.DecodeInt8(r.levels[:cap(r.levels)])
		if n == 0 {
			return 0, err
		}

		r.levels = r.levels[:n]
		r.offset = 0
	}
}

func (r *levelReader) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.levels = r.levels[:0]
	r.offset = 0
}

type booleanPageReader struct {
	typ     Type
	decoder encoding.Decoder
	values  []bool
	offset  uint
}

func newBooleanPageReader(typ Type, decoder encoding.Decoder, bufferSize int) *booleanPageReader {
	return &booleanPageReader{
		typ:     typ,
		decoder: decoder,
		values:  make([]bool, 0, atLeastOne(bufferSize)),
	}
}

func (r *booleanPageReader) ReadValue() (Value, error) {
	for {
		if r.offset < uint(len(r.values)) {
			v := r.values[r.offset]
			r.offset++
			return makeValueBoolean(v), nil
		}

		n, err := r.decoder.DecodeBoolean(r.values[:cap(r.values)])
		if n == 0 {
			return Value{}, err
		}

		r.values = r.values[:n]
		r.offset = 0
	}
}

func (r *booleanPageReader) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.values = r.values[:0]
	r.offset = 0
}

func (r *booleanPageReader) Type() Type { return r.typ }

type int32PageReader struct {
	typ     Type
	decoder encoding.Decoder
	values  []int32
	offset  uint
}

func newInt32PageReader(typ Type, decoder encoding.Decoder, bufferSize int) *int32PageReader {
	return &int32PageReader{
		typ:     typ,
		decoder: decoder,
		values:  make([]int32, 0, atLeastOne(bufferSize/4)),
	}
}

func (r *int32PageReader) ReadValue() (Value, error) {
	for {
		if r.offset < uint(len(r.values)) {
			v := r.values[r.offset]
			r.offset++
			return makeValueInt32(v), nil
		}

		n, err := r.decoder.DecodeInt32(r.values[:cap(r.values)])
		if n == 0 {
			return Value{}, err
		}

		r.values = r.values[:n]
		r.offset = 0
	}
}

func (r *int32PageReader) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.values = r.values[:0]
	r.offset = 0
}

func (r *int32PageReader) Type() Type { return r.typ }

type int64PageReader struct {
	typ     Type
	decoder encoding.Decoder
	values  []int64
	offset  uint
}

func newInt64PageReader(typ Type, decoder encoding.Decoder, bufferSize int) *int64PageReader {
	return &int64PageReader{
		typ:     typ,
		decoder: decoder,
		values:  make([]int64, 0, atLeastOne(bufferSize/8)),
	}
}

func (r *int64PageReader) ReadValue() (Value, error) {
	for {
		if r.offset < uint(len(r.values)) {
			v := r.values[r.offset]
			r.offset++
			return makeValueInt64(v), nil
		}

		n, err := r.decoder.DecodeInt64(r.values[:cap(r.values)])
		if n == 0 {
			return Value{}, err
		}

		r.values = r.values[:n]
		r.offset = 0
	}
}

func (r *int64PageReader) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.values = r.values[:0]
	r.offset = 0
}

func (r *int64PageReader) Type() Type { return r.typ }

type int96PageReader struct {
	typ     Type
	decoder encoding.Decoder
	values  []int96
	offset  uint
}

func newInt96PageReader(typ Type, decoder encoding.Decoder, bufferSize int) *int96PageReader {
	return &int96PageReader{
		typ:     typ,
		decoder: decoder,
		values:  make([]int96, 0, atLeastOne(bufferSize/12)),
	}
}

func (r *int96PageReader) ReadValue() (Value, error) {
	for {
		if r.offset < uint(len(r.values)) {
			v := r.values[r.offset]
			r.offset++
			return makeValueInt96(v), nil
		}

		n, err := r.decoder.DecodeInt96(r.values[:cap(r.values)])
		if n == 0 {
			return Value{}, err
		}

		r.values = r.values[:n]
		r.offset = 0
	}
}

func (r *int96PageReader) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.values = r.values[:0]
	r.offset = 0
}

func (r *int96PageReader) Type() Type { return r.typ }

type floatPageReader struct {
	typ     Type
	decoder encoding.Decoder
	values  []float32
	offset  uint
}

func newFloatPageReader(typ Type, decoder encoding.Decoder, bufferSize int) *floatPageReader {
	return &floatPageReader{
		typ:     typ,
		decoder: decoder,
		values:  make([]float32, 0, atLeastOne(bufferSize/4)),
	}
}

func (r *floatPageReader) ReadValue() (Value, error) {
	for {
		if r.offset < uint(len(r.values)) {
			v := r.values[r.offset]
			r.offset++
			return makeValueFloat(v), nil
		}

		n, err := r.decoder.DecodeFloat(r.values[:cap(r.values)])
		if n == 0 {
			return Value{}, err
		}

		r.values = r.values[:n]
		r.offset = 0
	}
}

func (r *floatPageReader) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.values = r.values[:0]
	r.offset = 0
}

func (r *floatPageReader) Type() Type { return r.typ }

type doublePageReader struct {
	typ     Type
	decoder encoding.Decoder
	values  []float64
	offset  uint
}

func newDoublePageReader(typ Type, decoder encoding.Decoder, bufferSize int) *doublePageReader {
	return &doublePageReader{
		typ:     typ,
		decoder: decoder,
		values:  make([]float64, 0, atLeastOne(bufferSize/8)),
	}
}

func (r *doublePageReader) ReadValue() (Value, error) {
	for {
		if r.offset < uint(len(r.values)) {
			v := r.values[r.offset]
			r.offset++
			return makeValueDouble(v), nil
		}

		n, err := r.decoder.DecodeDouble(r.values[:cap(r.values)])
		if n == 0 {
			return Value{}, err
		}

		r.values = r.values[:n]
		r.offset = 0
	}
}

func (r *doublePageReader) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.values = r.values[:0]
	r.offset = 0
}

func (r *doublePageReader) Type() Type { return r.typ }

type byteArrayPageReader struct {
	typ     Type
	decoder encoding.Decoder
	values  []byte
	offset  uint
	remain  uint
}

func newByteArrayPageReader(typ Type, decoder encoding.Decoder, bufferSize int) *byteArrayPageReader {
	return &byteArrayPageReader{
		typ:     typ,
		decoder: decoder,
		values:  make([]byte, atLeast(bufferSize, 4)),
	}
}

func (r *byteArrayPageReader) ReadValue() (Value, error) {
	for {
		if r.remain > 0 {
			n := plain.NextByteArrayLength(r.values[r.offset:])
			v := r.values[4+r.offset : 4+r.offset+uint(n)]
			r.offset += 4 + uint(n)
			r.remain -= 1
			return makeValueBytes(ByteArray, v), nil
		}

		n, err := r.decoder.DecodeByteArray(r.values)
		if n == 0 {
			if err == encoding.ErrValueTooLarge {
				size := 4 + uint32(plain.NextByteArrayLength(r.values))
				r.values = make([]byte, bits.NearestPowerOfTwo32(size))
				r.offset = 0
				r.remain = 0
				continue
			}
			return Value{}, err
		}

		r.offset = 0
		r.remain = uint(n)
	}
}

func (r *byteArrayPageReader) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.offset = 0
	r.remain = 0
}

func (r *byteArrayPageReader) Type() Type { return r.typ }

type fixedLenByteArrayPageReader struct {
	typ     Type
	decoder encoding.Decoder
	values  []byte
	offset  uint
	size    uint
}

func newFixedLenByteArrayPageReader(typ Type, decoder encoding.Decoder, bufferSize int) *fixedLenByteArrayPageReader {
	size := typ.Length()
	return &fixedLenByteArrayPageReader{
		typ:     typ,
		decoder: decoder,
		size:    uint(size),
		values:  make([]byte, 0, atLeast((bufferSize/size)*size, size)),
	}
}

func (r *fixedLenByteArrayPageReader) ReadValue() (Value, error) {
	for {
		if r.offset < uint(len(r.values)) {
			v := r.values[r.offset : r.offset+r.size]
			r.offset += r.size
			return makeValueBytes(FixedLenByteArray, v), nil
		}

		n, err := r.decoder.DecodeFixedLenByteArray(int(r.size), r.values[:cap(r.values)])
		if n == 0 {
			return Value{}, err
		}

		r.values = r.values[:uint(n)*r.size]
		r.offset = 0
	}
}

func (r *fixedLenByteArrayPageReader) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.values = r.values[:0]
	r.offset = 0
}

func (r *fixedLenByteArrayPageReader) Type() Type { return r.typ }

var (
	_ ValueReader = (*DataPageReader)(nil)
	_ PageReader  = (*int32PageReader)(nil)
	_ PageReader  = (*int64PageReader)(nil)
	_ PageReader  = (*int96PageReader)(nil)
	_ PageReader  = (*floatPageReader)(nil)
	_ PageReader  = (*doublePageReader)(nil)
	_ PageReader  = (*byteArrayPageReader)(nil)
	_ PageReader  = (*fixedLenByteArrayPageReader)(nil)
)
