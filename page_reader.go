package parquet

import (
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/encoding/plain"
	"github.com/segmentio/parquet/format"
	"github.com/segmentio/parquet/internal/bits"
)

type PageReader interface {
	ValueReader

	Type() Type

	NumValues() int

	NumNulls() int

	DistinctCount() int

	Bounds() (min, max Value)

	Reset(encoding.Decoder)
}

type dataPageReader struct {
	header             *format.PageHeader
	page               PageReader
	maxRepetitionLevel int8
	maxDefinitionLevel int8
	repetition         levelReader
	definition         levelReader
}

func newDataPageReader(repetition, definition encoding.Decoder, page PageReader, maxRepetitionLevel, maxDefinitionLevel int8, bufferSize int, header *format.PageHeader) *dataPageReader {
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

	return &dataPageReader{
		header:             header,
		page:               page,
		maxRepetitionLevel: maxRepetitionLevel,
		maxDefinitionLevel: maxDefinitionLevel,
		repetition:         makeLevelReader(repetition, repetitionBufferSize),
		definition:         makeLevelReader(definition, definitionBufferSize),
	}
}

func (r *dataPageReader) ReadValue() (Value, error) {
	// TODO: this is not correct, we need to read the levels first to determine
	// whether the record in null.
	v, err := r.page.ReadValue()
	if err != nil {
		return v, err
	}
	if r.maxRepetitionLevel > 0 {
		v.repetitionLevel, err = r.repetition.ReadLevel()
		if err != nil {
			return v, err
		}
	}
	if r.maxDefinitionLevel > 0 {
		v.definitionLevel, err = r.definition.ReadLevel()
		if err != nil {
			return v, err
		}
	}
	return v, nil
}

func (r *dataPageReader) Type() Type {
	return r.page.Type()
}

func (r *dataPageReader) NumValues() int {
	if r.header != nil {
		switch r.header.Type {
		case format.DataPage:
			return int(r.header.DataPageHeader.NumValues)
		case format.DataPageV2:
			return int(r.header.DataPageHeaderV2.NumValues)
		}
	}
	return 0
}

func (r *dataPageReader) NumNulls() int {
	if r.header != nil {
		switch r.header.Type {
		case format.DataPage:
			return int(r.header.DataPageHeader.Statistics.NullCount)
		case format.DataPageV2:
			return int(r.header.DataPageHeaderV2.NumNulls)
		}
	}
	return 0
}

func (r *dataPageReader) DistinctCount() int {
	if stats := r.statistics(); stats != nil {
		return int(stats.DistinctCount)
	}
	return 0
}

func (r *dataPageReader) Bounds() (min, max Value) {
	if stats := r.statistics(); stats != nil {
		t := r.Type()
		k := t.Kind()
		min = makeValueKind(k, stats.MinValue)
		max = makeValueKind(k, stats.MaxValue)
	}
	return min, max
}

func (r *dataPageReader) statistics() *format.Statistics {
	if r.header != nil {
		switch r.header.Type {
		case format.DataPage:
			return &r.header.DataPageHeader.Statistics
		case format.DataPageV2:
			return &r.header.DataPageHeaderV2.Statistics
		}
	}
	return nil
}

func (r *dataPageReader) Reset(decoder encoding.Decoder) {
	r.reset(r.repetition.decoder, r.definition.decoder, decoder, nil)
}

func (r *dataPageReader) reset(repetitionLevelDecoder, definitionLevelDecoder, valueDecoder encoding.Decoder, header *format.PageHeader) {
	r.header = header
	r.page.Reset(valueDecoder)
	r.repetition.Reset(repetitionLevelDecoder)
	r.definition.Reset(definitionLevelDecoder)
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

func (r *booleanPageReader) NumValues() int { return 0 }

func (r *booleanPageReader) NumNulls() int { return 0 }

func (r *booleanPageReader) DistinctCount() int { return 0 }

func (r *booleanPageReader) Bounds() (min, max Value) { return }

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

func (r *int32PageReader) NumValues() int { return 0 }

func (r *int32PageReader) NumNulls() int { return 0 }

func (r *int32PageReader) DistinctCount() int { return 0 }

func (r *int32PageReader) Bounds() (min, max Value) { return }

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

func (r *int64PageReader) NumValues() int { return 0 }

func (r *int64PageReader) NumNulls() int { return 0 }

func (r *int64PageReader) DistinctCount() int { return 0 }

func (r *int64PageReader) Bounds() (min, max Value) { return }

type int96PageReader struct {
	typ     Type
	decoder encoding.Decoder
	values  [][12]byte
	offset  uint
}

func newInt96PageReader(typ Type, decoder encoding.Decoder, bufferSize int) *int96PageReader {
	return &int96PageReader{
		typ:     typ,
		decoder: decoder,
		values:  make([][12]byte, 0, atLeastOne(bufferSize/12)),
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

func (r *int96PageReader) NumValues() int { return 0 }

func (r *int96PageReader) NumNulls() int { return 0 }

func (r *int96PageReader) DistinctCount() int { return 0 }

func (r *int96PageReader) Bounds() (min, max Value) { return }

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

func (r *floatPageReader) NumValues() int { return 0 }

func (r *floatPageReader) NumNulls() int { return 0 }

func (r *floatPageReader) DistinctCount() int { return 0 }

func (r *floatPageReader) Bounds() (min, max Value) { return }

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

func (r *doublePageReader) NumValues() int { return 0 }

func (r *doublePageReader) NumNulls() int { return 0 }

func (r *doublePageReader) DistinctCount() int { return 0 }

func (r *doublePageReader) Bounds() (min, max Value) { return }

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
			n := plain.ByteArrayLength(r.values[r.offset:])
			v := r.values[4+r.offset : 4+r.offset+uint(n)]
			r.offset += 4 + uint(n)
			r.remain -= 1
			return makeValueBytes(ByteArray, v), nil
		}

		n, err := r.decoder.DecodeByteArray(r.values)
		if n == 0 {
			if err == encoding.ErrValueTooLarge {
				size := 4 + uint32(plain.ByteArrayLength(r.values))
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

func (r *byteArrayPageReader) NumValues() int { return 0 }

func (r *byteArrayPageReader) NumNulls() int { return 0 }

func (r *byteArrayPageReader) DistinctCount() int { return 0 }

func (r *byteArrayPageReader) Bounds() (min, max Value) { return }

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

func (r *fixedLenByteArrayPageReader) NumValues() int { return 0 }

func (r *fixedLenByteArrayPageReader) NumNulls() int { return 0 }

func (r *fixedLenByteArrayPageReader) DistinctCount() int { return 0 }

func (r *fixedLenByteArrayPageReader) Bounds() (min, max Value) { return }

var (
	_ PageReader = (*dataPageReader)(nil)
	_ PageReader = (*int32PageReader)(nil)
	_ PageReader = (*int64PageReader)(nil)
	_ PageReader = (*int96PageReader)(nil)
	_ PageReader = (*floatPageReader)(nil)
	_ PageReader = (*doublePageReader)(nil)
	_ PageReader = (*byteArrayPageReader)(nil)
	_ PageReader = (*fixedLenByteArrayPageReader)(nil)
)
