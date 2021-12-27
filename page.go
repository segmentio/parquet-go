package parquet

import (
	"github.com/segmentio/parquet/deprecated"
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/internal/bits"
)

// Page values represent sequences of parquet values.
type Page interface {
	// Returns the type values written to the underlying page.
	Type() Type

	// Returns the size of the page in memory (in bytes).
	Size() int64

	// Returns the number of values currently buffered in the page.
	NumValues() int

	// Returns the number of null values in the page.
	NumNulls() int

	// Returns the min and max values currently buffered in the writter.
	Bounds() (min, max Value)

	// Returns a new page which is as slice of the receiver between value
	// indexes i and j.
	Slice(i, j int) Page

	// For pages belonging to optional or repeated columns, these methods
	// return the repetition and definition levels of the page values.
	RepetitionLevels() []int8
	DefinitionLevels() []int8

	// Writes the page values to the encoder given as argument.
	WriteTo(encoding.Encoder) error
}

type pageWithLevels struct {
	base               Page
	maxRepetitionLevel int8
	maxDefinitionLevel int8
	definitionLevels   []int8
	repetitionLevels   []int8
}

func newPageWithLevels(base Page, maxRepetitionLevel, maxDefinitionLevel int8, repetitionLevels, definitionLevels []int8) *pageWithLevels {
	return &pageWithLevels{
		base:               base,
		maxRepetitionLevel: maxRepetitionLevel,
		maxDefinitionLevel: maxDefinitionLevel,
		definitionLevels:   definitionLevels,
		repetitionLevels:   repetitionLevels,
	}
}

func (page *pageWithLevels) Type() Type { return page.base.Type() }

func (page *pageWithLevels) Size() int64 {
	return page.base.Size() + int64(len(page.repetitionLevels)) + int64(len(page.definitionLevels))
}

func (page *pageWithLevels) NumValues() int { return len(page.definitionLevels) }

func (page *pageWithLevels) NumNulls() int {
	numNulls := 0
	for _, def := range page.definitionLevels {
		if def != page.maxDefinitionLevel {
			numNulls++
		}
	}
	return numNulls
}

func (page *pageWithLevels) Bounds() (min, max Value) { return page.base.Bounds() }

func (page *pageWithLevels) Slice(i, j int) Page {
	return newPageWithLevels(page.base.Slice(i, j),
		page.maxRepetitionLevel,
		page.maxDefinitionLevel,
		page.definitionLevels[i:j],
		page.repetitionLevels[i:j],
	)
}

func (page *pageWithLevels) RepetitionLevels() []int8 { return page.repetitionLevels }

func (page *pageWithLevels) DefinitionLevels() []int8 { return page.definitionLevels }

func (page *pageWithLevels) WriteTo(enc encoding.Encoder) error { return page.base.WriteTo(enc) }

type booleanPage struct {
	typ    Type
	values []bool
}

func newBooleanPage(typ Type, values []bool) *booleanPage {
	return &booleanPage{typ: typ, values: values}
}

func (page *booleanPage) Type() Type { return page.typ }

func (page *booleanPage) Size() int64 { return int64(len(page.values)) }

func (page *booleanPage) NumValues() int { return len(page.values) }

func (page *booleanPage) NumNulls() int { return 0 }

func (page *booleanPage) Bounds() (min, max Value) {
	if len(page.values) > 0 {
		min = makeValueBoolean(false)
		max = makeValueBoolean(false)
		hasFalse, hasTrue := false, false

		for _, value := range page.values {
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

func (page *booleanPage) Slice(i, j int) Page { return newBooleanPage(page.typ, page.values[i:j]) }

func (page *booleanPage) RepetitionLevels() []int8 { return nil }

func (page *booleanPage) DefinitionLevels() []int8 { return nil }

func (page *booleanPage) WriteTo(enc encoding.Encoder) error { return enc.EncodeBoolean(page.values) }

type int32Page struct {
	typ    Type
	values []int32
}

func newInt32Page(typ Type, values []int32) *int32Page {
	return &int32Page{typ: typ, values: values}
}

func (page *int32Page) Type() Type { return page.typ }

func (page *int32Page) Size() int64 { return 4 * int64(len(page.values)) }

func (page *int32Page) NumValues() int { return len(page.values) }

func (page *int32Page) NumNulls() int { return 0 }

func (page *int32Page) Bounds() (min, max Value) {
	if len(page.values) > 0 {
		minInt32, maxInt32 := bits.MinMaxInt32(page.values)
		min = makeValueInt32(minInt32)
		max = makeValueInt32(maxInt32)
	}
	return min, max
}

func (page *int32Page) Slice(i, j int) Page { return newInt32Page(page.typ, page.values[i:j]) }

func (page *int32Page) RepetitionLevels() []int8 { return nil }

func (page *int32Page) DefinitionLevels() []int8 { return nil }

func (page *int32Page) WriteTo(enc encoding.Encoder) error { return enc.EncodeInt32(page.values) }

type int64Page struct {
	typ    Type
	values []int64
}

func newInt64Page(typ Type, values []int64) *int64Page {
	return &int64Page{typ: typ, values: values}
}

func (page *int64Page) Type() Type { return page.typ }

func (page *int64Page) Size() int64 { return 8 * int64(len(page.values)) }

func (page *int64Page) NumValues() int { return len(page.values) }

func (page *int64Page) NumNulls() int { return 0 }

func (page *int64Page) Bounds() (min, max Value) {
	if len(page.values) > 0 {
		minInt64, maxInt64 := bits.MinMaxInt64(page.values)
		min = makeValueInt64(minInt64)
		max = makeValueInt64(maxInt64)
	}
	return min, max
}

func (page *int64Page) Slice(i, j int) Page { return newInt64Page(page.typ, page.values[i:j]) }

func (page *int64Page) RepetitionLevels() []int8 { return nil }

func (page *int64Page) DefinitionLevels() []int8 { return nil }

func (page *int64Page) WriteTo(enc encoding.Encoder) error { return enc.EncodeInt64(page.values) }

type int96Page struct {
	typ    Type
	values []deprecated.Int96
}

func newInt96Page(typ Type, values []deprecated.Int96) *int96Page {
	return &int96Page{typ: typ, values: values}
}

func (page *int96Page) Type() Type { return page.typ }

func (page *int96Page) Size() int64 { return 12 * int64(len(page.values)) }

func (page *int96Page) NumValues() int { return len(page.values) }

func (page *int96Page) NumNulls() int { return 0 }

func (page *int96Page) Bounds() (min, max Value) {
	if len(page.values) > 0 {
		minInt96, maxInt96 := deprecated.MinMaxInt96(page.values)
		min = makeValueInt96(minInt96)
		max = makeValueInt96(maxInt96)
	}
	return min, max
}

func (page *int96Page) Slice(i, j int) Page { return newInt96Page(page.typ, page.values[i:j]) }

func (page *int96Page) RepetitionLevels() []int8 { return nil }

func (page *int96Page) DefinitionLevels() []int8 { return nil }

func (page *int96Page) WriteTo(enc encoding.Encoder) error { return enc.EncodeInt96(page.values) }

type floatPage struct {
	typ    Type
	values []float32
}

func newFloatPage(typ Type, values []float32) *floatPage {
	return &floatPage{typ: typ, values: values}
}

func (page *floatPage) Type() Type { return page.typ }

func (page *floatPage) Size() int64 { return 4 * int64(len(page.values)) }

func (page *floatPage) NumValues() int { return len(page.values) }

func (page *floatPage) NumNulls() int { return 0 }

func (page *floatPage) Bounds() (min, max Value) {
	if len(page.values) > 0 {
		minFloat32, maxFloat32 := bits.MinMaxFloat32(page.values)
		min = makeValueFloat(minFloat32)
		max = makeValueFloat(maxFloat32)
	}
	return min, max
}

func (page *floatPage) Slice(i, j int) Page { return newFloatPage(page.typ, page.values[i:j]) }

func (page *floatPage) RepetitionLevels() []int8 { return nil }

func (page *floatPage) DefinitionLevels() []int8 { return nil }

func (page *floatPage) WriteTo(enc encoding.Encoder) error { return enc.EncodeFloat(page.values) }

type doublePage struct {
	typ    Type
	values []float64
}

func newDoublePage(typ Type, values []float64) *doublePage {
	return &doublePage{typ: typ, values: values}
}

func (page *doublePage) Type() Type { return page.typ }

func (page *doublePage) Size() int64 { return 8 * int64(len(page.values)) }

func (page *doublePage) NumValues() int { return len(page.values) }

func (page *doublePage) NumNulls() int { return 0 }

func (page *doublePage) Bounds() (min, max Value) {
	if len(page.values) > 0 {
		minFloat64, maxFloat64 := bits.MinMaxFloat64(page.values)
		min = makeValueDouble(minFloat64)
		max = makeValueDouble(maxFloat64)
	}
	return min, max
}

func (page *doublePage) Slice(i, j int) Page { return newDoublePage(page.typ, page.values[i:j]) }

func (page *doublePage) RepetitionLevels() []int8 { return nil }

func (page *doublePage) DefinitionLevels() []int8 { return nil }

func (page *doublePage) WriteTo(enc encoding.Encoder) error { return enc.EncodeDouble(page.values) }

type byteArrayPage struct {
	typ    Type
	values encoding.ByteArrayList
}

func newByteArrayPage(typ Type, values encoding.ByteArrayList) *byteArrayPage {
	return &byteArrayPage{typ: typ, values: values}
}

func (page *byteArrayPage) Type() Type { return page.typ }

func (page *byteArrayPage) Size() int64 { return page.values.Size() }

func (page *byteArrayPage) NumValues() int { return page.values.Len() }

func (page *byteArrayPage) NumNulls() int { return 0 }

func (page *byteArrayPage) Bounds() (min, max Value) {
	if page.values.Len() > 0 {
		minBytes := page.values.Index(0)
		maxBytes := minBytes

		for i := 1; i < page.values.Len(); i++ {
			v := page.values.Index(i)
			switch {
			case string(v) < string(minBytes):
				minBytes = v
			case string(v) > string(maxBytes):
				maxBytes = v
			}
		}

		min = makeValueBytes(ByteArray, minBytes).Clone()
		max = makeValueBytes(ByteArray, maxBytes).Clone()
	}
	return min, max
}

func (page *byteArrayPage) Slice(i, j int) Page {
	return newByteArrayPage(page.typ, page.values.Slice(i, j))
}

func (page *byteArrayPage) RepetitionLevels() []int8 { return nil }

func (page *byteArrayPage) DefinitionLevels() []int8 { return nil }

func (page *byteArrayPage) WriteTo(enc encoding.Encoder) error {
	return enc.EncodeByteArray(page.values)
}

type fixedLenByteArrayPage struct {
	typ  Type
	size int
	data []byte
}

func newFixedLenByteArrayPage(typ Type, data []byte) *fixedLenByteArrayPage {
	return &fixedLenByteArrayPage{
		typ:  typ,
		size: typ.Length(),
		data: data,
	}
}

func (page *fixedLenByteArrayPage) Type() Type { return page.typ }

func (page *fixedLenByteArrayPage) Size() int64 { return int64(len(page.data)) }

func (page *fixedLenByteArrayPage) NumValues() int { return len(page.data) / page.size }

func (page *fixedLenByteArrayPage) NumNulls() int { return 0 }

func (page *fixedLenByteArrayPage) Bounds() (min, max Value) {
	if len(page.data) > 0 {
		minBytes, maxBytes := bits.MinMaxFixedLenByteArray(page.size, page.data)
		min = makeValueBytes(FixedLenByteArray, minBytes).Clone()
		max = makeValueBytes(FixedLenByteArray, maxBytes).Clone()
	}
	return min, max
}

func (page *fixedLenByteArrayPage) Slice(i, j int) Page {
	return newFixedLenByteArrayPage(page.typ, page.data[i*page.size:j*page.size])
}

func (page *fixedLenByteArrayPage) RepetitionLevels() []int8 { return nil }

func (page *fixedLenByteArrayPage) DefinitionLevels() []int8 { return nil }

func (page *fixedLenByteArrayPage) WriteTo(enc encoding.Encoder) error {
	return enc.EncodeFixedLenByteArray(page.size, page.data)
}

// The following two specializations for unsigned integer types are needed to
// apply an unsigned comparison when looking up the min and max page values.

type uint32Page struct{ *int32Page }

func newUint32Page(typ Type, values []int32) uint32Page {
	return uint32Page{newInt32Page(typ, values)}
}

func (page uint32Page) Bounds() (min, max Value) {
	if len(page.values) > 0 {
		minUint32, maxUint32 := bits.MinMaxUint32(bits.Int32ToUint32(page.values))
		min = makeValueInt32(int32(minUint32))
		max = makeValueInt32(int32(maxUint32))
	}
	return min, max
}

func (page uint32Page) Slice(i, j int) Page {
	return newUint32Page(page.typ, page.values[i:j])
}

type uint64Page struct{ *int64Page }

func newUint64Page(typ Type, values []int64) uint64Page {
	return uint64Page{newInt64Page(typ, values)}
}

func (page uint64Page) Bounds() (min, max Value) {
	if len(page.values) > 0 {
		minUint64, maxUint64 := bits.MinMaxUint64(bits.Int64ToUint64(page.values))
		min = makeValueInt64(int64(minUint64))
		max = makeValueInt64(int64(maxUint64))
	}
	return min, max
}

func (page uint64Page) Slice(i, j int) Page {
	return newUint64Page(page.typ, page.values[i:j])
}

var (
	_ Page = (*booleanPage)(nil)
	_ Page = (*int32Page)(nil)
	_ Page = (*int64Page)(nil)
	_ Page = (*int96Page)(nil)
	_ Page = (*floatPage)(nil)
	_ Page = (*doublePage)(nil)
	_ Page = (*byteArrayPage)(nil)
	_ Page = (*fixedLenByteArrayPage)(nil)
	_ Page = uint32Page{}
	_ Page = uint64Page{}
)
