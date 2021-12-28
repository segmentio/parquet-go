package parquet

import (
	"bytes"
	"fmt"
	"io"

	"github.com/segmentio/parquet/deprecated"
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/internal/bits"
)

// Page values represent sequences of parquet values.
type Page interface {
	ValueReaderAt

	// Returns the number of rows, values, and nulls in the page. The number of
	// rows may be less than the number of values in the page if the page is
	// part of a repeated column.
	NumRows() int
	NumValues() int
	NumNulls() int

	// Returns the min and max values currently buffered in the writter.
	Bounds() (min, max Value)

	// Returns a new page which is as slice of the receiver between value
	// row indexes i and j.
	Slice(i, j int) Page

	// Write levels and values of the page to the encoder given as argument.
	WriteRepetitionLevelsTo(encoding.Encoder) error
	WriteDefinitionLevelsTo(encoding.Encoder) error
	WriteTo(encoding.Encoder) error
}

type errorPage struct{ err error }

func (page *errorPage) NumRows() int                                   { return 0 }
func (page *errorPage) NumValues() int                                 { return 0 }
func (page *errorPage) NumNulls() int                                  { return 0 }
func (page *errorPage) Bounds() (min, max Value)                       { return }
func (page *errorPage) Slice(i, j int) Page                            { return page }
func (page *errorPage) WriteRepetitionLevelsTo(encoding.Encoder) error { return page.err }
func (page *errorPage) WriteDefinitionLevelsTo(encoding.Encoder) error { return page.err }
func (page *errorPage) WriteTo(encoding.Encoder) error                 { return page.err }
func (page *errorPage) ReadValuesAt(int, []Value) (int, error)         { return 0, page.err }

func countLevelsEqual(levels []int8, value int8) int {
	return bytes.Count(bits.Int8ToBytes(levels), []byte{byte(value)})
}

func countLevelsNotEqual(levels []int8, value int8) int {
	return len(levels) - countLevelsEqual(levels, value)
}

type optionalPage struct {
	base               Page
	maxDefinitionLevel int8
	definitionLevels   []int8
}

func newOptionalPage(base Page, maxDefinitionLevel int8, definitionLevels []int8) *optionalPage {
	return &optionalPage{
		base:               base,
		maxDefinitionLevel: maxDefinitionLevel,
		definitionLevels:   definitionLevels,
	}
}

func (page *optionalPage) NumRows() int {
	return page.base.NumRows()
}

func (page *optionalPage) NumValues() int {
	return page.base.NumValues() + page.NumNulls()
}

func (page *optionalPage) NumNulls() int {
	return countLevelsNotEqual(page.definitionLevels, page.maxDefinitionLevel)
}

func (page *optionalPage) Bounds() (min, max Value) {
	return page.base.Bounds()
}

func (page *optionalPage) Slice(i, j int) Page {
	numNulls1 := countLevelsNotEqual(page.definitionLevels[:i], page.maxDefinitionLevel)
	numNulls2 := countLevelsNotEqual(page.definitionLevels[i:j], page.maxDefinitionLevel)
	return newOptionalPage(
		page.base.Slice(i-numNulls1, j-(numNulls1+numNulls2)),
		page.maxDefinitionLevel,
		page.definitionLevels[i:j],
	)
}

func (page *optionalPage) WriteRepetitionLevelsTo(e encoding.Encoder) error {
	return nil
}

func (page *optionalPage) WriteDefinitionLevelsTo(e encoding.Encoder) error {
	return e.EncodeInt8(page.definitionLevels)
}

func (page *optionalPage) WriteTo(e encoding.Encoder) error {
	return page.base.WriteTo(e)
}

func (page *optionalPage) ReadValuesAt(index int, values []Value) (n int, err error) {
	if index >= len(page.definitionLevels) {
		return 0, io.EOF
	}
	offset := countLevelsEqual(page.definitionLevels[:index], page.maxDefinitionLevel)

	for n < len(values) && index < len(page.definitionLevels) {
		for n < len(values) && index < len(page.definitionLevels) && isNull(index, page.maxDefinitionLevel, page.definitionLevels) {
			values[n] = Value{definitionLevel: page.definitionLevels[index]}
			index++
			n++
		}

		i := index
		j := n
		for j < len(values) && i < len(page.definitionLevels) && !isNull(i, page.maxDefinitionLevel, page.definitionLevels) {
			i++
			j++
		}

		if j > n {
			for j, err = page.base.ReadValuesAt(offset, values[n:j]); j > 0; j-- {
				values[n].definitionLevel = page.maxDefinitionLevel
				offset++
				index++
				n++
			}
			if err != nil {
				return n, err
			}
		}
	}

	if index >= len(page.definitionLevels) {
		err = io.EOF
	}
	return n, err
}

type repeatedPage struct {
	base               Page
	maxRepetitionLevel int8
	maxDefinitionLevel int8
	definitionLevels   []int8
	repetitionLevels   []int8
}

func newRepeatedPage(base Page, maxRepetitionLevel, maxDefinitionLevel int8, repetitionLevels, definitionLevels []int8) *repeatedPage {
	return &repeatedPage{
		base:               base,
		maxRepetitionLevel: maxRepetitionLevel,
		maxDefinitionLevel: maxDefinitionLevel,
		definitionLevels:   definitionLevels,
		repetitionLevels:   repetitionLevels,
	}
}

func (page *repeatedPage) NumRows() int {
	return countLevelsNotEqual(page.repetitionLevels, page.maxRepetitionLevel)
}

func (page *repeatedPage) NumValues() int {
	return page.base.NumValues() + page.NumNulls()
}

func (page *repeatedPage) NumNulls() int {
	return countLevelsNotEqual(page.definitionLevels, page.maxDefinitionLevel)
}

func (page *repeatedPage) Bounds() (min, max Value) {
	return page.base.Bounds()
}

func errPageBoundsOutOfRange(i, j, n int) error {
	return fmt.Errorf("page bounds out of range [%d:%d]: with length %d", i, j, n)
}

func (page *repeatedPage) Slice(i, j int) Page {
	numRows := page.NumRows()
	if i < 0 || i > numRows {
		panic(errPageBoundsOutOfRange(i, j, numRows))
	}
	if j < 0 || j > numRows {
		panic(errPageBoundsOutOfRange(i, j, numRows))
	}
	if i > j {
		panic(errPageBoundsOutOfRange(i, j, numRows))
	}

	rowIndex0 := 0
	rowIndex1 := len(page.repetitionLevels)
	rowIndex2 := len(page.repetitionLevels)

	for k, def := range page.repetitionLevels {
		if def != page.maxRepetitionLevel {
			if rowIndex0 == i {
				rowIndex1 = k
			}
			if rowIndex0 == j {
				rowIndex2 = k
			}
			rowIndex0++
		}
	}

	numNulls1 := countLevelsNotEqual(page.definitionLevels[:rowIndex1], page.maxDefinitionLevel)
	numNulls2 := countLevelsNotEqual(page.definitionLevels[rowIndex1:rowIndex2], page.maxDefinitionLevel)
	return newRepeatedPage(
		page.base.Slice(i-numNulls1, j-(numNulls1+numNulls2)),
		page.maxRepetitionLevel,
		page.maxDefinitionLevel,
		page.repetitionLevels[rowIndex1:rowIndex2],
		page.definitionLevels[rowIndex1:rowIndex2],
	)
}

func (page *repeatedPage) WriteRepetitionLevelsTo(e encoding.Encoder) error {
	return e.EncodeInt8(page.repetitionLevels)
}

func (page *repeatedPage) WriteDefinitionLevelsTo(e encoding.Encoder) error {
	return e.EncodeInt8(page.definitionLevels)
}

func (page *repeatedPage) WriteTo(e encoding.Encoder) error {
	return page.base.WriteTo(e)
}

func (page *repeatedPage) ReadValuesAt(index int, values []Value) (n int, err error) {
	if index >= len(page.definitionLevels) {
		return 0, io.EOF
	}
	offset := countLevelsEqual(page.definitionLevels[:index], page.maxDefinitionLevel)

	for n < len(values) && index < len(page.definitionLevels) {
		for n < len(values) && index < len(page.definitionLevels) && isNull(index, page.maxDefinitionLevel, page.definitionLevels) {
			values[n] = Value{
				repetitionLevel: page.repetitionLevels[index],
				definitionLevel: page.definitionLevels[index],
			}
			index++
			n++
		}

		i := index
		j := n
		for j < len(values) && i < len(page.definitionLevels) && !isNull(i, page.maxDefinitionLevel, page.definitionLevels) {
			i++
			j++
		}

		if j > n {
			for j, err = page.base.ReadValuesAt(offset, values[n:j]); j > 0; j-- {
				values[n].repetitionLevel = page.repetitionLevels[index]
				values[n].definitionLevel = page.maxDefinitionLevel
				offset++
				index++
				n++
			}
			if err != nil {
				return n, err
			}
		}
	}

	if index >= len(page.definitionLevels) {
		err = io.EOF
	}
	return n, err
}

type booleanPage struct{ values []bool }

func newBooleanPage(values []bool) *booleanPage { return &booleanPage{values: values} }

func (page *booleanPage) NumRows() int { return len(page.values) }

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

func (page *booleanPage) Slice(i, j int) Page { return newBooleanPage(page.values[i:j]) }

func (page *booleanPage) WriteRepetitionLevelsTo(encoding.Encoder) error { return nil }

func (page *booleanPage) WriteDefinitionLevelsTo(encoding.Encoder) error { return nil }

func (page *booleanPage) WriteTo(e encoding.Encoder) error { return e.EncodeBoolean(page.values) }

func (page *booleanPage) ReadValuesAt(offset int, values []Value) (n int, err error) {
	if offset < len(page.values) {
		n = min(len(values), len(page.values)-offset)
		for i := 0; i < n; i++ {
			values[i] = makeValueBoolean(page.values[offset+i])
		}
	}
	if offset+n >= len(page.values) {
		err = io.EOF
	}
	return n, err
}

type int32Page struct{ values []int32 }

func newInt32Page(values []int32) *int32Page { return &int32Page{values: values} }

func (page *int32Page) NumRows() int { return len(page.values) }

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

func (page *int32Page) Slice(i, j int) Page { return newInt32Page(page.values[i:j]) }

func (page *int32Page) WriteRepetitionLevelsTo(encoding.Encoder) error { return nil }

func (page *int32Page) WriteDefinitionLevelsTo(encoding.Encoder) error { return nil }

func (page *int32Page) WriteTo(e encoding.Encoder) error { return e.EncodeInt32(page.values) }

func (page *int32Page) ReadValuesAt(offset int, values []Value) (n int, err error) {
	if offset < len(page.values) {
		n = min(len(values), len(page.values)-offset)
		for i := 0; i < n; i++ {
			values[i] = makeValueInt32(page.values[offset+i])
		}
	}
	if offset+n >= len(page.values) {
		err = io.EOF
	}
	return n, err
}

type int64Page struct{ values []int64 }

func newInt64Page(values []int64) *int64Page { return &int64Page{values: values} }

func (page *int64Page) NumRows() int { return len(page.values) }

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

func (page *int64Page) Slice(i, j int) Page { return newInt64Page(page.values[i:j]) }

func (page *int64Page) WriteRepetitionLevelsTo(encoding.Encoder) error { return nil }

func (page *int64Page) WriteDefinitionLevelsTo(encoding.Encoder) error { return nil }

func (page *int64Page) WriteTo(e encoding.Encoder) error { return e.EncodeInt64(page.values) }

func (page *int64Page) ReadValuesAt(offset int, values []Value) (n int, err error) {
	if offset < len(page.values) {
		n = min(len(values), len(page.values)-offset)
		for i := 0; i < n; i++ {
			values[i] = makeValueInt64(page.values[offset+i])
		}
	}
	if offset+n >= len(page.values) {
		err = io.EOF
	}
	return n, err
}

type int96Page struct{ values []deprecated.Int96 }

func newInt96Page(values []deprecated.Int96) *int96Page { return &int96Page{values: values} }

func (page *int96Page) NumRows() int { return len(page.values) }

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

func (page *int96Page) Slice(i, j int) Page { return newInt96Page(page.values[i:j]) }

func (page *int96Page) WriteRepetitionLevelsTo(encoding.Encoder) error { return nil }

func (page *int96Page) WriteDefinitionLevelsTo(encoding.Encoder) error { return nil }

func (page *int96Page) WriteTo(e encoding.Encoder) error { return e.EncodeInt96(page.values) }

func (page *int96Page) ReadValuesAt(offset int, values []Value) (n int, err error) {
	if offset < len(page.values) {
		n = min(len(values), len(page.values)-offset)
		for i := 0; i < n; i++ {
			values[i] = makeValueInt96(page.values[offset+i])
		}
	}
	if offset+n >= len(page.values) {
		err = io.EOF
	}
	return n, err
}

type floatPage struct{ values []float32 }

func newFloatPage(values []float32) *floatPage { return &floatPage{values: values} }

func (page *floatPage) NumRows() int { return len(page.values) }

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

func (page *floatPage) Slice(i, j int) Page { return newFloatPage(page.values[i:j]) }

func (page *floatPage) WriteRepetitionLevelsTo(encoding.Encoder) error { return nil }

func (page *floatPage) WriteDefinitionLevelsTo(encoding.Encoder) error { return nil }

func (page *floatPage) WriteTo(e encoding.Encoder) error { return e.EncodeFloat(page.values) }

func (page *floatPage) ReadValuesAt(offset int, values []Value) (n int, err error) {
	if offset < len(page.values) {
		n = min(len(values), len(page.values)-offset)
		for i := 0; i < n; i++ {
			values[i] = makeValueFloat(page.values[offset+i])
		}
	}
	if offset+n >= len(page.values) {
		err = io.EOF
	}
	return n, err
}

type doublePage struct{ values []float64 }

func newDoublePage(values []float64) *doublePage { return &doublePage{values: values} }

func (page *doublePage) NumRows() int { return len(page.values) }

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

func (page *doublePage) Slice(i, j int) Page { return newDoublePage(page.values[i:j]) }

func (page *doublePage) WriteRepetitionLevelsTo(encoding.Encoder) error { return nil }

func (page *doublePage) WriteDefinitionLevelsTo(encoding.Encoder) error { return nil }

func (page *doublePage) WriteTo(e encoding.Encoder) error { return e.EncodeDouble(page.values) }

func (page *doublePage) ReadValuesAt(offset int, values []Value) (n int, err error) {
	if offset < len(page.values) {
		n = min(len(values), len(page.values)-offset)
		for i := 0; i < n; i++ {
			values[i] = makeValueDouble(page.values[offset+i])
		}
	}
	if offset+n >= len(page.values) {
		err = io.EOF
	}
	return n, err
}

type byteArrayPage struct{ values encoding.ByteArrayList }

func newByteArrayPage(values encoding.ByteArrayList) *byteArrayPage {
	return &byteArrayPage{values: values}
}

func (page *byteArrayPage) NumRows() int { return page.values.Len() }

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

		min = makeValueBytes(ByteArray, minBytes)
		max = makeValueBytes(ByteArray, maxBytes)
	}
	return min, max
}

func (page *byteArrayPage) Slice(i, j int) Page {
	return newByteArrayPage(page.values.Slice(i, j))
}

func (page *byteArrayPage) WriteRepetitionLevelsTo(encoding.Encoder) error { return nil }

func (page *byteArrayPage) WriteDefinitionLevelsTo(encoding.Encoder) error { return nil }

func (page *byteArrayPage) WriteTo(e encoding.Encoder) error {
	return e.EncodeByteArray(page.values)
}

func (page *byteArrayPage) ReadValuesAt(offset int, values []Value) (n int, err error) {
	if offset < page.values.Len() {
		n = min(len(values), page.values.Len()-offset)
		for i := 0; i < n; i++ {
			values[i] = makeValueBytes(ByteArray, page.values.Index(offset+i))
		}
	}
	if offset+n >= page.values.Len() {
		err = io.EOF
	}
	return n, err
}

type fixedLenByteArrayPage struct {
	size int
	data []byte
}

func newFixedLenByteArrayPage(size int, data []byte) *fixedLenByteArrayPage {
	return &fixedLenByteArrayPage{size: size, data: data}
}

func (page *fixedLenByteArrayPage) NumRows() int { return len(page.data) / page.size }

func (page *fixedLenByteArrayPage) NumValues() int { return len(page.data) / page.size }

func (page *fixedLenByteArrayPage) NumNulls() int { return 0 }

func (page *fixedLenByteArrayPage) Bounds() (min, max Value) {
	if len(page.data) > 0 {
		minBytes, maxBytes := bits.MinMaxFixedLenByteArray(page.size, page.data)
		min = makeValueBytes(FixedLenByteArray, minBytes)
		max = makeValueBytes(FixedLenByteArray, maxBytes)
	}
	return min, max
}

func (page *fixedLenByteArrayPage) Slice(i, j int) Page {
	return newFixedLenByteArrayPage(page.size, page.data[i*page.size:j*page.size])
}

func (page *fixedLenByteArrayPage) WriteRepetitionLevelsTo(encoding.Encoder) error { return nil }

func (page *fixedLenByteArrayPage) WriteDefinitionLevelsTo(encoding.Encoder) error { return nil }

func (page *fixedLenByteArrayPage) WriteTo(e encoding.Encoder) error {
	return e.EncodeFixedLenByteArray(page.size, page.data)
}

func (page *fixedLenByteArrayPage) ReadValuesAt(offset int, values []Value) (n int, err error) {
	if offset >= page.NumValues() {
		return 0, io.EOF
	}
	i := offset * page.size
	for i < len(page.data) && n < len(values) {
		values[n] = makeValueBytes(FixedLenByteArray, page.data[i:i+page.size])
		i += page.size
		n++
	}
	if i >= len(page.data) {
		err = io.EOF
	}
	return n, err
}

// The following two specializations for unsigned integer types are needed to
// apply an unsigned comparison when looking up the min and max page values.

type uint32Page struct{ *int32Page }

func newUint32Page(values []int32) uint32Page {
	return uint32Page{newInt32Page(values)}
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
	return newUint32Page(page.values[i:j])
}

type uint64Page struct{ *int64Page }

func newUint64Page(values []int64) uint64Page {
	return uint64Page{newInt64Page(values)}
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
	return newUint64Page(page.values[i:j])
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
