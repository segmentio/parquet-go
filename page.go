package parquet

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/segmentio/parquet/deprecated"
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/internal/bits"
)

// Page values represent sequences of parquet values.
type Page interface {
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

	// Returns the size of the page in bytes.
	Size() int64

	// Write levels and values of the page to the encoder given as argument.
	WriteRepetitionLevelsTo(encoding.Encoder) error
	WriteDefinitionLevelsTo(encoding.Encoder) error
	WriteTo(encoding.Encoder) error

	// Returns a reader exposing the values contained in the page.
	Values() ValueReader
}

func sizeOfBytes(data []byte) int64 { return 1 * int64(len(data)) }

func sizeOfBool(data []bool) int64 { return 1 * int64(len(data)) }

func sizeOfInt8(data []int8) int64 { return 1 * int64(len(data)) }

func sizeOfInt32(data []int32) int64 { return 4 * int64(len(data)) }

func sizeOfInt64(data []int64) int64 { return 8 * int64(len(data)) }

func sizeOfInt96(data []deprecated.Int96) int64 { return 12 * int64(len(data)) }

func sizeOfFloat32(data []float32) int64 { return 4 * int64(len(data)) }

func sizeOfFloat64(data []float64) int64 { return 8 * int64(len(data)) }

func forEachPageSlice(page Page, wantSize int64, do func(Page) error) error {
	numRows := page.NumRows()
	if numRows == 0 {
		return nil
	}

	pageSize := page.Size()
	numPages := int((pageSize + (wantSize - 1)) / wantSize)
	rowIndex := 0
	if numPages < 2 {
		return do(page)
	}

	for numPages > 0 {
		lastRowIndex := rowIndex + ((numRows - rowIndex) / numPages)
		if err := do(page.Slice(rowIndex, lastRowIndex)); err != nil {
			return err
		}
		rowIndex = lastRowIndex
		numPages--
	}

	return nil
}

func pagesWithDefinitionLevels(pages []Page, maxDefinitionLevel int8, definitionLevels []int8, newPage func(Page, int, int) Page) []Page {
	rowIndex := 0

	for i, page := range pages {
		numRows := page.NumRows()

		lastRowIndex := rowIndex
		for lastRowIndex < len(definitionLevels) && numRows > 0 {
			if definitionLevels[lastRowIndex] == maxDefinitionLevel {
				numRows--
			}
			lastRowIndex++
		}
		for lastRowIndex < len(definitionLevels) && definitionLevels[lastRowIndex] != maxDefinitionLevel {
			lastRowIndex++
		}

		pages[i] = newPage(page, rowIndex, lastRowIndex)
		rowIndex = lastRowIndex
	}

	return pages
}

type errorPage struct{ err error }

func (page *errorPage) NumRows() int                                   { return 0 }
func (page *errorPage) NumValues() int                                 { return 0 }
func (page *errorPage) NumNulls() int                                  { return 0 }
func (page *errorPage) Bounds() (min, max Value)                       { return }
func (page *errorPage) Slice(i, j int) Page                            { return page }
func (page *errorPage) Size() int64                                    { return 0 }
func (page *errorPage) WriteRepetitionLevelsTo(encoding.Encoder) error { return page.err }
func (page *errorPage) WriteDefinitionLevelsTo(encoding.Encoder) error { return page.err }
func (page *errorPage) WriteTo(encoding.Encoder) error                 { return page.err }
func (page *errorPage) Values() ValueReader                            { return &errorValueReader{err: page.err} }

func errPageBoundsOutOfRange(i, j, n int) error {
	return fmt.Errorf("page bounds out of range [%d:%d]: with length %d", i, j, n)
}

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
	return len(page.definitionLevels)
}

func (page *optionalPage) NumValues() int {
	return len(page.definitionLevels)
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

func (page *optionalPage) Size() int64 {
	return page.base.Size() + sizeOfInt8(page.definitionLevels)
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

func (page *optionalPage) Values() ValueReader {
	return &optionalPageReader{
		values:             page.base.Values(),
		maxDefinitionLevel: page.maxDefinitionLevel,
		definitionLevels:   page.definitionLevels,
	}
}

type optionalPageReader struct {
	values             ValueReader
	offset             int
	maxDefinitionLevel int8
	definitionLevels   []int8
}

func (r *optionalPageReader) ReadValues(values []Value) (n int, err error) {
	for n < len(values) && r.offset < len(r.definitionLevels) {
		for n < len(values) && r.offset < len(r.definitionLevels) && r.definitionLevels[r.offset] != r.maxDefinitionLevel {
			values[n] = Value{definitionLevel: r.definitionLevels[r.offset]}
			r.offset++
			n++
		}

		i := n
		j := r.offset
		for i < len(values) && j < len(r.definitionLevels) && r.definitionLevels[j] == r.maxDefinitionLevel {
			i++
			j++
		}

		if n < i {
			for j, err = r.values.ReadValues(values[n:i]); j > 0; j-- {
				values[n].definitionLevel = r.maxDefinitionLevel
				r.offset++
				n++
			}
			if err != nil {
				return n, err
			}
		}
	}

	if r.offset == len(r.definitionLevels) {
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

	i -= numNulls1
	j = i + (rowIndex2 - (rowIndex1 + numNulls2))

	return newRepeatedPage(
		page.base.Slice(i, j),
		page.maxRepetitionLevel,
		page.maxDefinitionLevel,
		page.repetitionLevels[rowIndex1:rowIndex2],
		page.definitionLevels[rowIndex1:rowIndex2],
	)
}

func (page *repeatedPage) Size() int64 {
	return sizeOfInt8(page.repetitionLevels) + sizeOfInt8(page.definitionLevels) + page.base.Size()
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

func (page *repeatedPage) Values() ValueReader {
	return &repeatedPageReader{
		values:             page.base.Values(),
		maxRepetitionLevel: page.maxRepetitionLevel,
		maxDefinitionLevel: page.maxDefinitionLevel,
		definitionLevels:   page.definitionLevels,
		repetitionLevels:   page.repetitionLevels,
	}
}

type repeatedPageReader struct {
	values             ValueReader
	offset             int
	maxRepetitionLevel int8
	maxDefinitionLevel int8
	repetitionLevels   []int8
	definitionLevels   []int8
}

func (r *repeatedPageReader) ReadValues(values []Value) (n int, err error) {
	for n < len(values) && r.offset < len(r.definitionLevels) {
		for n < len(values) && r.offset < len(r.definitionLevels) && r.definitionLevels[r.offset] != r.maxDefinitionLevel {
			values[n] = Value{
				repetitionLevel: r.repetitionLevels[r.offset],
				definitionLevel: r.definitionLevels[r.offset],
			}
			r.offset++
			n++
		}

		i := n
		j := r.offset
		for i < len(values) && j < len(r.definitionLevels) && r.definitionLevels[j] == r.maxDefinitionLevel {
			i++
			j++
		}

		if n < i {
			for j, err = r.values.ReadValues(values[n:i]); j > 0; j-- {
				values[n].repetitionLevel = r.repetitionLevels[r.offset]
				values[n].definitionLevel = r.maxDefinitionLevel
				r.offset++
				n++
			}
			if err != nil {
				return n, err
			}
		}
	}

	if r.offset == len(r.definitionLevels) {
		err = io.EOF
	}
	return n, err
}

type booleanPage struct {
	values      []bool
	columnIndex int8
}

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

func (page *booleanPage) Slice(i, j int) Page {
	return &booleanPage{
		values:      page.values[i:j],
		columnIndex: page.columnIndex,
	}
}

func (page *booleanPage) Size() int64 { return sizeOfBool(page.values) }

func (page *booleanPage) WriteRepetitionLevelsTo(encoding.Encoder) error { return nil }

func (page *booleanPage) WriteDefinitionLevelsTo(encoding.Encoder) error { return nil }

func (page *booleanPage) WriteTo(e encoding.Encoder) error { return e.EncodeBoolean(page.values) }

func (page *booleanPage) Values() ValueReader { return &booleanPageReader{values: page.values} }

type booleanPageReader struct {
	values      []bool
	columnIndex int8
}

func (r *booleanPageReader) ReadValues(values []Value) (n int, err error) {
	n = min(len(values), len(r.values))
	for i := 0; i < n; i++ {
		values[i] = makeValueBoolean(r.values[i])
		values[i].columnIndex = r.columnIndex
	}
	if r.values = r.values[n:]; len(r.values) == 0 {
		err = io.EOF
	}
	return n, err
}

type int32Page struct {
	values      []int32
	columnIndex int8
}

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

func (page *int32Page) Slice(i, j int) Page {
	return &int32Page{
		values:      page.values[i:j],
		columnIndex: page.columnIndex,
	}
}

func (page *int32Page) Size() int64 { return sizeOfInt32(page.values) }

func (page *int32Page) WriteRepetitionLevelsTo(encoding.Encoder) error { return nil }

func (page *int32Page) WriteDefinitionLevelsTo(encoding.Encoder) error { return nil }

func (page *int32Page) WriteTo(e encoding.Encoder) error { return e.EncodeInt32(page.values) }

func (page *int32Page) Values() ValueReader { return &int32PageReader{values: page.values} }

type int32PageReader struct {
	values      []int32
	columnIndex int8
}

func (r *int32PageReader) ReadValues(values []Value) (n int, err error) {
	n = min(len(values), len(r.values))
	for i := 0; i < n; i++ {
		values[i] = makeValueInt32(r.values[i])
		values[i].columnIndex = r.columnIndex
	}
	if r.values = r.values[n:]; len(r.values) == 0 {
		err = io.EOF
	}
	return n, err
}

type int64Page struct {
	values      []int64
	columnIndex int8
}

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

func (page *int64Page) Slice(i, j int) Page {
	return &int64Page{
		values:      page.values[i:j],
		columnIndex: page.columnIndex,
	}
}

func (page *int64Page) Size() int64 { return sizeOfInt64(page.values) }

func (page *int64Page) WriteRepetitionLevelsTo(encoding.Encoder) error { return nil }

func (page *int64Page) WriteDefinitionLevelsTo(encoding.Encoder) error { return nil }

func (page *int64Page) WriteTo(e encoding.Encoder) error { return e.EncodeInt64(page.values) }

func (page *int64Page) Values() ValueReader { return &int64PageReader{values: page.values} }

type int64PageReader struct {
	values      []int64
	columnIndex int8
}

func (r *int64PageReader) ReadValues(values []Value) (n int, err error) {
	n = min(len(values), len(r.values))
	for i := 0; i < n; i++ {
		values[i] = makeValueInt64(r.values[i])
		values[i].columnIndex = r.columnIndex
	}
	if r.values = r.values[n:]; len(r.values) == 0 {
		err = io.EOF
	}
	return n, err
}

type int96Page struct {
	values      []deprecated.Int96
	columnIndex int8
}

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

func (page *int96Page) Slice(i, j int) Page {
	return &int96Page{
		values:      page.values[i:j],
		columnIndex: page.columnIndex,
	}
}

func (page *int96Page) Size() int64 { return sizeOfInt96(page.values) }

func (page *int96Page) WriteRepetitionLevelsTo(encoding.Encoder) error { return nil }

func (page *int96Page) WriteDefinitionLevelsTo(encoding.Encoder) error { return nil }

func (page *int96Page) WriteTo(e encoding.Encoder) error { return e.EncodeInt96(page.values) }

func (page *int96Page) Values() ValueReader { return &int96PageReader{values: page.values} }

type int96PageReader struct {
	values      []deprecated.Int96
	columnIndex int8
}

func (r *int96PageReader) ReadValues(values []Value) (n int, err error) {
	n = min(len(values), len(r.values))
	for i := 0; i < n; i++ {
		values[i] = makeValueInt96(r.values[i])
		values[i].columnIndex = r.columnIndex
	}
	if r.values = r.values[n:]; len(r.values) == 0 {
		err = io.EOF
	}
	return n, err
}

type floatPage struct {
	values      []float32
	columnIndex int8
}

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

func (page *floatPage) Slice(i, j int) Page {
	return &floatPage{
		values:      page.values[i:j],
		columnIndex: page.columnIndex,
	}
}

func (page *floatPage) Size() int64 { return sizeOfFloat32(page.values) }

func (page *floatPage) WriteRepetitionLevelsTo(encoding.Encoder) error { return nil }

func (page *floatPage) WriteDefinitionLevelsTo(encoding.Encoder) error { return nil }

func (page *floatPage) WriteTo(e encoding.Encoder) error { return e.EncodeFloat(page.values) }

func (page *floatPage) Values() ValueReader {
	return &floatPageReader{values: page.values}
}

type floatPageReader struct {
	values      []float32
	columnIndex int8
}

func (r *floatPageReader) ReadValues(values []Value) (n int, err error) {
	n = min(len(values), len(r.values))
	for i := 0; i < n; i++ {
		values[i] = makeValueFloat(r.values[i])
		values[i].columnIndex = r.columnIndex
	}
	if r.values = r.values[n:]; len(r.values) == 0 {
		err = io.EOF
	}
	return n, err
}

type doublePage struct {
	values      []float64
	columnIndex int8
}

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

func (page *doublePage) Slice(i, j int) Page {
	return &doublePage{
		values:      page.values[i:j],
		columnIndex: page.columnIndex,
	}
}

func (page *doublePage) Size() int64 { return sizeOfFloat64(page.values) }

func (page *doublePage) WriteRepetitionLevelsTo(encoding.Encoder) error { return nil }

func (page *doublePage) WriteDefinitionLevelsTo(encoding.Encoder) error { return nil }

func (page *doublePage) WriteTo(e encoding.Encoder) error { return e.EncodeDouble(page.values) }

func (page *doublePage) Values() ValueReader {
	return &doublePageReader{values: page.values}
}

type doublePageReader struct {
	values      []float64
	columnIndex int8
}

func (r *doublePageReader) ReadValues(values []Value) (n int, err error) {
	n = min(len(values), len(r.values))
	for i := 0; i < n; i++ {
		values[i] = makeValueDouble(r.values[i])
		values[i].columnIndex = r.columnIndex
	}
	if r.values = r.values[n:]; len(r.values) == 0 {
		err = io.EOF
	}
	return n, err
}

type byteArrayPage struct {
	values      encoding.ByteArrayList
	columnIndex int8
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
	return &byteArrayPage{
		values:      page.values.Slice(i, j),
		columnIndex: page.columnIndex,
	}
}

func (page *byteArrayPage) Size() int64 { return page.values.Size() }

func (page *byteArrayPage) WriteRepetitionLevelsTo(encoding.Encoder) error { return nil }

func (page *byteArrayPage) WriteDefinitionLevelsTo(encoding.Encoder) error { return nil }

func (page *byteArrayPage) WriteTo(e encoding.Encoder) error { return e.EncodeByteArray(page.values) }

func (page *byteArrayPage) Values() ValueReader { return &byteArrayPageReader{values: page.values} }

type byteArrayPageReader struct {
	values      encoding.ByteArrayList
	columnIndex int8
}

func (r *byteArrayPageReader) ReadValues(values []Value) (n int, err error) {
	n = min(len(values), r.values.Len())
	for i := 0; i < n; i++ {
		values[i] = makeValueBytes(ByteArray, r.values.Index(i))
		values[i].columnIndex = r.columnIndex
	}
	if r.values = r.values.Slice(n, r.values.Len()); r.values.Len() == 0 {
		err = io.EOF
	}
	return n, err
}

type fixedLenByteArrayPage struct {
	size        int
	data        []byte
	columnIndex int8
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
	return &fixedLenByteArrayPage{
		size:        page.size,
		data:        page.data[i*page.size : j*page.size],
		columnIndex: page.columnIndex,
	}
}

func (page *fixedLenByteArrayPage) Size() int64 { return sizeOfBytes(page.data) }

func (page *fixedLenByteArrayPage) WriteRepetitionLevelsTo(encoding.Encoder) error { return nil }

func (page *fixedLenByteArrayPage) WriteDefinitionLevelsTo(encoding.Encoder) error { return nil }

func (page *fixedLenByteArrayPage) WriteTo(e encoding.Encoder) error {
	return e.EncodeFixedLenByteArray(page.size, page.data)
}

func (page *fixedLenByteArrayPage) Values() ValueReader {
	return &fixedLenByteArrayPageReader{size: page.size, data: page.data}
}

type fixedLenByteArrayPageReader struct {
	size        int
	data        []byte
	columnIndex int8
}

func (r *fixedLenByteArrayPageReader) ReadValues(values []Value) (n int, err error) {
	for n < len(values) && r.size > 0 && len(r.data) >= r.size {
		values[n] = makeValueBytes(FixedLenByteArray, r.data[:r.size:r.size])
		values[n].columnIndex = r.columnIndex
		r.data = r.data[r.size:]
		n++
	}
	if len(r.data) < r.size {
		err = io.EOF
	}
	return n, err
}

// The following two specializations for unsigned integer types are needed to
// apply an unsigned comparison when looking up the min and max page values.

type uint32Page struct{ *int32Page }

func (page uint32Page) Bounds() (min, max Value) {
	if len(page.values) > 0 {
		minUint32, maxUint32 := bits.MinMaxUint32(bits.Int32ToUint32(page.values))
		min = makeValueInt32(int32(minUint32))
		max = makeValueInt32(int32(maxUint32))
	}
	return min, max
}

func (page uint32Page) Slice(i, j int) Page {
	return uint32Page{page.int32Page.Slice(i, j).(*int32Page)}
}

type uint64Page struct{ *int64Page }

func (page uint64Page) Bounds() (min, max Value) {
	if len(page.values) > 0 {
		minUint64, maxUint64 := bits.MinMaxUint64(bits.Int64ToUint64(page.values))
		min = makeValueInt64(int64(minUint64))
		max = makeValueInt64(int64(maxUint64))
	}
	return min, max
}

func (page uint64Page) Slice(i, j int) Page {
	return uint64Page{page.int64Page.Slice(i, j).(*int64Page)}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type PageBufferPool interface {
	GetPageBuffer() io.ReadWriter
	PutPageBuffer(io.ReadWriter)
}

func NewPageBufferPool() PageBufferPool { return new(pageBufferPool) }

type pageBufferPool struct{ sync.Pool }

func (pool *pageBufferPool) GetPageBuffer() io.ReadWriter {
	b, _ := pool.Get().(*bytes.Buffer)
	if b == nil {
		b = new(bytes.Buffer)
	} else {
		b.Reset()
	}
	return b
}

func (pool *pageBufferPool) PutPageBuffer(buf io.ReadWriter) {
	if b, _ := buf.(*bytes.Buffer); b != nil {
		pool.Put(b)
	}
}

type fileBufferPool struct {
	err     error
	tempdir string
	pattern string
}

func NewFileBufferPool(tempdir, pattern string) PageBufferPool {
	pool := &fileBufferPool{
		tempdir: tempdir,
		pattern: pattern,
	}
	pool.tempdir, pool.err = filepath.Abs(pool.tempdir)
	return pool
}

func (pool *fileBufferPool) GetPageBuffer() io.ReadWriter {
	if pool.err != nil {
		return &errorBuffer{err: pool.err}
	}
	f, err := os.CreateTemp(pool.tempdir, pool.pattern)
	if err != nil {
		return &errorBuffer{err: err}
	}
	return f
}

func (pool *fileBufferPool) PutPageBuffer(buf io.ReadWriter) {
	if f, _ := buf.(*os.File); f != nil {
		defer f.Close()
		os.Remove(f.Name())
	}
}

type errorBuffer struct{ err error }

func (errbuf *errorBuffer) Read([]byte) (int, error)          { return 0, errbuf.err }
func (errbuf *errorBuffer) Write([]byte) (int, error)         { return 0, errbuf.err }
func (errbuf *errorBuffer) ReadFrom(io.Reader) (int64, error) { return 0, errbuf.err }
func (errbuf *errorBuffer) WriteTo(io.Writer) (int64, error)  { return 0, errbuf.err }

var (
	defaultPageBufferPool pageBufferPool

	_ io.ReaderFrom = (*errorBuffer)(nil)
	_ io.WriterTo   = (*errorBuffer)(nil)
)
