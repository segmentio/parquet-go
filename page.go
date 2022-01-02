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
	// Returns the column index that this page belongs to.
	ColumnIndex() int

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

type PageReader interface {
	ReadPage() (Page, error)
}

type PageWriter interface {
	WritePage(Page) (int64, error)
}

func sizeOfBytes(data []byte) int64 { return 1 * int64(len(data)) }

func sizeOfBool(data []bool) int64 { return 1 * int64(len(data)) }

func sizeOfInt8(data []int8) int64 { return 1 * int64(len(data)) }

func sizeOfInt32(data []int32) int64 { return 4 * int64(len(data)) }

func sizeOfInt64(data []int64) int64 { return 8 * int64(len(data)) }

func sizeOfInt96(data []deprecated.Int96) int64 { return 12 * int64(len(data)) }

func sizeOfFloat32(data []float32) int64 { return 4 * int64(len(data)) }

func sizeOfFloat64(data []float64) int64 { return 8 * int64(len(data)) }

func forEachPageSlice(page Page, wantSize int64, do func(Page) bool) {
	numRows := page.NumRows()
	if numRows == 0 {
		return
	}

	pageSize := page.Size()
	numPages := int((pageSize + (wantSize - 1)) / wantSize)
	rowIndex := 0
	if numPages < 2 {
		do(page)
		return
	}

	for numPages > 0 {
		lastRowIndex := rowIndex + ((numRows - rowIndex) / numPages)
		if !do(page.Slice(rowIndex, lastRowIndex)) {
			break
		}
		rowIndex = lastRowIndex
		numPages--
	}
}

func writePageValuesTo(w ValueWriter, r ValueReader, p Page) (int64, error) {
	if pw, ok := w.(PageWriter); ok {
		return pw.WritePage(p)
	} else {
		return CopyValues(w, struct{ ValueReader }{r})
	}
}

type errorPage struct {
	err         error
	columnIndex int
}

func newErrorPage(columnIndex int, msg string, args ...interface{}) *errorPage {
	return &errorPage{
		err:         fmt.Errorf(msg, args...),
		columnIndex: columnIndex,
	}
}

func (page *errorPage) ColumnIndex() int                               { return page.columnIndex }
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

func appendLevel(levels []int8, value int8, count int) []int8 {
	if count > 0 {
		i := len(levels)
		j := len(levels) + 1

		if n := len(levels) + count; cap(levels) < n {
			newLevels := make([]int8, n)
			copy(newLevels, levels)
			levels = newLevels
		} else {
			levels = levels[:n]
		}

		for levels[i] = value; j < len(levels); j *= 2 {
			copy(levels[j:], levels[i:j])
		}
	}
	return levels
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

func (page *optionalPage) ColumnIndex() int {
	return page.base.ColumnIndex()
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
	return &optionalPageReader{page: page}
}

type optionalPageReader struct {
	page   *optionalPage
	values ValueReader
	offset int
}

func (r *optionalPageReader) ReadValues(values []Value) (n int, err error) {
	if r.values == nil {
		r.values = r.page.base.Values()
	}
	maxDefinitionLevel := r.page.maxDefinitionLevel

	for n < len(values) && r.offset < len(r.page.definitionLevels) {
		for n < len(values) && r.offset < len(r.page.definitionLevels) && r.page.definitionLevels[r.offset] != maxDefinitionLevel {
			values[n] = Value{definitionLevel: r.page.definitionLevels[r.offset]}
			r.offset++
			n++
		}

		i := n
		j := r.offset
		for i < len(values) && j < len(r.page.definitionLevels) && r.page.definitionLevels[j] == maxDefinitionLevel {
			i++
			j++
		}

		if n < i {
			for j, err = r.values.ReadValues(values[n:i]); j > 0; j-- {
				values[n].definitionLevel = maxDefinitionLevel
				r.offset++
				n++
			}
			if err != nil {
				return n, err
			}
		}
	}

	if r.offset == len(r.page.definitionLevels) {
		err = io.EOF
	}
	return n, err
}

func (r *optionalPageReader) WriteValuesTo(w ValueWriter) (int64, error) {
	return writePageValuesTo(w, r, r.page)
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

func (page *repeatedPage) ColumnIndex() int {
	return page.base.ColumnIndex()
}

func (page *repeatedPage) NumRows() int {
	return countLevelsNotEqual(page.repetitionLevels, page.maxRepetitionLevel)
}

func (page *repeatedPage) NumValues() int {
	return len(page.definitionLevels)
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
	return &repeatedPageReader{page: page}
}

type repeatedPageReader struct {
	page   *repeatedPage
	values ValueReader
	offset int
}

func (r *repeatedPageReader) ReadValues(values []Value) (n int, err error) {
	if r.values == nil {
		r.values = r.page.base.Values()
	}
	maxDefinitionLevel := r.page.maxDefinitionLevel

	for n < len(values) && r.offset < len(r.page.definitionLevels) {
		for n < len(values) && r.offset < len(r.page.definitionLevels) && r.page.definitionLevels[r.offset] != maxDefinitionLevel {
			values[n] = Value{
				repetitionLevel: r.page.repetitionLevels[r.offset],
				definitionLevel: r.page.definitionLevels[r.offset],
			}
			r.offset++
			n++
		}

		i := n
		j := r.offset
		for i < len(values) && j < len(r.page.definitionLevels) && r.page.definitionLevels[j] == maxDefinitionLevel {
			i++
			j++
		}

		if n < i {
			for j, err = r.values.ReadValues(values[n:i]); j > 0; j-- {
				values[n].repetitionLevel = r.page.repetitionLevels[r.offset]
				values[n].definitionLevel = maxDefinitionLevel
				r.offset++
				n++
			}
			if err != nil {
				return n, err
			}
		}
	}

	if r.offset == len(r.page.definitionLevels) {
		err = io.EOF
	}
	return n, err
}

func (r *repeatedPageReader) WriteValuesTo(w ValueWriter) (int64, error) {
	return writePageValuesTo(w, r, r.page)
}

type booleanPage struct {
	values      []bool
	columnIndex int8
}

func (page *booleanPage) ColumnIndex() int { return int(^page.columnIndex) }

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

func (page *booleanPage) Values() ValueReader { return &booleanPageReader{page: page} }

type booleanPageReader struct {
	page   *booleanPage
	offset int
}

func (r *booleanPageReader) ReadValues(values []Value) (n int, err error) {
	for n < len(values) && r.offset < len(r.page.values) {
		values[n] = makeValueBoolean(r.page.values[r.offset])
		values[n].columnIndex = r.page.columnIndex
		r.offset++
		n++
	}
	if r.offset == len(r.page.values) {
		err = io.EOF
	}
	return n, err
}

func (r *booleanPageReader) WriteValuesTo(w ValueWriter) (int64, error) {
	return writePageValuesTo(w, r, r.page)
}

type int32Page struct {
	values      []int32
	columnIndex int8
}

func (page *int32Page) ColumnIndex() int { return int(^page.columnIndex) }

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

func (page *int32Page) Values() ValueReader { return &int32PageReader{page: page} }

type int32PageReader struct {
	page   *int32Page
	offset int
}

func (r *int32PageReader) ReadValues(values []Value) (n int, err error) {
	for n < len(values) && r.offset < len(r.page.values) {
		values[n] = makeValueInt32(r.page.values[r.offset])
		values[n].columnIndex = r.page.columnIndex
		r.offset++
		n++
	}
	if r.offset == len(r.page.values) {
		err = io.EOF
	}
	return n, err
}

func (r *int32PageReader) WriteValuesTo(w ValueWriter) (int64, error) {
	return writePageValuesTo(w, r, r.page)
}

type int64Page struct {
	values      []int64
	columnIndex int8
}

func (page *int64Page) ColumnIndex() int { return int(^page.columnIndex) }

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

func (page *int64Page) Values() ValueReader { return &int64PageReader{page: page} }

type int64PageReader struct {
	page   *int64Page
	offset int
}

func (r *int64PageReader) ReadValues(values []Value) (n int, err error) {
	for n < len(values) && r.offset < len(r.page.values) {
		values[n] = makeValueInt64(r.page.values[r.offset])
		values[n].columnIndex = r.page.columnIndex
		r.offset++
		n++
	}
	if r.offset == len(r.page.values) {
		err = io.EOF
	}
	return n, err
}

func (r *int64PageReader) WriteValuesTo(w ValueWriter) (int64, error) {
	return writePageValuesTo(w, r, r.page)
}

type int96Page struct {
	values      []deprecated.Int96
	columnIndex int8
}

func (page *int96Page) ColumnIndex() int { return int(^page.columnIndex) }

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

func (page *int96Page) Values() ValueReader { return &int96PageReader{page: page} }

type int96PageReader struct {
	page   *int96Page
	offset int
}

func (r *int96PageReader) ReadValues(values []Value) (n int, err error) {
	for n < len(values) && r.offset < len(r.page.values) {
		values[n] = makeValueInt96(r.page.values[r.offset])
		values[n].columnIndex = r.page.columnIndex
		r.offset++
		n++
	}
	if r.offset == len(r.page.values) {
		err = io.EOF
	}
	return n, err
}

func (r *int96PageReader) WriteValuesTo(w ValueWriter) (int64, error) {
	return writePageValuesTo(w, r, r.page)
}

type floatPage struct {
	values      []float32
	columnIndex int8
}

func (page *floatPage) ColumnIndex() int { return int(^page.columnIndex) }

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

func (page *floatPage) Values() ValueReader { return &floatPageReader{page: page} }

type floatPageReader struct {
	page   *floatPage
	offset int
}

func (r *floatPageReader) ReadValues(values []Value) (n int, err error) {
	for n < len(values) && r.offset < len(r.page.values) {
		values[n] = makeValueFloat(r.page.values[r.offset])
		values[n].columnIndex = r.page.columnIndex
		r.offset++
		n++
	}
	if r.offset == len(r.page.values) {
		err = io.EOF
	}
	return n, err
}

func (r *floatPageReader) WriteValuesTo(w ValueWriter) (int64, error) {
	return writePageValuesTo(w, r, r.page)
}

type doublePage struct {
	values      []float64
	columnIndex int8
}

func (page *doublePage) ColumnIndex() int { return int(^page.columnIndex) }

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

func (page *doublePage) Values() ValueReader { return &doublePageReader{page: page} }

type doublePageReader struct {
	page   *doublePage
	offset int
}

func (r *doublePageReader) ReadValues(values []Value) (n int, err error) {
	for n < len(values) && r.offset < len(r.page.values) {
		values[n] = makeValueDouble(r.page.values[r.offset])
		values[n].columnIndex = r.page.columnIndex
		r.offset++
		n++
	}
	if r.offset == len(r.page.values) {
		err = io.EOF
	}
	return n, err
}

func (r *doublePageReader) WriteValuesTo(w ValueWriter) (int64, error) {
	return writePageValuesTo(w, r, r.page)
}

type byteArrayPage struct {
	values      encoding.ByteArrayList
	columnIndex int8
}

func (page *byteArrayPage) ColumnIndex() int { return int(^page.columnIndex) }

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

func (page *byteArrayPage) Values() ValueReader { return &byteArrayPageReader{page: page} }

type byteArrayPageReader struct {
	page   *byteArrayPage
	offset int
}

func (r *byteArrayPageReader) ReadValues(values []Value) (n int, err error) {
	for n < len(values) && r.offset < r.page.values.Len() {
		values[n] = makeValueBytes(ByteArray, r.page.values.Index(r.offset))
		values[n].columnIndex = r.page.columnIndex
		r.offset++
		n++
	}
	if r.offset == r.page.values.Len() {
		err = io.EOF
	}
	return n, err
}

func (r *byteArrayPageReader) WriteValuesTo(w ValueWriter) (int64, error) {
	return writePageValuesTo(w, r, r.page)
}

type fixedLenByteArrayPage struct {
	size        int
	data        []byte
	columnIndex int8
}

func (page *fixedLenByteArrayPage) ColumnIndex() int { return int(^page.columnIndex) }

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
	return &fixedLenByteArrayPageReader{page: page}
}

type fixedLenByteArrayPageReader struct {
	page   *fixedLenByteArrayPage
	offset int
}

func (r *fixedLenByteArrayPageReader) ReadValues(values []Value) (n int, err error) {
	for n < len(values) && r.offset < len(r.page.data) {
		values[n] = makeValueBytes(FixedLenByteArray, r.page.data[r.offset:r.offset+r.page.size])
		values[n].columnIndex = r.page.columnIndex
		r.offset += r.page.size
		n++
	}
	if r.offset == len(r.page.data) {
		err = io.EOF
	}
	return n, err
}

func (r *fixedLenByteArrayPageReader) WriteValuesTo(w ValueWriter) (int64, error) {
	return writePageValuesTo(w, r, r.page)
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

	_ ValueWriterTo = (*optionalPageReader)(nil)
	_ ValueWriterTo = (*repeatedPageReader)(nil)
	_ ValueWriterTo = (*booleanPageReader)(nil)
	_ ValueWriterTo = (*int32PageReader)(nil)
	_ ValueWriterTo = (*int64PageReader)(nil)
	_ ValueWriterTo = (*int96PageReader)(nil)
	_ ValueWriterTo = (*floatPageReader)(nil)
	_ ValueWriterTo = (*doublePageReader)(nil)
	_ ValueWriterTo = (*byteArrayPageReader)(nil)
	_ ValueWriterTo = (*fixedLenByteArrayPageReader)(nil)
)
