package parquet

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/internal/bits"
)

// Page values represent sequences of parquet values. From the Parquet
// documentation: "Column chunks are a chunk of the data for a particular
// column. They live in a particular row group and are guaranteed to be
// contiguous in the file. Column chunks are divided up into pages. A page is
// conceptually an indivisible unit (in terms of compression and encoding).
// There can be multiple page types which are interleaved in a column chunk."
//
// https://github.com/apache/parquet-format#glossary
type Page interface {
	// Returns the column index that this page belongs to.
	Column() int

	// If the page contains indexed values, calling this method returns the
	// dictionary in which the values are looked up. Otherwise, the method
	// returns nil.
	Dictionary() Dictionary

	// Returns the number of rows, values, and nulls in the page. The number of
	// rows may be less than the number of values in the page if the page is
	// part of a repeated column.
	NumRows() int64
	NumValues() int64
	NumNulls() int64

	// Returns the min and max values currently buffered in the writter.
	Bounds() (min, max Value)

	// Returns the size of the page in bytes (uncompressed).
	Size() int64

	// Returns a reader exposing the values contained in the page.
	Values() ValueReader

	// Buffer returns the page as a BufferedPage, which may be the page itself
	// if it was already buffered.
	//
	// Compressed pages will be consumed to create the returned buffered page,
	// their content will no be readable anymore after the call.
	Buffer() BufferedPage
}

// BufferedPage is an extension of the Page interface implemented by pages
// that are buffered in memory.
type BufferedPage interface {
	Page

	// Returns a new page which is as slice of the receiver between row indexes
	// i and j.
	Slice(i, j int64) BufferedPage

	// Expose the lists of repetition and definition levels of the page.
	//
	// The returned slices may be empty when the page has no repetition or
	// definition levels.
	RepetitionLevels() []int8
	DefinitionLevels() []int8

	// Writes the page to the given encoder.
	WriteTo(encoding.Encoder) error
}

// CompressedPage is an extension of the Page interface implemented by pages
// that have been compressed to their on-file representation.
type CompressedPage interface {
	Page

	// Returns a representation of the page header.
	PageHeader() PageHeader

	// Returns a reader exposing the content of the compressed page.
	PageData() io.Reader

	// Returns the size of the page data.
	PageSize() int64

	// CRC returns the IEEE CRC32 checksum of the page.
	CRC() uint32
}

// PageReader is an interface implemented by types that support producing a
// sequence of pages.
type PageReader interface {
	ReadPage() (Page, error)
}

// PageWriter is an interface implemented by types that support writing pages
// to an underlying storage medium.
type PageWriter interface {
	WritePage(Page) (int64, error)
}

type singlePage struct {
	page Page
	seek int64
}

func (r *singlePage) ReadPage() (Page, error) {
	if numRows := r.page.NumRows(); r.seek < numRows {
		seek := r.seek
		r.seek = numRows
		if seek > 0 {
			return r.page.Buffer().Slice(seek, numRows), nil
		}
		return r.page, nil
	}
	return nil, io.EOF
}

func (r *singlePage) SeekToRow(rowIndex int64) error {
	r.seek = rowIndex
	return nil
}

func onePage(page Page) Pages { return &singlePage{page: page} }

// CopyPages copies pages from src to dst, returning the number of values that
// were copied.
//
// The function returns any error it encounters reading or writing pages, except
// for io.EOF from the reader which indicates that there were no more pages to
// read.
func CopyPages(dst PageWriter, src PageReader) (numValues int64, err error) {
	for {
		p, err := src.ReadPage()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return numValues, err
		}
		n, err := dst.WritePage(p)
		numValues += n
		if err != nil {
			return numValues, err
		}
	}
}

func sizeOfBytes(data []byte) int64 { return 1 * int64(len(data)) }

func sizeOfBool(data []bool) int64 { return 1 * int64(len(data)) }

func sizeOfInt8(data []int8) int64 { return 1 * int64(len(data)) }

func sizeOfInt32(data []int32) int64 { return 4 * int64(len(data)) }

func sizeOfInt64(data []int64) int64 { return 8 * int64(len(data)) }

func sizeOfInt96(data []deprecated.Int96) int64 { return 12 * int64(len(data)) }

func sizeOfFloat32(data []float32) int64 { return 4 * int64(len(data)) }

func sizeOfFloat64(data []float64) int64 { return 8 * int64(len(data)) }

func forEachPageSlice(page BufferedPage, wantSize int64, do func(BufferedPage) error) error {
	numRows := page.NumRows()
	if numRows == 0 {
		return nil
	}

	pageSize := page.Size()
	numPages := (pageSize + (wantSize - 1)) / wantSize
	rowIndex := int64(0)
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

func (page *errorPage) Column() int                    { return page.columnIndex }
func (page *errorPage) Dictionary() Dictionary         { return nil }
func (page *errorPage) NumRows() int64                 { return 0 }
func (page *errorPage) NumValues() int64               { return 0 }
func (page *errorPage) NumNulls() int64                { return 0 }
func (page *errorPage) Bounds() (min, max Value)       { return }
func (page *errorPage) Slice(i, j int64) BufferedPage  { return page }
func (page *errorPage) Size() int64                    { return 0 }
func (page *errorPage) RepetitionLevels() []int8       { return nil }
func (page *errorPage) DefinitionLevels() []int8       { return nil }
func (page *errorPage) WriteTo(encoding.Encoder) error { return page.err }
func (page *errorPage) Values() ValueReader            { return &errorValueReader{err: page.err} }
func (page *errorPage) Buffer() BufferedPage           { return page }

func errPageBoundsOutOfRange(i, j, n int64) error {
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
	base               BufferedPage
	maxDefinitionLevel int8
	definitionLevels   []int8
}

func newOptionalPage(base BufferedPage, maxDefinitionLevel int8, definitionLevels []int8) *optionalPage {
	return &optionalPage{
		base:               base,
		maxDefinitionLevel: maxDefinitionLevel,
		definitionLevels:   definitionLevels,
	}
}

func (page *optionalPage) Column() int {
	return page.base.Column()
}

func (page *optionalPage) Dictionary() Dictionary {
	return page.base.Dictionary()
}

func (page *optionalPage) NumRows() int64 {
	return int64(len(page.definitionLevels))
}

func (page *optionalPage) NumValues() int64 {
	return int64(len(page.definitionLevels))
}

func (page *optionalPage) NumNulls() int64 {
	return int64(countLevelsNotEqual(page.definitionLevels, page.maxDefinitionLevel))
}

func (page *optionalPage) Bounds() (min, max Value) {
	return page.base.Bounds()
}

func (page *optionalPage) Slice(i, j int64) BufferedPage {
	numNulls1 := int64(countLevelsNotEqual(page.definitionLevels[:i], page.maxDefinitionLevel))
	numNulls2 := int64(countLevelsNotEqual(page.definitionLevels[i:j], page.maxDefinitionLevel))
	return newOptionalPage(
		page.base.Slice(i-numNulls1, j-(numNulls1+numNulls2)),
		page.maxDefinitionLevel,
		page.definitionLevels[i:j],
	)
}

func (page *optionalPage) Size() int64 {
	return page.base.Size() + sizeOfInt8(page.definitionLevels)
}

func (page *optionalPage) RepetitionLevels() []int8 {
	return nil
}

func (page *optionalPage) DefinitionLevels() []int8 {
	return page.definitionLevels
}

func (page *optionalPage) WriteTo(e encoding.Encoder) error {
	return page.base.WriteTo(e)
}

func (page *optionalPage) Values() ValueReader {
	return &optionalPageReader{page: page}
}

func (page *optionalPage) Buffer() BufferedPage {
	return page
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

type repeatedPage struct {
	base               BufferedPage
	maxRepetitionLevel int8
	maxDefinitionLevel int8
	definitionLevels   []int8
	repetitionLevels   []int8
}

func newRepeatedPage(base BufferedPage, maxRepetitionLevel, maxDefinitionLevel int8, repetitionLevels, definitionLevels []int8) *repeatedPage {
	return &repeatedPage{
		base:               base,
		maxRepetitionLevel: maxRepetitionLevel,
		maxDefinitionLevel: maxDefinitionLevel,
		definitionLevels:   definitionLevels,
		repetitionLevels:   repetitionLevels,
	}
}

func (page *repeatedPage) Column() int {
	return page.base.Column()
}

func (page *repeatedPage) Dictionary() Dictionary {
	return page.base.Dictionary()
}

func (page *repeatedPage) NumRows() int64 {
	return int64(countLevelsEqual(page.repetitionLevels, 0))
}

func (page *repeatedPage) NumValues() int64 {
	return int64(len(page.definitionLevels))
}

func (page *repeatedPage) NumNulls() int64 {
	return int64(countLevelsNotEqual(page.definitionLevels, page.maxDefinitionLevel))
}

func (page *repeatedPage) Bounds() (min, max Value) {
	return page.base.Bounds()
}

func (page *repeatedPage) Slice(i, j int64) BufferedPage {
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

	rowIndex0 := int64(0)
	rowIndex1 := int64(len(page.repetitionLevels))
	rowIndex2 := int64(len(page.repetitionLevels))

	for k, def := range page.repetitionLevels {
		if def != page.maxRepetitionLevel {
			if rowIndex0 == i {
				rowIndex1 = int64(k)
			}
			if rowIndex0 == j {
				rowIndex2 = int64(k)
			}
			rowIndex0++
		}
	}

	numNulls1 := int64(countLevelsNotEqual(page.definitionLevels[:rowIndex1], page.maxDefinitionLevel))
	numNulls2 := int64(countLevelsNotEqual(page.definitionLevels[rowIndex1:rowIndex2], page.maxDefinitionLevel))

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

func (page *repeatedPage) RepetitionLevels() []int8 {
	return page.repetitionLevels
}

func (page *repeatedPage) DefinitionLevels() []int8 {
	return page.definitionLevels
}

func (page *repeatedPage) WriteTo(e encoding.Encoder) error {
	return page.base.WriteTo(e)
}

func (page *repeatedPage) Values() ValueReader {
	return &repeatedPageReader{page: page}
}

func (page *repeatedPage) Buffer() BufferedPage {
	return page
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

type booleanPage struct {
	values      []bool
	columnIndex int8
}

func (page *booleanPage) Column() int { return int(^page.columnIndex) }

func (page *booleanPage) Dictionary() Dictionary { return nil }

func (page *booleanPage) NumRows() int64 { return int64(len(page.values)) }

func (page *booleanPage) NumValues() int64 { return int64(len(page.values)) }

func (page *booleanPage) NumNulls() int64 { return 0 }

func (page *booleanPage) min() bool {
	for _, value := range page.values {
		if !value {
			return false
		}
	}
	return len(page.values) > 0
}

func (page *booleanPage) max() bool {
	for _, value := range page.values {
		if value {
			return true
		}
	}
	return false
}

func (page *booleanPage) bounds() (min, max bool) {
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
		min = true
	}
	if hasTrue {
		max = true
	}
	return min, max
}

func (page *booleanPage) Bounds() (min, max Value) {
	if len(page.values) > 0 {
		minBool, maxBool := page.bounds()
		min = makeValueBoolean(minBool)
		max = makeValueBoolean(maxBool)
	}
	return min, max
}

func (page *booleanPage) Slice(i, j int64) BufferedPage {
	return &booleanPage{
		values:      page.values[i:j],
		columnIndex: page.columnIndex,
	}
}

func (page *booleanPage) Size() int64 { return sizeOfBool(page.values) }

func (page *booleanPage) RepetitionLevels() []int8 { return nil }

func (page *booleanPage) DefinitionLevels() []int8 { return nil }

func (page *booleanPage) WriteTo(e encoding.Encoder) error { return e.EncodeBoolean(page.values) }

func (page *booleanPage) Values() ValueReader { return &booleanPageReader{page: page} }

func (page *booleanPage) Buffer() BufferedPage { return page }

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

type int32Page struct {
	values      []int32
	columnIndex int8
}

func (page *int32Page) Column() int { return int(^page.columnIndex) }

func (page *int32Page) Dictionary() Dictionary { return nil }

func (page *int32Page) NumRows() int64 { return int64(len(page.values)) }

func (page *int32Page) NumValues() int64 { return int64(len(page.values)) }

func (page *int32Page) NumNulls() int64 { return 0 }

func (page *int32Page) min() int32 { return bits.MinInt32(page.values) }

func (page *int32Page) max() int32 { return bits.MaxInt32(page.values) }

func (page *int32Page) bounds() (min, max int32) { return bits.MinMaxInt32(page.values) }

func (page *int32Page) Bounds() (min, max Value) {
	if len(page.values) > 0 {
		minInt32, maxInt32 := page.bounds()
		min = makeValueInt32(minInt32)
		max = makeValueInt32(maxInt32)
	}
	return min, max
}

func (page *int32Page) Slice(i, j int64) BufferedPage {
	return &int32Page{
		values:      page.values[i:j],
		columnIndex: page.columnIndex,
	}
}

func (page *int32Page) Size() int64 { return sizeOfInt32(page.values) }

func (page *int32Page) RepetitionLevels() []int8 { return nil }

func (page *int32Page) DefinitionLevels() []int8 { return nil }

func (page *int32Page) WriteTo(e encoding.Encoder) error { return e.EncodeInt32(page.values) }

func (page *int32Page) Values() ValueReader { return &int32PageReader{page: page} }

func (page *int32Page) Buffer() BufferedPage { return page }

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

type int64Page struct {
	values      []int64
	columnIndex int8
}

func (page *int64Page) Column() int { return int(^page.columnIndex) }

func (page *int64Page) Dictionary() Dictionary { return nil }

func (page *int64Page) NumRows() int64 { return int64(len(page.values)) }

func (page *int64Page) NumValues() int64 { return int64(len(page.values)) }

func (page *int64Page) NumNulls() int64 { return 0 }

func (page *int64Page) min() int64 { return bits.MinInt64(page.values) }

func (page *int64Page) max() int64 { return bits.MaxInt64(page.values) }

func (page *int64Page) bounds() (min, max int64) { return bits.MinMaxInt64(page.values) }

func (page *int64Page) Bounds() (min, max Value) {
	if len(page.values) > 0 {
		minInt64, maxInt64 := page.bounds()
		min = makeValueInt64(minInt64)
		max = makeValueInt64(maxInt64)
	}
	return min, max
}

func (page *int64Page) Slice(i, j int64) BufferedPage {
	return &int64Page{
		values:      page.values[i:j],
		columnIndex: page.columnIndex,
	}
}

func (page *int64Page) Size() int64 { return sizeOfInt64(page.values) }

func (page *int64Page) RepetitionLevels() []int8 { return nil }

func (page *int64Page) DefinitionLevels() []int8 { return nil }

func (page *int64Page) WriteTo(e encoding.Encoder) error { return e.EncodeInt64(page.values) }

func (page *int64Page) Values() ValueReader { return &int64PageReader{page: page} }

func (page *int64Page) Buffer() BufferedPage { return page }

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

type int96Page struct {
	values      []deprecated.Int96
	columnIndex int8
}

func (page *int96Page) Column() int { return int(^page.columnIndex) }

func (page *int96Page) Dictionary() Dictionary { return nil }

func (page *int96Page) NumRows() int64 { return int64(len(page.values)) }

func (page *int96Page) NumValues() int64 { return int64(len(page.values)) }

func (page *int96Page) NumNulls() int64 { return 0 }

func (page *int96Page) min() deprecated.Int96 { return deprecated.MinInt96(page.values) }

func (page *int96Page) max() deprecated.Int96 { return deprecated.MaxInt96(page.values) }

func (page *int96Page) bounds() (min, max deprecated.Int96) {
	return deprecated.MinMaxInt96(page.values)
}

func (page *int96Page) Bounds() (min, max Value) {
	if len(page.values) > 0 {
		minInt96, maxInt96 := page.bounds()
		min = makeValueInt96(minInt96)
		max = makeValueInt96(maxInt96)
	}
	return min, max
}

func (page *int96Page) Slice(i, j int64) BufferedPage {
	return &int96Page{
		values:      page.values[i:j],
		columnIndex: page.columnIndex,
	}
}

func (page *int96Page) Size() int64 { return sizeOfInt96(page.values) }

func (page *int96Page) RepetitionLevels() []int8 { return nil }

func (page *int96Page) DefinitionLevels() []int8 { return nil }

func (page *int96Page) WriteTo(e encoding.Encoder) error { return e.EncodeInt96(page.values) }

func (page *int96Page) Values() ValueReader { return &int96PageReader{page: page} }

func (page *int96Page) Buffer() BufferedPage { return page }

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

type floatPage struct {
	values      []float32
	columnIndex int8
}

func (page *floatPage) Column() int { return int(^page.columnIndex) }

func (page *floatPage) Dictionary() Dictionary { return nil }

func (page *floatPage) NumRows() int64 { return int64(len(page.values)) }

func (page *floatPage) NumValues() int64 { return int64(len(page.values)) }

func (page *floatPage) NumNulls() int64 { return 0 }

func (page *floatPage) min() float32 { return bits.MinFloat32(page.values) }

func (page *floatPage) max() float32 { return bits.MaxFloat32(page.values) }

func (page *floatPage) bounds() (min, max float32) { return bits.MinMaxFloat32(page.values) }

func (page *floatPage) Bounds() (min, max Value) {
	if len(page.values) > 0 {
		minFloat32, maxFloat32 := page.bounds()
		min = makeValueFloat(minFloat32)
		max = makeValueFloat(maxFloat32)
	}
	return min, max
}

func (page *floatPage) Slice(i, j int64) BufferedPage {
	return &floatPage{
		values:      page.values[i:j],
		columnIndex: page.columnIndex,
	}
}

func (page *floatPage) Size() int64 { return sizeOfFloat32(page.values) }

func (page *floatPage) RepetitionLevels() []int8 { return nil }

func (page *floatPage) DefinitionLevels() []int8 { return nil }

func (page *floatPage) WriteTo(e encoding.Encoder) error { return e.EncodeFloat(page.values) }

func (page *floatPage) Values() ValueReader { return &floatPageReader{page: page} }

func (page *floatPage) Buffer() BufferedPage { return page }

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

type doublePage struct {
	values      []float64
	columnIndex int8
}

func (page *doublePage) Column() int { return int(^page.columnIndex) }

func (page *doublePage) Dictionary() Dictionary { return nil }

func (page *doublePage) NumRows() int64 { return int64(len(page.values)) }

func (page *doublePage) NumValues() int64 { return int64(len(page.values)) }

func (page *doublePage) NumNulls() int64 { return 0 }

func (page *doublePage) min() float64 { return bits.MinFloat64(page.values) }

func (page *doublePage) max() float64 { return bits.MaxFloat64(page.values) }

func (page *doublePage) bounds() (min, max float64) { return bits.MinMaxFloat64(page.values) }

func (page *doublePage) Bounds() (min, max Value) {
	if len(page.values) > 0 {
		minFloat64, maxFloat64 := page.bounds()
		min = makeValueDouble(minFloat64)
		max = makeValueDouble(maxFloat64)
	}
	return min, max
}

func (page *doublePage) Slice(i, j int64) BufferedPage {
	return &doublePage{
		values:      page.values[i:j],
		columnIndex: page.columnIndex,
	}
}

func (page *doublePage) Size() int64 { return sizeOfFloat64(page.values) }

func (page *doublePage) RepetitionLevels() []int8 { return nil }

func (page *doublePage) DefinitionLevels() []int8 { return nil }

func (page *doublePage) WriteTo(e encoding.Encoder) error { return e.EncodeDouble(page.values) }

func (page *doublePage) Values() ValueReader { return &doublePageReader{page: page} }

func (page *doublePage) Buffer() BufferedPage { return page }

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

type byteArrayPage struct {
	values      encoding.ByteArrayList
	columnIndex int8
}

func (page *byteArrayPage) Column() int { return int(^page.columnIndex) }

func (page *byteArrayPage) Dictionary() Dictionary { return nil }

func (page *byteArrayPage) NumRows() int64 { return int64(page.values.Len()) }

func (page *byteArrayPage) NumValues() int64 { return int64(page.values.Len()) }

func (page *byteArrayPage) NumNulls() int64 { return 0 }

func (page *byteArrayPage) min() (min []byte) {
	if page.values.Len() > 0 {
		min = page.values.Index(0)
		for i := 1; i < page.values.Len(); i++ {
			v := page.values.Index(i)
			if string(v) < string(min) {
				min = v
			}
		}
	}
	return min
}

func (page *byteArrayPage) max() (max []byte) {
	if page.values.Len() > 0 {
		max = page.values.Index(0)
		for i := 1; i < page.values.Len(); i++ {
			v := page.values.Index(i)
			if string(v) > string(max) {
				max = v
			}
		}
	}
	return max
}

func (page *byteArrayPage) bounds() (min, max []byte) {
	if page.values.Len() > 0 {
		min = page.values.Index(0)
		max = min

		for i := 1; i < page.values.Len(); i++ {
			v := page.values.Index(i)
			switch {
			case string(v) < string(min):
				min = v
			case string(v) > string(max):
				max = v
			}
		}
	}
	return min, max
}

func (page *byteArrayPage) Bounds() (min, max Value) {
	if page.values.Len() > 0 {
		minBytes, maxBytes := page.bounds()
		min = makeValueBytes(ByteArray, minBytes)
		max = makeValueBytes(ByteArray, maxBytes)
	}
	return min, max
}

func (page *byteArrayPage) Slice(i, j int64) BufferedPage {
	return &byteArrayPage{
		values:      page.values.Slice(int(i), int(j)),
		columnIndex: page.columnIndex,
	}
}

func (page *byteArrayPage) Size() int64 { return page.values.Size() }

func (page *byteArrayPage) RepetitionLevels() []int8 { return nil }

func (page *byteArrayPage) DefinitionLevels() []int8 { return nil }

func (page *byteArrayPage) WriteTo(e encoding.Encoder) error { return e.EncodeByteArray(page.values) }

func (page *byteArrayPage) Values() ValueReader { return &byteArrayPageReader{page: page} }

func (page *byteArrayPage) Buffer() BufferedPage { return page }

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

type fixedLenByteArrayPage struct {
	size        int
	data        []byte
	columnIndex int8
}

func (page *fixedLenByteArrayPage) Column() int { return int(^page.columnIndex) }

func (page *fixedLenByteArrayPage) Dictionary() Dictionary { return nil }

func (page *fixedLenByteArrayPage) NumRows() int64 { return int64(len(page.data) / page.size) }

func (page *fixedLenByteArrayPage) NumValues() int64 { return int64(len(page.data) / page.size) }

func (page *fixedLenByteArrayPage) NumNulls() int64 { return 0 }

func (page *fixedLenByteArrayPage) min() []byte {
	return bits.MinFixedLenByteArray(page.size, page.data)
}

func (page *fixedLenByteArrayPage) max() []byte {
	return bits.MaxFixedLenByteArray(page.size, page.data)
}

func (page *fixedLenByteArrayPage) bounds() (min, max []byte) {
	return bits.MinMaxFixedLenByteArray(page.size, page.data)
}

func (page *fixedLenByteArrayPage) Bounds() (min, max Value) {
	if len(page.data) > 0 {
		minBytes, maxBytes := page.bounds()
		min = makeValueBytes(FixedLenByteArray, minBytes)
		max = makeValueBytes(FixedLenByteArray, maxBytes)
	}
	return min, max
}

func (page *fixedLenByteArrayPage) Slice(i, j int64) BufferedPage {
	return &fixedLenByteArrayPage{
		size:        page.size,
		data:        page.data[i*int64(page.size) : j*int64(page.size)],
		columnIndex: page.columnIndex,
	}
}

func (page *fixedLenByteArrayPage) Size() int64 { return sizeOfBytes(page.data) }

func (page *fixedLenByteArrayPage) RepetitionLevels() []int8 { return nil }

func (page *fixedLenByteArrayPage) DefinitionLevels() []int8 { return nil }

func (page *fixedLenByteArrayPage) WriteTo(e encoding.Encoder) error {
	return e.EncodeFixedLenByteArray(page.size, page.data)
}

func (page *fixedLenByteArrayPage) Values() ValueReader {
	return &fixedLenByteArrayPageReader{page: page}
}

func (page *fixedLenByteArrayPage) Buffer() BufferedPage { return page }

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

// The following two specializations for unsigned integer types are needed to
// apply an unsigned comparison when looking up the min and max page values.

type uint32Page struct{ *int32Page }

func (page uint32Page) min() uint32 { return bits.MinUint32(bits.Int32ToUint32(page.values)) }

func (page uint32Page) max() uint32 { return bits.MaxUint32(bits.Int32ToUint32(page.values)) }

func (page uint32Page) bounds() (min, max uint32) {
	return bits.MinMaxUint32(bits.Int32ToUint32(page.values))
}

func (page uint32Page) Bounds() (min, max Value) {
	if len(page.values) > 0 {
		minUint32, maxUint32 := page.bounds()
		min = makeValueInt32(int32(minUint32))
		max = makeValueInt32(int32(maxUint32))
	}
	return min, max
}

func (page uint32Page) Slice(i, j int64) BufferedPage {
	return uint32Page{page.int32Page.Slice(i, j).(*int32Page)}
}

func (page uint32Page) Buffer() BufferedPage { return page }

type uint64Page struct{ *int64Page }

func (page uint64Page) min() uint64 { return bits.MinUint64(bits.Int64ToUint64(page.values)) }

func (page uint64Page) max() uint64 { return bits.MaxUint64(bits.Int64ToUint64(page.values)) }

func (page uint64Page) bounds() (min, max uint64) {
	return bits.MinMaxUint64(bits.Int64ToUint64(page.values))
}

func (page uint64Page) Bounds() (min, max Value) {
	if len(page.values) > 0 {
		minUint64, maxUint64 := page.bounds()
		min = makeValueInt64(int64(minUint64))
		max = makeValueInt64(int64(maxUint64))
	}
	return min, max
}

func (page uint64Page) Slice(i, j int64) BufferedPage {
	return uint64Page{page.int64Page.Slice(i, j).(*int64Page)}
}

func (page uint64Page) Buffer() BufferedPage { return page }

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
