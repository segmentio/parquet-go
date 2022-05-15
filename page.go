package parquet

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/encoding/plain"
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

	// Returns the min and max values currently buffered in the writer.
	//
	// The third value is a boolean indicating whether the page bounds were
	// available. Page bounds may not be known if the page contained no values
	// or only nulls, or if they were read from a parquet file which had neither
	// page statistics nor a page index.
	Bounds() (min, max Value, ok bool)

	// Returns the size of the page in bytes (uncompressed).
	Size() int64

	// Returns a reader exposing the values contained in the page.
	//
	// Depending on the underlying implementation, the returned reader may
	// support reading an array of typed Go values by implementing interfaces
	// like parquet.Int32Reader. Applications should use type assertions on
	// the returned reader to determine whether those optimizations are
	// available.
	Values() ValueReader

	// Buffer returns the page as a BufferedPage, which may be the page itself
	// if it was already buffered.
	Buffer() BufferedPage
}

// BufferedPage is an extension of the Page interface implemented by pages
// that are buffered in memory.
type BufferedPage interface {
	Page

	// Returns a copy of the page which does not share any of the buffers, but
	// contains the same values, repetition and definition levels.
	Clone() BufferedPage

	// Returns a new page which is as slice of the receiver between row indexes
	// i and j.
	Slice(i, j int64) BufferedPage

	// Expose the lists of repetition and definition levels of the page.
	//
	// The returned slices may be empty when the page has no repetition or
	// definition levels.
	RepetitionLevels() []byte
	DefinitionLevels() []byte

	// Writes the page data to dst with the given encoding.
	Encode(dst []byte, enc encoding.Encoding) ([]byte, error)
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

// errorPage is an implementation of the Page interface which always errors when
// attempting to read its values.
//
// The error page declares that it contains one value (even if it does not)
// as a way to ensure that it is not ignored due to being empty when written
// to a file.
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

func (page *errorPage) Column() int                                            { return page.columnIndex }
func (page *errorPage) Dictionary() Dictionary                                 { return nil }
func (page *errorPage) NumRows() int64                                         { return 1 }
func (page *errorPage) NumValues() int64                                       { return 1 }
func (page *errorPage) NumNulls() int64                                        { return 0 }
func (page *errorPage) Bounds() (min, max Value, ok bool)                      { return }
func (page *errorPage) Clone() BufferedPage                                    { return page }
func (page *errorPage) Slice(i, j int64) BufferedPage                          { return page }
func (page *errorPage) Size() int64                                            { return 1 }
func (page *errorPage) RepetitionLevels() []byte                               { return nil }
func (page *errorPage) DefinitionLevels() []byte                               { return nil }
func (page *errorPage) Values() ValueReader                                    { return &errorValueReader{err: page.err} }
func (page *errorPage) Buffer() BufferedPage                                   { return page }
func (page *errorPage) Encode(dst []byte, _ encoding.Encoding) ([]byte, error) { return dst, page.err }

func errPageBoundsOutOfRange(i, j, n int64) error {
	return fmt.Errorf("page bounds out of range [%d:%d]: with length %d", i, j, n)
}

func countLevelsEqual(levels []byte, value byte) int {
	return bits.CountByte(levels, value)
}

func countLevelsNotEqual(levels []byte, value byte) int {
	return len(levels) - countLevelsEqual(levels, value)
}

func appendLevel(levels []byte, value byte, count int) []byte {
	if count > 0 {
		i := len(levels)
		j := len(levels) + 1

		if n := len(levels) + count; cap(levels) < n {
			newLevels := make([]byte, n)
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
	maxDefinitionLevel byte
	definitionLevels   []byte
}

func newOptionalPage(base BufferedPage, maxDefinitionLevel byte, definitionLevels []byte) *optionalPage {
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

func (page *optionalPage) Bounds() (min, max Value, ok bool) {
	return page.base.Bounds()
}

func (page *optionalPage) Clone() BufferedPage {
	return newOptionalPage(
		page.base.Clone(),
		page.maxDefinitionLevel,
		append([]byte{}, page.definitionLevels...),
	)
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
	return page.base.Size() + sizeOfBytes(page.definitionLevels)
}

func (page *optionalPage) RepetitionLevels() []byte {
	return nil
}

func (page *optionalPage) DefinitionLevels() []byte {
	return page.definitionLevels
}

func (page *optionalPage) Values() ValueReader {
	return &optionalPageReader{page: page}
}

func (page *optionalPage) Buffer() BufferedPage {
	return page
}

func (page *optionalPage) Encode(dst []byte, enc encoding.Encoding) ([]byte, error) {
	return page.base.Encode(dst, enc)
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
	columnIndex := ^int16(r.page.Column())

	for n < len(values) && r.offset < len(r.page.definitionLevels) {
		for n < len(values) && r.offset < len(r.page.definitionLevels) && r.page.definitionLevels[r.offset] != maxDefinitionLevel {
			values[n] = Value{
				definitionLevel: r.page.definitionLevels[r.offset],
				columnIndex:     columnIndex,
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
				values[n].definitionLevel = maxDefinitionLevel
				r.offset++
				n++
			}
			// Do not return on an io.EOF here as we may still have null values to read.
			if err != nil && err != io.EOF {
				return n, err
			}
			err = nil
		}
	}

	if r.offset == len(r.page.definitionLevels) {
		err = io.EOF
	}
	return n, err
}

type repeatedPage struct {
	base               BufferedPage
	maxRepetitionLevel byte
	maxDefinitionLevel byte
	definitionLevels   []byte
	repetitionLevels   []byte
}

func newRepeatedPage(base BufferedPage, maxRepetitionLevel, maxDefinitionLevel byte, repetitionLevels, definitionLevels []byte) *repeatedPage {
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

func (page *repeatedPage) Bounds() (min, max Value, ok bool) {
	return page.base.Bounds()
}

func (page *repeatedPage) Clone() BufferedPage {
	return newRepeatedPage(
		page.base.Clone(),
		page.maxRepetitionLevel,
		page.maxDefinitionLevel,
		append([]byte{}, page.repetitionLevels...),
		append([]byte{}, page.definitionLevels...),
	)
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

	i = rowIndex1 - numNulls1
	j = rowIndex2 - (numNulls1 + numNulls2)

	return newRepeatedPage(
		page.base.Slice(i, j),
		page.maxRepetitionLevel,
		page.maxDefinitionLevel,
		page.repetitionLevels[rowIndex1:rowIndex2],
		page.definitionLevels[rowIndex1:rowIndex2],
	)
}

func (page *repeatedPage) Size() int64 {
	return sizeOfBytes(page.repetitionLevels) + sizeOfBytes(page.definitionLevels) + page.base.Size()
}

func (page *repeatedPage) RepetitionLevels() []byte {
	return page.repetitionLevels
}

func (page *repeatedPage) DefinitionLevels() []byte {
	return page.definitionLevels
}

func (page *repeatedPage) Encode(dst []byte, enc encoding.Encoding) ([]byte, error) {
	return page.base.Encode(dst, enc)
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
	columnIndex := ^int16(r.page.Column())

	for n < len(values) && r.offset < len(r.page.definitionLevels) {
		for n < len(values) && r.offset < len(r.page.definitionLevels) && r.page.definitionLevels[r.offset] != maxDefinitionLevel {
			values[n] = Value{
				repetitionLevel: r.page.repetitionLevels[r.offset],
				definitionLevel: r.page.definitionLevels[r.offset],
				columnIndex:     columnIndex,
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
			if err != nil && err != io.EOF {
				return n, err
			}
			err = nil
		}
	}

	if r.offset == len(r.page.definitionLevels) {
		err = io.EOF
	}
	return n, err
}

type byteArrayPage struct {
	values      []byte
	numValues   int32
	columnIndex int16
}

func newByteArrayPage(columnIndex int16, numValues int32, values []byte) *byteArrayPage {
	return &byteArrayPage{
		values:      values,
		numValues:   numValues,
		columnIndex: ^columnIndex,
	}
}

func (page *byteArrayPage) Column() int { return int(^page.columnIndex) }

func (page *byteArrayPage) Dictionary() Dictionary { return nil }

func (page *byteArrayPage) NumRows() int64 { return int64(page.numValues) }

func (page *byteArrayPage) NumValues() int64 { return int64(page.numValues) }

func (page *byteArrayPage) NumNulls() int64 { return 0 }

func (page *byteArrayPage) valueAt(offset uint32) []byte {
	length := binary.LittleEndian.Uint32(page.values[offset:])
	j := 4 + offset
	k := 4 + offset + length
	return page.values[j:k:k]
}

func (page *byteArrayPage) min() (min []byte) {
	if len(page.values) > 0 {
		min = page.valueAt(0)

		for i := 4 + len(min); i < len(page.values); {
			v := page.valueAt(uint32(i))

			if string(v) < string(min) {
				min = v
			}

			i += 4
			i += len(v)
		}
	}
	return min
}

func (page *byteArrayPage) max() (max []byte) {
	if len(page.values) > 0 {
		max = page.valueAt(0)

		for i := 4 + len(max); i < len(page.values); {
			v := page.valueAt(uint32(i))

			if string(v) > string(max) {
				max = v
			}

			i += 4
			i += len(v)
		}
	}
	return max
}

func (page *byteArrayPage) bounds() (min, max []byte) {
	if len(page.values) > 0 {
		min = page.valueAt(0)
		max = min

		for i := 4 + len(min); i < len(page.values); {
			v := page.valueAt(uint32(i))

			switch {
			case string(v) < string(min):
				min = v
			case string(v) > string(max):
				max = v
			}

			i += 4
			i += len(v)
		}
	}
	return min, max
}

func (page *byteArrayPage) Bounds() (min, max Value, ok bool) {
	if ok = len(page.values) > 0; ok {
		minBytes, maxBytes := page.bounds()
		min = makeValueBytes(ByteArray, minBytes)
		max = makeValueBytes(ByteArray, maxBytes)
	}
	return min, max, ok
}

func (page *byteArrayPage) cloneValues() []byte {
	values := make([]byte, len(page.values))
	copy(values, page.values)
	return values
}

func (page *byteArrayPage) Clone() BufferedPage {
	return &byteArrayPage{
		values:      page.cloneValues(),
		numValues:   page.numValues,
		columnIndex: page.columnIndex,
	}
}

func (page *byteArrayPage) Slice(i, j int64) BufferedPage {
	numValues := j - i

	off0 := uint32(0)
	for i > 0 {
		off0 += binary.LittleEndian.Uint32(page.values[off0:])
		off0 += plain.ByteArrayLengthSize
		i--
		j--
	}

	off1 := off0
	for j > 0 {
		off1 += binary.LittleEndian.Uint32(page.values[off1:])
		off1 += plain.ByteArrayLengthSize
		j--
	}

	return &byteArrayPage{
		values:      page.values[off0:off1:off1],
		numValues:   int32(numValues),
		columnIndex: page.columnIndex,
	}
}

func (page *byteArrayPage) Size() int64 { return int64(len(page.values)) }

func (page *byteArrayPage) RepetitionLevels() []byte { return nil }

func (page *byteArrayPage) DefinitionLevels() []byte { return nil }

func (page *byteArrayPage) Values() ValueReader { return &byteArrayPageReader{page: page} }

func (page *byteArrayPage) Buffer() BufferedPage { return page }

func (page *byteArrayPage) Encode(dst []byte, enc encoding.Encoding) ([]byte, error) {
	return enc.EncodeByteArray(dst, page.values)
}

type byteArrayPageReader struct {
	page   *byteArrayPage
	offset int
}

func (r *byteArrayPageReader) Read(b []byte) (int, error) {
	_, n, err := r.readByteArrays(b)
	return n, err
}

func (r *byteArrayPageReader) ReadRequired(values []byte) (int, error) {
	return r.ReadByteArrays(values)
}

func (r *byteArrayPageReader) ReadByteArrays(values []byte) (int, error) {
	n, _, err := r.readByteArrays(values)
	return n, err
}

func (r *byteArrayPageReader) readByteArrays(values []byte) (c, n int, err error) {
	for r.offset < len(r.page.values) {
		b := r.page.valueAt(uint32(r.offset))
		k := plain.ByteArrayLengthSize + len(b)
		if k > (len(values) - n) {
			break
		}
		plain.PutByteArrayLength(values[n:], len(b))
		n += plain.ByteArrayLengthSize
		n += copy(values[n:], b)
		r.offset += plain.ByteArrayLengthSize
		r.offset += len(b)
		c++
	}
	if r.offset == len(r.page.values) {
		err = io.EOF
	} else if n == 0 && len(values) > 0 {
		err = io.ErrShortBuffer
	}
	return c, n, err
}

func (r *byteArrayPageReader) ReadValues(values []Value) (n int, err error) {
	for n < len(values) && r.offset < len(r.page.values) {
		value := r.page.valueAt(uint32(r.offset))
		values[n] = makeValueBytes(ByteArray, value)
		values[n].columnIndex = r.page.columnIndex
		r.offset += plain.ByteArrayLengthSize
		r.offset += len(value)
		n++
	}
	if r.offset == len(r.page.values) {
		err = io.EOF
	}
	return n, err
}

type fixedLenByteArrayPage struct {
	data        []byte
	size        int
	columnIndex int16
}

func newFixedLenByteArrayPage(columnIndex int16, numValues int32, data []byte, size int) *fixedLenByteArrayPage {
	if (len(data) % size) != 0 {
		panic("cannot create fixed-length byte array page from input which is not a multiple of the type size")
	}
	if int(numValues) != len(data)/size {
		panic(fmt.Errorf("number of values mismatch in numValues and data arguments: %d != %d", numValues, len(data)/size))
	}
	return &fixedLenByteArrayPage{
		data:        data,
		size:        size,
		columnIndex: ^columnIndex,
	}
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

func (page *fixedLenByteArrayPage) Bounds() (min, max Value, ok bool) {
	if ok = len(page.data) > 0; ok {
		minBytes, maxBytes := page.bounds()
		min = makeValueBytes(FixedLenByteArray, minBytes)
		max = makeValueBytes(FixedLenByteArray, maxBytes)
	}
	return min, max, ok
}

func (page *fixedLenByteArrayPage) Clone() BufferedPage {
	return &fixedLenByteArrayPage{
		data:        append([]byte{}, page.data...),
		size:        page.size,
		columnIndex: page.columnIndex,
	}
}

func (page *fixedLenByteArrayPage) Slice(i, j int64) BufferedPage {
	return &fixedLenByteArrayPage{
		data:        page.data[i*int64(page.size) : j*int64(page.size)],
		size:        page.size,
		columnIndex: page.columnIndex,
	}
}

func (page *fixedLenByteArrayPage) Size() int64 { return sizeOfBytes(page.data) }

func (page *fixedLenByteArrayPage) RepetitionLevels() []byte { return nil }

func (page *fixedLenByteArrayPage) DefinitionLevels() []byte { return nil }

func (page *fixedLenByteArrayPage) Values() ValueReader {
	return &fixedLenByteArrayPageReader{page: page}
}

func (page *fixedLenByteArrayPage) Buffer() BufferedPage { return page }

func (page *fixedLenByteArrayPage) Encode(dst []byte, enc encoding.Encoding) ([]byte, error) {
	return enc.EncodeFixedLenByteArray(dst, page.data, page.size)
}

type fixedLenByteArrayPageReader struct {
	page   *fixedLenByteArrayPage
	offset int
}

func (r *fixedLenByteArrayPageReader) Read(b []byte) (n int, err error) {
	n, err = r.ReadFixedLenByteArrays(b)
	return n * r.page.size, err
}

func (r *fixedLenByteArrayPageReader) ReadRequired(values []byte) (int, error) {
	return r.ReadFixedLenByteArrays(values)
}

func (r *fixedLenByteArrayPageReader) ReadFixedLenByteArrays(values []byte) (n int, err error) {
	n = copy(values, r.page.data[r.offset:]) / r.page.size
	r.offset += n * r.page.size
	if r.offset == len(r.page.data) {
		err = io.EOF
	} else if n == 0 && len(values) > 0 {
		err = io.ErrShortBuffer
	}
	return n, err
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

type nullPage struct {
	column int
	count  int
}

func newNullPage(columnIndex int16, numValues int32) *nullPage {
	return &nullPage{
		column: int(columnIndex),
		count:  int(numValues),
	}
}

func (p *nullPage) Column() int                       { return p.column }
func (p *nullPage) Dictionary() Dictionary            { return nil }
func (p *nullPage) NumRows() int64                    { return int64(p.count) }
func (p *nullPage) NumValues() int64                  { return int64(p.count) }
func (p *nullPage) NumNulls() int64                   { return int64(p.count) }
func (p *nullPage) Bounds() (min, max Value, ok bool) { return }
func (p *nullPage) Size() int64                       { return 1 }
func (p *nullPage) Values() ValueReader {
	return &nullPageReader{column: p.column, remain: p.count}
}
func (p *nullPage) Buffer() BufferedPage { return p }
func (p *nullPage) Clone() BufferedPage  { return p }
func (p *nullPage) Slice(i, j int64) BufferedPage {
	return &nullPage{column: p.column, count: p.count - int(j-i)}
}
func (p *nullPage) RepetitionLevels() []byte { return nil }
func (p *nullPage) DefinitionLevels() []byte { return nil }
func (p *nullPage) Encode(dst []byte, enc encoding.Encoding) ([]byte, error) {
	return dst[:0], nil
}

type nullPageReader struct {
	column int
	remain int
}

func (r *nullPageReader) ReadValues(values []Value) (n int, err error) {
	columnIndex := ^int16(r.column)
	values = values[:min(r.remain, len(values))]
	for i := range values {
		values[i] = Value{columnIndex: columnIndex}
	}
	r.remain -= len(values)
	if r.remain == 0 {
		err = io.EOF
	}
	return len(values), err
}

/*
type bufferedPage struct {
	class       pageClass
	values      []byte
	numValues   int32
	columnIndex int16
}

func (p *bufferedPage) Column() int {
	return int(^p.columnIndex)
}

func (p *bufferedPage) Dictionary() Dictionary {
	return nil
}

func (p *bufferedPage) NumRows() int64 {
	return int64(p.numValues)
}

func (p *bufferedPage) NumValues() int64 {
	return int64(p.numValues)
}

func (p *bufferedPage) NumNulls() int64 {
	return 0
}

func (p *bufferedPage) Bounds() (min, max Value, ok bool) {
	return p.class.bounds(p.values)
}

func (p *bufferedPage) Size() int64 {
	return int64(p.values)
}

func (p *bufferedPage) Values() ValueReader {
	return p.class.values(p.values)
}

func (p *bufferedPage) Buffer() BufferedPage {
	return p
}

func (p *bufferedPage) Clone() BufferedPage {
	values := make([]byte, len(p.values))
	copy(values, p.values)
	return &bufferedPage{
		values:      values,
		numValues:   p.numValues,
		columnIndex: p.columnIndex,
	}
}

func (p *bufferedPage) Slice(i, j int64) BufferedPage {
	return &bufferedPage{
		values:      p.class.slice(p.values, i, j),
		numValues:   int32(j - i),
		columnIndex: p.columnIndex,
	}
}

func (p *bufferedPage) RepetitionLevels() []byte {
	return nil
}

func (p *bufferedPage) DefinitionLevels() []byte {
	return nil
}

func (p *bufferedPage) Encode(dst []byte, enc encoding.Encoding) ([]byte, error) {
	return p.class.encode(dst, p.values, enc)
}

type pageClass interface {
	bounds(values []byte) (min, max Value, ok bool)
	values(values []byte) ValueReader
	slice(values []byte, i, j int) []byte
	encode(dst, src []byte, enc encoding.Encoding) ([]byte, error)
}

type booleanPageClass struct{}

func (booleanPageClass) bounds() (min, max Value, ok bool) {

}
*/
