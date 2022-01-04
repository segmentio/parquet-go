package parquet

import (
	"bytes"
	"io"
	"sort"

	"github.com/segmentio/parquet/deprecated"
	"github.com/segmentio/parquet/encoding"
)

// ColumnBuffer is an interface representing columns of a row group.
//
// ColumnBuffer implements sort.Interface as a way to support reordering the
// rows that have been written to it.
type ColumnBuffer interface {
	// Exposes a read-only view of the column buffer.
	RowGroupColumn

	// Allows reading rows back from the column by calling ReadRowAt.
	RowReaderAt

	// Provides the main mechanism for writing values to the column via the
	// WriteRow method. WriteRow must be called with a row containing only
	// the value for this column; unless this is a repeated column, the row
	// must contain a single value.
	RowWriter

	// The column implements ValueWriter as a mechanism to optimize the copy
	// of values into the buffer in contexts where the row information is
	// provided by the values because the repetition and definition levels
	// are set.
	ValueWriter

	// For indexed columns, returns the underlying dictionary holding the column
	// values. If the column is not indexed, nil is returned.
	Dictionary() Dictionary

	// Returns a copy of the column. The returned copy shares no memory with
	// the original, mutations of either column will not modify the other.
	Clone() ColumnBuffer

	// Returns the column as a BufferedPage.
	Page() BufferedPage

	// Clears all rows written to the column.
	Reset()

	// Returns the current capacity of the column (rows).
	Cap() int

	// Returns the number of rows currently written to the column.
	Len() int

	// Compares rows at index i and j and returns whether i < j.
	Less(i, j int) bool

	// Swaps rows at index i and j.
	Swap(i, j int)

	// Returns the size of the column buffer in bytes.
	Size() int64
}

type nullOrdering func(column ColumnBuffer, i, j int, maxDefinitionLevel, definitionLevel1, definitionLevel2 int8) bool

func nullsGoFirst(column ColumnBuffer, i, j int, maxDefinitionLevel, definitionLevel1, definitionLevel2 int8) bool {
	if definitionLevel1 != maxDefinitionLevel {
		return definitionLevel2 == maxDefinitionLevel
	} else {
		return definitionLevel2 == maxDefinitionLevel && column.Less(i, j)
	}
}

func nullsGoLast(column ColumnBuffer, i, j int, maxDefinitionLevel, definitionLevel1, definitionLevel2 int8) bool {
	return definitionLevel1 == maxDefinitionLevel && (definitionLevel2 != maxDefinitionLevel || column.Less(i, j))
}

type reversedColumnBuffer struct{ ColumnBuffer }

func (col *reversedColumnBuffer) Less(i, j int) bool { return col.ColumnBuffer.Less(j, i) }

type optionalColumnBuffer struct {
	base               ColumnBuffer
	maxDefinitionLevel int8
	rows               []int32
	index              []int32
	definitionLevels   []int8
	nullOrdering       nullOrdering
}

func newOptionalColumnBuffer(base ColumnBuffer, maxDefinitionLevel int8, nullOrdering nullOrdering) *optionalColumnBuffer {
	n := base.Cap()
	return &optionalColumnBuffer{
		base:               base,
		maxDefinitionLevel: maxDefinitionLevel,
		rows:               make([]int32, 0, n),
		definitionLevels:   make([]int8, 0, n),
		nullOrdering:       nullOrdering,
	}
}

func (col *optionalColumnBuffer) Clone() ColumnBuffer {
	return &optionalColumnBuffer{
		base:               col.base.Clone(),
		maxDefinitionLevel: col.maxDefinitionLevel,
		rows:               append([]int32{}, col.rows...),
		definitionLevels:   append([]int8{}, col.definitionLevels...),
		nullOrdering:       col.nullOrdering,
	}
}

func (col *optionalColumnBuffer) Dictionary() Dictionary {
	return col.base.Dictionary()
}

func (col *optionalColumnBuffer) Column() int {
	return col.base.Column()
}

func (col *optionalColumnBuffer) Pages() PageReader {
	return onePage(col.Page())
}

func (col *optionalColumnBuffer) Page() BufferedPage {
	numNulls := countLevelsNotEqual(col.definitionLevels, col.maxDefinitionLevel)
	numValues := len(col.rows) - numNulls

	if numValues > 0 {
		if cap(col.index) < numValues {
			col.index = make([]int32, numValues)
		}
		index := col.index[:numValues]
		i := 0
		for _, j := range col.rows {
			if j >= 0 {
				index[j] = int32(i)
				i++
			}
		}

		// Cyclic sort: O(N)
		for i := range index {
			for j := int(index[i]); i != j; j = int(index[i]) {
				col.base.Swap(i, j)
				index[i], index[j] = index[j], index[i]
			}
		}
	}

	i := 0
	for _, r := range col.rows {
		if r >= 0 {
			col.rows[i] = int32(i)
			i++
		}
	}

	return newOptionalPage(col.base.Page(), col.maxDefinitionLevel, col.definitionLevels)
}

func (col *optionalColumnBuffer) Reset() {
	col.base.Reset()
	col.rows = col.rows[:0]
	col.definitionLevels = col.definitionLevels[:0]
}

func (col *optionalColumnBuffer) Size() int64 {
	return sizeOfInt32(col.rows) + sizeOfInt32(col.index) + sizeOfInt8(col.definitionLevels) + col.base.Size()
}

func (col *optionalColumnBuffer) Cap() int { return cap(col.rows) }

func (col *optionalColumnBuffer) Len() int { return len(col.rows) }

func (col *optionalColumnBuffer) Less(i, j int) bool {
	return col.nullOrdering(
		col.base,
		int(col.rows[i]),
		int(col.rows[j]),
		col.maxDefinitionLevel,
		col.definitionLevels[i],
		col.definitionLevels[j],
	)
}

func (col *optionalColumnBuffer) Swap(i, j int) {
	col.rows[i], col.rows[j] = col.rows[j], col.rows[i]
	col.definitionLevels[i], col.definitionLevels[j] = col.definitionLevels[j], col.definitionLevels[i]
}

func (col *optionalColumnBuffer) WriteValues(values []Value) (n int, err error) {
	rowIndex := int32(col.base.Len())

	for n < len(values) {
		i := n
		for n < len(values) && values[n].definitionLevel != col.maxDefinitionLevel {
			n++
		}

		for _, v := range values[i:n] {
			col.rows = append(col.rows, rowIndex)
			col.definitionLevels = append(col.definitionLevels, v.definitionLevel)
			rowIndex++
		}

		i = n
		for n < len(values) && values[n].definitionLevel == col.maxDefinitionLevel {
			n++
		}

		if i < n {
			count, err := col.base.WriteValues(values[i:n])
			col.definitionLevels = appendLevel(col.definitionLevels, col.maxDefinitionLevel, count)
			n += count

			for count > 0 {
				col.rows = append(col.rows, rowIndex)
				rowIndex++
				count--
			}

			if err != nil {
				return n, err
			}
		}
	}

	return n, nil
}

func (col *optionalColumnBuffer) WriteRow(row Row) error {
	if len(row) == 0 {
		return errRowHasTooFewValues(len(row))
	}
	if len(row) > 1 {
		return errRowHasTooManyValues(len(row))
	}
	var idx = -1
	var err error
	if row[0].definitionLevel == col.maxDefinitionLevel {
		idx = col.base.Len()
		err = col.base.WriteRow(row)
	}
	if err == nil {
		col.rows = append(col.rows, int32(idx))
		col.definitionLevels = append(col.definitionLevels, row[0].definitionLevel)
	}
	return err
}

func (col *optionalColumnBuffer) ReadRowAt(row Row, index int) (Row, error) {
	if index < 0 {
		return row, errRowIndexOutOfBounds(index, len(col.definitionLevels))
	}
	if index >= len(col.definitionLevels) {
		return row, io.EOF
	}

	if definitionLevel := col.definitionLevels[index]; definitionLevel != col.maxDefinitionLevel {
		row = append(row, Value{definitionLevel: definitionLevel})
	} else {
		var err error
		var n = len(row)

		if row, err = col.base.ReadRowAt(row, int(col.rows[index])); err != nil {
			return row, err
		}

		for n < len(row) {
			row[n].definitionLevel = definitionLevel
			n++
		}
	}

	return row, nil
}

func (col *optionalColumnBuffer) Values() ValueReader {
	return &optionalPageReader{page: col.Page().(*optionalPage)}
}

type repeatedColumnBuffer struct {
	base               ColumnBuffer
	maxRepetitionLevel int8
	maxDefinitionLevel int8
	rows               []region
	repetitionLevels   []int8
	definitionLevels   []int8
	buffer             []Value
	reordering         *repeatedColumnBuffer
	nullOrdering       nullOrdering
}

type region struct {
	offset uint32
	length uint32
}

func sizeOfRegion(regions []region) int64 { return 8 * int64(len(regions)) }

func newRepeatedColumnBuffer(base ColumnBuffer, maxRepetitionLevel, maxDefinitionLevel int8, nullOrdering nullOrdering) *repeatedColumnBuffer {
	n := base.Cap()
	return &repeatedColumnBuffer{
		base:               base,
		maxRepetitionLevel: maxRepetitionLevel,
		maxDefinitionLevel: maxDefinitionLevel,
		rows:               make([]region, 0, n/8),
		repetitionLevels:   make([]int8, 0, n),
		definitionLevels:   make([]int8, 0, n),
		nullOrdering:       nullOrdering,
	}
}

func (col *repeatedColumnBuffer) Clone() ColumnBuffer {
	return &repeatedColumnBuffer{
		base:               col.base.Clone(),
		maxRepetitionLevel: col.maxRepetitionLevel,
		maxDefinitionLevel: col.maxDefinitionLevel,
		rows:               append([]region{}, col.rows...),
		repetitionLevels:   append([]int8{}, col.repetitionLevels...),
		definitionLevels:   append([]int8{}, col.definitionLevels...),
		nullOrdering:       col.nullOrdering,
	}
}

func (col *repeatedColumnBuffer) Dictionary() Dictionary {
	return col.base.Dictionary()
}

func (col *repeatedColumnBuffer) Column() int {
	return col.base.Column()
}

func (col *repeatedColumnBuffer) Pages() PageReader {
	return onePage(col.Page())
}

func (col *repeatedColumnBuffer) Page() BufferedPage {
	if rowsHaveBeenReordered(col.rows) {
		if col.reordering == nil {
			col.reordering = col.Clone().(*repeatedColumnBuffer)
		}

		maxLen := maxRowLengthOf(col.rows)
		if maxLen > uint32(cap(col.buffer)) {
			col.buffer = make([]Value, maxLen)
		}

		buffer := col.buffer[:maxLen]
		column := col.reordering
		column.Reset()

		for _, row := range col.rows {
			numNulls := countLevelsNotEqual(col.definitionLevels[row.offset:row.offset+row.length], col.maxDefinitionLevel)
			numValues := int(row.length) - numNulls

			for i := 0; i < numValues; i++ {
				var err error
				if buffer, err = col.base.ReadRowAt(buffer[:0], int(row.offset)+i); err != nil {
					return newErrorPage(col.Column(), "reordering rows of repeated column: %w", err)
				}
				if err = column.base.WriteRow(buffer); err != nil {
					return newErrorPage(col.Column(), "reordering rows of repeated column: %w", err)
				}
			}
		}

		for _, row := range col.rows {
			column.rows = append(column.rows, region{
				offset: uint32(len(column.repetitionLevels)),
				length: row.length,
			})
			column.repetitionLevels = append(column.repetitionLevels, col.repetitionLevels[row.offset:row.offset+row.length]...)
			column.definitionLevels = append(column.definitionLevels, col.definitionLevels[row.offset:row.offset+row.length]...)
		}

		col.swapReorderingBuffer(column)
	}

	return newRepeatedPage(
		col.base.Page(),
		col.maxRepetitionLevel,
		col.maxDefinitionLevel,
		col.repetitionLevels,
		col.definitionLevels,
	)
}

func (col *repeatedColumnBuffer) swapReorderingBuffer(buf *repeatedColumnBuffer) {
	col.base, buf.base = buf.base, col.base
	col.rows, buf.rows = buf.rows, col.rows
	col.repetitionLevels, buf.repetitionLevels = buf.repetitionLevels, col.repetitionLevels
	col.definitionLevels, buf.definitionLevels = buf.definitionLevels, col.definitionLevels
}

func (col *repeatedColumnBuffer) Reset() {
	col.base.Reset()
	col.rows = col.rows[:0]
	col.repetitionLevels = col.repetitionLevels[:0]
	col.definitionLevels = col.definitionLevels[:0]
}

func (col *repeatedColumnBuffer) Size() int64 {
	return sizeOfRegion(col.rows) + sizeOfInt8(col.repetitionLevels) + sizeOfInt8(col.definitionLevels) + col.base.Size()
}

func (col *repeatedColumnBuffer) Cap() int { return cap(col.rows) }

func (col *repeatedColumnBuffer) Len() int { return len(col.rows) }

func (col *repeatedColumnBuffer) Less(i, j int) bool {
	row1 := col.rows[i]
	row2 := col.rows[j]
	less := col.nullOrdering

	for k := uint32(0); k < row1.length && k < row2.length; k++ {
		x := int(row1.offset + k)
		y := int(row2.offset + k)
		definitionLevel1 := col.definitionLevels[j+int(k)]
		definitionLevel2 := col.definitionLevels[j+int(k)]
		switch {
		case less(col.base, x, y, col.maxDefinitionLevel, definitionLevel1, definitionLevel2):
			return true
		case less(col.base, y, x, col.maxDefinitionLevel, definitionLevel2, definitionLevel1):
			return false
		}
	}

	return row1.length < row2.length
}

func (col *repeatedColumnBuffer) Swap(i, j int) {
	col.rows[i], col.rows[j] = col.rows[j], col.rows[i]
}

func (col *repeatedColumnBuffer) WriteValues(values []Value) (n int, err error) {
	// The values may belong to the last row that was written if they do not
	// start with a repetition level less than the column's maximum.
	i := 0

	for i < len(values) && values[i].repetitionLevel == col.maxRepetitionLevel {
		i++
	}

	if i > 0 {
		row := values[:i]
		values = values[i:]
		lastRow := &col.rows[len(col.rows)-1]

		for i, v := range row {
			if v.definitionLevel == col.maxDefinitionLevel {
				if _, err := col.base.WriteValues(row[i : i+1]); err != nil {
					return n, err
				}
				n++
			}
			lastRow.length++
			col.repetitionLevels = append(col.repetitionLevels, v.repetitionLevel)
			col.definitionLevels = append(col.definitionLevels, v.definitionLevel)
		}
	}

	forEachRowOf(values, col.maxRepetitionLevel, func(row Row) bool {
		if err = col.WriteRow(row); err != nil {
			return false
		}
		n += len(row)
		return true
	})

	return n, err
}

func (col *repeatedColumnBuffer) WriteRow(row Row) error {
	if len(row) == 0 {
		return errRowHasTooFewValues(len(row))
	}

	col.rows = append(col.rows, region{
		offset: uint32(len(col.repetitionLevels)),
		length: uint32(len(row)),
	})

	for _, v := range row {
		col.repetitionLevels = append(col.repetitionLevels, v.repetitionLevel)
		col.definitionLevels = append(col.definitionLevels, v.definitionLevel)
	}

	for i, v := range row {
		if v.definitionLevel == col.maxDefinitionLevel {
			if err := col.base.WriteRow(row[i : i+1]); err != nil {
				// TODO: this is not transactional, the column may be left in
				// an undetermined state. Do we care?
				return err
			}
		}
	}

	return nil
}

func (col *repeatedColumnBuffer) ReadRowAt(row Row, index int) (Row, error) {
	if index < 0 {
		return row, errRowIndexOutOfBounds(index, len(col.rows))
	}
	if index >= len(col.rows) {
		return row, io.EOF
	}

	reset := len(row)
	region := col.rows[index]
	maxDefinitionLevel := col.maxDefinitionLevel
	repetitionLevels := col.repetitionLevels[region.offset : region.offset+region.length]
	definitionLevels := col.definitionLevels[region.offset : region.offset+region.length]
	baseIndex := 0

	for i := range definitionLevels {
		if definitionLevels[i] != maxDefinitionLevel {
			row = append(row, Value{
				repetitionLevel: repetitionLevels[i],
				definitionLevel: definitionLevels[i],
			})
		} else {
			var err error
			var n = len(row)

			if row, err = col.base.ReadRowAt(row, int(region.offset)+baseIndex); err != nil {
				return row[:reset], err
			}

			baseIndex += n
			for n < len(row) {
				row[n].repetitionLevel = repetitionLevels[i]
				row[n].definitionLevel = definitionLevels[i]
				n++
			}
		}
	}

	return row, nil
}

func (col *repeatedColumnBuffer) Values() ValueReader {
	return &repeatedPageReader{page: col.Page().(*repeatedPage)}
}

func (col *repeatedColumnBuffer) reorder() error {

	return nil
}

func rowsHaveBeenReordered(rows []region) bool {
	offset := uint32(0)
	for _, row := range rows {
		if row.offset != offset {
			return true
		}
		offset += row.length
	}
	return false
}

func maxRowLengthOf(rows []region) (maxLength uint32) {
	for _, row := range rows {
		if row.length > maxLength {
			maxLength = row.length
		}
	}
	return maxLength
}

type booleanColumnBuffer struct{ booleanPage }

func newBooleanColumnBuffer(columnIndex, bufferSize int) *booleanColumnBuffer {
	return &booleanColumnBuffer{
		booleanPage: booleanPage{
			values:      make([]bool, 0, bufferSize),
			columnIndex: ^int8(columnIndex),
		},
	}
}

func (col *booleanColumnBuffer) Clone() ColumnBuffer {
	return &booleanColumnBuffer{
		booleanPage: booleanPage{
			values:      append([]bool{}, col.values...),
			columnIndex: col.columnIndex,
		},
	}
}

func (col *booleanColumnBuffer) Dictionary() Dictionary { return nil }

func (col *booleanColumnBuffer) Pages() PageReader { return onePage(col.Page()) }

func (col *booleanColumnBuffer) Page() BufferedPage { return &col.booleanPage }

func (col *booleanColumnBuffer) Reset() { col.values = col.values[:0] }

func (col *booleanColumnBuffer) Cap() int { return cap(col.values) }

func (col *booleanColumnBuffer) Len() int { return len(col.values) }

func (col *booleanColumnBuffer) Less(i, j int) bool {
	return col.values[i] != col.values[j] && !col.values[i]
}

func (col *booleanColumnBuffer) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *booleanColumnBuffer) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Boolean())
	}
	return len(values), nil
}

func (col *booleanColumnBuffer) WriteRow(row Row) error {
	if len(row) == 0 {
		return errRowHasTooFewValues(len(row))
	}
	if len(row) > 1 {
		return errRowHasTooManyValues(len(row))
	}
	col.values = append(col.values, row[0].Boolean())
	return nil
}

func (col *booleanColumnBuffer) ReadRowAt(row Row, index int) (Row, error) {
	switch {
	case index < 0:
		return row, errRowIndexOutOfBounds(index, len(col.values))
	case index >= len(col.values):
		return row, io.EOF
	default:
		v := makeValueBoolean(col.values[index])
		v.columnIndex = col.columnIndex
		return append(row, v), nil
	}
}

type int32ColumnBuffer struct{ int32Page }

func newInt32ColumnBuffer(columnIndex, bufferSize int) *int32ColumnBuffer {
	return &int32ColumnBuffer{
		int32Page: int32Page{
			values:      make([]int32, 0, bufferSize/4),
			columnIndex: ^int8(columnIndex),
		},
	}
}

func (col *int32ColumnBuffer) Clone() ColumnBuffer {
	return &int32ColumnBuffer{
		int32Page: int32Page{
			values:      append([]int32{}, col.values...),
			columnIndex: col.columnIndex,
		},
	}
}

func (col *int32ColumnBuffer) Dictionary() Dictionary { return nil }

func (col *int32ColumnBuffer) Pages() PageReader { return onePage(col.Page()) }

func (col *int32ColumnBuffer) Page() BufferedPage { return &col.int32Page }

func (col *int32ColumnBuffer) Reset() { col.values = col.values[:0] }

func (col *int32ColumnBuffer) Cap() int { return cap(col.values) }

func (col *int32ColumnBuffer) Len() int { return len(col.values) }

func (col *int32ColumnBuffer) Less(i, j int) bool { return col.values[i] < col.values[j] }

func (col *int32ColumnBuffer) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *int32ColumnBuffer) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Int32())
	}
	return len(values), nil
}

func (col *int32ColumnBuffer) WriteRow(row Row) error {
	if len(row) == 0 {
		return errRowHasTooFewValues(len(row))
	}
	if len(row) > 1 {
		return errRowHasTooManyValues(len(row))
	}
	col.values = append(col.values, row[0].Int32())
	return nil
}

func (col *int32ColumnBuffer) ReadRowAt(row Row, index int) (Row, error) {
	switch {
	case index < 0:
		return row, errRowIndexOutOfBounds(index, len(col.values))
	case index >= len(col.values):
		return row, io.EOF
	default:
		v := makeValueInt32(col.values[index])
		v.columnIndex = col.columnIndex
		return append(row, v), nil
	}
}

type int64ColumnBuffer struct{ int64Page }

func newInt64ColumnBuffer(columnIndex, bufferSize int) *int64ColumnBuffer {
	return &int64ColumnBuffer{
		int64Page: int64Page{
			values:      make([]int64, 0, bufferSize/8),
			columnIndex: ^int8(columnIndex),
		},
	}
}

func (col *int64ColumnBuffer) Clone() ColumnBuffer {
	return &int64ColumnBuffer{
		int64Page: int64Page{
			values:      append([]int64{}, col.values...),
			columnIndex: col.columnIndex,
		},
	}
}

func (col *int64ColumnBuffer) Dictionary() Dictionary { return nil }

func (col *int64ColumnBuffer) Pages() PageReader { return onePage(col.Page()) }

func (col *int64ColumnBuffer) Page() BufferedPage { return &col.int64Page }

func (col *int64ColumnBuffer) Reset() { col.values = col.values[:0] }

func (col *int64ColumnBuffer) Cap() int { return cap(col.values) }

func (col *int64ColumnBuffer) Len() int { return len(col.values) }

func (col *int64ColumnBuffer) Less(i, j int) bool { return col.values[i] < col.values[j] }

func (col *int64ColumnBuffer) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *int64ColumnBuffer) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Int64())
	}
	return len(values), nil
}

func (col *int64ColumnBuffer) WriteRow(row Row) error {
	if len(row) == 0 {
		return errRowHasTooFewValues(len(row))
	}
	if len(row) > 1 {
		return errRowHasTooManyValues(len(row))
	}
	col.values = append(col.values, row[0].Int64())
	return nil
}

func (col *int64ColumnBuffer) ReadRowAt(row Row, index int) (Row, error) {
	switch {
	case index < 0:
		return row, errRowIndexOutOfBounds(index, len(col.values))
	case index >= len(col.values):
		return row, io.EOF
	default:
		v := makeValueInt64(col.values[index])
		v.columnIndex = col.columnIndex
		return append(row, v), nil
	}
}

type int96ColumnBuffer struct{ int96Page }

func newInt96ColumnBuffer(columnIndex, bufferSize int) *int96ColumnBuffer {
	return &int96ColumnBuffer{
		int96Page: int96Page{
			values:      make([]deprecated.Int96, 0, bufferSize/12),
			columnIndex: ^int8(columnIndex),
		},
	}
}

func (col *int96ColumnBuffer) Clone() ColumnBuffer {
	return &int96ColumnBuffer{
		int96Page: int96Page{
			values:      append([]deprecated.Int96{}, col.values...),
			columnIndex: col.columnIndex,
		},
	}
}

func (col *int96ColumnBuffer) Dictionary() Dictionary { return nil }

func (col *int96ColumnBuffer) Pages() PageReader { return onePage(col.Page()) }

func (col *int96ColumnBuffer) Page() BufferedPage { return &col.int96Page }

func (col *int96ColumnBuffer) Reset() { col.values = col.values[:0] }

func (col *int96ColumnBuffer) Cap() int { return cap(col.values) }

func (col *int96ColumnBuffer) Len() int { return len(col.values) }

func (col *int96ColumnBuffer) Less(i, j int) bool { return col.values[i].Less(col.values[j]) }

func (col *int96ColumnBuffer) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *int96ColumnBuffer) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Int96())
	}
	return len(values), nil
}

func (col *int96ColumnBuffer) WriteRow(row Row) error {
	if len(row) == 0 {
		return errRowHasTooFewValues(len(row))
	}
	if len(row) > 1 {
		return errRowHasTooManyValues(len(row))
	}
	col.values = append(col.values, row[0].Int96())
	return nil
}

func (col *int96ColumnBuffer) ReadRowAt(row Row, index int) (Row, error) {
	switch {
	case index < 0:
		return row, errRowIndexOutOfBounds(index, len(col.values))
	case index >= len(col.values):
		return row, io.EOF
	default:
		v := makeValueInt96(col.values[index])
		v.columnIndex = col.columnIndex
		return append(row, v), nil
	}
}

type floatColumnBuffer struct{ floatPage }

func newFloatColumnBuffer(columnIndex, bufferSize int) *floatColumnBuffer {
	return &floatColumnBuffer{
		floatPage: floatPage{
			values:      make([]float32, 0, bufferSize/4),
			columnIndex: ^int8(columnIndex),
		},
	}
}

func (col *floatColumnBuffer) Clone() ColumnBuffer {
	return &floatColumnBuffer{
		floatPage: floatPage{
			values:      append([]float32{}, col.values...),
			columnIndex: col.columnIndex,
		},
	}
}

func (col *floatColumnBuffer) Dictionary() Dictionary { return nil }

func (col *floatColumnBuffer) Pages() PageReader { return onePage(col.Page()) }

func (col *floatColumnBuffer) Page() BufferedPage { return &col.floatPage }

func (col *floatColumnBuffer) Reset() { col.values = col.values[:0] }

func (col *floatColumnBuffer) Cap() int { return cap(col.values) }

func (col *floatColumnBuffer) Len() int { return len(col.values) }

func (col *floatColumnBuffer) Less(i, j int) bool { return col.values[i] < col.values[j] }

func (col *floatColumnBuffer) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *floatColumnBuffer) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Float())
	}
	return len(values), nil
}

func (col *floatColumnBuffer) WriteRow(row Row) error {
	if len(row) == 0 {
		return errRowHasTooFewValues(len(row))
	}
	if len(row) > 1 {
		return errRowHasTooManyValues(len(row))
	}
	col.values = append(col.values, row[0].Float())
	return nil
}

func (col *floatColumnBuffer) ReadRowAt(row Row, index int) (Row, error) {
	switch {
	case index < 0:
		return row, errRowIndexOutOfBounds(index, len(col.values))
	case index >= len(col.values):
		return row, io.EOF
	default:
		v := makeValueFloat(col.values[index])
		v.columnIndex = col.columnIndex
		return append(row, v), nil
	}
}

type doubleColumnBuffer struct{ doublePage }

func newDoubleColumnBuffer(columnIndex, bufferSize int) *doubleColumnBuffer {
	return &doubleColumnBuffer{
		doublePage: doublePage{
			values:      make([]float64, 0, bufferSize/8),
			columnIndex: ^int8(columnIndex),
		},
	}
}

func (col *doubleColumnBuffer) Clone() ColumnBuffer {
	return &doubleColumnBuffer{
		doublePage: doublePage{
			values:      append([]float64{}, col.values...),
			columnIndex: col.columnIndex,
		},
	}
}

func (col *doubleColumnBuffer) Dictionary() Dictionary { return nil }

func (col *doubleColumnBuffer) Pages() PageReader { return onePage(col.Page()) }

func (col *doubleColumnBuffer) Page() BufferedPage { return &col.doublePage }

func (col *doubleColumnBuffer) Reset() { col.values = col.values[:0] }

func (col *doubleColumnBuffer) Cap() int { return cap(col.values) }

func (col *doubleColumnBuffer) Len() int { return len(col.values) }

func (col *doubleColumnBuffer) Less(i, j int) bool { return col.values[i] < col.values[j] }

func (col *doubleColumnBuffer) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *doubleColumnBuffer) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Double())
	}
	return len(values), nil
}

func (col *doubleColumnBuffer) WriteRow(row Row) error {
	if len(row) == 0 {
		return errRowHasTooFewValues(len(row))
	}
	if len(row) > 1 {
		return errRowHasTooManyValues(len(row))
	}
	col.values = append(col.values, row[0].Double())
	return nil
}

func (col *doubleColumnBuffer) ReadRowAt(row Row, index int) (Row, error) {
	switch {
	case index < 0:
		return row, errRowIndexOutOfBounds(index, len(col.values))
	case index >= len(col.values):
		return row, io.EOF
	default:
		v := makeValueDouble(col.values[index])
		v.columnIndex = col.columnIndex
		return append(row, v), nil
	}
}

type byteArrayColumnBuffer struct{ byteArrayPage }

func newByteArrayColumnBuffer(columnIndex, bufferSize int) *byteArrayColumnBuffer {
	return &byteArrayColumnBuffer{
		byteArrayPage: byteArrayPage{
			values:      encoding.MakeByteArrayList(bufferSize / 16),
			columnIndex: ^int8(columnIndex),
		},
	}
}

func (col *byteArrayColumnBuffer) Clone() ColumnBuffer {
	return &byteArrayColumnBuffer{
		byteArrayPage: byteArrayPage{
			values:      col.values.Clone(),
			columnIndex: col.columnIndex,
		},
	}
}

func (col *byteArrayColumnBuffer) Dictionary() Dictionary { return nil }

func (col *byteArrayColumnBuffer) Pages() PageReader { return onePage(col.Page()) }

func (col *byteArrayColumnBuffer) Page() BufferedPage { return &col.byteArrayPage }

func (col *byteArrayColumnBuffer) Reset() { col.values.Reset() }

func (col *byteArrayColumnBuffer) Cap() int { return col.values.Cap() }

func (col *byteArrayColumnBuffer) Len() int { return col.values.Len() }

func (col *byteArrayColumnBuffer) Less(i, j int) bool { return col.values.Less(i, j) }

func (col *byteArrayColumnBuffer) Swap(i, j int) { col.values.Swap(i, j) }

func (col *byteArrayColumnBuffer) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values.Push(v.ByteArray())
	}
	return len(values), nil
}

func (col *byteArrayColumnBuffer) WriteRow(row Row) error {
	if len(row) == 0 {
		return errRowHasTooFewValues(len(row))
	}
	if len(row) > 1 {
		return errRowHasTooManyValues(len(row))
	}
	col.values.Push(row[0].ByteArray())
	return nil
}

func (col *byteArrayColumnBuffer) ReadRowAt(row Row, index int) (Row, error) {
	switch {
	case index < 0:
		return row, errRowIndexOutOfBounds(index, col.values.Len())
	case index >= col.values.Len():
		return row, io.EOF
	default:
		v := makeValueBytes(ByteArray, col.values.Index(index))
		v.columnIndex = col.columnIndex
		return append(row, v), nil
	}
}

type fixedLenByteArrayColumnBuffer struct {
	fixedLenByteArrayPage
	tmp []byte
}

func newFixedLenByteArrayColumnBuffer(size, columnIndex, bufferSize int) *fixedLenByteArrayColumnBuffer {
	return &fixedLenByteArrayColumnBuffer{
		fixedLenByteArrayPage: fixedLenByteArrayPage{
			size:        size,
			data:        make([]byte, 0, bufferSize),
			columnIndex: ^int8(columnIndex),
		},
		tmp: make([]byte, size),
	}
}

func (col *fixedLenByteArrayColumnBuffer) Clone() ColumnBuffer {
	return &fixedLenByteArrayColumnBuffer{
		fixedLenByteArrayPage: fixedLenByteArrayPage{
			size:        col.size,
			data:        append([]byte{}, col.data...),
			columnIndex: col.columnIndex,
		},
		tmp: make([]byte, col.size),
	}
}

func (col *fixedLenByteArrayColumnBuffer) Dictionary() Dictionary { return nil }

func (col *fixedLenByteArrayColumnBuffer) Pages() PageReader { return onePage(col.Page()) }

func (col *fixedLenByteArrayColumnBuffer) Page() BufferedPage { return &col.fixedLenByteArrayPage }

func (col *fixedLenByteArrayColumnBuffer) Reset() { col.data = col.data[:0] }

func (col *fixedLenByteArrayColumnBuffer) Cap() int { return cap(col.data) / col.size }

func (col *fixedLenByteArrayColumnBuffer) Len() int { return len(col.data) / col.size }

func (col *fixedLenByteArrayColumnBuffer) Less(i, j int) bool {
	return bytes.Compare(col.index(i), col.index(j)) < 0
}

func (col *fixedLenByteArrayColumnBuffer) Swap(i, j int) {
	t, u, v := col.tmp[:col.size], col.index(i), col.index(j)
	copy(t, u)
	copy(u, v)
	copy(v, t)
}

func (col *fixedLenByteArrayColumnBuffer) index(i int) []byte {
	j := (i + 0) * col.size
	k := (i + 1) * col.size
	return col.data[j:k:k]
}

func (col *fixedLenByteArrayColumnBuffer) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.data = append(col.data, v.ByteArray()...)
	}
	return len(values), nil
}

func (col *fixedLenByteArrayColumnBuffer) WriteRow(row Row) error {
	if len(row) == 0 {
		return errRowHasTooFewValues(len(row))
	}
	if len(row) > 1 {
		return errRowHasTooManyValues(len(row))
	}
	col.data = append(col.data, row[0].ByteArray()...)
	return nil
}

func (col *fixedLenByteArrayColumnBuffer) ReadRowAt(row Row, index int) (Row, error) {
	i := (index + 0) * col.size
	j := (index + 1) * col.size
	switch {
	case i < 0:
		return row, errRowIndexOutOfBounds(index, col.Len())
	case j > len(col.data):
		return row, io.EOF
	default:
		v := makeValueBytes(FixedLenByteArray, col.data[i:j])
		v.columnIndex = col.columnIndex
		return append(row, v), nil
	}
}

type uint32ColumnBuffer struct{ *int32ColumnBuffer }

func newUint32ColumnBuffer(columnIndex, bufferSize int) uint32ColumnBuffer {
	return uint32ColumnBuffer{newInt32ColumnBuffer(columnIndex, bufferSize)}
}

func (col uint32ColumnBuffer) Page() BufferedPage {
	return uint32Page{&col.int32Page}
}

func (col uint32ColumnBuffer) Clone() ColumnBuffer {
	return uint32ColumnBuffer{col.int32ColumnBuffer.Clone().(*int32ColumnBuffer)}
}

func (col uint32ColumnBuffer) Less(i, j int) bool {
	return uint32(col.values[i]) < uint32(col.values[j])
}

type uint64ColumnBuffer struct{ *int64ColumnBuffer }

func newUint64ColumnBuffer(columnIndex, bufferSize int) uint64ColumnBuffer {
	return uint64ColumnBuffer{newInt64ColumnBuffer(columnIndex, bufferSize)}
}

func (col uint64ColumnBuffer) Clone() ColumnBuffer {
	return uint64ColumnBuffer{col.int64ColumnBuffer.Clone().(*int64ColumnBuffer)}
}

func (col uint64ColumnBuffer) Page() BufferedPage {
	return uint64Page{&col.int64Page}
}

func (col uint64ColumnBuffer) Less(i, j int) bool {
	return uint64(col.values[i]) < uint64(col.values[j])
}

var (
	_ sort.Interface = (ColumnBuffer)(nil)
)
