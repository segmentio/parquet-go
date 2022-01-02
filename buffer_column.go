package parquet

import (
	"bytes"
	"io"
	"sort"

	"github.com/segmentio/parquet/deprecated"
	"github.com/segmentio/parquet/encoding"
)

// BufferColumn is an interface representing columns of a row group.
//
// BufferColumn implements sort.Interface as a way to support reordering the
// rows that have been written to it.
type BufferColumn interface {
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
	// of vlaues into the buffer in contexts where the row information is
	// provided by the values because the repetition and definition levels
	// are set.
	ValueWriter

	// Returns a copy of the column. The returned copy shares no memory with
	// the original, mutations of either column will not modify the other.
	Clone() BufferColumn

	// Returns the column as a Page.
	Page() Page

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

type nullOrdering func(column BufferColumn, i, j int, maxDefinitionLevel, definitionLevel1, definitionLevel2 int8) bool

func nullsGoFirst(column BufferColumn, i, j int, maxDefinitionLevel, definitionLevel1, definitionLevel2 int8) bool {
	if definitionLevel1 != maxDefinitionLevel {
		return definitionLevel2 == maxDefinitionLevel
	} else {
		return definitionLevel2 == maxDefinitionLevel && column.Less(i, j)
	}
}

func nullsGoLast(column BufferColumn, i, j int, maxDefinitionLevel, definitionLevel1, definitionLevel2 int8) bool {
	return definitionLevel1 == maxDefinitionLevel && (definitionLevel2 != maxDefinitionLevel || column.Less(i, j))
}

type reversedBufferColumn struct{ BufferColumn }

func (col *reversedBufferColumn) Less(i, j int) bool { return col.BufferColumn.Less(j, i) }

type optionalBufferColumn struct {
	base               BufferColumn
	maxDefinitionLevel int8
	rows               []int32
	index              []int32
	definitionLevels   []int8
	nullOrdering       nullOrdering
}

func newOptionalBufferColumn(base BufferColumn, maxDefinitionLevel int8, nullOrdering nullOrdering) *optionalBufferColumn {
	n := base.Cap()
	return &optionalBufferColumn{
		base:               base,
		maxDefinitionLevel: maxDefinitionLevel,
		rows:               make([]int32, 0, n),
		definitionLevels:   make([]int8, 0, n),
		nullOrdering:       nullOrdering,
	}
}

func (col *optionalBufferColumn) Clone() BufferColumn {
	return &optionalBufferColumn{
		base:               col.base.Clone(),
		maxDefinitionLevel: col.maxDefinitionLevel,
		rows:               append([]int32{}, col.rows...),
		definitionLevels:   append([]int8{}, col.definitionLevels...),
		nullOrdering:       col.nullOrdering,
	}
}

func (col *optionalBufferColumn) Dictionary() Dictionary {
	return col.base.Dictionary()
}

func (col *optionalBufferColumn) ColumnIndex() int {
	return col.base.ColumnIndex()
}

func (col *optionalBufferColumn) Page() Page {
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

func (col *optionalBufferColumn) Reset() {
	col.base.Reset()
	col.rows = col.rows[:0]
	col.definitionLevels = col.definitionLevels[:0]
}

func (col *optionalBufferColumn) Size() int64 {
	return sizeOfInt32(col.rows) + sizeOfInt32(col.index) + sizeOfInt8(col.definitionLevels) + col.base.Size()
}

func (col *optionalBufferColumn) Cap() int { return cap(col.rows) }

func (col *optionalBufferColumn) Len() int { return len(col.rows) }

func (col *optionalBufferColumn) Less(i, j int) bool {
	return col.nullOrdering(
		col.base,
		int(col.rows[i]),
		int(col.rows[j]),
		col.maxDefinitionLevel,
		col.definitionLevels[i],
		col.definitionLevels[j],
	)
}

func (col *optionalBufferColumn) Swap(i, j int) {
	col.rows[i], col.rows[j] = col.rows[j], col.rows[i]
	col.definitionLevels[i], col.definitionLevels[j] = col.definitionLevels[j], col.definitionLevels[i]
}

func (col *optionalBufferColumn) WriteValues(values []Value) (n int, err error) {
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

func (col *optionalBufferColumn) WriteRow(row Row) error {
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

func (col *optionalBufferColumn) ReadRowAt(row Row, index int) (Row, error) {
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

func (col *optionalBufferColumn) Values() ValueReader {
	return &optionalPageReader{page: col.Page().(*optionalPage)}
}

type repeatedBufferColumn struct {
	base               BufferColumn
	maxRepetitionLevel int8
	maxDefinitionLevel int8
	rows               []region
	repetitionLevels   []int8
	definitionLevels   []int8
	buffer             []Value
	reordering         *repeatedBufferColumn
	nullOrdering       nullOrdering
}

type region struct {
	offset uint32
	length uint32
}

func sizeOfRegion(regions []region) int64 { return 8 * int64(len(regions)) }

func newRepeatedBufferColumn(base BufferColumn, maxRepetitionLevel, maxDefinitionLevel int8, nullOrdering nullOrdering) *repeatedBufferColumn {
	n := base.Cap()
	return &repeatedBufferColumn{
		base:               base,
		maxRepetitionLevel: maxRepetitionLevel,
		maxDefinitionLevel: maxDefinitionLevel,
		rows:               make([]region, 0, n/8),
		repetitionLevels:   make([]int8, 0, n),
		definitionLevels:   make([]int8, 0, n),
		nullOrdering:       nullOrdering,
	}
}

func (col *repeatedBufferColumn) Clone() BufferColumn {
	return &repeatedBufferColumn{
		base:               col.base.Clone(),
		maxRepetitionLevel: col.maxRepetitionLevel,
		maxDefinitionLevel: col.maxDefinitionLevel,
		rows:               append([]region{}, col.rows...),
		repetitionLevels:   append([]int8{}, col.repetitionLevels...),
		definitionLevels:   append([]int8{}, col.definitionLevels...),
		nullOrdering:       col.nullOrdering,
	}
}

func (col *repeatedBufferColumn) Dictionary() Dictionary {
	return col.base.Dictionary()
}

func (col *repeatedBufferColumn) ColumnIndex() int {
	return col.base.ColumnIndex()
}

func (col *repeatedBufferColumn) Page() Page {
	if rowsHaveBeenReordered(col.rows) {
		if col.reordering == nil {
			col.reordering = col.Clone().(*repeatedBufferColumn)
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
					return newErrorPage(col.ColumnIndex(), "reordering rows of repeated column: %w", err)
				}
				if err = column.base.WriteRow(buffer); err != nil {
					return newErrorPage(col.ColumnIndex(), "reordering rows of repeated column: %w", err)
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

func (col *repeatedBufferColumn) swapReorderingBuffer(buf *repeatedBufferColumn) {
	col.base, buf.base = buf.base, col.base
	col.rows, buf.rows = buf.rows, col.rows
	col.repetitionLevels, buf.repetitionLevels = buf.repetitionLevels, col.repetitionLevels
	col.definitionLevels, buf.definitionLevels = buf.definitionLevels, col.definitionLevels
}

func (col *repeatedBufferColumn) Reset() {
	col.base.Reset()
	col.rows = col.rows[:0]
	col.repetitionLevels = col.repetitionLevels[:0]
	col.definitionLevels = col.definitionLevels[:0]
}

func (col *repeatedBufferColumn) Size() int64 {
	return sizeOfRegion(col.rows) + sizeOfInt8(col.repetitionLevels) + sizeOfInt8(col.definitionLevels) + col.base.Size()
}

func (col *repeatedBufferColumn) Cap() int { return cap(col.rows) }

func (col *repeatedBufferColumn) Len() int { return len(col.rows) }

func (col *repeatedBufferColumn) Less(i, j int) bool {
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

func (col *repeatedBufferColumn) Swap(i, j int) {
	col.rows[i], col.rows[j] = col.rows[j], col.rows[i]
}

func (col *repeatedBufferColumn) WriteValues(values []Value) (n int, err error) {
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

func (col *repeatedBufferColumn) WriteRow(row Row) error {
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

func (col *repeatedBufferColumn) ReadRowAt(row Row, index int) (Row, error) {
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

func (col *repeatedBufferColumn) Values() ValueReader {
	return &repeatedPageReader{page: col.Page().(*repeatedPage)}
}

func (col *repeatedBufferColumn) reorder() error {

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

type booleanBufferColumn struct{ booleanPage }

func newBooleanBufferColumn(columnIndex, bufferSize int) *booleanBufferColumn {
	return &booleanBufferColumn{
		booleanPage: booleanPage{
			values:      make([]bool, 0, bufferSize),
			columnIndex: ^int8(columnIndex),
		},
	}
}

func (col *booleanBufferColumn) Clone() BufferColumn {
	return &booleanBufferColumn{
		booleanPage: booleanPage{
			values:      append([]bool{}, col.values...),
			columnIndex: col.columnIndex,
		},
	}
}

func (col *booleanBufferColumn) Dictionary() Dictionary { return nil }

func (col *booleanBufferColumn) Page() Page { return &col.booleanPage }

func (col *booleanBufferColumn) Reset() { col.values = col.values[:0] }

func (col *booleanBufferColumn) Cap() int { return cap(col.values) }

func (col *booleanBufferColumn) Len() int { return len(col.values) }

func (col *booleanBufferColumn) Less(i, j int) bool {
	return col.values[i] != col.values[j] && !col.values[i]
}

func (col *booleanBufferColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *booleanBufferColumn) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Boolean())
	}
	return len(values), nil
}

func (col *booleanBufferColumn) WriteRow(row Row) error {
	if len(row) == 0 {
		return errRowHasTooFewValues(len(row))
	}
	if len(row) > 1 {
		return errRowHasTooManyValues(len(row))
	}
	col.values = append(col.values, row[0].Boolean())
	return nil
}

func (col *booleanBufferColumn) ReadRowAt(row Row, index int) (Row, error) {
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

type int32BufferColumn struct{ int32Page }

func newInt32BufferColumn(columnIndex, bufferSize int) *int32BufferColumn {
	return &int32BufferColumn{
		int32Page: int32Page{
			values:      make([]int32, 0, bufferSize/4),
			columnIndex: ^int8(columnIndex),
		},
	}
}

func (col *int32BufferColumn) Clone() BufferColumn {
	return &int32BufferColumn{
		int32Page: int32Page{
			values:      append([]int32{}, col.values...),
			columnIndex: col.columnIndex,
		},
	}
}

func (col *int32BufferColumn) Dictionary() Dictionary { return nil }

func (col *int32BufferColumn) Page() Page { return &col.int32Page }

func (col *int32BufferColumn) Reset() { col.values = col.values[:0] }

func (col *int32BufferColumn) Cap() int { return cap(col.values) }

func (col *int32BufferColumn) Len() int { return len(col.values) }

func (col *int32BufferColumn) Less(i, j int) bool { return col.values[i] < col.values[j] }

func (col *int32BufferColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *int32BufferColumn) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Int32())
	}
	return len(values), nil
}

func (col *int32BufferColumn) WriteRow(row Row) error {
	if len(row) == 0 {
		return errRowHasTooFewValues(len(row))
	}
	if len(row) > 1 {
		return errRowHasTooManyValues(len(row))
	}
	col.values = append(col.values, row[0].Int32())
	return nil
}

func (col *int32BufferColumn) ReadRowAt(row Row, index int) (Row, error) {
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

type int64BufferColumn struct{ int64Page }

func newInt64BufferColumn(columnIndex, bufferSize int) *int64BufferColumn {
	return &int64BufferColumn{
		int64Page: int64Page{
			values:      make([]int64, 0, bufferSize/8),
			columnIndex: ^int8(columnIndex),
		},
	}
}

func (col *int64BufferColumn) Clone() BufferColumn {
	return &int64BufferColumn{
		int64Page: int64Page{
			values:      append([]int64{}, col.values...),
			columnIndex: col.columnIndex,
		},
	}
}

func (col *int64BufferColumn) Dictionary() Dictionary { return nil }

func (col *int64BufferColumn) Page() Page { return &col.int64Page }

func (col *int64BufferColumn) Reset() { col.values = col.values[:0] }

func (col *int64BufferColumn) Cap() int { return cap(col.values) }

func (col *int64BufferColumn) Len() int { return len(col.values) }

func (col *int64BufferColumn) Less(i, j int) bool { return col.values[i] < col.values[j] }

func (col *int64BufferColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *int64BufferColumn) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Int64())
	}
	return len(values), nil
}

func (col *int64BufferColumn) WriteRow(row Row) error {
	if len(row) == 0 {
		return errRowHasTooFewValues(len(row))
	}
	if len(row) > 1 {
		return errRowHasTooManyValues(len(row))
	}
	col.values = append(col.values, row[0].Int64())
	return nil
}

func (col *int64BufferColumn) ReadRowAt(row Row, index int) (Row, error) {
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

type int96BufferColumn struct{ int96Page }

func newInt96BufferColumn(columnIndex, bufferSize int) *int96BufferColumn {
	return &int96BufferColumn{
		int96Page: int96Page{
			values:      make([]deprecated.Int96, 0, bufferSize/12),
			columnIndex: ^int8(columnIndex),
		},
	}
}

func (col *int96BufferColumn) Clone() BufferColumn {
	return &int96BufferColumn{
		int96Page: int96Page{
			values:      append([]deprecated.Int96{}, col.values...),
			columnIndex: col.columnIndex,
		},
	}
}

func (col *int96BufferColumn) Dictionary() Dictionary { return nil }

func (col *int96BufferColumn) Page() Page { return &col.int96Page }

func (col *int96BufferColumn) Reset() { col.values = col.values[:0] }

func (col *int96BufferColumn) Cap() int { return cap(col.values) }

func (col *int96BufferColumn) Len() int { return len(col.values) }

func (col *int96BufferColumn) Less(i, j int) bool { return col.values[i].Less(col.values[j]) }

func (col *int96BufferColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *int96BufferColumn) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Int96())
	}
	return len(values), nil
}

func (col *int96BufferColumn) WriteRow(row Row) error {
	if len(row) == 0 {
		return errRowHasTooFewValues(len(row))
	}
	if len(row) > 1 {
		return errRowHasTooManyValues(len(row))
	}
	col.values = append(col.values, row[0].Int96())
	return nil
}

func (col *int96BufferColumn) ReadRowAt(row Row, index int) (Row, error) {
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

type floatBufferColumn struct{ floatPage }

func newFloatBufferColumn(columnIndex, bufferSize int) *floatBufferColumn {
	return &floatBufferColumn{
		floatPage: floatPage{
			values:      make([]float32, 0, bufferSize/4),
			columnIndex: ^int8(columnIndex),
		},
	}
}

func (col *floatBufferColumn) Clone() BufferColumn {
	return &floatBufferColumn{
		floatPage: floatPage{
			values:      append([]float32{}, col.values...),
			columnIndex: col.columnIndex,
		},
	}
}

func (col *floatBufferColumn) Dictionary() Dictionary { return nil }

func (col *floatBufferColumn) Page() Page { return &col.floatPage }

func (col *floatBufferColumn) Reset() { col.values = col.values[:0] }

func (col *floatBufferColumn) Cap() int { return cap(col.values) }

func (col *floatBufferColumn) Len() int { return len(col.values) }

func (col *floatBufferColumn) Less(i, j int) bool { return col.values[i] < col.values[j] }

func (col *floatBufferColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *floatBufferColumn) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Float())
	}
	return len(values), nil
}

func (col *floatBufferColumn) WriteRow(row Row) error {
	if len(row) == 0 {
		return errRowHasTooFewValues(len(row))
	}
	if len(row) > 1 {
		return errRowHasTooManyValues(len(row))
	}
	col.values = append(col.values, row[0].Float())
	return nil
}

func (col *floatBufferColumn) ReadRowAt(row Row, index int) (Row, error) {
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

type doubleBufferColumn struct{ doublePage }

func newDoubleBufferColumn(columnIndex, bufferSize int) *doubleBufferColumn {
	return &doubleBufferColumn{
		doublePage: doublePage{
			values:      make([]float64, 0, bufferSize/8),
			columnIndex: ^int8(columnIndex),
		},
	}
}

func (col *doubleBufferColumn) Clone() BufferColumn {
	return &doubleBufferColumn{
		doublePage: doublePage{
			values:      append([]float64{}, col.values...),
			columnIndex: col.columnIndex,
		},
	}
}

func (col *doubleBufferColumn) Dictionary() Dictionary { return nil }

func (col *doubleBufferColumn) Page() Page { return &col.doublePage }

func (col *doubleBufferColumn) Reset() { col.values = col.values[:0] }

func (col *doubleBufferColumn) Cap() int { return cap(col.values) }

func (col *doubleBufferColumn) Len() int { return len(col.values) }

func (col *doubleBufferColumn) Less(i, j int) bool { return col.values[i] < col.values[j] }

func (col *doubleBufferColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *doubleBufferColumn) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Double())
	}
	return len(values), nil
}

func (col *doubleBufferColumn) WriteRow(row Row) error {
	if len(row) == 0 {
		return errRowHasTooFewValues(len(row))
	}
	if len(row) > 1 {
		return errRowHasTooManyValues(len(row))
	}
	col.values = append(col.values, row[0].Double())
	return nil
}

func (col *doubleBufferColumn) ReadRowAt(row Row, index int) (Row, error) {
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

type byteArrayBufferColumn struct{ byteArrayPage }

func newByteArrayBufferColumn(columnIndex, bufferSize int) *byteArrayBufferColumn {
	return &byteArrayBufferColumn{
		byteArrayPage: byteArrayPage{
			values:      encoding.MakeByteArrayList(bufferSize / 16),
			columnIndex: ^int8(columnIndex),
		},
	}
}

func (col *byteArrayBufferColumn) Clone() BufferColumn {
	return &byteArrayBufferColumn{
		byteArrayPage: byteArrayPage{
			values:      col.values.Clone(),
			columnIndex: col.columnIndex,
		},
	}
}

func (col *byteArrayBufferColumn) Dictionary() Dictionary { return nil }

func (col *byteArrayBufferColumn) Page() Page { return &col.byteArrayPage }

func (col *byteArrayBufferColumn) Reset() { col.values.Reset() }

func (col *byteArrayBufferColumn) Cap() int { return col.values.Cap() }

func (col *byteArrayBufferColumn) Len() int { return col.values.Len() }

func (col *byteArrayBufferColumn) Less(i, j int) bool { return col.values.Less(i, j) }

func (col *byteArrayBufferColumn) Swap(i, j int) { col.values.Swap(i, j) }

func (col *byteArrayBufferColumn) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values.Push(v.ByteArray())
	}
	return len(values), nil
}

func (col *byteArrayBufferColumn) WriteRow(row Row) error {
	if len(row) == 0 {
		return errRowHasTooFewValues(len(row))
	}
	if len(row) > 1 {
		return errRowHasTooManyValues(len(row))
	}
	col.values.Push(row[0].ByteArray())
	return nil
}

func (col *byteArrayBufferColumn) ReadRowAt(row Row, index int) (Row, error) {
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

type fixedLenByteArrayBufferColumn struct {
	fixedLenByteArrayPage
	tmp []byte
}

func newFixedLenByteArrayBufferColumn(size, columnIndex, bufferSize int) *fixedLenByteArrayBufferColumn {
	return &fixedLenByteArrayBufferColumn{
		fixedLenByteArrayPage: fixedLenByteArrayPage{
			size:        size,
			data:        make([]byte, 0, bufferSize),
			columnIndex: ^int8(columnIndex),
		},
		tmp: make([]byte, size),
	}
}

func (col *fixedLenByteArrayBufferColumn) Clone() BufferColumn {
	return &fixedLenByteArrayBufferColumn{
		fixedLenByteArrayPage: fixedLenByteArrayPage{
			size:        col.size,
			data:        append([]byte{}, col.data...),
			columnIndex: col.columnIndex,
		},
		tmp: make([]byte, col.size),
	}
}

func (col *fixedLenByteArrayBufferColumn) Dictionary() Dictionary { return nil }

func (col *fixedLenByteArrayBufferColumn) Page() Page { return &col.fixedLenByteArrayPage }

func (col *fixedLenByteArrayBufferColumn) Reset() { col.data = col.data[:0] }

func (col *fixedLenByteArrayBufferColumn) Cap() int { return cap(col.data) / col.size }

func (col *fixedLenByteArrayBufferColumn) Len() int { return len(col.data) / col.size }

func (col *fixedLenByteArrayBufferColumn) Less(i, j int) bool {
	return bytes.Compare(col.index(i), col.index(j)) < 0
}

func (col *fixedLenByteArrayBufferColumn) Swap(i, j int) {
	t, u, v := col.tmp[:col.size], col.index(i), col.index(j)
	copy(t, u)
	copy(u, v)
	copy(v, t)
}

func (col *fixedLenByteArrayBufferColumn) index(i int) []byte {
	j := (i + 0) * col.size
	k := (i + 1) * col.size
	return col.data[j:k:k]
}

func (col *fixedLenByteArrayBufferColumn) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.data = append(col.data, v.ByteArray()...)
	}
	return len(values), nil
}

func (col *fixedLenByteArrayBufferColumn) WriteRow(row Row) error {
	if len(row) == 0 {
		return errRowHasTooFewValues(len(row))
	}
	if len(row) > 1 {
		return errRowHasTooManyValues(len(row))
	}
	col.data = append(col.data, row[0].ByteArray()...)
	return nil
}

func (col *fixedLenByteArrayBufferColumn) ReadRowAt(row Row, index int) (Row, error) {
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

type uint32BufferColumn struct{ *int32BufferColumn }

func newUint32BufferColumn(columnIndex, bufferSize int) uint32BufferColumn {
	return uint32BufferColumn{newInt32BufferColumn(columnIndex, bufferSize)}
}

func (col uint32BufferColumn) Page() Page {
	return uint32Page{&col.int32Page}
}

func (col uint32BufferColumn) Clone() BufferColumn {
	return uint32BufferColumn{col.int32BufferColumn.Clone().(*int32BufferColumn)}
}

func (col uint32BufferColumn) Less(i, j int) bool {
	return uint32(col.values[i]) < uint32(col.values[j])
}

type uint64BufferColumn struct{ *int64BufferColumn }

func newUint64BufferColumn(columnIndex, bufferSize int) uint64BufferColumn {
	return uint64BufferColumn{newInt64BufferColumn(columnIndex, bufferSize)}
}

func (col uint64BufferColumn) Clone() BufferColumn {
	return uint64BufferColumn{col.int64BufferColumn.Clone().(*int64BufferColumn)}
}

func (col uint64BufferColumn) Page() Page {
	return uint64Page{&col.int64Page}
}

func (col uint64BufferColumn) Less(i, j int) bool {
	return uint64(col.values[i]) < uint64(col.values[j])
}

var (
	_ sort.Interface = (BufferColumn)(nil)
)
