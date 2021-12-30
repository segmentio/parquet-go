package parquet

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/segmentio/parquet/deprecated"
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
)

// SortingColumn represents a column by which a row group is sorted.
type SortingColumn interface {
	// Returns the path of the column in the row group schema, omitting the name
	// of the root node.
	Path() []string
	// Returns true if the column will sort values in descending order.
	Descending() bool
	// Returns true if the column will put null values at the beginning.
	NullsFirst() bool
}

// Ascending constructs a SortingColumn value which dictates to sort the column
// at the path given as argument in ascending order.
func Ascending(path ...string) SortingColumn { return ascending(path) }

// Descending constructs a SortingColumn value which dictates to sort the column
// at the path given as argument in descending order.
func Descending(path ...string) SortingColumn { return descending(path) }

// NullsFirst wraps the SortingColumn passed as argument so that it instructs
// the row group to place null values first in the column.
func NullsFirst(sortingColumn SortingColumn) SortingColumn { return nullsFirst{sortingColumn} }

type ascending []string

func (asc ascending) Path() []string   { return asc }
func (asc ascending) Descending() bool { return false }
func (asc ascending) NullsFirst() bool { return false }

type descending []string

func (desc descending) Path() []string   { return desc }
func (desc descending) Descending() bool { return true }
func (desc descending) NullsFirst() bool { return false }

type nullsFirst struct{ SortingColumn }

func (nullsFirst) NullsFirst() bool { return true }

// RowGroupColumn is an interface representing columns of a row group.
//
// RowGroupColumn implements sort.Interface as a way to support reordering the
// rows that have been written to it.
type RowGroupColumn interface {
	ValueWriter

	// Returns a copy of the column. The returned copy shares no memory with
	// the original, mutations of either column will not modify the other.
	Clone() RowGroupColumn

	// For indexed columns, returns the underlying dictionary holding the column
	// values. If the column is not indexed, nil is returned.
	Dictionary() Dictionary

	// Converts the column to a page, allowing the application to read the
	// values previously written to the column.
	//
	// The returned page shares the column memory, it remains valid until the
	// next call to Reset.
	//
	// After calling this method, the state of the column is undefined; the only
	// valid operation is calling Reset, which invalidates the page.
	Page() Page

	// Clears all rows written to the column.
	Reset()

	// Returns the size of the column (bytes).
	Size() int64

	// Returns the current capacity of the column (rows).
	Cap() int

	// Returns the number of rows currently written to the column.
	Len() int

	// Compares rows at index i and j and returns whether i < j.
	Less(i, j int) bool

	// Swaps rows at index i and j.
	Swap(i, j int)
}

// RowGroup represents an in-memory group of parquet rows.
//
// The main purpose of the RowGroup type is to provide a way to sort rows before
// writting them to a parquet file. RowGroup implements sort.Interface as a way
// to support reordering the rows that have been written to it.
type RowGroup struct {
	config  *RowGroupConfig
	schema  *Schema
	rowbuf  []Value
	colbuf  [][]Value
	columns []RowGroupColumn
	sorted  []RowGroupColumn
	sorting []format.SortingColumn
	numRows int
}

func NewRowGroup(options ...RowGroupOption) *RowGroup {
	config := DefaultRowGroupConfig()
	config.Apply(options...)
	if err := config.Validate(); err != nil {
		panic(err)
	}
	rg := &RowGroup{
		config: config,
	}
	if config.Schema != nil {
		rg.configure(config.Schema)
	}
	return rg
}

func (rg *RowGroup) configure(schema *Schema) {
	rg.schema = schema
	rg.init(schema, make([]string, 0, 10), 0, 0, rg.config)
	rg.rowbuf = make([]Value, 0, 10)
	rg.colbuf = make([][]Value, len(rg.columns))
	rg.sorted = make([]RowGroupColumn, len(rg.sorting))
	for i, sort := range rg.sorting {
		rg.sorted[i] = rg.columns[sort.ColumnIdx]
	}
}

func (rg *RowGroup) init(node Node, path []string, repetitionLevel, definitionLevel int8, config *RowGroupConfig) {
	switch {
	case node.Optional():
		definitionLevel++
	case node.Repeated():
		repetitionLevel++
		definitionLevel++
	}

	if isLeaf(node) {
		nullOrdering := nullsGoLast
		columnIndex := len(rg.columns)
		columnType := node.Type()
		bufferSize := config.ColumnBufferSize
		dictionary := (Dictionary)(nil)
		encoding, _ := encodingAndCompressionOf(node)

		if isDictionaryEncoding(encoding) {
			bufferSize /= 2
			dictionary = columnType.NewDictionary(bufferSize)
			columnType = dictionary.Type()
		}

		column := columnType.NewRowGroupColumn(bufferSize)

		for _, sorting := range config.SortingColumns {
			if stringsAreEqual(sorting.Path(), path) {
				sortingColumn := format.SortingColumn{
					ColumnIdx:  int32(columnIndex),
					Descending: sorting.Descending(),
					NullsFirst: sorting.NullsFirst(),
				}
				if sortingColumn.Descending {
					column = &reversedRowGroupColumn{column}
				}
				if sortingColumn.NullsFirst {
					nullOrdering = nullsGoFirst
				}
				rg.sorting = append(rg.sorting, sortingColumn)
				break
			}
		}

		switch {
		case repetitionLevel > 0:
			column = newRepeatedRowGroupColumn(column, repetitionLevel, definitionLevel, nullOrdering)
		case definitionLevel > 0:
			column = newOptionalRowGroupColumn(column, definitionLevel, nullOrdering)
		}

		rg.columns = append(rg.columns, column)
		return
	}

	i := len(path)
	path = append(path, "")

	for _, name := range node.ChildNames() {
		path[i] = name
		rg.init(node.ChildByName(name), path, repetitionLevel, definitionLevel, config)
	}
}

func (rg *RowGroup) Column(i int) RowGroupColumn {
	return rg.columns[i]
}

func (rg *RowGroup) Size() int64 {
	size := int64(0)
	for _, col := range rg.columns {
		size += col.Size()
	}
	return size
}

func (rg *RowGroup) Len() int { return rg.numRows }

func (rg *RowGroup) Less(i, j int) bool {
	for _, col := range rg.sorted {
		switch {
		case col.Less(i, j):
			return true
		case col.Less(j, i): // not equal?
			return false
		}
	}
	return false
}

func (rg *RowGroup) Swap(i, j int) {
	for _, col := range rg.columns {
		col.Swap(i, j)
	}
}

func (rg *RowGroup) Reset() {
	for _, col := range rg.columns {
		col.Reset()
	}
	rg.numRows = 0
}

func (rg *RowGroup) Write(row interface{}) error {
	if rg.schema == nil {
		rg.configure(SchemaOf(row))
	}
	defer func() {
		clearValues(rg.rowbuf)
	}()
	rg.rowbuf = rg.schema.Deconstruct(rg.rowbuf[:0], row)
	return rg.WriteRow(rg.rowbuf)
}

func (rg *RowGroup) WriteRow(row Row) error {
	defer func() {
		for i, colbuf := range rg.colbuf {
			clearValues(colbuf)
			rg.colbuf[i] = colbuf[:0]
		}
	}()

	for _, value := range row {
		columnIndex := value.ColumnIndex()
		rg.colbuf[columnIndex] = append(rg.colbuf[columnIndex], value)
	}

	for columnIndex, values := range rg.colbuf {
		if _, err := rg.columns[columnIndex].WriteValues(values); err != nil {
			return err
		}
	}

	rg.numRows++
	return nil
}

type nullOrdering func(RowGroupColumn, int, int, int8, []int8) bool

func nullsGoFirst(column RowGroupColumn, i, j int, maxDefinitionLevel int8, definitionLevels []int8) bool {
	if isNull(i, maxDefinitionLevel, definitionLevels) {
		return !isNull(j, maxDefinitionLevel, definitionLevels)
	} else {
		return !isNull(j, maxDefinitionLevel, definitionLevels) && column.Less(i, j)
	}
}

func nullsGoLast(column RowGroupColumn, i, j int, maxDefinitionLevel int8, definitionLevels []int8) bool {
	if isNull(i, maxDefinitionLevel, definitionLevels) {
		return false
	} else {
		return isNull(j, maxDefinitionLevel, definitionLevels) || column.Less(i, j)
	}
}

func isNull(i int, maxDefinitionLevel int8, definitionLevels []int8) bool {
	return definitionLevels[i] != maxDefinitionLevel
}

func rowGroupColumnPageWithoutNulls(column RowGroupColumn, maxDefinitionLevel int8, definitionLevels []int8) Page {
	n := 0
	for i := 0; i < len(definitionLevels); {
		j := i
		for j < len(definitionLevels) && isNull(j, maxDefinitionLevel, definitionLevels) {
			j++
		}
		if j < len(definitionLevels) {
			column.Swap(n, j)
			n++
		}
		i = j + 1
	}
	return column.Page().Slice(0, n)
}

type reversedRowGroupColumn struct{ RowGroupColumn }

func (col *reversedRowGroupColumn) Less(i, j int) bool { return col.RowGroupColumn.Less(j, i) }

type optionalRowGroupColumn struct {
	base               RowGroupColumn
	maxDefinitionLevel int8
	definitionLevels   []int8
	nullOrdering       nullOrdering
}

func newOptionalRowGroupColumn(base RowGroupColumn, maxDefinitionLevel int8, nullOrdering nullOrdering) *optionalRowGroupColumn {
	return &optionalRowGroupColumn{
		base:               base,
		maxDefinitionLevel: maxDefinitionLevel,
		definitionLevels:   make([]int8, 0, base.Cap()),
		nullOrdering:       nullOrdering,
	}
}

func (col *optionalRowGroupColumn) Clone() RowGroupColumn {
	return &optionalRowGroupColumn{
		base:               col.base.Clone(),
		maxDefinitionLevel: col.maxDefinitionLevel,
		definitionLevels:   append([]int8{}, col.definitionLevels...),
		nullOrdering:       col.nullOrdering,
	}
}

func (col *optionalRowGroupColumn) Dictionary() Dictionary {
	return col.base.Dictionary()
}

func (col *optionalRowGroupColumn) Page() Page {
	return newOptionalPage(
		rowGroupColumnPageWithoutNulls(col.base, col.maxDefinitionLevel, col.definitionLevels),
		col.maxDefinitionLevel,
		col.definitionLevels,
	)
}

func (col *optionalRowGroupColumn) Reset() {
	col.base.Reset()
	col.definitionLevels = col.definitionLevels[:0]
}

func (col *optionalRowGroupColumn) Size() int64 {
	return col.base.Size() + int64(len(col.definitionLevels))
}

func (col *optionalRowGroupColumn) Cap() int { return cap(col.definitionLevels) }

func (col *optionalRowGroupColumn) Len() int { return len(col.definitionLevels) }

func (col *optionalRowGroupColumn) Less(i, j int) bool {
	return col.nullOrdering(col.base, i, j, col.maxDefinitionLevel, col.definitionLevels)
}

func (col *optionalRowGroupColumn) Swap(i, j int) {
	col.base.Swap(i, j)
	col.definitionLevels[i], col.definitionLevels[j] = col.definitionLevels[j], col.definitionLevels[i]
}

func (col *optionalRowGroupColumn) WriteValues(values []Value) (int, error) {
	n, err := col.base.WriteValues(values)
	if err == nil {
		for _, v := range values {
			col.definitionLevels = append(col.definitionLevels, v.DefinitionLevel())
		}
	}
	return n, err
}

type repeatedRowGroupColumn struct {
	base               RowGroupColumn
	maxRepetitionLevel int8
	maxDefinitionLevel int8
	rows               []region
	repetitionLevels   []int8
	definitionLevels   []int8
	buffer             []Value
	reordering         *repeatedRowGroupColumn
	nullOrdering       nullOrdering
}

type region struct {
	offset uint32
	length uint32
}

func newRepeatedRowGroupColumn(base RowGroupColumn, maxRepetitionLevel, maxDefinitionLevel int8, nullOrdering nullOrdering) *repeatedRowGroupColumn {
	n := base.Cap()
	return &repeatedRowGroupColumn{
		base:               base,
		maxRepetitionLevel: maxRepetitionLevel,
		maxDefinitionLevel: maxDefinitionLevel,
		rows:               make([]region, 0, n/8),
		repetitionLevels:   make([]int8, 0, n),
		definitionLevels:   make([]int8, 0, n),
		nullOrdering:       nullOrdering,
	}
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

func (col *repeatedRowGroupColumn) Clone() RowGroupColumn {
	return &repeatedRowGroupColumn{
		base:               col.base.Clone(),
		maxRepetitionLevel: col.maxRepetitionLevel,
		maxDefinitionLevel: col.maxDefinitionLevel,
		rows:               append([]region{}, col.rows...),
		repetitionLevels:   append([]int8{}, col.repetitionLevels...),
		definitionLevels:   append([]int8{}, col.definitionLevels...),
		nullOrdering:       col.nullOrdering,
	}
}

func (col *repeatedRowGroupColumn) Dictionary() Dictionary {
	return col.base.Dictionary()
}

func (col *repeatedRowGroupColumn) Page() Page {
	base := col.base
	repetitionLevels := col.repetitionLevels
	definitionLevels := col.definitionLevels

	if rowsHaveBeenReordered(col.rows) {
		if col.reordering == nil {
			col.reordering = col.Clone().(*repeatedRowGroupColumn)
		}

		maxLen := maxRowLengthOf(col.rows)
		if maxLen > uint32(cap(col.buffer)) {
			col.buffer = make([]Value, maxLen)
		}

		column := col.reordering
		buffer := col.buffer[:maxLen]
		page := base.Page()
		column.Reset()

		for _, row := range col.rows {
			values := buffer[:row.length]
			n, err := page.ReadValuesAt(int(row.offset), values)
			if err != nil && n < len(values) {
				return &errorPage{err: fmt.Errorf("reordering values of repeated column: %w", err)}
			}
			if _, err := column.base.WriteValues(values); err != nil {
				return &errorPage{err: fmt.Errorf("reordering values of repeated column: %w", err)}
			}
		}

		for _, row := range col.rows {
			column.rows = append(column.rows, column.row(int(row.length)))
			column.repetitionLevels = append(column.repetitionLevels, col.repetitionLevels[row.offset:row.offset+row.length]...)
			column.definitionLevels = append(column.definitionLevels, col.definitionLevels[row.offset:row.offset+row.length]...)
		}

		base = column.base
		repetitionLevels = column.repetitionLevels
		definitionLevels = column.definitionLevels
	}

	return newRepeatedPage(
		rowGroupColumnPageWithoutNulls(base, col.maxDefinitionLevel, definitionLevels),
		col.maxRepetitionLevel,
		col.maxDefinitionLevel,
		repetitionLevels,
		definitionLevels,
	)
}

func (col *repeatedRowGroupColumn) Reset() {
	col.base.Reset()
	col.rows = col.rows[:0]
	col.repetitionLevels = col.repetitionLevels[:0]
	col.definitionLevels = col.definitionLevels[:0]
}

func (col *repeatedRowGroupColumn) Size() int64 {
	return 8*int64(len(col.rows)) + int64(len(col.repetitionLevels)) + int64(len(col.definitionLevels)) + col.base.Size()
}

func (col *repeatedRowGroupColumn) Cap() int { return cap(col.rows) }

func (col *repeatedRowGroupColumn) Len() int { return len(col.rows) }

func (col *repeatedRowGroupColumn) Less(i, j int) bool {
	row1 := col.rows[i]
	row2 := col.rows[j]
	less := col.nullOrdering

	for k := uint32(0); k < row1.length && k < row2.length; k++ {
		x := int(row1.offset + k)
		y := int(row2.offset + k)
		switch {
		case less(col.base, x, y, col.maxDefinitionLevel, col.definitionLevels):
			return true
		case less(col.base, y, x, col.maxDefinitionLevel, col.definitionLevels):
			return false
		}
	}

	return row1.length < row2.length
}

func (col *repeatedRowGroupColumn) Swap(i, j int) {
	col.rows[i], col.rows[j] = col.rows[j], col.rows[i]
}

func (col *repeatedRowGroupColumn) WriteValues(values []Value) (int, error) {
	n, err := col.base.WriteValues(values)
	if err == nil {
		col.rows = append(col.rows, col.row(n))
		for _, v := range values[:n] {
			col.repetitionLevels = append(col.repetitionLevels, v.RepetitionLevel())
			col.definitionLevels = append(col.definitionLevels, v.DefinitionLevel())
		}
	}
	return n, err
}

func (col *repeatedRowGroupColumn) row(n int) region {
	return region{
		offset: uint32(len(col.repetitionLevels)),
		length: uint32(n),
	}
}

type booleanRowGroupColumn struct{ booleanPage }

func newBooleanRowGroupColumn(bufferSize int) *booleanRowGroupColumn {
	return &booleanRowGroupColumn{
		booleanPage: booleanPage{
			values: make([]bool, 0, bufferSize),
		},
	}
}

func (col *booleanRowGroupColumn) Clone() RowGroupColumn {
	return &booleanRowGroupColumn{
		booleanPage: booleanPage{
			values: append([]bool{}, col.values...),
		},
	}
}

func (col *booleanRowGroupColumn) Dictionary() Dictionary { return nil }

func (col *booleanRowGroupColumn) Page() Page { return &col.booleanPage }

func (col *booleanRowGroupColumn) Reset() { col.values = col.values[:0] }

func (col *booleanRowGroupColumn) Size() int64 { return int64(len(col.values)) }

func (col *booleanRowGroupColumn) Cap() int { return cap(col.values) }

func (col *booleanRowGroupColumn) Len() int { return len(col.values) }

func (col *booleanRowGroupColumn) Less(i, j int) bool {
	return col.values[i] != col.values[j] && !col.values[i]
}

func (col *booleanRowGroupColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *booleanRowGroupColumn) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Boolean())
	}
	return len(values), nil
}

type int32RowGroupColumn struct{ int32Page }

func newInt32RowGroupColumn(bufferSize int) *int32RowGroupColumn {
	return &int32RowGroupColumn{
		int32Page: int32Page{
			values: make([]int32, 0, bufferSize/4),
		},
	}
}

func (col *int32RowGroupColumn) Clone() RowGroupColumn {
	return &int32RowGroupColumn{
		int32Page: int32Page{
			values: append([]int32{}, col.values...),
		},
	}
}

func (col *int32RowGroupColumn) Dictionary() Dictionary { return nil }

func (col *int32RowGroupColumn) Page() Page { return &col.int32Page }

func (col *int32RowGroupColumn) Reset() { col.values = col.values[:0] }

func (col *int32RowGroupColumn) Size() int64 { return 4 * int64(len(col.values)) }

func (col *int32RowGroupColumn) Cap() int { return cap(col.values) }

func (col *int32RowGroupColumn) Len() int { return len(col.values) }

func (col *int32RowGroupColumn) Less(i, j int) bool { return col.values[i] < col.values[j] }

func (col *int32RowGroupColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *int32RowGroupColumn) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Int32())
	}
	return len(values), nil
}

type int64RowGroupColumn struct{ int64Page }

func newInt64RowGroupColumn(bufferSize int) *int64RowGroupColumn {
	return &int64RowGroupColumn{
		int64Page: int64Page{
			values: make([]int64, 0, bufferSize/8),
		},
	}
}

func (col *int64RowGroupColumn) Clone() RowGroupColumn {
	return &int64RowGroupColumn{
		int64Page: int64Page{
			values: append([]int64{}, col.values...),
		},
	}
}

func (col *int64RowGroupColumn) Dictionary() Dictionary { return nil }

func (col *int64RowGroupColumn) Page() Page { return &col.int64Page }

func (col *int64RowGroupColumn) Reset() { col.values = col.values[:0] }

func (col *int64RowGroupColumn) Size() int64 { return 8 * int64(len(col.values)) }

func (col *int64RowGroupColumn) Cap() int { return cap(col.values) }

func (col *int64RowGroupColumn) Len() int { return len(col.values) }

func (col *int64RowGroupColumn) Less(i, j int) bool { return col.values[i] < col.values[j] }

func (col *int64RowGroupColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *int64RowGroupColumn) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Int64())
	}
	return len(values), nil
}

type int96RowGroupColumn struct{ int96Page }

func newInt96RowGroupColumn(bufferSize int) *int96RowGroupColumn {
	return &int96RowGroupColumn{
		int96Page: int96Page{
			values: make([]deprecated.Int96, 0, bufferSize/12),
		},
	}
}

func (col *int96RowGroupColumn) Clone() RowGroupColumn {
	return &int96RowGroupColumn{
		int96Page: int96Page{
			values: append([]deprecated.Int96{}, col.values...),
		},
	}
}

func (col *int96RowGroupColumn) Dictionary() Dictionary { return nil }

func (col *int96RowGroupColumn) Page() Page { return &col.int96Page }

func (col *int96RowGroupColumn) Reset() { col.values = col.values[:0] }

func (col *int96RowGroupColumn) Size() int64 { return 12 * int64(len(col.values)) }

func (col *int96RowGroupColumn) Cap() int { return cap(col.values) }

func (col *int96RowGroupColumn) Len() int { return len(col.values) }

func (col *int96RowGroupColumn) Less(i, j int) bool { return col.values[i].Less(col.values[j]) }

func (col *int96RowGroupColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *int96RowGroupColumn) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Int96())
	}
	return len(values), nil
}

type floatRowGroupColumn struct{ floatPage }

func newFloatRowGroupColumn(bufferSize int) *floatRowGroupColumn {
	return &floatRowGroupColumn{
		floatPage: floatPage{
			values: make([]float32, 0, bufferSize/4),
		},
	}
}

func (col *floatRowGroupColumn) Clone() RowGroupColumn {
	return &floatRowGroupColumn{
		floatPage: floatPage{
			values: append([]float32{}, col.values...),
		},
	}
}

func (col *floatRowGroupColumn) Dictionary() Dictionary { return nil }

func (col *floatRowGroupColumn) Page() Page { return &col.floatPage }

func (col *floatRowGroupColumn) Reset() { col.values = col.values[:0] }

func (col *floatRowGroupColumn) Size() int64 { return 4 * int64(len(col.values)) }

func (col *floatRowGroupColumn) Cap() int { return cap(col.values) }

func (col *floatRowGroupColumn) Len() int { return len(col.values) }

func (col *floatRowGroupColumn) Less(i, j int) bool { return col.values[i] < col.values[j] }

func (col *floatRowGroupColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *floatRowGroupColumn) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Float())
	}
	return len(values), nil
}

type doubleRowGroupColumn struct{ doublePage }

func newDoubleRowGroupColumn(bufferSize int) *doubleRowGroupColumn {
	return &doubleRowGroupColumn{
		doublePage: doublePage{
			values: make([]float64, 0, bufferSize/8),
		},
	}
}

func (col *doubleRowGroupColumn) Clone() RowGroupColumn {
	return &doubleRowGroupColumn{
		doublePage: doublePage{
			values: append([]float64{}, col.values...),
		},
	}
}

func (col *doubleRowGroupColumn) Dictionary() Dictionary { return nil }

func (col *doubleRowGroupColumn) Page() Page { return &col.doublePage }

func (col *doubleRowGroupColumn) Reset() { col.values = col.values[:0] }

func (col *doubleRowGroupColumn) Size() int64 { return 8 * int64(len(col.values)) }

func (col *doubleRowGroupColumn) Cap() int { return cap(col.values) }

func (col *doubleRowGroupColumn) Len() int { return len(col.values) }

func (col *doubleRowGroupColumn) Less(i, j int) bool { return col.values[i] < col.values[j] }

func (col *doubleRowGroupColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *doubleRowGroupColumn) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Double())
	}
	return len(values), nil
}

type byteArrayRowGroupColumn struct{ byteArrayPage }

func newByteArrayRowGroupColumn(bufferSize int) *byteArrayRowGroupColumn {
	return &byteArrayRowGroupColumn{
		byteArrayPage: byteArrayPage{
			values: encoding.MakeByteArrayList(bufferSize / 16),
		},
	}
}

func (col *byteArrayRowGroupColumn) Clone() RowGroupColumn {
	return &byteArrayRowGroupColumn{
		byteArrayPage: byteArrayPage{
			values: col.values.Clone(),
		},
	}
}

func (col *byteArrayRowGroupColumn) Dictionary() Dictionary { return nil }

func (col *byteArrayRowGroupColumn) Page() Page { return &col.byteArrayPage }

func (col *byteArrayRowGroupColumn) Reset() { col.values.Reset() }

func (col *byteArrayRowGroupColumn) Size() int64 { return col.values.Size() }

func (col *byteArrayRowGroupColumn) Cap() int { return col.values.Cap() }

func (col *byteArrayRowGroupColumn) Len() int { return col.values.Len() }

func (col *byteArrayRowGroupColumn) Less(i, j int) bool { return col.values.Less(i, j) }

func (col *byteArrayRowGroupColumn) Swap(i, j int) { col.values.Swap(i, j) }

func (col *byteArrayRowGroupColumn) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values.Push(v.ByteArray())
	}
	return len(values), nil
}

type fixedLenByteArrayRowGroupColumn struct {
	fixedLenByteArrayPage
	tmp []byte
}

func newFixedLenByteArrayRowGroupColumn(size, bufferSize int) *fixedLenByteArrayRowGroupColumn {
	return &fixedLenByteArrayRowGroupColumn{
		fixedLenByteArrayPage: fixedLenByteArrayPage{
			size: size,
			data: make([]byte, 0, bufferSize),
		},
		tmp: make([]byte, size),
	}
}

func (col *fixedLenByteArrayRowGroupColumn) Clone() RowGroupColumn {
	return &fixedLenByteArrayRowGroupColumn{
		fixedLenByteArrayPage: fixedLenByteArrayPage{
			size: col.size,
			data: append([]byte{}, col.data...),
		},
		tmp: make([]byte, col.size),
	}
}

func (col *fixedLenByteArrayRowGroupColumn) Dictionary() Dictionary { return nil }

func (col *fixedLenByteArrayRowGroupColumn) Page() Page { return &col.fixedLenByteArrayPage }

func (col *fixedLenByteArrayRowGroupColumn) Reset() { col.data = col.data[:0] }

func (col *fixedLenByteArrayRowGroupColumn) Size() int64 { return int64(len(col.data)) }

func (col *fixedLenByteArrayRowGroupColumn) Cap() int { return cap(col.data) / col.size }

func (col *fixedLenByteArrayRowGroupColumn) Len() int { return len(col.data) / col.size }

func (col *fixedLenByteArrayRowGroupColumn) Less(i, j int) bool {
	return bytes.Compare(col.index(i), col.index(j)) < 0
}

func (col *fixedLenByteArrayRowGroupColumn) Swap(i, j int) {
	t, u, v := col.tmp[:col.size], col.index(i), col.index(j)
	copy(t, u)
	copy(u, v)
	copy(v, t)
}

func (col *fixedLenByteArrayRowGroupColumn) index(i int) []byte {
	j := (i + 0) * col.size
	k := (i + 1) * col.size
	return col.data[j:k:k]
}

func (col *fixedLenByteArrayRowGroupColumn) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.data = append(col.data, v.ByteArray()...)
	}
	return len(values), nil
}

type uint32RowGroupColumn struct{ *int32RowGroupColumn }

func newUint32RowGroupColumn(bufferSize int) uint32RowGroupColumn {
	return uint32RowGroupColumn{newInt32RowGroupColumn(bufferSize)}
}

func (col uint32RowGroupColumn) Clone() RowGroupColumn {
	return uint32RowGroupColumn{col.int32RowGroupColumn.Clone().(*int32RowGroupColumn)}
}

func (col uint32RowGroupColumn) Page() Page {
	return uint32Page{&col.int32Page}
}

func (col uint32RowGroupColumn) Less(i, j int) bool {
	return uint32(col.values[i]) < uint32(col.values[j])
}

type uint64RowGroupColumn struct{ *int64RowGroupColumn }

func newUint64RowGroupColumn(bufferSize int) uint64RowGroupColumn {
	return uint64RowGroupColumn{newInt64RowGroupColumn(bufferSize)}
}

func (col uint64RowGroupColumn) Clone() RowGroupColumn {
	return uint64RowGroupColumn{col.int64RowGroupColumn.Clone().(*int64RowGroupColumn)}
}

func (col uint64RowGroupColumn) Page() Page {
	return uint64Page{&col.int64Page}
}

func (col uint64RowGroupColumn) Less(i, j int) bool {
	return uint64(col.values[i]) < uint64(col.values[j])
}

var (
	_ sort.Interface = (*RowGroup)(nil)
	_ sort.Interface = (RowGroupColumn)(nil)
)
