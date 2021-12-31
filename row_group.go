package parquet

import (
	"container/heap"
	"io"
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

func sortingColumnOf(sortingColumns []SortingColumn, path []string) SortingColumn {
	for _, sorting := range sortingColumns {
		if stringsAreEqual(sorting.Path(), path) {
			return sorting
		}
	}
	return nil
}

// RowGroup is an interface representing a parquet row group.
type RowGroup interface {
	// Returns the list of column that that row group has values for.
	Columns() []RowGroupColumn

	// Returns the number of rows in the group.
	NumRows() int

	// Returns the schema of rows in the group.
	Schema() *Schema

	// Returns the list of sorting columns describing how rows are sorted in the
	// group.
	//
	// The method will return an empty slice if the rows are not sorted.
	SortingColumns() []SortingColumn

	// Rows returns a reader exposing the rows of the row group.
	Rows() RowReader
}

// The RowGroupColumn interface represents individual columns of a row group.
type RowGroupColumn interface {
	// For indexed columns, returns the underlying dictionary holding the column
	// values. If the column is not indexed, nil is returned.
	Dictionary() Dictionary

	// Returns the list of pages in the colunn.
	Pages() []Page
}

// RowGroupReader is an interface implemented by types that expose sequences of
// row groups to the application.
type RowGroupReader interface {
	ReadRowGroup() (RowGroup, error)
}

// RowGroupWriter is an interface implemented by tyeps that allow the program
// to write row groups.
type RowGroupWriter interface {
	WriteRowGroup(RowGroup) (int64, error)
}

func MergeRowGroups(schema Node, rowGroups ...RowGroup) (RowGroup, error) {
	m := &mergedRowGroup{
		rowGroups: make([]RowGroup, len(rowGroups)),
	}
	copy(m.rowGroups, rowGroups)

	if m.schema, _ = schema.(*Schema); m.schema == nil {
		m.schema = NewSchema("", schema)
	}

	forEachLeafColumnOf(schema, func(leaf leafColumn) {
		if sorting := sortingColumnOf(m.sorting, leaf.path); sorting != nil {
			m.sortFuncs = append(m.sortFuncs, columnSortFunc{
				columnIndex: leaf.columnIndex,
				compare: sortFuncOf(
					leaf.node.Type(),
					leaf.maxRepetitionLevel,
					leaf.maxDefinitionLevel,
					sorting.Descending(),
					sorting.NullsFirst(),
				),
			})
		}
	})

	return m, nil
}

type mergedRowGroup struct {
	schema    *Schema
	rowGroups []RowGroup
	sorting   []SortingColumn
	sortFuncs []columnSortFunc
}

func (m *mergedRowGroup) Columns() []RowGroupColumn {
	panic("NOT IMPLEMENTED")
}

func (m *mergedRowGroup) NumRows() (numRows int) {
	for _, rowGroup := range m.rowGroups {
		numRows += rowGroup.NumRows()
	}
	return numRows
}

func (m *mergedRowGroup) Schema() *Schema { return m.schema }

func (m *mergedRowGroup) SortingColumns() []SortingColumn { return m.sorting }

func (m *mergedRowGroup) Rows() RowReader {
	r := &mergedRowGroupReader{
		schema:  m.schema,
		sorting: m.sortFuncs,
		cursors: make([]*cursor, len(m.rowGroups)),
	}

	cursors := make([]cursor, len(m.rowGroups))
	for i, rowGroup := range m.rowGroups {
		cursors[i].reader = rowGroup.Rows()
	}
	for i := range cursors {
		r.cursors[i] = &cursors[i]
	}

	return r
}

type columnSortFunc struct {
	columnIndex int
	compare     sortFunc
}

type mergedRowGroupReader struct {
	schema  *Schema
	sorting []columnSortFunc
	cursors []*cursor
	init    bool
	err     error
}

func (r *mergedRowGroupReader) initReadRow() {
	for _, cur := range r.cursors {
		cur.next()
	}

	i := 0
	for _, cur := range r.cursors {
		if cur.done() {
			if cur.err != io.EOF {
				r.err = cur.err
			}
		} else {
			r.cursors[i] = cur
			i++
		}
	}

	r.cursors = r.cursors[:i]
	heap.Init(r)
}

func (r *mergedRowGroupReader) ReadRow(row Row) (Row, error) {
	if !r.init {
		r.init = true
		r.initReadRow()
	}

	if r.err != nil {
		return row, r.err
	}

	if len(r.cursors) == 0 {
		return row, io.EOF
	}

	min := r.cursors[0]
	row = append(row, min.row...)

	if min.next() {
		heap.Fix(r, 0)
	} else {
		if err := min.err; err != io.EOF {
			r.err = err
			return row, err
		}
		heap.Pop(r)
	}

	return row, nil
}

func (r *mergedRowGroupReader) Schema() *Schema { return r.schema }

func (r *mergedRowGroupReader) Len() int { return len(r.cursors) }

func (r *mergedRowGroupReader) Less(i, j int) bool {
	for _, sorting := range r.sorting {
		col1 := r.cursors[i].columns[sorting.columnIndex]
		col2 := r.cursors[j].columns[sorting.columnIndex]
		comp := sorting.compare(col1, col2)
		switch {
		case comp < 0:
			return true
		case comp > 0:
			return false
		}
	}
	return false
}

func (r *mergedRowGroupReader) Swap(i, j int) {
	r.cursors[i], r.cursors[j] = r.cursors[j], r.cursors[i]
}

func (r *mergedRowGroupReader) Push(interface{}) {
	panic("NOT IMPLEMENTED")
}

func (r *mergedRowGroupReader) Pop() interface{} {
	n := len(r.cursors) - 1
	c := r.cursors[n]
	r.cursors = r.cursors[:n]
	return c
}

type cursor struct {
	reader  RowReader
	row     Row
	err     error
	columns [][]Value
}

func (cur *cursor) done() bool {
	return cur.err != nil
}

func (cur *cursor) next() bool {
	if cur.done() {
		return false
	}

	clearValues(cur.row)
	cur.row, cur.err = cur.reader.ReadRow(cur.row[:0])
	if cur.done() {
		return false
	}

	for i, c := range cur.columns {
		cur.columns[i] = c[:0]
	}

	for _, v := range cur.row {
		columnIndex := v.ColumnIndex()
		cur.columns[columnIndex] = append(cur.columns[columnIndex], v)
	}

	return true
}

type sortFunc func(a, b []Value) int

func sortFuncOf(t Type, maxDefinitionLevel, maxRepetitionLevel int8, descending, nullsFirst bool) (sort sortFunc) {
	switch {
	case maxRepetitionLevel > 0:
		sort = sortFuncOfRepeated(t, nullsFirst)
	case maxDefinitionLevel > 0:
		sort = sortFuncOfOptional(t, nullsFirst)
	default:
		sort = sortFuncOfRequired(t)
	}
	if descending {
		sort = sortFuncOfDescending(sort)
	}
	return sort
}

//go:noinline
func sortFuncOfDescending(sort sortFunc) sortFunc {
	return func(a, b []Value) int { return ^sort(a, b) }
}

func sortFuncOfOptional(t Type, nullsFirst bool) sortFunc {
	if nullsFirst {
		return sortFuncOfOptionalNullsFirst(t)
	} else {
		return sortFuncOfOptionalNullsLast(t)
	}
}

//go:noinline
func sortFuncOfOptionalNullsFirst(t Type) sortFunc {
	compare := sortFuncOfRequired(t)
	return func(a, b []Value) int {
		switch {
		case a[0].IsNull():
			if b[0].IsNull() {
				return 0
			}
			return -1
		case b[0].IsNull():
			return +1
		default:
			return compare(a, b)
		}
	}
}

//go:noinline
func sortFuncOfOptionalNullsLast(t Type) sortFunc {
	compare := sortFuncOfRequired(t)
	return func(a, b []Value) int {
		switch {
		case a[0].IsNull():
			if b[0].IsNull() {
				return 0
			}
			return +1
		case b[0].IsNull():
			return -1
		default:
			return compare(a, b)
		}
	}
}

//go:noinline
func sortFuncOfRepeated(t Type, nullsFirst bool) sortFunc {
	compare := sortFuncOfOptional(t, nullsFirst)
	return func(a, b []Value) int {
		n := min(len(a), len(b))

		for i := 0; i < n; i++ {
			k := compare(a[i:i+1], b[i:i+1])
			if k != 0 {
				return k
			}
		}

		return len(a) - len(b)
	}
}

//go:noinline
func sortFuncOfRequired(t Type) sortFunc {
	less := t.Less
	return func(a, b []Value) int {
		switch {
		case less(a[0], b[0]):
			return -1
		case less(b[0], a[0]):
			return +1
		default:
			return 0
		}
	}
}

var (
	_ RowReaderWithSchema = (*mergedRowGroupReader)(nil)
)
