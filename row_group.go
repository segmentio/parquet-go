package parquet

import (
	"container/heap"
	"errors"
	"fmt"
	"io"
	"sort"
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

type sortingColumnsOrder []SortingColumn

func (sorting sortingColumnsOrder) Len() int {
	return len(sorting)
}

func (sorting sortingColumnsOrder) Less(i, j int) bool {
	path1 := sorting[i].Path()
	path2 := sorting[j].Path()
	switch {
	case stringsAreOrdered(path1, path2):
		return true
	case stringsAreOrdered(path2, path2):
		return false
	}
	desc1 := sorting[i].Descending()
	desc2 := sorting[j].Descending()
	if desc1 != desc2 {
		return !desc1
	}
	null1 := sorting[i].NullsFirst()
	null2 := sorting[j].NullsFirst()
	return null1 != null2 && !null1
}

func (sorting sortingColumnsOrder) Swap(i, j int) {
	sorting[i], sorting[j] = sorting[i], sorting[i]
}

func sortedSortingColumns(sortingColumns []SortingColumn) []SortingColumn {
	if !sortingColumnsAreSorted(sortingColumns) {
		sortedSortingColumns := make([]SortingColumn, len(sortingColumns))
		copy(sortedSortingColumns, sortingColumns)
		sort.Sort(sortingColumnsOrder(sortedSortingColumns))
		sortingColumns = sortedSortingColumns
	}
	return sortingColumns
}

func sortingColumnsAreSorted(sortingColumns []SortingColumn) bool {
	return sort.IsSorted(sortingColumnsOrder(sortingColumns))
}

func sortingColumnsAreEqual(sortingColumns1, sortingColumns2 []SortingColumn) bool {
	if len(sortingColumns1) != len(sortingColumns2) {
		return false
	}

	sortingColumns1 = sortedSortingColumns(sortingColumns1)
	sortingColumns2 = sortedSortingColumns(sortingColumns2)

	for i := range sortingColumns1 {
		if !stringsAreEqual(sortingColumns1[i].Path(), sortingColumns2[i].Path()) {
			return false
		}
		if sortingColumns1[i].Descending() != sortingColumns2[i].Descending() {
			return false
		}
		if sortingColumns1[i].NullsFirst() != sortingColumns2[i].NullsFirst() {
			return false
		}
	}

	return true
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

var (
	errRowGroupSchemaMismatch         = errors.New("cannot merge row groups with mismatching schemas")
	errRowGroupSortingColumnsMismatch = errors.New("cannot merge row groups with mismatching sorting columns")
)

func MergeRowGroups(rowGroups []RowGroup, options ...RowGroupOption) (RowGroup, error) {
	config := DefaultRowGroupConfig()
	config.Apply(options...)
	if err := config.Validate(); err != nil {
		return nil, err
	}

	m := &mergedRowGroup{
		schema:  config.Schema,
		sorting: config.SortingColumns,
	}

	if len(rowGroups) == 0 {
		return m, nil
	}

	if m.schema == nil {
		m.schema = rowGroups[0].Schema()

		for _, rowGroup := range rowGroups[1:] {
			if !nodesAreEqual(m.schema, rowGroup.Schema()) {
				return nil, errRowGroupSchemaMismatch
			}
		}
	}

	if m.sorting == nil {
		m.sorting = rowGroups[0].SortingColumns()

		for _, rowGroup := range rowGroups[1:] {
			if !sortingColumnsAreEqual(m.sorting, rowGroup.SortingColumns()) {
				return nil, errRowGroupSortingColumnsMismatch
			}
		}
	}

	if len(m.sorting) > 0 {
		forEachLeafColumnOf(m.schema, func(leaf leafColumn) {
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
	}

	m.rowGroups = make([]RowGroup, len(rowGroups))
	copy(m.rowGroups, rowGroups)

	for i, rowGroup := range m.rowGroups {
		if schema := rowGroup.Schema(); !nodesAreEqual(m.schema, schema) {
			conv, err := Convert(m.schema, schema)
			if err != nil {
				return nil, fmt.Errorf("cannot merge row groups: %w", err)
			}
			m.rowGroups[i] = ConvertRowGroup(rowGroup, conv)
		}
	}

	return m, nil
}

type mergedRowGroup struct {
	schema    *Schema
	sorting   []SortingColumn
	sortFuncs []columnSortFunc
	rowGroups []RowGroup
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
