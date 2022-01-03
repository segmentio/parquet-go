package parquet

import (
	"container/heap"
	"errors"
	"fmt"
	"io"
	"sort"
)

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

	// Returns a reader exposing the rows of the row group.
	Rows() RowReader

	// Returns a reader exposing the pages of the row group.
	Pages() PageReader
}

// The RowGroupColumn interface represents individual columns of a row group.
type RowGroupColumn interface {
	// Returns the index of this column in its parent row group.
	ColumnIndex() int

	// For indexed columns, returns the underlying dictionary holding the column
	// values. If the column is not indexed, nil is returned.
	Dictionary() Dictionary

	// Returns a reader exposing the pages of the column.
	Pages() PageReader
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

func (asc ascending) String() string   { return fmt.Sprintf("ascending(%s)", columnPath(asc)) }
func (asc ascending) Path() []string   { return asc }
func (asc ascending) Descending() bool { return false }
func (asc ascending) NullsFirst() bool { return false }

type descending []string

func (desc descending) String() string   { return fmt.Sprintf("descending(%s)", columnPath(desc)) }
func (desc descending) Path() []string   { return desc }
func (desc descending) Descending() bool { return true }
func (desc descending) NullsFirst() bool { return false }

type nullsFirst struct{ SortingColumn }

func (nf nullsFirst) String() string   { return fmt.Sprintf("nulls_first+%s", nf.SortingColumn) }
func (nf nullsFirst) NullsFirst() bool { return true }

func searchSortingColumn(sortingColumns []SortingColumn, path columnPath) int {
	// There are usually a few sorting columns in a row group, so the linear
	// scan is the fastest option and works whether the sorting column list
	// os sorted or not. Please revisit this decision if this code path ends
	// up being more costly than necessary.
	for i, sorting := range sortingColumns {
		if path.equal(sorting.Path()) {
			return i
		}
	}
	return len(sortingColumns)
}

type sortingColumnsOrder []SortingColumn

func (sorting sortingColumnsOrder) Len() int {
	return len(sorting)
}

func (sorting sortingColumnsOrder) Less(i, j int) bool {
	path1 := columnPath(sorting[i].Path())
	path2 := columnPath(sorting[j].Path())
	switch {
	case path1.less(path2):
		return true
	case path2.less(path2):
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
		if !columnPath(sortingColumns1[i].Path()).equal(sortingColumns2[i].Path()) {
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

var (
	errRowGroupSchemaMissing          = errors.New("cannot write rows to a row group which has no schema")
	errRowGroupSchemaMismatch         = errors.New("cannot write row groups with mismatching schemas")
	errRowGroupSortingColumnsMismatch = errors.New("cannot write row groups with mismatching sorting columns")
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
		m.sortFuncs = make([]columnSortFunc, len(m.sorting))
		forEachLeafColumnOf(m.schema, func(leaf leafColumn) {
			if sortingIndex := searchSortingColumn(m.sorting, leaf.path); sortingIndex < len(m.sorting) {
				m.sortFuncs[sortingIndex] = columnSortFunc{
					columnIndex: leaf.columnIndex,
					compare: sortFuncOf(
						leaf.node.Type(),
						leaf.maxRepetitionLevel,
						leaf.maxDefinitionLevel,
						m.sorting[sortingIndex].Descending(),
						m.sorting[sortingIndex].NullsFirst(),
					),
				}
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

func (m *mergedRowGroup) NumRows() (numRows int) {
	for _, rowGroup := range m.rowGroups {
		numRows += rowGroup.NumRows()
	}
	return numRows
}

func (m *mergedRowGroup) Columns() []RowGroupColumn { return nil } // TODO

func (m *mergedRowGroup) Schema() *Schema { return m.schema }

func (m *mergedRowGroup) SortingColumns() []SortingColumn { return m.sorting }

func (m *mergedRowGroup) Rows() RowReader {
	switch {
	case len(m.sortFuncs) != 0:
		// The row group needs to respect a sorting order; the merged row reader
		// uses a heap to merge rows fomr the row groups.
		return &mergedRowGroupReader{
			rowGroup: m,
			schema:   m.schema,
		}
	default:
		// When the row group has no ordering, use a simpler verison of the
		// merger which simply concatenates rows from each of the row groups.
		// This is preferrable because it makes the output deterministic, the
		// heap merge may otherwise reorder rows across groups.
		return &concatenatedRowGroupReader{
			rowGroup: m,
			schema:   m.schema,
		}
	}
}

func (m *mergedRowGroup) Pages() PageReader {
	switch {
	case len(m.sortFuncs) != 0:
		panic("NOT IMPLEMENTED") // TODO
	default:
		// When the row group has no ordering, simply expose the concatenation
		// of pages from each of the sub groups.
		r := &multiPageReader{
			readers: make([]PageReader, len(m.rowGroups)),
		}
		for i, g := range m.rowGroups {
			r.readers[i] = g.Pages()
		}
		return r
	}
}

type columnSortFunc struct {
	columnIndex int
	compare     sortFunc
}

type concatenatedRowGroupReader struct {
	rowGroup  *mergedRowGroup
	schema    *Schema
	rowReader RowReader
	rowGroups []RowGroup
}

func (r *concatenatedRowGroupReader) Schema() *Schema { return r.schema }

func (r *concatenatedRowGroupReader) ReadRow(row Row) (Row, error) {
	if m := r.rowGroup; m != nil {
		r.rowGroup = nil
		r.rowGroups = m.rowGroups
	}

	for {
		if r.rowReader != nil {
			var err error
			row, err = r.rowReader.ReadRow(row)
			if err == nil || err != io.EOF {
				return row, err
			}
			r.rowReader = nil
		}

		if len(r.rowGroups) == 0 {
			return row, io.EOF
		}

		r.rowReader = r.rowGroups[0].Rows()
		r.rowGroups = r.rowGroups[1:]
	}
}

func (r *concatenatedRowGroupReader) WriteRowsTo(w RowWriter) (int64, error) {
	if r.rowGroup != nil {
		if rgw, ok := w.(RowGroupWriter); ok {
			defer func() { r.rowGroup = nil }()
			return rgw.WriteRowGroup(r.rowGroup)
		}
	}
	return CopyRows(w, struct{ RowReader }{r})
}

type mergedRowGroupReader struct {
	rowGroup *mergedRowGroup
	schema   *Schema
	sorting  []columnSortFunc
	cursors  []*cursor
	err      error
}

func (r *mergedRowGroupReader) initReadRow(m *mergedRowGroup) {
	if r.schema != nil {
		numColumns := numColumnsOf(r.schema)

		cursors := make([]cursor, len(m.rowGroups))
		for i, rowGroup := range m.rowGroups {
			cursors[i].reader = rowGroup.Rows()
			cursors[i].columns = make([][]Value, numColumns)
		}

		r.cursors = make([]*cursor, len(cursors))
		for i := range cursors {
			r.cursors[i] = &cursors[i]
		}

		for _, cur := range r.cursors {
			cur.next()
		}

		i := 0
		for _, cur := range r.cursors {
			if cur.err != nil {
				if cur.err != io.EOF {
					r.err = cur.err
				}
			} else {
				r.cursors[i] = cur
				i++
			}
		}

		r.cursors = r.cursors[:i]
		r.sorting = m.sortFuncs
		heap.Init(r)
	}
}

func (r *mergedRowGroupReader) ReadRow(row Row) (Row, error) {
	if r.rowGroup != nil {
		r.initReadRow(r.rowGroup)
		r.rowGroup = nil
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

// func (r *mergedRowGroupReader) WriteRowsTo(w RowWriter) (int64, error) {
// 	if r.rowGroup != nil {
// 		if rgw, ok := w.(RowGroupWriter); ok {
// 			defer func() { r.rowGroup = nil }()
// 			return rgw.WriteRowGroup(r.rowGroup)
// 		}
// 	}
// 	return CopyRows(w, struct{ RowReader }{r})
// }

func (r *mergedRowGroupReader) Schema() *Schema { return r.schema }

func (r *mergedRowGroupReader) Len() int {
	return len(r.cursors)
}

func (r *mergedRowGroupReader) Less(i, j int) bool {
	columns1 := r.cursors[i].columns
	columns2 := r.cursors[j].columns

	for _, sorting := range r.sorting {
		col1 := columns1[sorting.columnIndex]
		col2 := columns2[sorting.columnIndex]
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

func (cur *cursor) next() bool {
	clearValues(cur.row)
	cur.row, cur.err = cur.reader.ReadRow(cur.row[:0])
	if cur.err != nil {
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
	return func(a, b []Value) int { return -sort(a, b) }
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
		n := len(a)
		if n < len(b) {
			n = len(b)
		}

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
	return func(a, b []Value) int { return t.Compare(a[0], b[0]) }
}

var (
	_ RowReaderWithSchema = (*concatenatedRowGroupReader)(nil)
	_ RowWriterTo         = (*concatenatedRowGroupReader)(nil)
	_ RowReaderWithSchema = (*mergedRowGroupReader)(nil)
)
