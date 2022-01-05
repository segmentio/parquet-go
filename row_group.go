package parquet

import (
	"container/heap"
	"fmt"
	"io"
)

// RowGroup is an interface representing a parquet row group.
type RowGroup interface {
	// Returns the number of rows in the group.
	NumRows() int

	// Returns the number of columns in the group.
	NumColumns() int

	// Returns the column at the given index in the group.
	Column(int) ColumnChunk

	// Returns the schema of rows in the group.
	Schema() *Schema

	// Returns the list of sorting columns describing how rows are sorted in the
	// group.
	//
	// The method will return an empty slice if the rows are not sorted.
	SortingColumns() []SortingColumn

	// Returns a reader exposing the rows of the row group.
	Rows() RowReader
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
	// is sorted or not. Please revisit this decision if this code path ends
	// up being more costly than necessary.
	for i, sorting := range sortingColumns {
		if path.equal(sorting.Path()) {
			return i
		}
	}
	return len(sortingColumns)
}

func sortingColumnsHavePrefix(sortingColumns, prefix []SortingColumn) bool {
	if len(sortingColumns) < len(prefix) {
		return false
	}
	for i, sortingColumn := range prefix {
		if !sortingColumnsAreEqual(sortingColumns[i], sortingColumn) {
			return false
		}
	}
	return true
}

func sortingColumnsAreEqual(s1, s2 SortingColumn) bool {
	path1 := columnPath(s1.Path())
	path2 := columnPath(s2.Path())
	return path1.equal(path2) && s1.Descending() == s2.Descending() && s1.NullsFirst() == s2.NullsFirst()
}

// MergeRowGroups constructs a row group which is a merged view of the row
// groups in the slice passed as first argument.
//
// The function performs validation to ensure that the merge operation is
// possible, ensuring that the schemas match or can be converted to an
// optionally configured target schema passed as argument to the option list.
//
// The sorting columns of each row group are also consulted to determine whether
// the output can be represented. If sorting columns are configured on the merge
// they must be a prefix of sorting columns of all row groups being merged.
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
				return nil, ErrRowGroupSchemaMismatch
			}
		}
	}

	if len(m.sorting) > 0 {
		for _, rowGroup := range rowGroups {
			if !sortingColumnsHavePrefix(rowGroup.SortingColumns(), m.sorting) {
				return nil, ErrRowGroupSortingColumnsMismatch
			}
		}

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

func (m *mergedRowGroup) NumColumns() int { return 0 } // TODO

func (m *mergedRowGroup) Column(int) ColumnChunk { return nil } // TODO

func (m *mergedRowGroup) Schema() *Schema { return m.schema }

func (m *mergedRowGroup) SortingColumns() []SortingColumn { return m.sorting }

func (m *mergedRowGroup) Rows() RowReader {
	if len(m.sortFuncs) == 0 {
		// When the row group has no ordering, use a simpler version of the
		// merger which simply concatenates rows from each of the row groups.
		// This is preferrable because it makes the output deterministic, the
		// heap merge may otherwise reorder rows across groups.
		return &concatenatedRowGroupRowReader{
			rowGroup:  m,
			schema:    m.schema,
			rowGroups: m.rowGroups,
		}
	} else {
		// The row group needs to respect a sorting order; the merged row reader
		// uses a heap to merge rows form the row groups.
		return &mergedRowGroupRowReader{
			rowGroup: m,
			schema:   m.schema,
		}
	}
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

type rowGroupRowReader struct {
	rowGroup RowGroup
	schema   *Schema
	readRow  columnReadRowFunc
	readers  []columnValueReader
}

func (r *rowGroupRowReader) Schema() *Schema {
	switch {
	case r.schema != nil:
		return r.schema
	case r.rowGroup != nil:
		return r.rowGroup.Schema()
	default:
		return nil
	}
}

func (r *rowGroupRowReader) ReadRow(row Row) (Row, error) {
	if r.rowGroup != nil {
		r.schema = r.rowGroup.Schema()
		r.readers = makeColumnValueReaders(r.rowGroup.NumColumns(), func(i int) reusablePageReader {
			return r.rowGroup.Column(i).Pages().(reusablePageReader)
		})
		r.rowGroup = nil
	}
	if r.schema == nil {
		return row, io.EOF
	}
	n := len(row)
	row, err := r.schema.readRow(row, 0, r.readers)
	if err == nil && len(row) == n {
		err = io.EOF
	}
	return row, err
}

func (r *rowGroupRowReader) WriteRowsTo(w RowWriter) (int64, error) {
	if r.rowGroup == nil {
		return 0, nil
	}
	defer func() {
		r.rowGroup = nil
	}()

	switch dst := w.(type) {
	case RowGroupWriter:
		return dst.WriteRowGroup(r.rowGroup)

	case PageWriter:
		for i, n := 0, r.rowGroup.NumColumns(); i < n; i++ {
			_, err := CopyPages(dst, r.rowGroup.Column(i).Pages())
			if err != nil {
				return 0, err
			}
		}
		return int64(r.rowGroup.NumRows()), nil
	}

	return CopyRows(w, struct{ RowReaderWithSchema }{r})
}

type concatenatedRowGroupRowReader struct {
	rowGroup  *mergedRowGroup
	schema    *Schema
	rowReader RowReader
	rowGroups []RowGroup
}

func (r *concatenatedRowGroupRowReader) Schema() *Schema {
	return r.schema
}

func (r *concatenatedRowGroupRowReader) ReadRow(row Row) (Row, error) {
	r.rowGroup = nil // disable WriteRowGroup optimization
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

func (r *concatenatedRowGroupRowReader) WriteRowsTo(w RowWriter) (int64, error) {
	if r.rowGroup != nil {
		if rgw, ok := w.(RowGroupWriter); ok {
			defer func() { r.rowGroup = nil }()
			return rgw.WriteRowGroup(r.rowGroup)
		}
	}
	return CopyRows(w, struct{ RowReaderWithSchema }{r})
}

type columnSortFunc struct {
	columnIndex int
	compare     sortFunc
}

type columnRowCursor struct {
	reader  RowReader
	row     Row
	err     error
	columns [][]Value
}

func (cur *columnRowCursor) next() bool {
	clearValues(cur.row)
	cur.row, cur.err = cur.reader.ReadRow(cur.row[:0])
	if cur.err != nil {
		return false
	}

	for i, c := range cur.columns {
		cur.columns[i] = c[:0]
	}

	for _, v := range cur.row {
		columnIndex := v.Column()
		cur.columns[columnIndex] = append(cur.columns[columnIndex], v)
	}

	return true
}

type mergedRowGroupRowReader struct {
	rowGroup *mergedRowGroup
	schema   *Schema
	sorting  []columnSortFunc
	cursors  []*columnRowCursor
	err      error
}

func (r *mergedRowGroupRowReader) init(m *mergedRowGroup) {
	if r.schema != nil {
		numColumns := numColumnsOf(r.schema)

		cursors := make([]columnRowCursor, len(m.rowGroups))
		for i, rowGroup := range m.rowGroups {
			cursors[i].reader = rowGroup.Rows()
			cursors[i].columns = make([][]Value, numColumns)
		}

		r.cursors = make([]*columnRowCursor, len(cursors))
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

func (r *mergedRowGroupRowReader) ReadRow(row Row) (Row, error) {
	if r.rowGroup != nil {
		r.init(r.rowGroup)
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

// func (r *mergedRowGroupRowReader) WriteRowsTo(w RowWriter) (int64, error) {
// 	if r.rowGroup != nil {
// 		if rgw, ok := w.(RowGroupWriter); ok {
// 			defer func() { r.rowGroup = nil }()
// 			return rgw.WriteRowGroup(r.rowGroup)
// 		}
// 	}
// 	return CopyRows(w, struct{ RowReader }{r})
// }

func (r *mergedRowGroupRowReader) Schema() *Schema {
	return r.schema
}

func (r *mergedRowGroupRowReader) Len() int {
	return len(r.cursors)
}

func (r *mergedRowGroupRowReader) Less(i, j int) bool {
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

func (r *mergedRowGroupRowReader) Swap(i, j int) {
	r.cursors[i], r.cursors[j] = r.cursors[j], r.cursors[i]
}

func (r *mergedRowGroupRowReader) Push(interface{}) {
	panic("BUG: unreachable")
}

func (r *mergedRowGroupRowReader) Pop() interface{} {
	n := len(r.cursors) - 1
	c := r.cursors[n]
	r.cursors = r.cursors[:n]
	return c
}

var (
	_ RowWriterTo         = (*rowGroupRowReader)(nil)
	_ RowReaderWithSchema = (*mergedRowGroupRowReader)(nil)
	_ RowReaderWithSchema = (*concatenatedRowGroupRowReader)(nil)
	_ RowWriterTo         = (*concatenatedRowGroupRowReader)(nil)
)
