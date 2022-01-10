package parquet

import (
	"container/heap"
	"fmt"
	"io"
)

// The ColumnChunk interface represents individual columns of a row group.
type ColumnChunk interface {
	// Returns the index of this column in its parent row group.
	Column() int

	// Returns a reader exposing the pages of the column.
	Pages() PageReader

	// Returns the components of the page index for this column chunk,
	// containing details about the content and location of pages within the
	// chunk.
	//
	// Note that the returned value may be the same across calls to these
	// methods, programs must treat those as read-only.
	//
	// If the column chunk does not have a page index, the methods return nil.
	ColumnIndex() *ColumnIndex
	OffsetIndex() *OffsetIndex
}

// RowGroup is an interface representing a parquet row group. From the Parquet
// docs, a RowGroup is "a logical horizontal partitioning of the data into rows.
// There is no physical structure that is guaranteed for a row group. A row
// group consists of a column chunk for each column in the dataset."
//
// https://github.com/apache/parquet-format#glossary
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

// RowGroupWriter is an interface implemented by types that allow the program
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

// MergeRowGroups constructs a row group which is a merged view of rowGroups. If
// rowGroups are sorted and the passed options include sorting, the merged row
// group will also be sorted.
//
// The function validates the input to ensure that the merge operation is
// possible, ensuring that the schemas match or can be converted to an
// optionally configured target schema passed as argument in the option list.
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

type rowGroupRowReader struct {
	rowGroup RowGroup
	schema   *Schema
	columns  []columnValueReader
}

func (r *rowGroupRowReader) init(rowGroup RowGroup) {
	r.schema = rowGroup.Schema()
	r.columns = makeColumnValueReaders(rowGroup.NumColumns(), func(i int) PageReader {
		return rowGroup.Column(i).Pages()
	})
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
		r.init(r.rowGroup)
		r.rowGroup = nil
	}
	if r.schema == nil {
		return row, io.EOF
	}
	n := len(row)
	row, err := r.schema.readRow(row, 0, r.columns)
	if err == nil && len(row) == n {
		err = io.EOF
	}
	return row, err
}

func (r *rowGroupRowReader) WriteRowsTo(w RowWriter) (int64, error) {
	if r.rowGroup == nil {
		return CopyRows(w, struct{ RowReaderWithSchema }{r})
	}
	defer func() { r.rowGroup = nil }()

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

func (r *rowGroupRowReader) writeRowsTo(w pageAndValueWriter, limit int64) (numRows int64, err error) {
	for i := range r.columns {
		n, err := r.columns[i].writeRowsTo(w, limit)
		if err != nil {
			return numRows, err
		}
		if i == 0 {
			numRows = n
		} else if numRows != n {
			return numRows, fmt.Errorf("column %d wrote %d rows but the previous column(s) wrote %d rows", i, n, numRows)
		}
	}
	return numRows, nil
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

type mergedRowGroupRowReader struct {
	rowGroup *mergedRowGroup
	schema   *Schema
	sorting  []columnSortFunc
	cursors  []rowGroupCursor
	values1  []Value
	values2  []Value
	err      error
}

func (r *mergedRowGroupRowReader) init(m *mergedRowGroup) {
	if r.schema != nil {
		numColumns := numColumnsOf(r.schema)
		cursors := make([]bufferedRowGroupCursor, len(m.rowGroups))
		buffers := make([][]Value, numColumns*len(m.rowGroups))

		for i, rowGroup := range m.rowGroups {
			cursors[i].reader = rowGroup.Rows()
			cursors[i].columns, buffers = buffers[:numColumns:numColumns], buffers[numColumns:]
		}

		r.cursors = make([]rowGroupCursor, 0, len(cursors))
		r.sorting = m.sortFuncs

		for i := range cursors {
			c := rowGroupCursor(&cursors[i])
			// TODO: this is a bit of a weak model, it only works with types
			// declared in this package; we may want to define an API to allow
			// applications to participate in it.
			if rd, ok := cursors[i].reader.(*rowGroupRowReader); ok {
				rd.init(rd.rowGroup)
				rd.rowGroup = nil
				// TODO: this optimization is disabled for now, there are
				// remaining blockers:
				//
				// * The optimized merge of pges for non-overlapping ranges is
				//   not yet implemented in the mergedRowGroupRowReader type.
				//
				// * Using pages min/max to determine overlapping ranges does
				//   not work for repeated columns; sorting by repeated columns
				//   seems to be an edge case so probably not worth optimizing,
				//   we still need to find a way to disable the optimization in
				//   those cases.
				//
				//c = optimizedRowGroupCursor{rd}
			}

			if err := c.readNext(); err != nil {
				if err == io.EOF {
					continue
				}
				r.err = err
				return
			}

			r.cursors = append(r.cursors, c)
		}

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
	row, err := min.readRow(row)
	if err != nil {
		return row, err
	}

	if err := min.readNext(); err != nil {
		if err != io.EOF {
			r.err = err
			return row, err
		}
		heap.Pop(r)
	} else {
		heap.Fix(r, 0)
	}
	return row, nil
}

// func (r *mergedRowGroupRowReader) WriteRowsTo(w RowWriter) (int64, error) {
// 	if r.rowGroup != nil {
// 		defer func() { r.rowGroup = nil }()
// 		switch dst := w.(type) {
// 		case RowGroupWriter:
// 			return dst.WriteRowGroup(r.rowGroup)
// 		case pageAndValueWriter:
// 			r.init(r.rowGroup)
// 			return r.writeRowsTo(dst)
// 		}
// 	}
// 	return CopyRows(w, struct{ RowReaderWithSchema }{r})
// }

func (r *mergedRowGroupRowReader) writeRowsTo(w pageAndValueWriter) (numRows int64, err error) {
	// TODO: the intent of this method is to optimize the merge of rows by
	// copying entire pages instead of individual rows when we detect ranges
	// that don't overlap.
	return
}

func (r *mergedRowGroupRowReader) Schema() *Schema {
	return r.schema
}

func (r *mergedRowGroupRowReader) Len() int {
	return len(r.cursors)
}

func (r *mergedRowGroupRowReader) Less(i, j int) bool {
	cursor1 := r.cursors[i]
	cursor2 := r.cursors[j]

	for _, sorting := range r.sorting {
		r.values1 = cursor1.nextRowValuesOf(r.values1[:0], sorting.columnIndex)
		r.values2 = cursor2.nextRowValuesOf(r.values2[:0], sorting.columnIndex)
		comp := sorting.compare(r.values1, r.values2)
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

type rowGroupCursor interface {
	readRow(Row) (Row, error)
	readNext() error
	nextRowValuesOf([]Value, int) []Value
}

type columnSortFunc struct {
	columnIndex int
	compare     sortFunc
}

type bufferedRowGroupCursor struct {
	reader  RowReader
	rowbuf  Row
	columns [][]Value
}

func (cur *bufferedRowGroupCursor) readRow(row Row) (Row, error) {
	return append(row, cur.rowbuf...), nil
}

func (cur *bufferedRowGroupCursor) readNext() (err error) {
	cur.rowbuf, err = cur.reader.ReadRow(cur.rowbuf[:0])
	if err != nil {
		return err
	}
	for i, c := range cur.columns {
		cur.columns[i] = c[:0]
	}
	for _, v := range cur.rowbuf {
		columnIndex := v.Column()
		cur.columns[columnIndex] = append(cur.columns[columnIndex], v)
	}
	return nil
}

func (cur *bufferedRowGroupCursor) nextRowValuesOf(values []Value, columnIndex int) []Value {
	return append(values, cur.columns[columnIndex]...)
}

type optimizedRowGroupCursor struct{ *rowGroupRowReader }

func (cur optimizedRowGroupCursor) readRow(row Row) (Row, error) { return cur.ReadRow(row) }

func (cur optimizedRowGroupCursor) readNext() error {
	for i := range cur.columns {
		c := &cur.columns[i]
		if c.buffered() == 0 {
			if err := c.readPage(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (cur optimizedRowGroupCursor) nextRowValuesOf(values []Value, columnIndex int) []Value {
	col := &cur.columns[columnIndex]
	err := col.readValues()
	if err != nil {
		values = append(values, Value{})
	} else {
		values = append(values, col.buffer[col.offset])
	}
	return values
}

var (
	_ RowReaderWithSchema = (*rowGroupRowReader)(nil)
	_ RowWriterTo         = (*rowGroupRowReader)(nil)

	_ RowReaderWithSchema = (*mergedRowGroupRowReader)(nil)
	//_ RowWriterTo         = (*mergedRowGroupRowReader)(nil)

	_ RowReaderWithSchema = (*concatenatedRowGroupRowReader)(nil)
	_ RowWriterTo         = (*concatenatedRowGroupRowReader)(nil)
)

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
