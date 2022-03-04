package parquet

import (
	"container/heap"
	"fmt"
	"io"
)

type mergedRowGroup struct {
	concatenatedRowGroup
	sorting   []SortingColumn
	sortFuncs []columnSortFunc
}

func (m *mergedRowGroup) SortingColumns() []SortingColumn {
	return m.sorting
}

func (m *mergedRowGroup) Rows() Rows {
	// The row group needs to respect a sorting order; the merged row reader
	// uses a heap to merge rows from the row groups.
	return &mergedRowGroupRowReader{rowGroup: m, schema: m.schema}
}

type mergedRowGroupRowReader struct {
	rowGroup *mergedRowGroup
	schema   *Schema
	sorting  []columnSortFunc
	cursors  []rowGroupCursor
	values1  []Value
	values2  []Value
	seek     int64
	index    int64
	err      error
}

func (r *mergedRowGroupRowReader) init(m *mergedRowGroup) {
	if r.schema != nil {
		numColumns := numLeafColumnsOf(r.schema)
		cursors := make([]bufferedRowGroupCursor, len(m.rowGroups))
		buffers := make([][]Value, int(numColumns)*len(m.rowGroups))

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
				// * The optimized merge of pages for non-overlapping ranges is
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

func (r *mergedRowGroupRowReader) SeekToRow(rowIndex int64) error {
	if rowIndex >= r.index {
		r.seek = rowIndex
		return nil
	}
	return fmt.Errorf("SeekToRow: merged row reader cannot seek backward from row %d to %d", r.index, rowIndex)
}

func (r *mergedRowGroupRowReader) ReadRow(row Row) (Row, error) {
	if r.rowGroup != nil {
		r.init(r.rowGroup)
		r.rowGroup = nil
	}
	if r.err != nil {
		return row, r.err
	}

	for {
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

		ret := r.index >= r.seek
		r.index++
		if ret {
			return row, nil
		}
	}
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
	nextRowValuesOf([]Value, int16) []Value
}

type columnSortFunc struct {
	columnIndex int16
	compare     SortFunc
}

type bufferedRowGroupCursor struct {
	reader  Rows
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

func (cur *bufferedRowGroupCursor) nextRowValuesOf(values []Value, columnIndex int16) []Value {
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

func (cur optimizedRowGroupCursor) nextRowValuesOf(values []Value, columnIndex int16) []Value {
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
	_ RowReaderWithSchema = (*mergedRowGroupRowReader)(nil)
	//_ RowWriterTo         = (*mergedRowGroupRowReader)(nil)
)

type SortFunc func(a, b []Value) int

func SortFuncOf(t Type, maxDefinitionLevel, maxRepetitionLevel int8, descending, nullsFirst bool) (sort SortFunc) {
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
func sortFuncOfDescending(sort SortFunc) SortFunc {
	return func(a, b []Value) int { return -sort(a, b) }
}

func sortFuncOfOptional(t Type, nullsFirst bool) SortFunc {
	if nullsFirst {
		return sortFuncOfOptionalNullsFirst(t)
	} else {
		return sortFuncOfOptionalNullsLast(t)
	}
}

//go:noinline
func sortFuncOfOptionalNullsFirst(t Type) SortFunc {
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
func sortFuncOfOptionalNullsLast(t Type) SortFunc {
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
func sortFuncOfRepeated(t Type, nullsFirst bool) SortFunc {
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
func sortFuncOfRequired(t Type) SortFunc {
	return func(a, b []Value) int { return t.Compare(a[0], b[0]) }
}
