package parquet

import (
	"fmt"
	"io"
)

// RowGroup is an interface representing a parquet row group. From the Parquet
// docs, a RowGroup is "a logical horizontal partitioning of the data into rows.
// There is no physical structure that is guaranteed for a row group. A row
// group consists of a column chunk for each column in the dataset."
//
// https://github.com/apache/parquet-format#glossary
type RowGroup interface {
	// Returns the number of rows in the group.
	NumRows() int64

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
	Rows() Rows
}

// Rows is an interface implemented by row readers returned by calling the Rows
// method of RowGroup instances.
type Rows interface {
	RowReaderWithSchema
	RowSeeker
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
	config, err := NewRowGroupConfig(options...)
	if err != nil {
		return nil, err
	}

	schema := config.Schema
	if len(rowGroups) == 0 {
		return newEmptyRowGroup(schema), nil
	}
	if schema == nil {
		schema = rowGroups[0].Schema()

		for _, rowGroup := range rowGroups[1:] {
			if !nodesAreEqual(schema, rowGroup.Schema()) {
				return nil, ErrRowGroupSchemaMismatch
			}
		}
	}

	mergedRowGroups := make([]RowGroup, len(rowGroups))
	copy(mergedRowGroups, rowGroups)

	for i, rowGroup := range mergedRowGroups {
		if rowGroupSchema := rowGroup.Schema(); !nodesAreEqual(schema, rowGroupSchema) {
			conv, err := Convert(schema, rowGroupSchema)
			if err != nil {
				return nil, fmt.Errorf("cannot merge row groups: %w", err)
			}
			mergedRowGroups[i] = ConvertRowGroup(rowGroup, conv)
		}
	}

	m := &mergedRowGroup{sorting: config.SortingColumns}
	m.init(schema, mergedRowGroups)

	if len(m.sorting) == 0 {
		// When the row group has no ordering, use a simpler version of the
		// merger which simply concatenates rows from each of the row groups.
		// This is preferrable because it makes the output deterministic, the
		// heap merge may otherwise reorder rows across groups.
		return &m.concatenatedRowGroup, nil
	}

	for _, rowGroup := range m.rowGroups {
		if !sortingColumnsHavePrefix(rowGroup.SortingColumns(), m.sorting) {
			return nil, ErrRowGroupSortingColumnsMismatch
		}
	}

	m.sortFuncs = make([]columnSortFunc, len(m.sorting))
	forEachLeafColumnOf(schema, func(leaf leafColumn) {
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

	return m, nil
}

type rowGroupRowReader struct {
	rowGroup RowGroup
	schema   *Schema
	columns  []columnChunkReader
	seek     int64
}

func (r *rowGroupRowReader) init(rowGroup RowGroup) error {
	const columnBufferSize = defaultValueBufferSize
	numColumns := rowGroup.NumColumns()
	buffer := make([]Value, columnBufferSize*numColumns)

	r.schema = rowGroup.Schema()
	r.columns = make([]columnChunkReader, numColumns)

	for i := 0; i < numColumns; i++ {
		r.columns[i].column = rowGroup.Column(i)
		r.columns[i].buffer = buffer[:0:columnBufferSize]
		buffer = buffer[columnBufferSize:]
	}

	if r.seek > 0 {
		return r.SeekToRow(r.seek)
	}
	return nil
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

func (r *rowGroupRowReader) SeekToRow(rowIndex int64) error {
	for i := range r.columns {
		if err := r.columns[i].seekToRow(rowIndex); err != nil {
			return err
		}
	}
	r.seek = rowIndex
	return nil
}

func (r *rowGroupRowReader) ReadRow(row Row) (Row, error) {
	if r.rowGroup != nil {
		err := r.init(r.rowGroup)
		r.rowGroup = nil
		if err != nil {
			return row, err
		}
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
	defer func() { r.rowGroup, r.seek = nil, 0 }()
	rowGroup := r.rowGroup
	if r.seek > 0 {
		rowGroup = &seekRowGroup{base: rowGroup, seek: r.seek}
	}

	switch dst := w.(type) {
	case RowGroupWriter:
		return dst.WriteRowGroup(rowGroup)

	case PageWriter:
		for i, n := 0, rowGroup.NumColumns(); i < n; i++ {
			_, err := CopyPages(dst, rowGroup.Column(i).Pages())
			if err != nil {
				return 0, err
			}
		}
		return rowGroup.NumRows(), nil
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

type seekRowGroup struct {
	base RowGroup
	seek int64
}

func (g *seekRowGroup) NumRows() int64 {
	return g.base.NumRows() - g.seek
}

func (g *seekRowGroup) NumColumns() int {
	return g.base.NumColumns()
}

func (g *seekRowGroup) Column(i int) ColumnChunk {
	return &seekColumnChunk{base: g.base.Column(i), seek: g.seek}
}

func (g *seekRowGroup) Schema() *Schema {
	return g.base.Schema()
}

func (g *seekRowGroup) SortingColumns() []SortingColumn {
	return g.base.SortingColumns()
}

func (g *seekRowGroup) Rows() Rows {
	rows := g.base.Rows()
	rows.SeekToRow(g.seek)
	return rows
}

type seekColumnChunk struct {
	base ColumnChunk
	seek int64
}

func (c *seekColumnChunk) Type() Type {
	return c.base.Type()
}

func (c *seekColumnChunk) Column() int {
	return c.base.Column()
}

func (c *seekColumnChunk) Pages() Pages {
	pages := c.base.Pages()
	pages.SeekToRow(c.seek)
	return pages
}

func (c *seekColumnChunk) ColumnIndex() ColumnIndex {
	return c.base.ColumnIndex()
}

func (c *seekColumnChunk) OffsetIndex() OffsetIndex {
	return c.base.OffsetIndex()
}

type emptyRowGroup struct {
	schema  *Schema
	columns []emptyColumnChunk
}

func newEmptyRowGroup(schema *Schema) *emptyRowGroup {
	g := &emptyRowGroup{
		schema:  schema,
		columns: make([]emptyColumnChunk, numColumnsOf(schema)),
	}
	forEachLeafColumnOf(schema, func(leaf leafColumn) {
		g.columns[leaf.columnIndex].typ = leaf.node.Type()
		g.columns[leaf.columnIndex].column = leaf.columnIndex
	})
	return g
}

func (g *emptyRowGroup) NumRows() int64                  { return 0 }
func (g *emptyRowGroup) NumColumns() int                 { return len(g.columns) }
func (g *emptyRowGroup) Column(i int) ColumnChunk        { return &g.columns[i] }
func (g *emptyRowGroup) SortingColumns() []SortingColumn { return nil }
func (g *emptyRowGroup) Schema() *Schema                 { return g.schema }
func (g *emptyRowGroup) Rows() Rows                      { return emptyRowReader{g.schema} }

type emptyColumnChunk struct {
	typ    Type
	column int
}

func (c *emptyColumnChunk) Type() Type               { return c.typ }
func (c *emptyColumnChunk) Column() int              { return c.column }
func (c *emptyColumnChunk) Pages() Pages             { return emptyPages{} }
func (c *emptyColumnChunk) ColumnIndex() ColumnIndex { return &emptyColumnIndex }
func (c *emptyColumnChunk) OffsetIndex() OffsetIndex { return &emptyOffsetIndex }

type emptyRowReader struct{ schema *Schema }

func (r emptyRowReader) Schema() *Schema                      { return r.schema }
func (r emptyRowReader) ReadRow(row Row) (Row, error)         { return row, io.EOF }
func (r emptyRowReader) SeekToRow(int64) error                { return nil }
func (r emptyRowReader) WriteRowsTo(RowWriter) (int64, error) { return 0, nil }

type emptyPages struct{}

func (emptyPages) ReadPage() (Page, error) { return nil, io.EOF }
func (emptyPages) SeekToRow(int64) error   { return nil }

var (
	emptyColumnIndex = columnIndex{}
	emptyOffsetIndex = offsetIndex{}

	_ RowReaderWithSchema = (*rowGroupRowReader)(nil)
	_ RowWriterTo         = (*rowGroupRowReader)(nil)

	_ RowReaderWithSchema = emptyRowReader{}
	_ RowWriterTo         = emptyRowReader{}
)
