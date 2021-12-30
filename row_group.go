package parquet

import "github.com/segmentio/parquet/format"

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

type RowGroup interface {
	NumColumns() int

	NumRows() int

	ColumnIndex(int) RowGroupColumn

	Schema() *Schema

	SortingColumns() []format.SortingColumn
}

type RowGroupColumn interface {
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

	// Returns the size of the column in bytes.
	Size() int64
}

type RowGroupReader interface {
	ReadRowGroup() (RowGroup, error)
}

type RowGroupWriter interface {
	WriteRowGroup(RowGroup) error
}

func Merge(rowGroups ...RowGroup) RowGroup {
	return nil
}
