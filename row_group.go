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
	Columns() []RowGroupColumn

	NumRows() int

	Schema() *Schema

	SortingColumns() []format.SortingColumn

	// Rows returns a reader exposing the rows of the row group.
	Rows() RowReader
}

type RowGroupColumn interface {
	// For indexed columns, returns the underlying dictionary holding the column
	// values. If the column is not indexed, nil is returned.
	Dictionary() Dictionary

	// Returns a reader exposing the list of pages in the column.
	Pages() []Page

	// Returns a reader exposing the values currently held in the buffer.
	Values() ValueReader
}

type RowGroupReader interface {
	ReadRowGroup() (RowGroup, error)
}

type RowGroupWriter interface {
	WriteRowGroup(RowGroup) error
}

func MergeRowGroups(rowGroups ...RowGroup) (RowGroup, error) {
	return nil, nil
}
