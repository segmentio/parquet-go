package parquet

import (
	"fmt"
	"strings"
)

// ColumnMapping is an interface representing types which map column paths to
// their zero-based index in a parquet schema.
//
// ColumnMapping is a read-only type, and therefore can be used concurrently
// from multiple goroutines.
type ColumnMapping struct {
	mapping columnMapping
	columns [][]string
}

// ColumnIndex returns the column index of the column at the given path.
//
// If the path was not found in the mapping, or if it did not represent a
// leaf column of the parquet schema, the method returns a negative value.
func (m *ColumnMapping) ColumnIndex(path []string) (columnIndex int) {
	return int(m.mapping.lookup(path))
}

// ColumnPaths returns the list of column paths available in the mapping.
//
// The method always returns the same slice value across calls to ColumnPaths,
// applications should treat it as immutable.
func (m *ColumnMapping) ColumnPaths() [][]string {
	return m.columns
}

// String returns a string representation of the column mapping.
func (m *ColumnMapping) String() string {
	s := new(strings.Builder)
	s.WriteString("{")

	if len(m.columns) > 0 {
		for _, path := range m.columns {
			fmt.Fprintf(s, "\n  % 2d => %q", m.ColumnIndex(path), columnPath(path))
		}
		s.WriteByte('\n')
	}

	s.WriteString("}")
	return s.String()
}

// ColumnMappingOf constructs the column mapping of the given schema.
func ColumnMappingOf(schema Node) *ColumnMapping {
	mapping := make(columnMappingGroup)
	columns := make([][]string, 0, 16)

	forEachLeafColumnOf(schema, func(leaf leafColumn) {
		column := make([]string, len(leaf.path))
		copy(column, leaf.path)
		columns = append(columns, column)

		group, path := mapping, leaf.path

		for len(path) > 1 {
			columnName := path[0]
			g, ok := group[columnName].(columnMappingGroup)
			if !ok {
				g = make(columnMappingGroup)
				group[columnName] = g
			}
			group, path = g, path[1:]
		}

		group[path[0]] = columnMappingLeaf(leaf.columnIndex)
	})

	return &ColumnMapping{
		mapping: mapping,
		columns: columns,
	}
}

type columnMapping interface {
	lookup(path columnPath) (columnIndex int16)
}

type columnMappingGroup map[string]columnMapping

func (group columnMappingGroup) lookup(path columnPath) int16 {
	if len(path) > 0 {
		c, ok := group[path[0]]
		if ok {
			return c.lookup(path[1:])
		}
	}
	return -1
}

type columnMappingLeaf int16

func (leaf columnMappingLeaf) lookup(path columnPath) int16 {
	if len(path) == 0 {
		return int16(leaf)
	}
	return -1
}
