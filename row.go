package parquet

import (
	"math"
	"reflect"
)

const (
	MaxRepetitionLevel = math.MaxInt8
	MaxDefinitionLevel = math.MaxInt8
	MaxColumnIndex     = math.MaxInt8
)

// Row represents a parquet row as a slice of values.
//
// Each value should embed a column index, repetition level, and definition
// level allowing the program to determine how to reconstruct the original
// object from the row. Repeated values share the same column index, their
// relative position of repeated values is represented by their relative
// position in the row.
type Row []Value

// Columns splits the row into lists of values groups by column index.
func (row Row) Columns() [][]Value {
	maxColumnIndex := 0
	for _, value := range row {
		if columnIndex := int(value.ColumnIndex()); columnIndex > maxColumnIndex {
			maxColumnIndex = columnIndex
		}
	}
	columns := make([][]Value, maxColumnIndex+1)
	for _, value := range row {
		columnIndex := value.ColumnIndex()
		columns[columnIndex] = append(columns[columnIndex], value)
	}
	return columns
}

type levels struct {
	repetitionDepth int8
	repetitionLevel int8
	definitionLevel int8
}

type deconstructFunc func(Row, levels, reflect.Value) (Row, error)

func deconstructFuncOf(columnIndex int, node Node) (int, deconstructFunc) {
	optional := node.Optional()
	repeated := node.Repeated()

	if optional {
		return deconstructFuncOfOptional(columnIndex, node)
	}

	if logicalType := node.Type().LogicalType(); logicalType != nil {
		switch {
		case logicalType.List != nil:
			elem := node.ChildByName("list").ChildByName("element")
			return deconstructFuncOf(columnIndex, Repeated(elem))
		}
	}

	if repeated {
		return deconstructFuncOfRepeated(columnIndex, node)
	}

	return deconstructFuncOfRequired(columnIndex, node)
}

func deconstructFuncOfOptional(columnIndex int, node Node) (int, deconstructFunc) {
	columnIndex, deconstruct := deconstructFuncOf(columnIndex, Required(node))
	return columnIndex, func(row Row, levels levels, value reflect.Value) (Row, error) {
		if value.IsValid() {
			if value.IsZero() {
				value = reflect.Value{}
			} else {
				if value.Kind() == reflect.Ptr {
					value = value.Elem()
				}
				levels.definitionLevel++
			}
		}
		return deconstruct(row, levels, value)
	}
}

func deconstructFuncOfRepeated(columnIndex int, node Node) (int, deconstructFunc) {
	columnIndex, deconstruct := deconstructFuncOf(columnIndex, Required(node))
	return columnIndex, func(row Row, levels levels, value reflect.Value) (Row, error) {
		var numValues int
		var err error

		if value.IsValid() {
			numValues = value.Len()
			levels.repetitionDepth++
			if !value.IsNil() {
				levels.definitionLevel++
			}
		}

		if numValues == 0 {
			row, err = deconstruct(row, levels, reflect.Value{})
		} else {
			for i := 0; i < numValues && err == nil; i++ {
				row, err = deconstruct(row, levels, value.Index(i))
				levels.repetitionLevel = levels.repetitionDepth
			}
		}

		return row, err
	}
}

func deconstructFuncOfRequired(columnIndex int, node Node) (int, deconstructFunc) {
	switch {
	case isLeaf(node):
		return deconstructFuncOfLeaf(columnIndex, node)
	default:
		return deconstructFuncOfGroup(columnIndex, node)
	}
}

func deconstructFuncOfGroup(columnIndex int, node Node) (int, deconstructFunc) {
	names := node.ChildNames()
	funcs := make([]deconstructFunc, len(names))

	for i, name := range names {
		columnIndex, funcs[i] = deconstructFuncOf(columnIndex, node.ChildByName(name))
	}

	valueByIndex := func(base reflect.Value, index int) reflect.Value {
		return node.ValueByName(base, names[index])
	}

	switch n := unwrap(node).(type) {
	case IndexedNode:
		valueByIndex = n.ValueByIndex
	}

	return columnIndex, func(row Row, levels levels, value reflect.Value) (Row, error) {
		var valueAt = valueByIndex
		var err error

		if !value.IsValid() {
			valueAt = func(base reflect.Value, _ int) reflect.Value {
				return base
			}
		}

		for i, f := range funcs {
			if row, err = f(row, levels, valueAt(value, i)); err != nil {
				break
			}
		}

		return row, err
	}
}

func deconstructFuncOfLeaf(columnIndex int, node Node) (int, deconstructFunc) {
	if columnIndex > MaxColumnIndex {
		panic("row cannot be deconstructed because it has more than 127 columns")
	}
	kind := node.Type().Kind()
	valueColumnIndex := ^int8(columnIndex)
	return columnIndex + 1, func(row Row, levels levels, value reflect.Value) (Row, error) {
		var v Value

		if value.IsValid() {
			v = makeValue(kind, value)
		}

		v.repetitionLevel = levels.repetitionLevel
		v.definitionLevel = levels.definitionLevel
		v.columnIndex = valueColumnIndex
		return append(row, v), nil
	}
}
