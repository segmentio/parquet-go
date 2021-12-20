package parquet

import (
	"fmt"
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

func (row Row) indexOf(columnIndex int) int {
	for i, v := range row {
		if c := int(v.ColumnIndex()); c == columnIndex {
			return i
		}
	}
	return len(row)
}

type levels struct {
	repetitionDepth int8
	repetitionLevel int8
	definitionLevel int8
}

type deconstructFunc func(Row, levels, reflect.Value) Row

func deconstructFuncOf(columnIndex int, node Node) (int, deconstructFunc) {
	switch {
	case node.Optional():
		return deconstructFuncOfOptional(columnIndex, node)
	case isList(node):
		return deconstructFuncOf(columnIndex, Repeated(listElementOf(node)))
	case node.Repeated():
		return deconstructFuncOfRepeated(columnIndex, node)
	default:
		return deconstructFuncOfRequired(columnIndex, node)
	}
}

func deconstructFuncOfOptional(columnIndex int, node Node) (int, deconstructFunc) {
	columnIndex, deconstruct := deconstructFuncOf(columnIndex, Required(node))
	return columnIndex, func(row Row, levels levels, value reflect.Value) Row {
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
	return columnIndex, func(row Row, levels levels, value reflect.Value) Row {
		numValues := 0

		if value.IsValid() {
			numValues = value.Len()
			levels.repetitionDepth++
			if numValues > 0 { // !value.IsNil() {
				levels.definitionLevel++
			}
		}

		if numValues == 0 {
			return deconstruct(row, levels, reflect.Value{})
		}

		for i := 0; i < numValues; i++ {
			row = deconstruct(row, levels, value.Index(i))
			levels.repetitionLevel = levels.repetitionDepth
		}

		return row
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

	valueByIndex := func(value reflect.Value, index int) reflect.Value {
		return node.ValueByName(value, names[index])
	}

	switch n := unwrap(node).(type) {
	case IndexedNode:
		valueByIndex = n.ValueByIndex
	}

	return columnIndex, func(row Row, levels levels, value reflect.Value) Row {
		valueAt := valueByIndex

		if !value.IsValid() {
			valueAt = func(value reflect.Value, _ int) reflect.Value {
				return value
			}
		}

		for i, f := range funcs {
			row = f(row, levels, valueAt(value, i))
		}

		return row
	}
}

func deconstructFuncOfLeaf(columnIndex int, node Node) (int, deconstructFunc) {
	if columnIndex > MaxColumnIndex {
		panic("row cannot be deconstructed because it has more than 127 columns")
	}
	kind := node.Type().Kind()
	valueColumnIndex := ^int8(columnIndex)
	return columnIndex + 1, func(row Row, levels levels, value reflect.Value) Row {
		v := Value{}

		if value.IsValid() {
			v = makeValue(kind, value)
		}

		v.repetitionLevel = levels.repetitionLevel
		v.definitionLevel = levels.definitionLevel
		v.columnIndex = valueColumnIndex
		return append(row, v)
	}
}

type reconstructFunc func(reflect.Value, levels, Row) error

func reconstructFuncOf(columnIndex int, node Node) (int, reconstructFunc) {
	switch {
	case node.Optional():
		return reconstructFuncOfOptional(columnIndex, node)
	case isList(node):
		return reconstructFuncOf(columnIndex, Repeated(listElementOf(node)))
	case node.Repeated():
		return reconstructFuncOfRepeated(columnIndex, node)
	default:
		return reconstructFuncOfRequired(columnIndex, node)
	}
}

func reconstructFuncOfOptional(columnIndex int, node Node) (int, reconstructFunc) {
	nextColumnIndex, reconstruct := reconstructFuncOf(columnIndex, Required(node))
	return nextColumnIndex, func(value reflect.Value, levels levels, row Row) error {
		if len(row) > 0 && row[0].definitionLevel == levels.definitionLevel {
			return nil
		}

		if value.Kind() == reflect.Ptr {
			if value.IsNil() {
				value.Set(reflect.New(value.Type().Elem()))
			}
			value = value.Elem()
		}

		levels.definitionLevel++
		return reconstruct(value, levels, row)
	}
}

func reconstructFuncOfRepeated(columnIndex int, node Node) (int, reconstructFunc) {
	nextColumnIndex, reconstruct := reconstructFuncOf(columnIndex, Required(node))
	maxColumnIndex := nextColumnIndex - 1
	return nextColumnIndex, func(value reflect.Value, levels levels, row Row) error {
		typ := value.Type()

		if typ.Kind() != reflect.Slice {
			return fmt.Errorf("cannot reconstruct repeated parquet column into go value of type %s", value.Type())
		}

		if len(row) > 0 && row[0].definitionLevel == levels.definitionLevel {
			value.Set(reflect.MakeSlice(typ, 0, 0))
			return nil
		}

		levels.definitionLevel++
		levels.repetitionLevel++

		if len(row) == 1 &&
			row[0].IsNull() &&
			row[0].definitionLevel == levels.definitionLevel &&
			row[0].repetitionLevel < levels.repetitionLevel {
			value.Set(reflect.MakeSlice(typ, 0, 0))
			return nil
		}

		t := typ.Elem()
		elem := reflect.Value{}
		zero := reflect.Zero(t)
		size := t.Size()

		for len(row) > 0 {
			i := row.indexOf(maxColumnIndex) + 1
			if i > len(row) {
				return fmt.Errorf("cannot reconstruct repeated parquet column from row missing column index %d", maxColumnIndex)
			}

			if row[0].repetitionLevel <= levels.repetitionLevel {
				elem = reflectAppend(value, zero, size)
			}

			if !elem.IsValid() {
				return fmt.Errorf("cannot reconstruct repeated parquet column from row missing a repetition level instructing to create the first record (first repetition level is %d)", row[0].repetitionLevel)
			}

			if err := reconstruct(elem, levels, row[:i]); err != nil {
				return err
			}

			row = row[i:]
		}

		return nil
	}
}

func reconstructFuncOfRequired(columnIndex int, node Node) (int, reconstructFunc) {
	switch {
	case isLeaf(node):
		return reconstructFuncOfLeaf(columnIndex, node)
	default:
		return reconstructFuncOfGroup(columnIndex, node)
	}
}

func reconstructFuncOfGroup(columnIndex int, node Node) (int, reconstructFunc) {
	names := node.ChildNames()
	funcs := make([]reconstructFunc, len(names))
	columnIndexes := make([]int, len(names))

	for i, name := range names {
		columnIndex, funcs[i] = reconstructFuncOf(columnIndex, node.ChildByName(name))
		columnIndexes[i] = columnIndex
	}

	valueByIndex := func(value reflect.Value, index int) reflect.Value {
		return node.ValueByName(value, names[index])
	}

	switch n := unwrap(node).(type) {
	case IndexedNode:
		valueByIndex = n.ValueByIndex
	}

	return columnIndex, func(value reflect.Value, levels levels, row Row) error {
		valueAt := valueByIndex

		for i, f := range funcs {
			n := row.indexOf(columnIndexes[i])
			if err := f(valueAt(value, i), levels, row[:n]); err != nil {
				return fmt.Errorf("%s → %w", names[i], err)
			}
			row = row[n:]
		}

		return nil
	}
}

func reconstructFuncOfLeaf(columnIndex int, node Node) (int, reconstructFunc) {
	return columnIndex + 1, func(value reflect.Value, _ levels, row Row) error {
		if len(row) != 1 {
			return fmt.Errorf("expected one value to reconstruct leaf parquet row for column %d but found %d", columnIndex, len(row))
		}
		if int(row[0].ColumnIndex()) != columnIndex {
			return fmt.Errorf("no values found in parquet row for column %d", columnIndex)
		}
		return assignValue(value, row[0])
	}
}
