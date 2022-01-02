package parquet

import (
	"fmt"
	"io"
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

// Equal returns true if row and other contain the same sequence of values.
func (row Row) Equal(other Row) bool {
	if len(row) != len(other) {
		return false
	}
	for i := range row {
		if !Equal(row[i], other[i]) {
			return false
		}
		if row[i].repetitionLevel != other[i].repetitionLevel {
			return false
		}
		if row[i].definitionLevel != other[i].definitionLevel {
			return false
		}
		if row[i].columnIndex != other[i].columnIndex {
			return false
		}
	}
	return true
}

func (row Row) startsWith(columnIndex int) bool {
	return len(row) > 0 && int(row[0].ColumnIndex()) == columnIndex
}

// RowReader reads a sequence of parquet rows.
type RowReader interface {
	ReadRow(Row) (Row, error)
}

// RowReaderAt reads parquet rows at specific indexes.
type RowReaderAt interface {
	ReadRowAt(Row, int) (Row, error)
}

// RowReaderFrom reads parquet rows from reader.
type RowReaderFrom interface {
	ReadRowsFrom(RowReader) (int64, error)
}

// RowReaderWtihSchema is an extension of the RowReader interface which
// advertizes the schema of rows returned by ReadRow calls.
type RowReaderWithSchema interface {
	RowReader
	Schema() *Schema
}

// RowWriter writes parquet rows to an underlying medium.
type RowWriter interface {
	WriteRow(Row) error
}

// RowWriterAt writes parquet rows at specific indexes.
type RowWriterAt interface {
	WriteRowAt(Row, int) error
}

// RowWriterTo writes parquet rows to a writer.
type RowWriterTo interface {
	WriteRowsTo(RowWriter) (int64, error)
}

// RowWriterWithSchema is an extension of the RowWriter interface which
// advertizes the schema of rows expected to be passed to WriteRow calls.
type RowWriterWithSchema interface {
	RowWriter
	Schema() *Schema
}

// CopyRows copies rows from src to dst.
//
// The underlying types of src and dst are tested to determine if they expose
// information about the schema of rows that are read and expected to be
// written. If the schema information are available but do not match, the
// function will attempt to automatically convert the rows from the source
// schema to the destination.
//
// As an optimization, the src argument may implemnt RowWriterTo to bypass
// the default row copy logic and provide its own. The dst argument may also
// implement RowReaderFrom for the same purpose.
//
// The function returns the number of rows written, or any error encountered
// other than io.EOF.
func CopyRows(dst RowWriter, src RowReader) (int64, error) {
	n, _, err := copyRows(dst, src, nil)
	return n, err
}

func copyRows(dst RowWriter, src RowReader, buf []Value) (written int64, ret []Value, err error) {
	targetSchema := targetSchemaOf(dst)
	sourceSchema := sourceSchemaOf(src)

	if targetSchema != nil && sourceSchema != nil {
		if !nodesAreEqual(targetSchema, sourceSchema) {
			conv, err := Convert(targetSchema, sourceSchema)
			if err != nil {
				return 0, buf, err
			}
			// The conversion effectively disables a potential optimization
			// if the source reader implemented RowWriterTo. It is a trade off
			// we are making to optimize for safety rather than performance.
			//
			// Entering this code path should not be the common case tho, it is
			// most often used when parquet schemas are evolving, but we expect
			// that the majority of files of an application to be sharing a
			// common schema.
			src = ConvertRowReader(src, conv)
		}
	}

	if wt, ok := src.(RowWriterTo); ok {
		written, err = wt.WriteRowsTo(dst)
		return written, buf, err
	}

	if rf, ok := dst.(RowReaderFrom); ok {
		written, err = rf.ReadRowsFrom(src)
		return written, buf, err
	}

	if len(buf) == 0 {
		buf = make([]Value, 42)
	}

	defer func() {
		clearValues(buf)
	}()

	for {
		if buf, err = src.ReadRow(buf[:0]); err != nil {
			if err == io.EOF {
				err = nil
			}
			return written, buf, err
		}
		if err = dst.WriteRow(buf); err != nil {
			return written, buf, err
		}
		written++
	}
}

func sourceSchemaOf(r RowReader) *Schema {
	if rrs, ok := r.(RowReaderWithSchema); ok {
		return rrs.Schema()
	}
	return nil
}

func targetSchemaOf(w RowWriter) *Schema {
	if rws, ok := w.(RowWriterWithSchema); ok {
		return rws.Schema()
	}
	return nil
}

func forEachRowOf(values []Value, maxReptitionLevel int8, do func(Row) bool) {
	for len(values) > 0 {
		i := 1

		for i < len(values) && values[i].repetitionLevel == maxReptitionLevel {
			i++
		}

		if !do(values[:i]) {
			break
		}

		values = values[i:]
	}
}

func errRowIndexOutOfBounds(rowIndex, rowCount int) error {
	return fmt.Errorf("row index out of bounds: %d/%d", rowIndex, rowCount)
}

func errRowHasTooFewValues(numValues int) error {
	return fmt.Errorf("row has too few values to be written to the column: %d", numValues)
}

func errRowHasTooManyValues(numValues int) error {
	return fmt.Errorf("row has too many values to be written to the column: %d", numValues)
}

// =============================================================================
// Functions returning closures are marked with "go:noinline" below to prevent
// losing naming information of the closure in stack traces.
//
// Because some of the functions are very short (simply return a closure), the
// compiler inlines when at their call site, which result in the closure being
// named something like parquet.deconstructFuncOf.func2 instead of the original
// parquet.deconstructFuncOfLeaf.func1; the latter being much more meanginful
// when reading CPU or memory profiles.
// =============================================================================

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
	case node.Repeated():
		return deconstructFuncOfRepeated(columnIndex, node)
	case isList(node):
		return deconstructFuncOfList(columnIndex, node)
	case isMap(node):
		return deconstructFuncOfMap(columnIndex, node)
	default:
		return deconstructFuncOfRequired(columnIndex, node)
	}
}

//go:noinline
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

//go:noinline
func deconstructFuncOfRepeated(columnIndex int, node Node) (int, deconstructFunc) {
	columnIndex, deconstruct := deconstructFuncOf(columnIndex, Required(node))
	return columnIndex, func(row Row, levels levels, value reflect.Value) Row {
		if !value.IsValid() || value.Len() == 0 {
			return deconstruct(row, levels, reflect.Value{})
		}

		levels.repetitionDepth++
		levels.definitionLevel++

		for i, n := 0, value.Len(); i < n; i++ {
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

func deconstructFuncOfList(columnIndex int, node Node) (int, deconstructFunc) {
	return deconstructFuncOf(columnIndex, Repeated(listElementOf(node)))
}

//go:noinline
func deconstructFuncOfMap(columnIndex int, node Node) (int, deconstructFunc) {
	keyValue := mapKeyValueOf(node)
	keyValueType := keyValue.GoType()
	keyValueElem := keyValueType.Elem()
	keyType := keyValueElem.Field(0).Type
	valueType := keyValueElem.Field(1).Type
	columnIndex, deconstruct := deconstructFuncOf(columnIndex, schemaOf(keyValueElem))
	return columnIndex, func(row Row, levels levels, mapValue reflect.Value) Row {
		if !mapValue.IsValid() || mapValue.Len() == 0 {
			return deconstruct(row, levels, reflect.Value{})
		}

		levels.repetitionDepth++
		levels.definitionLevel++

		elem := reflect.New(keyValueElem).Elem()
		k := elem.Field(0)
		v := elem.Field(1)

		for _, key := range mapValue.MapKeys() {
			k.Set(key.Convert(keyType))
			v.Set(mapValue.MapIndex(key).Convert(valueType))
			row = deconstruct(row, levels, elem)
			levels.repetitionLevel = levels.repetitionDepth
		}

		return row
	}
}

//go:noinline
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

//go:noinline
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

type reconstructFunc func(reflect.Value, levels, Row) (Row, error)

func reconstructFuncOf(columnIndex int, node Node) (int, reconstructFunc) {
	switch {
	case node.Optional():
		return reconstructFuncOfOptional(columnIndex, node)
	case node.Repeated():
		return reconstructFuncOfRepeated(columnIndex, node)
	case isList(node):
		return reconstructFuncOfList(columnIndex, node)
	case isMap(node):
		return reconstructFuncOfMap(columnIndex, node)
	default:
		return reconstructFuncOfRequired(columnIndex, node)
	}
}

//go:noinline
func reconstructFuncOfOptional(columnIndex int, node Node) (int, reconstructFunc) {
	nextColumnIndex, reconstruct := reconstructFuncOf(columnIndex, Required(node))
	rowLength := nextColumnIndex - columnIndex
	return nextColumnIndex, func(value reflect.Value, levels levels, row Row) (Row, error) {
		if !row.startsWith(columnIndex) {
			return row, fmt.Errorf("row is missing optional column %d", columnIndex)
		}
		if len(row) < rowLength {
			return row, fmt.Errorf("expected optional column %d to have at least %d values but got %d", columnIndex, rowLength, len(row))
		}

		levels.definitionLevel++

		if row[0].definitionLevel < levels.definitionLevel {
			value.Set(reflect.Zero(value.Type()))
			return row[rowLength:], nil
		}

		if value.Kind() == reflect.Ptr {
			if value.IsNil() {
				value.Set(reflect.New(value.Type().Elem()))
			}
			value = value.Elem()
		}

		return reconstruct(value, levels, row)
	}
}

//go:noinline
func reconstructFuncOfRepeated(columnIndex int, node Node) (int, reconstructFunc) {
	nextColumnIndex, reconstruct := reconstructFuncOf(columnIndex, Required(node))
	rowLength := nextColumnIndex - columnIndex
	return nextColumnIndex, func(value reflect.Value, lvls levels, row Row) (Row, error) {
		t := value.Type()
		c := value.Cap()
		n := 0
		if c > 0 {
			value.Set(value.Slice(0, c))
		} else {
			c = 10
			value.Set(reflect.MakeSlice(t, c, c))
		}

		defer func() {
			value.Set(value.Slice(0, n))
		}()

		return reconstructRepeated(columnIndex, rowLength, lvls, row, func(levels levels, row Row) (Row, error) {
			if n == c {
				c *= 2
				newValue := reflect.MakeSlice(t, c, c)
				reflect.Copy(newValue, value)
				value.Set(newValue)
			}
			row, err := reconstruct(value.Index(n), levels, row)
			n++
			return row, err
		})
	}
}

func reconstructRepeated(columnIndex, rowLength int, levels levels, row Row, do func(levels, Row) (Row, error)) (Row, error) {
	if !row.startsWith(columnIndex) {
		return row, fmt.Errorf("row is missing repeated column %d", columnIndex)
	}
	if len(row) < rowLength {
		return row, fmt.Errorf("expected repeated column %d to have at least %d values but got %d", columnIndex, rowLength, len(row))
	}

	levels.repetitionDepth++
	levels.definitionLevel++

	if row[0].definitionLevel < levels.definitionLevel {
		return row[rowLength:], nil
	}

	var err error
	for row.startsWith(columnIndex) && row[0].repetitionLevel == levels.repetitionLevel {
		if row, err = do(levels, row); err != nil {
			break
		}
		levels.repetitionLevel = levels.repetitionDepth
	}
	return row, err
}

func reconstructFuncOfRequired(columnIndex int, node Node) (int, reconstructFunc) {
	switch {
	case isLeaf(node):
		return reconstructFuncOfLeaf(columnIndex, node)
	default:
		return reconstructFuncOfGroup(columnIndex, node)
	}
}

func reconstructFuncOfList(columnIndex int, node Node) (int, reconstructFunc) {
	return reconstructFuncOf(columnIndex, Repeated(listElementOf(node)))
}

//go:noinline
func reconstructFuncOfMap(columnIndex int, node Node) (int, reconstructFunc) {
	keyValue := mapKeyValueOf(node)
	keyValueType := keyValue.GoType()
	keyValueElem := keyValueType.Elem()
	keyValueZero := reflect.Zero(keyValueElem)
	nextColumnIndex, reconstruct := reconstructFuncOf(columnIndex, schemaOf(keyValueElem))
	rowLength := nextColumnIndex - columnIndex
	return nextColumnIndex, func(mapValue reflect.Value, lvls levels, row Row) (Row, error) {
		t := mapValue.Type()
		k := t.Key()
		v := t.Elem()

		if mapValue.IsNil() {
			mapValue.Set(reflect.MakeMap(t))
		}

		elem := reflect.New(keyValueElem).Elem()
		return reconstructRepeated(columnIndex, rowLength, lvls, row, func(levels levels, row Row) (Row, error) {
			row, err := reconstruct(elem, levels, row)
			if err == nil {
				mapValue.SetMapIndex(elem.Field(0).Convert(k), elem.Field(1).Convert(v))
				elem.Set(keyValueZero)
			}
			return row, err
		})
	}
}

//go:noinline
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

	return columnIndex, func(value reflect.Value, levels levels, row Row) (Row, error) {
		var valueAt = valueByIndex
		var err error

		for i, f := range funcs {
			if row, err = f(valueAt(value, i), levels, row); err != nil {
				err = fmt.Errorf("%s → %w", names[i], err)
				break
			}
		}

		return row, err
	}
}

//go:noinline
func reconstructFuncOfLeaf(columnIndex int, node Node) (int, reconstructFunc) {
	return columnIndex + 1, func(value reflect.Value, _ levels, row Row) (Row, error) {
		if !row.startsWith(columnIndex) {
			return row, fmt.Errorf("no values found in parquet row for column %d", columnIndex)
		}
		return row[1:], assignValue(value, row[0])
	}
}
