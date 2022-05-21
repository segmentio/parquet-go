//go:build go1.18

package parquet

import (
	"bytes"
	"reflect"
	"unsafe"

	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/internal/unsafecast"
)

type array struct {
	ptr unsafe.Pointer
	len int
}

func (a array) index(i int, size, offset uintptr) unsafe.Pointer {
	return unsafe.Add(a.ptr, uintptr(i)*size+offset)
}

func makeArray[T any](s []T) array {
	return array{
		ptr: *(*unsafe.Pointer)(unsafe.Pointer(&s)),
		len: len(s),
	}
}

type nullIndexFunc func(unsafe.Pointer, int) int

func nullIndexByte(p unsafe.Pointer, n int) int {
	i := bytes.IndexByte(unsafe.Slice((*byte)(p), n), 0)
	if i < 0 {
		i = n
	}
	return i
}

func nullIndexSlice(p unsafe.Pointer, n int) int {
	const size = unsafe.Sizeof(([]byte)(nil))
	for i := 0; i < n; i++ {
		p := *(*unsafe.Pointer)(unsafe.Add(p, uintptr(i)*size))
		if p == nil {
			return i
		}
	}
	return n
}

func nullIndex[T comparable](p unsafe.Pointer, n int) int {
	var zero T
	for i, v := range unsafe.Slice((*T)(p), n) {
		if v == zero {
			return i
		}
	}
	return n
}

func nullIndexFuncOf(t reflect.Type) nullIndexFunc {
	switch t {
	case reflect.TypeOf(deprecated.Int96{}):
		return nullIndex[deprecated.Int96]
	}

	switch t.Kind() {
	case reflect.Bool:
		return nullIndexByte

	case reflect.Int, reflect.Uint:
		return nullIndex[int]

	case reflect.Int8, reflect.Uint8:
		return nullIndexByte

	case reflect.Int16, reflect.Uint16:
		return nullIndex[int16]

	case reflect.Int32, reflect.Uint32:
		return nullIndex[int32]

	case reflect.Int64, reflect.Uint64:
		return nullIndex[int64]

	case reflect.Float32:
		return nullIndex[float32]

	case reflect.Float64:
		return nullIndex[float64]

	case reflect.String:
		return nullIndex[string]

	case reflect.Slice:
		return nullIndexSlice

	case reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			return nullIndexFuncOfArray(t)
		}

	case reflect.Ptr:
		return nullIndex[unsafe.Pointer]

	case reflect.Struct:
		return func(p unsafe.Pointer, n int) int { return n }
	}

	panic("cannot convert Go values of type " + t.String() + " to parquet value")
}

func nullIndexFuncOfArray(t reflect.Type) nullIndexFunc {
	arrayLen := t.Len()
	return func(p unsafe.Pointer, n int) int {
		for i := 0; i < n; i++ {
			p := unsafe.Add(p, i*arrayLen)
			b := unsafe.Slice((*byte)(p), arrayLen)
			if bytes.Count(b, []byte{0}) == len(b) {
				return i
			}
		}
		return n
	}
}

type nonNullIndexFunc func(unsafe.Pointer, int) int

func nonNullIndexSlice(p unsafe.Pointer, n int) int {
	const size = unsafe.Sizeof(([]byte)(nil))
	for i := 0; i < n; i++ {
		p := *(*unsafe.Pointer)(unsafe.Add(p, uintptr(i)*size))
		if p != nil {
			return i
		}
	}
	return n
}

func nonNullIndex[T comparable](p unsafe.Pointer, n int) int {
	var zero T
	for i, v := range unsafe.Slice((*T)(p), n) {
		if v != zero {
			return i
		}
	}
	return n
}

func nonNullIndexFuncOf(t reflect.Type) nonNullIndexFunc {
	switch t {
	case reflect.TypeOf(deprecated.Int96{}):
		return nonNullIndex[deprecated.Int96]
	}

	switch t.Kind() {
	case reflect.Bool:
		return nonNullIndex[bool]

	case reflect.Int, reflect.Uint:
		return nonNullIndex[int]

	case reflect.Int8, reflect.Uint8:
		return nonNullIndex[int8]

	case reflect.Int16, reflect.Uint16:
		return nonNullIndex[int16]

	case reflect.Int32, reflect.Uint32:
		return nonNullIndex[int32]

	case reflect.Int64, reflect.Uint64:
		return nonNullIndex[int64]

	case reflect.Float32:
		return nonNullIndex[float32]

	case reflect.Float64:
		return nonNullIndex[float64]

	case reflect.String:
		return nonNullIndex[string]

	case reflect.Slice:
		return nonNullIndexSlice

	case reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			return nonNullIndexFuncOfArray(t)
		}

	case reflect.Ptr:
		return nonNullIndex[unsafe.Pointer]

	case reflect.Struct:
		return func(p unsafe.Pointer, n int) int { return 0 }
	}

	panic("cannot convert Go values of type " + t.String() + " to parquet value")
}

func nonNullIndexFuncOfArray(t reflect.Type) nonNullIndexFunc {
	arrayLen := t.Len()
	return func(p unsafe.Pointer, n int) int {
		for i := 0; i < n; i++ {
			p := unsafe.Add(p, i*arrayLen)
			b := unsafe.Slice((*byte)(p), arrayLen)
			if bytes.Count(b, []byte{0}) != len(b) {
				return i
			}
		}
		return n
	}
}

type columnLevels struct {
	columnIndex     int16
	repetitionDepth byte
	repetitionLevel byte
	definitionLevel byte
}

type columnBufferWriter struct {
	columns []ColumnBuffer
	values  []Value
	maxLen  int
}

type writeRowsFunc func(*columnBufferWriter, array, uintptr, uintptr, columnLevels) error

func writeRowsFuncOf(t reflect.Type, schema *Schema, path []string) writeRowsFunc {
	switch t {
	case reflect.TypeOf(deprecated.Int96{}):
		return (*columnBufferWriter).writeRowsInt96
	}

	switch t.Kind() {
	case reflect.Bool:
		return (*columnBufferWriter).writeRowsBool

	case reflect.Int, reflect.Uint:
		return (*columnBufferWriter).writeRowsInt

	case reflect.Int8, reflect.Uint8:
		return (*columnBufferWriter).writeRowsInt8

	case reflect.Int16, reflect.Uint16:
		return (*columnBufferWriter).writeRowsInt16

	case reflect.Int32, reflect.Uint32:
		return (*columnBufferWriter).writeRowsInt32

	case reflect.Int64, reflect.Uint64:
		return (*columnBufferWriter).writeRowsInt64

	case reflect.Float32:
		return (*columnBufferWriter).writeRowsFloat32

	case reflect.Float64:
		return (*columnBufferWriter).writeRowsFloat64

	case reflect.String:
		return (*columnBufferWriter).writeRowsString

	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return (*columnBufferWriter).writeRowsString
		} else {
			return writeRowsFuncOfSlice(t, schema, path)
		}

	case reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			return writeRowsFuncOfArray(t, schema, path)
		}

	case reflect.Ptr:
		return writeRowsFuncOfPointer(t, schema, path)

	case reflect.Struct:
		return writeRowsFuncOfStruct(t, schema, path)
	}

	panic("cannot convert Go values of type " + t.String() + " to parquet value")
}

func writeRowsFuncOfArray(t reflect.Type, schema *Schema, path []string) writeRowsFunc {
	len := t.Len()
	if len == 16 {
		return (*columnBufferWriter).writeRowsUUID
	}
	return func(w *columnBufferWriter, rows array, size, offset uintptr, levels columnLevels) error {
		return w.writeRowsArray(rows, size, offset, levels, len)
	}
}

func writeRowsFuncOfOptional(t reflect.Type, schema *Schema, path []string, writeRows writeRowsFunc) writeRowsFunc {
	nullIndex, nonNullIndex := nullIndexFuncOf(t), nonNullIndexFuncOf(t)
	return func(w *columnBufferWriter, rows array, size, offset uintptr, levels columnLevels) error {
		if rows.len == 0 {
			return writeRows(w, rows, size, 0, levels)
		}

		nonNullLevels := levels
		nonNullLevels.definitionLevel++
		// In this function, we are dealing with optional values which are
		// neither pointers nor slices; for example, a []int32 field marked
		// "optional" in its parent struct.
		//
		// We need to find zero values, which should be represented as nulls
		// in the parquet column. In order to minimize the calls to writeRows
		// and maximize throughput, we use the nullIndex and nonNullIndex
		// functions, which are type-specific implementations of the algorithm.
		//
		// Sections of the input that are contiguous nulls or non-nulls can be
		// sent to a single call to writeRows to be written to the underlying
		// buffer since they share the same definition level.
		//
		// This optimization is defeated by inputs alterning null and non-null
		// sequences of single values, we do not expect this condition to be a
		// common case.
		for i := 0; i < rows.len; {
			a := array{}
			p := rows.index(i, size, 0)
			j := i + nonNullIndex(p, rows.len-i)

			if i < j {
				a.ptr = p
				a.len = j - i
				if err := writeRows(w, a, size, 0, levels); err != nil {
					return err
				}
			}

			if j < rows.len {
				p = rows.index(j, size, 0)
				i = j
				j = j + nullIndex(p, rows.len-j)
				a.ptr = p
				a.len = j - i
				if err := writeRows(w, a, size, 0, nonNullLevels); err != nil {
					return err
				}
			}

			i = j
		}

		return nil
	}
}

func writeRowsFuncOfPointer(t reflect.Type, schema *Schema, path []string) writeRowsFunc {
	elemType := t.Elem()
	elemSize := elemType.Size()
	writeRows := writeRowsFuncOf(elemType, schema, path)
	return func(w *columnBufferWriter, rows array, size, offset uintptr, levels columnLevels) error {
		if rows.len == 0 {
			return writeRows(w, rows, size, 0, levels)
		}

		for i := 0; i < rows.len; i++ {
			p := *(*unsafe.Pointer)(rows.index(i, size, offset))
			a := array{}
			elemLevels := levels
			if p != nil {
				a.ptr = p
				a.len = 1
				elemLevels.definitionLevel++
			}
			if err := writeRows(w, a, elemSize, 0, elemLevels); err != nil {
				return err
			}
		}

		return nil
	}
}

func writeRowsFuncOfSlice(t reflect.Type, schema *Schema, path []string) writeRowsFunc {
	elemType := t.Elem()
	elemSize := elemType.Size()
	writeRows := writeRowsFuncOf(elemType, schema, path)
	return func(w *columnBufferWriter, rows array, size, offset uintptr, levels columnLevels) error {
		if rows.len == 0 {
			return writeRows(w, rows, size, 0, levels)
		}

		levels.repetitionDepth++

		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			a := *(*array)(p)
			n := a.len

			elemLevels := levels
			if n > 0 {
				a.len = 1
				elemLevels.definitionLevel++
			}

			if err := writeRows(w, a, elemSize, 0, elemLevels); err != nil {
				return err
			}

			if n > 1 {
				elemLevels.repetitionLevel = elemLevels.repetitionDepth
				a.ptr = a.index(1, elemSize, 0)
				a.len = n - 1

				if err := writeRows(w, a, elemSize, 0, elemLevels); err != nil {
					return err
				}
			}
		}

		return nil
	}
}

func writeRowsFuncOfStruct(t reflect.Type, schema *Schema, path []string) writeRowsFunc {
	type column struct {
		columnIndex int16
		optional    bool
		offset      uintptr
		writeRows   writeRowsFunc
	}

	fields := structFieldsOf(t)
	columns := make([]column, len(fields))

	for i, f := range fields {
		optional := false
		columnPath := append(path[:len(path):len(path)], f.Name)
		forEachStructTagOption(f.Tag, func(option, _ string) {
			switch option {
			case "list":
				columnPath = append(columnPath, "list", "element")
			case "optional":
				optional = true
			}
		})

		writeRows := writeRowsFuncOf(f.Type, schema, columnPath)
		if optional {
			switch f.Type.Kind() {
			case reflect.Pointer, reflect.Slice:
			default:
				writeRows = writeRowsFuncOfOptional(f.Type, schema, columnPath, writeRows)
			}
		}

		columnInfo := schema.mapping.lookup(columnPath)
		columns[i] = column{
			columnIndex: columnInfo.columnIndex,
			offset:      f.Offset,
			writeRows:   writeRows,
		}
	}

	return func(w *columnBufferWriter, rows array, size, offset uintptr, levels columnLevels) error {
		for _, column := range columns {
			levels.columnIndex = column.columnIndex
			if err := column.writeRows(w, rows, size, offset+column.offset, levels); err != nil {
				return err
			}
		}
		return nil
	}
}

func (w *columnBufferWriter) writeRowsBool(rows array, size, offset uintptr, levels columnLevels) (err error) {
	if rows.len == 0 {
		return w.writeRowsNull(levels)
	}

	switch c := w.columns[levels.columnIndex].(type) {
	case *booleanColumnBuffer:
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			c.writeValue(*(*bool)(p))
		}

	default:
		w.reset()
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			v := makeValueBoolean(*(*bool)(p))
			v.repetitionLevel = levels.repetitionLevel
			v.definitionLevel = levels.definitionLevel
			w.values = append(w.values, v)
		}
		_, err = w.columns[levels.columnIndex].WriteValues(w.values)
	}

	return err
}

func (w *columnBufferWriter) writeRowsInt(rows array, size, offset uintptr, levels columnLevels) (err error) {
	if rows.len == 0 {
		return w.writeRowsNull(levels)
	}

	switch c := w.columns[levels.columnIndex].(type) {
	case *int64ColumnBuffer:
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			c.values = append(c.values, int64(*(*int)(p)))
		}

	case *uint64ColumnBuffer:
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			c.values = append(c.values, uint64(*(*uint)(p)))
		}

	default:
		w.reset()
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			v := makeValueInt64(int64(*(*int)(p)))
			v.repetitionLevel = levels.repetitionLevel
			v.definitionLevel = levels.definitionLevel
			w.values = append(w.values, v)
		}
		_, err = w.columns[levels.columnIndex].WriteValues(w.values)
	}

	return err
}

func (w *columnBufferWriter) writeRowsInt8(rows array, size, offset uintptr, levels columnLevels) (err error) {
	if rows.len == 0 {
		return w.writeRowsNull(levels)
	}

	switch c := w.columns[levels.columnIndex].(type) {
	case *int32ColumnBuffer:
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			c.values = append(c.values, int32(*(*int8)(p)))
		}

	case *uint32ColumnBuffer:
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			c.values = append(c.values, uint32(*(*uint8)(p)))
		}

	default:
		w.reset()
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			v := makeValueInt32(int32(*(*int8)(p)))
			v.repetitionLevel = levels.repetitionLevel
			v.definitionLevel = levels.definitionLevel
			w.values = append(w.values, v)
		}
		_, err = w.columns[levels.columnIndex].WriteValues(w.values)
	}

	return err
}

func (w *columnBufferWriter) writeRowsInt16(rows array, size, offset uintptr, levels columnLevels) (err error) {
	if rows.len == 0 {
		return w.writeRowsNull(levels)
	}

	switch c := w.columns[levels.columnIndex].(type) {
	case *int32ColumnBuffer:
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			c.values = append(c.values, int32(*(*int16)(p)))
		}

	case *uint32ColumnBuffer:
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			c.values = append(c.values, uint32(*(*uint16)(p)))
		}

	default:
		w.reset()
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			v := makeValueInt32(int32(*(*int16)(p)))
			v.repetitionLevel = levels.repetitionLevel
			v.definitionLevel = levels.definitionLevel
			w.values = append(w.values, v)
		}
		_, err = w.columns[levels.columnIndex].WriteValues(w.values)
	}

	return err
}

func (w *columnBufferWriter) writeRowsInt32(rows array, size, offset uintptr, levels columnLevels) (err error) {
	if rows.len == 0 {
		return w.writeRowsNull(levels)
	}

	switch c := w.columns[levels.columnIndex].(type) {
	case *int32ColumnBuffer:
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			c.values = append(c.values, *(*int32)(p))
		}

	case *uint32ColumnBuffer:
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			c.values = append(c.values, *(*uint32)(p))
		}

	default:
		w.reset()
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			v := makeValueInt32(*(*int32)(p))
			v.repetitionLevel = levels.repetitionLevel
			v.definitionLevel = levels.definitionLevel
			w.values = append(w.values, v)
		}
		_, err = w.columns[levels.columnIndex].WriteValues(w.values)
	}

	return err
}

func (w *columnBufferWriter) writeRowsInt64(rows array, size, offset uintptr, levels columnLevels) (err error) {
	if rows.len == 0 {
		return w.writeRowsNull(levels)
	}

	switch c := w.columns[levels.columnIndex].(type) {
	case *int64ColumnBuffer:
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			c.values = append(c.values, *(*int64)(p))
		}

	case *uint64ColumnBuffer:
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			c.values = append(c.values, *(*uint64)(p))
		}

	default:
		w.reset()
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			v := makeValueInt64(*(*int64)(p))
			v.repetitionLevel = levels.repetitionLevel
			v.definitionLevel = levels.definitionLevel
			w.values = append(w.values, v)
		}
		_, err = w.columns[levels.columnIndex].WriteValues(w.values)
	}

	return err
}

func (w *columnBufferWriter) writeRowsInt96(rows array, size, offset uintptr, levels columnLevels) (err error) {
	if rows.len == 0 {
		return w.writeRowsNull(levels)
	}

	switch c := w.columns[levels.columnIndex].(type) {
	case *int96ColumnBuffer:
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			c.values = append(c.values, *(*deprecated.Int96)(p))
		}

	default:
		w.reset()
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			v := makeValueInt96(*(*deprecated.Int96)(p))
			v.repetitionLevel = levels.repetitionLevel
			v.definitionLevel = levels.definitionLevel
			w.values = append(w.values, v)
		}
		_, err = w.columns[levels.columnIndex].WriteValues(w.values)
	}

	return err
}

func (w *columnBufferWriter) writeRowsFloat32(rows array, size, offset uintptr, levels columnLevels) (err error) {
	if rows.len == 0 {
		return w.writeRowsNull(levels)
	}

	switch c := w.columns[levels.columnIndex].(type) {
	case *floatColumnBuffer:
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			c.values = append(c.values, *(*float32)(p))
		}

	default:
		w.reset()
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			v := makeValueFloat(*(*float32)(p))
			v.repetitionLevel = levels.repetitionLevel
			v.definitionLevel = levels.definitionLevel
			w.values = append(w.values, v)
		}
		_, err = w.columns[levels.columnIndex].WriteValues(w.values)
	}

	return err
}

func (w *columnBufferWriter) writeRowsFloat64(rows array, size, offset uintptr, levels columnLevels) (err error) {
	if rows.len == 0 {
		return w.writeRowsNull(levels)
	}

	switch c := w.columns[levels.columnIndex].(type) {
	case *doubleColumnBuffer:
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			c.values = append(c.values, *(*float64)(p))
		}

	default:
		w.reset()
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			v := makeValueDouble(*(*float64)(p))
			v.repetitionLevel = levels.repetitionLevel
			v.definitionLevel = levels.definitionLevel
			w.values = append(w.values, v)
		}
		_, err = w.columns[levels.columnIndex].WriteValues(w.values)
	}

	return err
}

func (w *columnBufferWriter) writeRowsString(rows array, size, offset uintptr, levels columnLevels) (err error) {
	if rows.len == 0 {
		return w.writeRowsNull(levels)
	}

	switch c := w.columns[levels.columnIndex].(type) {
	case *byteArrayColumnBuffer:
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			c.append(unsafecast.StringToBytes(*(*string)(p)))
		}
		return nil

	default:
		w.reset()
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			v := makeValueString(ByteArray, *(*string)(p))
			v.repetitionLevel = levels.repetitionLevel
			v.definitionLevel = levels.definitionLevel
			w.values = append(w.values, v)
		}
		_, err = w.columns[levels.columnIndex].WriteValues(w.values)
	}

	return err
}

func (w *columnBufferWriter) writeRowsUUID(rows array, size, offset uintptr, levels columnLevels) (err error) {
	if rows.len == 0 {
		return w.writeRowsNull(levels)
	}

	switch c := w.columns[levels.columnIndex].(type) {
	case *fixedLenByteArrayColumnBuffer:
		uuids := unsafecast.Slice[[16]byte](c.data)
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			uuids = append(uuids, *(*[16]byte)(p))
		}
		c.data = unsafecast.Slice[byte](uuids)
		return nil

	default:
		w.reset()
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			v := makeValueByteArray(FixedLenByteArray, (*byte)(p), 16)
			v.repetitionLevel = levels.repetitionLevel
			v.definitionLevel = levels.definitionLevel
			w.values = append(w.values, v)
		}
		_, err = w.columns[levels.columnIndex].WriteValues(w.values)
	}

	return err
}

func (w *columnBufferWriter) writeRowsArray(rows array, size, offset uintptr, levels columnLevels, len int) (err error) {
	if rows.len == 0 {
		return w.writeRowsNull(levels)
	}

	switch c := w.columns[levels.columnIndex].(type) {
	case *fixedLenByteArrayColumnBuffer:
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			c.data = append(c.data, unsafe.Slice((*byte)(p), len)...)
		}
		return nil

	default:
		w.reset()
		for i := 0; i < rows.len; i++ {
			p := rows.index(i, size, offset)
			v := makeValueByteArray(FixedLenByteArray, (*byte)(p), len)
			v.repetitionLevel = levels.repetitionLevel
			v.definitionLevel = levels.definitionLevel
			w.values = append(w.values, v)
		}
		_, err = w.columns[levels.columnIndex].WriteValues(w.values)
	}

	return err
}

func (w *columnBufferWriter) writeRowsNull(levels columnLevels) error {
	w.reset()
	w.values = append(w.values[:0], Value{
		repetitionLevel: levels.repetitionLevel,
		definitionLevel: levels.definitionLevel,
	})
	_, err := w.columns[levels.columnIndex].WriteValues(w.values)
	return err
}

func (w *columnBufferWriter) reset() {
	if len(w.values) > w.maxLen {
		w.maxLen = len(w.values)
	}
	w.values = w.values[:0]
}

func (w *columnBufferWriter) clear() {
	clearValues(w.values[:w.maxLen])
	w.maxLen = 0
}
