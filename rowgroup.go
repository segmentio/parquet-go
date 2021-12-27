package parquet

import (
	"bytes"
	"sort"

	"github.com/segmentio/parquet/deprecated"
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
)

type SortingColumn interface {
	Path() []string
	Descending() bool
	NullsFirst() bool
}

func Ascending(path ...string) SortingColumn { return ascending(copyPath(path)) }

func Descending(path ...string) SortingColumn { return descending(copyPath(path)) }

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

type RowGroup struct {
	values  [][]Value
	columns []RowGroupColumn
	sorting []format.SortingColumn
	numRows int
}

type RowGroupColumn interface {
	Type() Type

	Page() Page

	Reset()

	Size() int64

	Cap() int

	Len() int

	Less(i, j int) bool

	Swap(i, j int)

	ValueWriter
}

func NewRowGroup(schema Node, options ...RowGroupOption) *RowGroup {
	config := DefaultRowGroupConfig()
	config.Apply(schema, options...)
	if err := config.Validate(); err != nil {
		panic(err)
	}
	rg := &RowGroup{sorting: config.SortingColumns}
	rg.init(schema, 0, 0, config)
	rg.values = make([][]Value, len(rg.columns))
	return rg
}

func (rg *RowGroup) init(node Node, repetitionLevel, definitionLevel int8, config *RowGroupConfig) {
	switch {
	case node.Optional():
		definitionLevel++
	case node.Repeated():
		repetitionLevel++
		definitionLevel++
	}

	if isLeaf(node) {
		nullOrdering := nullsGoLast
		columnIndex := len(rg.columns)
		column := node.Type().NewRowGroupColumn(config.ColumnBufferSize)

		for _, ordering := range rg.sorting {
			if ordering.ColumnIdx == int32(columnIndex) {
				if ordering.Descending {
					column = &descendingRowGroupColumn{RowGroupColumn: column}
				}
				if ordering.NullsFirst {
					nullOrdering = nullsGoFirst
				}
				break
			}
		}

		switch {
		case repetitionLevel > 0:
			column = newRepeatedRowGroupColumn(column, repetitionLevel, definitionLevel, nullOrdering)
		case definitionLevel > 0:
			column = newOptionalRowGroupColumn(column, definitionLevel, nullOrdering)
		}

		rg.columns = append(rg.columns, column)
		return
	}

	for _, name := range node.ChildNames() {
		rg.init(node.ChildByName(name), repetitionLevel, definitionLevel, config)
	}
}

func (rg *RowGroup) Column(i int) RowGroupColumn { return rg.columns[i] }

func (rg *RowGroup) SortingColumns() []format.SortingColumn { return rg.sorting }

func (rg *RowGroup) Size() int64 {
	size := int64(0)
	for _, col := range rg.columns {
		size += col.Size()
	}
	return size
}

func (rg *RowGroup) Len() int { return rg.numRows }

func (rg *RowGroup) Less(i, j int) bool {
	for k := range rg.sorting {
		c := rg.columns[uint(rg.sorting[k].ColumnIdx)]
		switch {
		case c.Less(i, j):
			return true
		case c.Less(j, i): // not equal?
			return false
		}
	}
	return false
}

func (rg *RowGroup) Swap(i, j int) {
	for _, col := range rg.columns {
		col.Swap(i, j)
	}
}

func (rg *RowGroup) Reset() {
	for _, col := range rg.columns {
		col.Reset()
	}
	rg.numRows = 0
}

func (rg *RowGroup) WriteRow(row Row) error {
	defer func() {
		for i, values := range rg.values {
			for j := range values {
				values[j] = Value{}
			}
			rg.values[i] = values[:0]
		}
	}()

	for _, value := range row {
		if columnIndex := int(value.ColumnIndex()); columnIndex >= 0 && columnIndex < len(rg.values) {
			rg.values[columnIndex] = append(rg.values[columnIndex], value)
		}
	}

	for columnIndex, values := range rg.values {
		if _, err := rg.columns[columnIndex].WriteValues(values); err != nil {
			return err
		}
	}

	rg.numRows++
	return nil
}

type nullOrdering func(RowGroupColumn, int, int, int8, []int8) bool

func nullsGoFirst(column RowGroupColumn, i, j int, maxDefinitionLevel int8, definitionLevels []int8) bool {
	if isNull(i, maxDefinitionLevel, definitionLevels) {
		return !isNull(j, maxDefinitionLevel, definitionLevels)
	} else {
		return !isNull(j, maxDefinitionLevel, definitionLevels) && column.Less(i, j)
	}
}

func nullsGoLast(column RowGroupColumn, i, j int, maxDefinitionLevel int8, definitionLevels []int8) bool {
	if isNull(i, maxDefinitionLevel, definitionLevels) {
		return false
	} else {
		return isNull(j, maxDefinitionLevel, definitionLevels) || column.Less(i, j)
	}
}

func isNull(i int, maxDefinitionLevel int8, definitionLevels []int8) bool {
	return definitionLevels[i] != maxDefinitionLevel
}

func rowGroupColumnPageWithLevels(column RowGroupColumn, maxRepetitionLevel, maxDefinitionLevel int8, repetitionLevels, definitionLevels []int8) Page {
	n := 0
	for i := 0; i < len(definitionLevels); {
		j := i
		for j < len(definitionLevels) && isNull(j, maxDefinitionLevel, definitionLevels) {
			j++
		}
		if j < len(definitionLevels) {
			if i != j {
				column.Swap(n, n+(j-i))
			}
			n++
		}
		i = j + 1
	}
	return newPageWithLevels(column.Page().Slice(0, n), maxRepetitionLevel, maxDefinitionLevel, repetitionLevels, definitionLevels)
}

type descendingRowGroupColumn struct{ RowGroupColumn }

func (col *descendingRowGroupColumn) Less(i, j int) bool { return col.RowGroupColumn.Less(j, i) }

type optionalRowGroupColumn struct {
	base               RowGroupColumn
	maxDefinitionLevel int8
	definitionLevels   []int8
	nullOrdering       nullOrdering
}

func newOptionalRowGroupColumn(base RowGroupColumn, maxDefinitionLevel int8, nullOrdering nullOrdering) *optionalRowGroupColumn {
	return &optionalRowGroupColumn{
		base:               base,
		maxDefinitionLevel: maxDefinitionLevel,
		definitionLevels:   make([]int8, 0, base.Cap()),
		nullOrdering:       nullOrdering,
	}
}

func (col *optionalRowGroupColumn) Type() Type { return col.base.Type() }

func (col *optionalRowGroupColumn) Page() Page {
	return rowGroupColumnPageWithLevels(col.base, 0, col.maxDefinitionLevel, nil, col.definitionLevels)
}

func (col *optionalRowGroupColumn) Reset() {
	col.base.Reset()
	col.definitionLevels = col.definitionLevels[:0]
}

func (col *optionalRowGroupColumn) Size() int64 {
	return col.base.Size() + int64(len(col.definitionLevels))
}

func (col *optionalRowGroupColumn) Cap() int { return cap(col.definitionLevels) }

func (col *optionalRowGroupColumn) Len() int { return len(col.definitionLevels) }

func (col *optionalRowGroupColumn) Less(i, j int) bool {
	return col.nullOrdering(col.base, i, j, col.maxDefinitionLevel, col.definitionLevels)
}

func (col *optionalRowGroupColumn) Swap(i, j int) {
	col.base.Swap(i, j)
	col.definitionLevels[i], col.definitionLevels[j] = col.definitionLevels[j], col.definitionLevels[i]
}

func (col *optionalRowGroupColumn) WriteValues(values []Value) (int, error) {
	n, err := col.base.WriteValues(values)
	if err == nil {
		for _, v := range values {
			col.definitionLevels = append(col.definitionLevels, v.DefinitionLevel())
		}
	}
	return n, err
}

type repeatedRowGroupColumn struct {
	base               RowGroupColumn
	maxRepetitionLevel int8
	maxDefinitionLevel int8
	rows               []region
	repetitionLevels   []int8
	definitionLevels   []int8
	nullOrdering       nullOrdering
}

type region struct {
	offset uint32
	length uint32
}

func newRepeatedRowGroupColumn(base RowGroupColumn, maxRepetitionLevel, maxDefinitionLevel int8, nullOrdering nullOrdering) *repeatedRowGroupColumn {
	n := base.Cap()
	return &repeatedRowGroupColumn{
		base:               base,
		maxRepetitionLevel: maxRepetitionLevel,
		maxDefinitionLevel: maxDefinitionLevel,
		rows:               make([]region, 0, n/8),
		repetitionLevels:   make([]int8, 0, n),
		definitionLevels:   make([]int8, 0, n),
		nullOrdering:       nullOrdering,
	}
}

func (col *repeatedRowGroupColumn) Type() Type { return col.base.Type() }

func (col *repeatedRowGroupColumn) Page() Page {
	return rowGroupColumnPageWithLevels(col.base, col.maxRepetitionLevel, col.maxDefinitionLevel, col.repetitionLevels, col.definitionLevels)
}

func (col *repeatedRowGroupColumn) Reset() {
	col.base.Reset()
	col.rows = col.rows[:0]
	col.repetitionLevels = col.repetitionLevels[:0]
	col.definitionLevels = col.definitionLevels[:0]
}

func (col *repeatedRowGroupColumn) Size() int64 {
	return 8*int64(len(col.rows)) + int64(len(col.repetitionLevels)) + int64(len(col.definitionLevels)) + col.base.Size()
}

func (col *repeatedRowGroupColumn) Cap() int { return cap(col.rows) }

func (col *repeatedRowGroupColumn) Len() int { return len(col.rows) }

func (col *repeatedRowGroupColumn) Less(i, j int) bool {
	row1 := col.rows[i]
	row2 := col.rows[j]
	less := col.nullOrdering

	for k := uint32(0); k < row1.length && k < row2.length; k++ {
		x := int(row1.offset + k)
		y := int(row2.offset + k)
		switch {
		case less(col.base, x, y, col.maxDefinitionLevel, col.definitionLevels):
			return true
		case less(col.base, y, x, col.maxDefinitionLevel, col.definitionLevels):
			return false
		}
	}

	return row1.length < row2.length
}

func (col *repeatedRowGroupColumn) Swap(i, j int) {
	col.rows[i], col.rows[j] = col.rows[j], col.rows[i]
}

func (col *repeatedRowGroupColumn) WriteValues(values []Value) (int, error) {
	n, err := col.base.WriteValues(values)
	if err == nil {
		col.rows = append(col.rows, region{
			offset: uint32(len(col.repetitionLevels)),
			length: uint32(n),
		})
		for _, v := range values {
			col.repetitionLevels = append(col.repetitionLevels, v.RepetitionLevel())
			col.definitionLevels = append(col.definitionLevels, v.DefinitionLevel())
		}
	}
	return n, err
}

func splitRange(length, count int, do func(i, j, k int)) {
	for i, j := 0, 0; count > 0; i++ {
		n := (length - j) / count
		do(i, j, j+n)
		j += n
		count--
	}
}

type booleanRowGroupColumn struct{ booleanPage }

func newBooleanRowGroupColumn(typ Type, bufferSize int) *booleanRowGroupColumn {
	return &booleanRowGroupColumn{
		booleanPage: booleanPage{
			typ:    typ,
			values: make([]bool, 0, bufferSize),
		},
	}
}

func (col *booleanRowGroupColumn) Type() Type { return col.typ }

func (col *booleanRowGroupColumn) Page() Page { return &col.booleanPage }

func (col *booleanRowGroupColumn) Reset() { col.values = col.values[:0] }

func (col *booleanRowGroupColumn) Size() int64 { return int64(len(col.values)) }

func (col *booleanRowGroupColumn) Cap() int { return cap(col.values) }

func (col *booleanRowGroupColumn) Len() int { return len(col.values) }

func (col *booleanRowGroupColumn) Less(i, j int) bool {
	return col.values[i] != col.values[j] && !col.values[i]
}

func (col *booleanRowGroupColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *booleanRowGroupColumn) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Boolean())
	}
	return len(values), nil
}

type int32RowGroupColumn struct{ int32Page }

func newInt32RowGroupColumn(typ Type, bufferSize int) *int32RowGroupColumn {
	return &int32RowGroupColumn{
		int32Page: int32Page{
			typ:    typ,
			values: make([]int32, 0, bufferSize/4),
		},
	}
}

func (col *int32RowGroupColumn) Type() Type { return col.typ }

func (col *int32RowGroupColumn) Page() Page { return &col.int32Page }

func (col *int32RowGroupColumn) Reset() { col.values = col.values[:0] }

func (col *int32RowGroupColumn) Size() int64 { return 4 * int64(len(col.values)) }

func (col *int32RowGroupColumn) Cap() int { return cap(col.values) }

func (col *int32RowGroupColumn) Len() int { return len(col.values) }

func (col *int32RowGroupColumn) Less(i, j int) bool { return col.values[i] < col.values[j] }

func (col *int32RowGroupColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *int32RowGroupColumn) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Int32())
	}
	return len(values), nil
}

type int64RowGroupColumn struct{ int64Page }

func newInt64RowGroupColumn(typ Type, bufferSize int) *int64RowGroupColumn {
	return &int64RowGroupColumn{
		int64Page: int64Page{
			typ:    typ,
			values: make([]int64, 0, bufferSize/8),
		},
	}
}

func (col *int64RowGroupColumn) Type() Type { return col.typ }

func (col *int64RowGroupColumn) Page() Page { return &col.int64Page }

func (col *int64RowGroupColumn) Reset() { col.values = col.values[:0] }

func (col *int64RowGroupColumn) Size() int64 { return 8 * int64(len(col.values)) }

func (col *int64RowGroupColumn) Cap() int { return cap(col.values) }

func (col *int64RowGroupColumn) Len() int { return len(col.values) }

func (col *int64RowGroupColumn) Less(i, j int) bool { return col.values[i] < col.values[j] }

func (col *int64RowGroupColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *int64RowGroupColumn) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Int64())
	}
	return len(values), nil
}

type int96RowGroupColumn struct{ int96Page }

func newInt96RowGroupColumn(typ Type, bufferSize int) *int96RowGroupColumn {
	return &int96RowGroupColumn{
		int96Page: int96Page{
			typ:    typ,
			values: make([]deprecated.Int96, 0, bufferSize/12),
		},
	}
}

func (col *int96RowGroupColumn) Type() Type { return col.typ }

func (col *int96RowGroupColumn) Page() Page { return &col.int96Page }

func (col *int96RowGroupColumn) Reset() { col.values = col.values[:0] }

func (col *int96RowGroupColumn) Size() int64 { return 12 * int64(len(col.values)) }

func (col *int96RowGroupColumn) Cap() int { return cap(col.values) }

func (col *int96RowGroupColumn) Len() int { return len(col.values) }

func (col *int96RowGroupColumn) Less(i, j int) bool { return col.values[i].Less(col.values[j]) }

func (col *int96RowGroupColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *int96RowGroupColumn) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Int96())
	}
	return len(values), nil
}

type floatRowGroupColumn struct{ floatPage }

func newFloatRowGroupColumn(typ Type, bufferSize int) *floatRowGroupColumn {
	return &floatRowGroupColumn{
		floatPage: floatPage{
			typ:    typ,
			values: make([]float32, 0, bufferSize/4),
		},
	}
}

func (col *floatRowGroupColumn) Type() Type { return col.typ }

func (col *floatRowGroupColumn) Page() Page { return &col.floatPage }

func (col *floatRowGroupColumn) Reset() { col.values = col.values[:0] }

func (col *floatRowGroupColumn) Size() int64 { return 4 * int64(len(col.values)) }

func (col *floatRowGroupColumn) Cap() int { return cap(col.values) }

func (col *floatRowGroupColumn) Len() int { return len(col.values) }

func (col *floatRowGroupColumn) Less(i, j int) bool { return col.values[i] < col.values[j] }

func (col *floatRowGroupColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *floatRowGroupColumn) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Float())
	}
	return len(values), nil
}

type doubleRowGroupColumn struct{ doublePage }

func newDoubleRowGroupColumn(typ Type, bufferSize int) *doubleRowGroupColumn {
	return &doubleRowGroupColumn{
		doublePage: doublePage{
			typ:    typ,
			values: make([]float64, 0, bufferSize/8),
		},
	}
}

func (col *doubleRowGroupColumn) Type() Type { return col.typ }

func (col *doubleRowGroupColumn) Page() Page { return &col.doublePage }

func (col *doubleRowGroupColumn) Reset() { col.values = col.values[:0] }

func (col *doubleRowGroupColumn) Size() int64 { return 8 * int64(len(col.values)) }

func (col *doubleRowGroupColumn) Cap() int { return cap(col.values) }

func (col *doubleRowGroupColumn) Len() int { return len(col.values) }

func (col *doubleRowGroupColumn) Less(i, j int) bool { return col.values[i] < col.values[j] }

func (col *doubleRowGroupColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *doubleRowGroupColumn) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Double())
	}
	return len(values), nil
}

type byteArrayRowGroupColumn struct{ byteArrayPage }

func newByteArrayRowGroupColumn(typ Type, bufferSize int) *byteArrayRowGroupColumn {
	return &byteArrayRowGroupColumn{
		byteArrayPage: byteArrayPage{
			typ:    typ,
			values: encoding.MakeByteArrayList(bufferSize / 16),
		},
	}
}

func (col *byteArrayRowGroupColumn) Type() Type { return col.typ }

func (col *byteArrayRowGroupColumn) Page() Page { return &col.byteArrayPage }

func (col *byteArrayRowGroupColumn) Reset() { col.values.Reset() }

func (col *byteArrayRowGroupColumn) Size() int64 { return col.values.Size() }

func (col *byteArrayRowGroupColumn) Cap() int { return col.values.Cap() }

func (col *byteArrayRowGroupColumn) Len() int { return col.values.Len() }

func (col *byteArrayRowGroupColumn) Less(i, j int) bool { return col.values.Less(i, j) }

func (col *byteArrayRowGroupColumn) Swap(i, j int) { col.values.Swap(i, j) }

func (col *byteArrayRowGroupColumn) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values.Push(v.ByteArray())
	}
	return len(values), nil
}

type fixedLenByteArrayRowGroupColumn struct {
	fixedLenByteArrayPage
	tmp []byte
}

func newFixedLenByteArrayRowGroupColumn(typ Type, bufferSize int) *fixedLenByteArrayRowGroupColumn {
	size := typ.Length()
	return &fixedLenByteArrayRowGroupColumn{
		fixedLenByteArrayPage: fixedLenByteArrayPage{
			size: size,
			typ:  typ,
			data: make([]byte, 0, bufferSize),
		},
		tmp: make([]byte, size),
	}
}

func (col *fixedLenByteArrayRowGroupColumn) Type() Type { return col.typ }

func (col *fixedLenByteArrayRowGroupColumn) Page() Page { return &col.fixedLenByteArrayPage }

func (col *fixedLenByteArrayRowGroupColumn) Reset() { col.data = col.data[:0] }

func (col *fixedLenByteArrayRowGroupColumn) Size() int64 { return int64(len(col.data)) }

func (col *fixedLenByteArrayRowGroupColumn) Cap() int { return cap(col.data) / col.size }

func (col *fixedLenByteArrayRowGroupColumn) Len() int { return len(col.data) / col.size }

func (col *fixedLenByteArrayRowGroupColumn) Less(i, j int) bool {
	return bytes.Compare(col.index(i), col.index(j)) < 0
}

func (col *fixedLenByteArrayRowGroupColumn) Swap(i, j int) {
	t, u, v := col.tmp[:col.size], col.index(i), col.index(j)
	copy(t, u)
	copy(u, v)
	copy(v, t)
}

func (col *fixedLenByteArrayRowGroupColumn) index(i int) []byte {
	j := (i + 0) * col.size
	k := (i + 1) * col.size
	return col.data[j:k:k]
}

func (col *fixedLenByteArrayRowGroupColumn) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.data = append(col.data, v.ByteArray()...)
	}
	return len(values), nil
}

type uint32RowGroupColumn struct{ *int32RowGroupColumn }

func newUint32RowGroupColumn(typ Type, bufferSize int) uint32RowGroupColumn {
	return uint32RowGroupColumn{newInt32RowGroupColumn(typ, bufferSize)}
}

func (col uint32RowGroupColumn) Page() Page {
	return uint32Page{&col.int32Page}
}

func (col uint32RowGroupColumn) Less(i, j int) bool {
	return uint32(col.values[i]) < uint32(col.values[j])
}

type uint64RowGroupColumn struct{ *int64RowGroupColumn }

func newUint64RowGroupColumn(typ Type, bufferSize int) uint64RowGroupColumn {
	return uint64RowGroupColumn{newInt64RowGroupColumn(typ, bufferSize)}
}

func (col uint64RowGroupColumn) Page() Page {
	return uint64Page{&col.int64Page}
}

func (col uint64RowGroupColumn) Less(i, j int) bool {
	return uint64(col.values[i]) < uint64(col.values[j])
}

var (
	_ sort.Interface = (*RowGroup)(nil)
	_ sort.Interface = (RowGroupColumn)(nil)
)
