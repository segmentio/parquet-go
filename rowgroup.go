package parquet

import (
	"bytes"
	"sort"

	"github.com/segmentio/parquet/deprecated"
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

	if !isLeaf(node) {
		nullsFirst := false
		columnIndex := len(rg.columns)
		column := node.Type().NewRowGroupColumn(config.ColumnBufferSize)

		for _, ordering := range rg.sorting {
			if ordering.ColumnIdx == int32(columnIndex) {
				if ordering.Descending {
					column = &descendingRowGroupColumn{RowGroupColumn: column}
				}
				nullsFirst = ordering.NullsFirst
				break
			}
		}

		if definitionLevel > 0 {
			if nullsFirst {
				column = newOptionalRowGroupColumnNullsFirst(column, definitionLevel)
			} else {
				column = newOptionalRowGroupColumn(column, definitionLevel)
			}
		}

		if repetitionLevel > 0 {
			column = newRepeatedRowGroupColumn(column)
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
		if _, err := rg.columns[columnIndex].WriteValueBatch(values); err != nil {
			return err
		}
	}

	rg.numRows++
	return nil
}

type RowGroupColumn interface {
	Type() Type

	Size() int64

	Cap() int

	Len() int

	Less(i, j int) bool

	Swap(i, j int)

	//Pages(pageSize int) []Page

	Reset()

	ValueBatchWriter
}

type descendingRowGroupColumn struct {
	RowGroupColumn
}

func (col *descendingRowGroupColumn) Less(i, j int) bool {
	return col.RowGroupColumn.Less(j, i)
}

type optionalRowGroupColumn struct {
	base  RowGroupColumn
	def   []int8
	level int8
}

func newOptionalRowGroupColumn(base RowGroupColumn, definitionLevel int8) *optionalRowGroupColumn {
	return &optionalRowGroupColumn{
		base:  base,
		def:   make([]int8, 0, base.Cap()),
		level: definitionLevel,
	}
}

func (col *optionalRowGroupColumn) Type() Type { return col.base.Type() }

func (col *optionalRowGroupColumn) Reset() {
	col.base.Reset()
	col.def = col.def[:0]
}

func (col *optionalRowGroupColumn) Size() int64 { return col.base.Size() + int64(len(col.def)) }

func (col *optionalRowGroupColumn) Cap() int { return cap(col.def) }

func (col *optionalRowGroupColumn) Len() int { return len(col.def) }

func (col *optionalRowGroupColumn) Less(i, j int) bool {
	return !col.isNull(i) && (col.isNull(j) || col.base.Less(i, j))
}

func (col *optionalRowGroupColumn) Swap(i, j int) {
	col.base.Swap(i, j)
	col.def[i], col.def[j] = col.def[j], col.def[i]
}

func (col *optionalRowGroupColumn) WriteValueBatch(values []Value) (int, error) {
	n, err := col.base.WriteValueBatch(values)
	if err == nil {
		for _, v := range values {
			col.def = append(col.def, v.DefinitionLevel())
		}
	}
	return n, err
}

func (col *optionalRowGroupColumn) isNull(i int) bool { return col.def[i] != col.level }

type optionalRowGroupColumnNullsFirst struct{ *optionalRowGroupColumn }

func newOptionalRowGroupColumnNullsFirst(base RowGroupColumn, definitionLevel int8) optionalRowGroupColumnNullsFirst {
	return optionalRowGroupColumnNullsFirst{newOptionalRowGroupColumn(base, definitionLevel)}
}

func (col optionalRowGroupColumnNullsFirst) Less(i, j int) bool {
	return col.isNull(i) && (!col.isNull(j) || col.base.Less(i, j))
}

type repeatedRowGroupColumn struct {
	base RowGroupColumn
	rows []region
	rep  []int8
}

func newRepeatedRowGroupColumn(base RowGroupColumn) *repeatedRowGroupColumn {
	n := base.Cap()
	return &repeatedRowGroupColumn{
		base: base,
		rows: make([]region, 0, n/8),
		rep:  make([]int8, 0, n),
	}
}

func (col *repeatedRowGroupColumn) Type() Type { return col.base.Type() }

func (col *repeatedRowGroupColumn) Reset() {
	col.base.Reset()
	col.rows = col.rows[:0]
	col.rep = col.rep[:0]
}

func (col *repeatedRowGroupColumn) Size() int64 {
	return 8*int64(len(col.rows)) + int64(len(col.rep)) + col.base.Size()
}

func (col *repeatedRowGroupColumn) Cap() int { return cap(col.rows) }

func (col *repeatedRowGroupColumn) Len() int { return len(col.rows) }

func (col *repeatedRowGroupColumn) Less(i, j int) bool {
	row1 := col.rows[i]
	row2 := col.rows[j]

	for k := uint32(0); k < row1.length && k < row2.length; k++ {
		x := int(row1.offset + k)
		y := int(row2.offset + k)
		switch {
		case col.base.Less(x, y):
			return true
		case col.base.Less(y, x):
			return false
		}
	}

	return row1.length < row2.length
}

func (col *repeatedRowGroupColumn) Swap(i, j int) {
	col.rows[i], col.rows[j] = col.rows[j], col.rows[i]
}

func (col *repeatedRowGroupColumn) WriteValueBatch(values []Value) (int, error) {
	n, err := col.base.WriteValueBatch(values)
	if err == nil {
		col.rows = append(col.rows, region{
			offset: uint32(len(col.rep)),
			length: uint32(n),
		})
		for _, v := range values {
			col.rep = append(col.rep, v.RepetitionLevel())
		}
	}
	return n, err
}

type booleanRowGroupColumn struct {
	typ    Type
	values []bool
}

func newBooleanRowGroupColumn(typ Type, bufferSize int) *booleanRowGroupColumn {
	return &booleanRowGroupColumn{
		typ:    typ,
		values: make([]bool, 0, bufferSize),
	}
}

func (col *booleanRowGroupColumn) Type() Type { return col.typ }

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

func (col *booleanRowGroupColumn) WriteValueBatch(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Boolean())
	}
	return len(values), nil
}

type int32RowGroupColumn struct {
	typ    Type
	values []int32
}

func newInt32RowGroupColumn(typ Type, bufferSize int) *int32RowGroupColumn {
	return &int32RowGroupColumn{
		typ:    typ,
		values: make([]int32, 0, bufferSize/4),
	}
}

func (col *int32RowGroupColumn) Type() Type { return col.typ }

func (col *int32RowGroupColumn) Reset() { col.values = col.values[:0] }

func (col *int32RowGroupColumn) Size() int64 { return 4 * int64(len(col.values)) }

func (col *int32RowGroupColumn) Cap() int { return cap(col.values) }

func (col *int32RowGroupColumn) Len() int { return len(col.values) }

func (col *int32RowGroupColumn) Less(i, j int) bool { return col.values[i] < col.values[j] }

func (col *int32RowGroupColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *int32RowGroupColumn) WriteValueBatch(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Int32())
	}
	return len(values), nil
}

type int64RowGroupColumn struct {
	typ    Type
	values []int64
}

func newInt64RowGroupColumn(typ Type, bufferSize int) *int64RowGroupColumn {
	return &int64RowGroupColumn{
		typ:    typ,
		values: make([]int64, 0, bufferSize/8),
	}
}

func (col *int64RowGroupColumn) Type() Type { return col.typ }

func (col *int64RowGroupColumn) Reset() { col.values = col.values[:0] }

func (col *int64RowGroupColumn) Size() int64 { return 8 * int64(len(col.values)) }

func (col *int64RowGroupColumn) Cap() int { return cap(col.values) }

func (col *int64RowGroupColumn) Len() int { return len(col.values) }

func (col *int64RowGroupColumn) Less(i, j int) bool { return col.values[i] < col.values[j] }

func (col *int64RowGroupColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *int64RowGroupColumn) WriteValueBatch(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Int64())
	}
	return len(values), nil
}

type int96RowGroupColumn struct {
	typ    Type
	values []deprecated.Int96
}

func newInt96RowGroupColumn(typ Type, bufferSize int) *int96RowGroupColumn {
	return &int96RowGroupColumn{
		typ:    typ,
		values: make([]deprecated.Int96, 0, bufferSize/12),
	}
}

func (col *int96RowGroupColumn) Type() Type { return col.typ }

func (col *int96RowGroupColumn) Reset() { col.values = col.values[:0] }

func (col *int96RowGroupColumn) Size() int64 { return 12 * int64(len(col.values)) }

func (col *int96RowGroupColumn) Cap() int { return cap(col.values) }

func (col *int96RowGroupColumn) Len() int { return len(col.values) }

func (col *int96RowGroupColumn) Less(i, j int) bool { return col.values[i].Less(col.values[j]) }

func (col *int96RowGroupColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *int96RowGroupColumn) WriteValueBatch(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Int96())
	}
	return len(values), nil
}

type floatRowGroupColumn struct {
	typ    Type
	values []float32
}

func newFloatRowGroupColumn(typ Type, bufferSize int) *floatRowGroupColumn {
	return &floatRowGroupColumn{
		typ:    typ,
		values: make([]float32, 0, bufferSize/4),
	}
}

func (col *floatRowGroupColumn) Type() Type { return col.typ }

func (col *floatRowGroupColumn) Reset() { col.values = col.values[:0] }

func (col *floatRowGroupColumn) Size() int64 { return 4 * int64(len(col.values)) }

func (col *floatRowGroupColumn) Cap() int { return cap(col.values) }

func (col *floatRowGroupColumn) Len() int { return len(col.values) }

func (col *floatRowGroupColumn) Less(i, j int) bool { return col.values[i] < col.values[j] }

func (col *floatRowGroupColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *floatRowGroupColumn) WriteValueBatch(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Float())
	}
	return len(values), nil
}

type doubleRowGroupColumn struct {
	typ    Type
	values []float64
}

func newDoubleRowGroupColumn(typ Type, bufferSize int) *doubleRowGroupColumn {
	return &doubleRowGroupColumn{
		typ:    typ,
		values: make([]float64, 0, bufferSize/8),
	}
}

func (col *doubleRowGroupColumn) Type() Type { return col.typ }

func (col *doubleRowGroupColumn) Reset() { col.values = col.values[:0] }

func (col *doubleRowGroupColumn) Size() int64 { return 8 * int64(len(col.values)) }

func (col *doubleRowGroupColumn) Cap() int { return cap(col.values) }

func (col *doubleRowGroupColumn) Len() int { return len(col.values) }

func (col *doubleRowGroupColumn) Less(i, j int) bool { return col.values[i] < col.values[j] }

func (col *doubleRowGroupColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *doubleRowGroupColumn) WriteValueBatch(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.Double())
	}
	return len(values), nil
}

type byteArrayRowGroupColumn struct {
	typ    Type
	slices []region
	values []byte
}

type region struct {
	offset uint32
	length uint32
}

func newByteArrayRowGroupColumn(typ Type, bufferSize int) *byteArrayRowGroupColumn {
	const estimatedAverageValueLength = 16
	const sizeOfRegion = 8
	return &byteArrayRowGroupColumn{
		typ:    typ,
		slices: make([]region, 0, (bufferSize/estimatedAverageValueLength)/sizeOfRegion),
		values: make([]byte, 0, bufferSize-(bufferSize/estimatedAverageValueLength)),
	}
}

func (col *byteArrayRowGroupColumn) Type() Type { return col.typ }

func (col *byteArrayRowGroupColumn) Reset() {
	col.slices = col.slices[:0]
	col.values = col.values[:0]
}

func (col *byteArrayRowGroupColumn) Size() int64 {
	return 8*int64(len(col.slices)) + int64(len(col.values))
}

func (col *byteArrayRowGroupColumn) Cap() int { return cap(col.slices) }

func (col *byteArrayRowGroupColumn) Len() int { return len(col.slices) }

func (col *byteArrayRowGroupColumn) Less(i, j int) bool {
	return bytes.Compare(col.index(i), col.index(j)) < 0
}

func (col *byteArrayRowGroupColumn) Swap(i, j int) {
	col.slices[i], col.slices[j] = col.slices[j], col.slices[i]
}

func (col *byteArrayRowGroupColumn) index(i int) []byte {
	s := col.slices[i]
	return col.values[s.offset : s.offset+s.length]
}

func (col *byteArrayRowGroupColumn) WriteValueBatch(values []Value) (int, error) {
	for _, v := range values {
		b := v.ByteArray()
		s := region{
			offset: uint32(len(col.values)),
			length: uint32(len(b)),
		}
		col.slices = append(col.slices, s)
		col.values = append(col.values, b...)
	}
	return len(values), nil
}

type fixedLenByteArrayRowGroupColumn struct {
	size   uint
	typ    Type
	tmp    []byte
	values []byte
}

func newFixedLenByteArrayRowGroupColumn(typ Type, bufferSize int) *fixedLenByteArrayRowGroupColumn {
	size := uint(typ.Length())
	return &fixedLenByteArrayRowGroupColumn{
		size:   size,
		typ:    typ,
		tmp:    make([]byte, size),
		values: make([]byte, 0, bufferSize),
	}
}

func (col *fixedLenByteArrayRowGroupColumn) Type() Type { return col.typ }

func (col *fixedLenByteArrayRowGroupColumn) Reset() { col.values = col.values[:0] }

func (col *fixedLenByteArrayRowGroupColumn) Size() int64 { return int64(len(col.values)) }

func (col *fixedLenByteArrayRowGroupColumn) Cap() int { return cap(col.values) / int(col.size) }

func (col *fixedLenByteArrayRowGroupColumn) Len() int { return len(col.values) / int(col.size) }

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
	return col.values[uint(i)*col.size : uint(i+1)*col.size]
}

func (col *fixedLenByteArrayRowGroupColumn) WriteValueBatch(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, v.ByteArray()...)
	}
	return len(values), nil
}

type uint32RowGroupColumn struct{ *int32RowGroupColumn }

func newUint32RowGroupColumn(typ Type, bufferSize int) uint32RowGroupColumn {
	return uint32RowGroupColumn{newInt32RowGroupColumn(typ, bufferSize)}
}

func (col uint32RowGroupColumn) Less(i, j int) bool {
	return uint32(col.values[i]) < uint32(col.values[j])
}

type uint64RowGroupColumn struct{ *int64RowGroupColumn }

func newUint64RowGroupColumn(typ Type, bufferSize int) uint64RowGroupColumn {
	return uint64RowGroupColumn{newInt64RowGroupColumn(typ, bufferSize)}
}

func (col uint64RowGroupColumn) Less(i, j int) bool {
	return uint64(col.values[i]) < uint64(col.values[j])
}

var (
	_ sort.Interface = (*RowGroup)(nil)
	_ sort.Interface = (RowGroupColumn)(nil)
)
