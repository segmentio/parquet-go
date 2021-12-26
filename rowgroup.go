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
}

func Ascending(path ...string) SortingColumn { return ascending(path) }

func Descending(path ...string) SortingColumn { return descending(path) }

type ascending []string

func (asc ascending) Path() []string   { return asc }
func (asc ascending) Descending() bool { return false }

type descending []string

func (desc descending) Path() []string   { return desc }
func (desc descending) Descending() bool { return true }

type RowGroup struct {
	columns []RowGroupColumn
	orderBy []format.SortingColumn
	numRows int
}

func NewRowGroup(schema Node, orderBy ...SortingColumn) *RowGroup {
	rg := &RowGroup{
		orderBy: make([]format.SortingColumn, 0, len(orderBy)),
	}
	rg.init(schema, make([]string, 0, 16), orderBy, 0, 0)
	return rg
}

func pathEqual(p1, p2 []string) bool {
	if len(p1) != len(p2) {
		return false
	}
	for i := range p1 {
		if p1[i] != p2[i] {
			return false
		}
	}
	return true
}

func (rg *RowGroup) init(node Node, path []string, orderBy []SortingColumn, repetitionLevel, definitionLevel int8) {
	optional := node.Optional()
	repeated := node.Repeated()

	switch {
	case optional:
		definitionLevel++
	case repeated:
		repetitionLevel++
		definitionLevel++
	}

	if node.NumChildren() == 0 {
		var ordering SortingColumn
		var column RowGroupColumn

		for _, sort := range orderBy {
			if pathEqual(sort.Path(), path) {
				ordering = sort
			}
		}

		//
		switch {
		case optional:
			column = newOptionalRowGroupColumn(column, definitionLevel)
		case repeated:
			//
		}

		if ordering != nil {
			descending := ordering.Descending()
			if descending {
				column = &descendingRowGroupColumn{RowGroupColumn: column}
			}
			rg.orderBy = append(rg.orderBy, format.SortingColumn{
				ColumnIdx:  int32(len(rg.columns)),
				Descending: descending,
			})
		}

		rg.columns = append(rg.columns, column)
	} else {
		i := len(path)
		path = append(path, "")

		for _, name := range node.ChildNames() {
			path[i] = name
			rg.init(node, path, orderBy, definitionLevel, repetitionLevel)
		}
	}
}

func (rg *RowGroup) Len() int {
	return rg.numRows
}

func (rg *RowGroup) Less(i, j int) bool {
	for k := range rg.orderBy {
		c := rg.columns[uint(rg.orderBy[k].ColumnIdx)]
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

func (rg *RowGroup) WriteRow(row Row) error {
	for _, v := range row {
		col := rg.columns[v.ColumnIndex()]
		col.Append(v)
	}
	rg.numRows++
	return nil
}

type RowGroupColumn interface {
	sort.Interface

	Type() Type

	Cap() int

	Append(Value)

	Index(int) Value

	Reset()
}

type descendingRowGroupColumn struct {
	RowGroupColumn
}

func (col *descendingRowGroupColumn) Less(i, j int) bool {
	return col.RowGroupColumn.Less(j, i)
}

type optionalRowGroupColumn struct {
	base RowGroupColumn
	def  []int8
	lvl  int8
}

func newOptionalRowGroupColumn(base RowGroupColumn, definitionLevel int8) *optionalRowGroupColumn {
	return &optionalRowGroupColumn{
		base: base,
		def:  make([]int8, 0, base.Cap()),
		lvl:  definitionLevel,
	}
}

func (col *optionalRowGroupColumn) Type() Type {
	return col.base.Type()
}

func (col *optionalRowGroupColumn) Reset() {
	col.base.Reset()
	col.def = col.def[:0]
}

func (col *optionalRowGroupColumn) Append(v Value) {
	col.base.Append(v)
	col.def = append(col.def, v.DefinitionLevel())
}

func (col *optionalRowGroupColumn) Index(i int) Value {
	return col.base.Index(i).Level(0, col.def[i])
}

func (col *optionalRowGroupColumn) Cap() int { return cap(col.def) }

func (col *optionalRowGroupColumn) Len() int { return len(col.def) }

func (col *optionalRowGroupColumn) Less(i, j int) bool {
	return !col.isNull(i) && (col.isNull(j) || col.base.Less(i, j))
}

func (col *optionalRowGroupColumn) Swap(i, j int) {
	col.base.Swap(i, j)
	col.def[i], col.def[j] = col.def[j], col.def[i]
}

func (col *optionalRowGroupColumn) isNull(i int) bool {
	return col.def[i] != col.lvl
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

func (col *booleanRowGroupColumn) Type() Type {
	return col.typ
}

func (col *booleanRowGroupColumn) Reset() {
	col.values = col.values[:0]
}

func (col *booleanRowGroupColumn) Append(v Value) {
	col.values = append(col.values, v.Boolean())
}

func (col *booleanRowGroupColumn) Index(i int) Value {
	return makeValueBoolean(col.values[i])
}

func (col *booleanRowGroupColumn) Cap() int {
	return cap(col.values)
}

func (col *booleanRowGroupColumn) Len() int {
	return len(col.values)
}

func (col *booleanRowGroupColumn) Less(i, j int) bool {
	return col.values[i] != col.values[j] && !col.values[i]
}

func (col *booleanRowGroupColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
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

func (col *int32RowGroupColumn) Type() Type {
	return col.typ
}

func (col *int32RowGroupColumn) Reset() {
	col.values = col.values[:0]
}

func (col *int32RowGroupColumn) Append(v Value) {
	col.values = append(col.values, v.Int32())
}

func (col *int32RowGroupColumn) Index(i int) Value {
	return makeValueInt32(col.values[i])
}

func (col *int32RowGroupColumn) Cap() int {
	return cap(col.values)
}

func (col *int32RowGroupColumn) Len() int {
	return len(col.values)
}

func (col *int32RowGroupColumn) Less(i, j int) bool {
	return col.values[i] < col.values[j]
}

func (col *int32RowGroupColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
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

func (col *int64RowGroupColumn) Type() Type {
	return col.typ
}

func (col *int64RowGroupColumn) Reset() {
	col.values = col.values[:0]
}

func (col *int64RowGroupColumn) Append(v Value) {
	col.values = append(col.values, v.Int64())
}

func (col *int64RowGroupColumn) Index(i int) Value {
	return makeValueInt64(col.values[i])
}

func (col *int64RowGroupColumn) Cap() int {
	return cap(col.values)
}

func (col *int64RowGroupColumn) Len() int {
	return len(col.values)
}

func (col *int64RowGroupColumn) Less(i, j int) bool {
	return col.values[i] < col.values[j]
}

func (col *int64RowGroupColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
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

func (col *int96RowGroupColumn) Type() Type {
	return col.typ
}

func (col *int96RowGroupColumn) Reset() {
	col.values = col.values[:0]
}

func (col *int96RowGroupColumn) Append(v Value) {
	col.values = append(col.values, v.Int96())
}

func (col *int96RowGroupColumn) Index(i int) Value {
	return makeValueInt96(col.values[i])
}

func (col *int96RowGroupColumn) Cap() int {
	return cap(col.values)
}

func (col *int96RowGroupColumn) Len() int {
	return len(col.values)
}

func (col *int96RowGroupColumn) Less(i, j int) bool {
	return col.values[i].Less(col.values[j])
}

func (col *int96RowGroupColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
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

func (col *floatRowGroupColumn) Type() Type {
	return col.typ
}

func (col *floatRowGroupColumn) Reset() {
	col.values = col.values[:0]
}

func (col *floatRowGroupColumn) Append(v Value) {
	col.values = append(col.values, v.Float())
}

func (col *floatRowGroupColumn) Index(i int) Value {
	return makeValueFloat(col.values[i])
}

func (col *floatRowGroupColumn) Cap() int {
	return cap(col.values)
}

func (col *floatRowGroupColumn) Len() int {
	return len(col.values)
}

func (col *floatRowGroupColumn) Less(i, j int) bool {
	return col.values[i] < col.values[j]
}

func (col *floatRowGroupColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
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

func (col *doubleRowGroupColumn) Type() Type {
	return col.typ
}

func (col *doubleRowGroupColumn) Reset() {
	col.values = col.values[:0]
}

func (col *doubleRowGroupColumn) Append(v Value) {
	col.values = append(col.values, v.Double())
}

func (col *doubleRowGroupColumn) Index(i int) Value {
	return makeValueDouble(col.values[i])
}

func (col *doubleRowGroupColumn) Cap() int {
	return cap(col.values)
}

func (col *doubleRowGroupColumn) Len() int {
	return len(col.values)
}

func (col *doubleRowGroupColumn) Less(i, j int) bool {
	return col.values[i] < col.values[j]
}

func (col *doubleRowGroupColumn) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
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

func (col *byteArrayRowGroupColumn) Type() Type {
	return col.typ
}

func (col *byteArrayRowGroupColumn) Reset() {
	col.slices = col.slices[:0]
	col.values = col.values[:0]
}

func (col *byteArrayRowGroupColumn) Append(v Value) {
	b := v.ByteArray()
	s := region{
		offset: uint32(len(col.values)),
		length: uint32(len(b)),
	}
	col.slices = append(col.slices, s)
	col.values = append(col.values, b...)
}

func (col *byteArrayRowGroupColumn) Index(i int) Value {
	return makeValueBytes(FixedLenByteArray, col.index(i))
}

func (col *byteArrayRowGroupColumn) Cap() int {
	return cap(col.slices)
}

func (col *byteArrayRowGroupColumn) Len() int {
	return len(col.slices)
}

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

func (col *fixedLenByteArrayRowGroupColumn) Type() Type {
	return col.typ
}

func (col *fixedLenByteArrayRowGroupColumn) Reset() {
	col.values = col.values[:0]
}

func (col *fixedLenByteArrayRowGroupColumn) Append(v Value) {
	col.values = append(col.values, v.ByteArray()...)
}

func (col *fixedLenByteArrayRowGroupColumn) Index(i int) Value {
	return makeValueBytes(FixedLenByteArray, col.index(i))
}

func (col *fixedLenByteArrayRowGroupColumn) Cap() int {
	return cap(col.values) / int(col.size)
}

func (col *fixedLenByteArrayRowGroupColumn) Len() int {
	return len(col.values) / int(col.size)
}

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
