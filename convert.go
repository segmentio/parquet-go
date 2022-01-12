package parquet

import (
	"fmt"
	"io"
	"sort"
)

// ConvertError is an error type returned by calls to Convert when the conversion
// of parquet schemas is impossible or the input row for the conversion is
// malformed.
type ConvertError struct {
	Reason string
	Path   []string
	From   Node
	To     Node
}

// Error satisfies the error interface.
func (e *ConvertError) Error() string {
	return fmt.Sprintf("parquet conversion error: %s %q", e.Reason, columnPath(e.Path))
}

// Conversion is an interface implemented by types that provide conversion of
// parquet rows from one schema to another.
//
// Conversion instances must be safe to use concurrently from multiple goroutines.
type Conversion interface {
	// Applies the conversion logic on the src row, returning the result
	// appended to dst.
	Convert(dst, src Row) (Row, error)
	// Converts the given column index in the target schema to the original
	// column index in the source schema of the conversion.
	Column(int) int
	// Returns the target schema of the conversion.
	Schema() *Schema
}

type conversion struct {
	convert convertFunc
	columns []int
	schema  *Schema
}

func (c *conversion) Convert(dst, src Row) (Row, error) {
	dst, src, err := c.convert(dst, src, levels{})
	if len(src) != 0 && err == nil {
		err = fmt.Errorf("%d values remain unused after converting parquet row", len(src))
	}
	return dst, err
}

func (c *conversion) Column(i int) int { return c.columns[i] }

func (c *conversion) Schema() *Schema { return c.schema }

// Convert constructs a conversion function from one parquet schema to another.
//
// The function supports converting between schemas where the source or target
// have extra columns; if there are more columns in the source, they will be
// stripped out of the rows. Extra columns in the target schema will be set to
// null or zero values.
//
// The returned function is intended to be used to append the converted soruce
// row to the destinatination buffer.
func Convert(to, from Node) (conv Conversion, err error) {
	defer func() {
		switch e := recover().(type) {
		case nil:
		case *ConvertError:
			err = e
		default:
			panic(e)
		}
	}()

	columns := make([]int, numColumnsOf(to))
	for i := range columns {
		columns[i] = -1
	}

	_, _, convertFunc := convert(convertNode{node: to}, convertNode{node: from}, columns)

	c := &conversion{
		convert: convertFunc,
		columns: columns,
	}
	if c.schema, _ = to.(*Schema); c.schema == nil {
		c.schema = NewSchema("", to)
	}
	return c, nil
}

type convertFunc func(Row, Row, levels) (Row, Row, error)

type convertNode struct {
	columnIndex int
	node        Node
	path        columnPath
}

func (c convertNode) child(name string) convertNode {
	c.node = c.node.ChildByName(name)
	c.path = c.path.append(name)
	return c
}

func convert(to, from convertNode, columns []int) (int, int, convertFunc) {
	switch {
	case from.node.Optional():
		if to.node.Optional() {
			return convertFuncOfOptional(to, from, columns)
		}
		panic(convertError(to, from, "cannot convert from optional to required column"))

	case from.node.Repeated():
		if to.node.Repeated() {
			return convertFuncOfRepeated(to, from, columns)
		}
		panic(convertError(to, from, "cannot convert from repeated to required column"))

	case to.node.Optional():
		panic(convertError(to, from, "cannot convert from required to optional column"))

	case to.node.Repeated():
		panic(convertError(to, from, "cannot convert from required to repeated column"))

	case isLeaf(from.node):
		if isLeaf(to.node) {
			return convertFuncOfLeaf(to, from, columns)
		}
		panic(convertError(to, from, "cannot convert from leaf to group column"))

	case isLeaf(to.node):
		panic(convertError(to, from, "cannot convert from group to leaf column"))

	default:
		return convertFuncOfGroup(to, from, columns)
	}
}

func convertError(to, from convertNode, reason string) *ConvertError {
	return &ConvertError{Reason: reason, Path: from.path, From: from.node, To: to.node}
}

//go:noinline
func convertFuncOfOptional(to, from convertNode, columns []int) (int, int, convertFunc) {
	to.node = Required(to.node)
	from.node = Required(from.node)

	toColumnIndex, fromColumnIndex, conv := convert(to, from, columns)
	return toColumnIndex, fromColumnIndex, func(dst, src Row, levels levels) (Row, Row, error) {
		levels.definitionLevel++
		return conv(dst, src, levels)
	}
}

//go:noinline
func convertFuncOfRepeated(to, from convertNode, columns []int) (int, int, convertFunc) {
	to.node = Required(to.node)
	from.node = Required(from.node)
	srcColumnIndex := ^int8(from.columnIndex)

	toColumnIndex, fromColumnIndex, conv := convert(to, from, columns)
	return toColumnIndex, fromColumnIndex, func(dst, src Row, levels levels) (Row, Row, error) {
		var err error

		levels.repetitionDepth++
		levels.definitionLevel++

		for len(src) > 0 && src[0].columnIndex == srcColumnIndex {
			if dst, src, err = conv(dst, src, levels); err != nil {
				break
			}
			levels.repetitionLevel = levels.repetitionDepth
		}

		return dst, src, err
	}
}

//go:noinline
func convertFuncOfLeaf(to, from convertNode, columns []int) (int, int, convertFunc) {
	if !typesAreEqual(to.node, from.node) {
		panic(convertError(to, from, fmt.Sprintf("unsupported type conversion from %s to %s for parquet column", from.node.Type(), to.node.Type())))
	}

	srcColumnIndex := ^int8(from.columnIndex)
	dstColumnIndex := ^int8(to.columnIndex)
	columns[to.columnIndex] = from.columnIndex

	return to.columnIndex + 1, from.columnIndex + 1, func(dst, src Row, levels levels) (Row, Row, error) {
		if len(src) == 0 || src[0].columnIndex != srcColumnIndex {
			return dst, src, convertError(to, from, "no value found in row for parquet column")
		}
		v := src[0]
		v.repetitionLevel = levels.repetitionLevel
		v.definitionLevel = levels.definitionLevel
		v.columnIndex = dstColumnIndex
		return append(dst, v), src[1:], nil
	}
}

//go:noinline
func convertFuncOfGroup(to, from convertNode, columns []int) (int, int, convertFunc) {
	extra, missing, names := comm(to.node.ChildNames(), from.node.ChildNames())
	funcs := make([]convertFunc, 0, len(extra)+len(missing)+len(names))

	for _, name := range merge(extra, missing, names) {
		var conv convertFunc
		switch {
		case contains(extra, name):
			to.columnIndex, conv = convertFuncOfExtraColumn(to.child(name))
		case contains(missing, name):
			from.columnIndex, conv = convertFuncOfMissingColumn(from.child(name))
		default:
			to.columnIndex, from.columnIndex, conv = convert(to.child(name), from.child(name), columns)
		}
		funcs = append(funcs, conv)
	}

	return to.columnIndex, from.columnIndex, makeGroupConvertFunc(funcs)
}

func makeGroupConvertFunc(funcs []convertFunc) convertFunc {
	return func(dst, src Row, levels levels) (Row, Row, error) {
		var err error

		for _, conv := range funcs {
			if dst, src, err = conv(dst, src, levels); err != nil {
				break
			}
		}

		return dst, src, err
	}
}

func convertFuncOfExtraColumn(to convertNode) (int, convertFunc) {
	switch {
	case isLeaf(to.node):
		return convertFuncOfExtraLeaf(to)
	default:
		return convertFuncOfExtraGroup(to)
	}
}

//go:noinline
func convertFuncOfExtraLeaf(to convertNode) (int, convertFunc) {
	kind := ^int8(to.node.Type().Kind())
	columnIndex := ^int8(to.columnIndex)
	return to.columnIndex + 1, func(dst, src Row, levels levels) (Row, Row, error) {
		dst = append(dst, Value{
			kind:            kind,
			repetitionLevel: levels.repetitionLevel,
			definitionLevel: levels.definitionLevel,
			columnIndex:     columnIndex,
		})
		return dst, src, nil
	}
}

//go:noinline
func convertFuncOfExtraGroup(to convertNode) (int, convertFunc) {
	names := to.node.ChildNames()
	funcs := make([]convertFunc, len(names))
	for i := range funcs {
		to.columnIndex, funcs[i] = convertFuncOfExtraColumn(to.child(names[i]))
	}
	return to.columnIndex, makeGroupConvertFunc(funcs)
}

//go:noinline
func convertFuncOfMissingColumn(from convertNode) (int, convertFunc) {
	rowLength := numColumnsOf(from.node)
	columnIndex := ^int8(from.columnIndex)
	return from.columnIndex + rowLength, func(dst, src Row, levels levels) (Row, Row, error) {
		for len(src) > 0 && src[0].columnIndex == columnIndex {
			if len(src) < rowLength {
				break
			}
			src = src[rowLength:]
		}
		return dst, src, nil
	}
}

func nodesAreEqual(node1, node2 Node) bool {
	if isLeaf(node1) {
		return isLeaf(node2) && leafNodesAreEqual(node1, node2)
	} else {
		return !isLeaf(node2) && groupNodesAreEqual(node1, node2)
	}
}

func typesAreEqual(node1, node2 Node) bool {
	return node1.Type().Kind() == node2.Type().Kind()
}

func repetitionsAreEqual(node1, node2 Node) bool {
	return node1.Optional() == node2.Optional() && node1.Repeated() == node2.Repeated()
}

func leafNodesAreEqual(node1, node2 Node) bool {
	return typesAreEqual(node1, node2) && repetitionsAreEqual(node1, node2)
}

func groupNodesAreEqual(node1, node2 Node) bool {
	names1 := node1.ChildNames()
	names2 := node2.ChildNames()

	if !stringsAreEqual(names1, names2) {
		return false
	}

	for _, name := range names1 {
		if !nodesAreEqual(node1.ChildByName(name), node2.ChildByName(name)) {
			return false
		}
	}

	return true
}

func comm(sortedStrings1, sortedStrings2 []string) (only1, only2, both []string) {
	i1 := 0
	i2 := 0

	for i1 < len(sortedStrings1) && i2 < len(sortedStrings2) {
		switch {
		case sortedStrings1[i1] < sortedStrings2[i2]:
			only1 = append(only1, sortedStrings1[i1])
			i1++
		case sortedStrings1[i1] > sortedStrings2[i2]:
			only2 = append(only2, sortedStrings2[i2])
			i2++
		default:
			both = append(both, sortedStrings1[i1])
			i1++
			i2++
		}
	}

	only1 = append(only1, sortedStrings1[i1:]...)
	only2 = append(only2, sortedStrings2[i2:]...)
	return only1, only2, both
}

func contains(sortedStrings []string, value string) bool {
	i := sort.Search(len(sortedStrings), func(i int) bool {
		return sortedStrings[i] >= value
	})
	return i < len(sortedStrings) && sortedStrings[i] == value
}

func merge(s1, s2, s3 []string) []string {
	merged := make([]string, 0, len(s1)+len(s2)+len(s3))
	merged = append(merged, s1...)
	merged = append(merged, s2...)
	merged = append(merged, s3...)
	sort.Strings(merged)
	return merged
}

// ConvertRowGroup constructs a wrapper of the given row group which appies
// the given schema conversion on its rows.
func ConvertRowGroup(rowGroup RowGroup, conv Conversion) RowGroup {
	schema := conv.Schema()
	numRows := rowGroup.NumRows()

	columns := make([]ColumnChunk, numColumnsOf(schema))
	forEachLeafColumnOf(schema, func(leaf leafColumn) {
		i := leaf.columnIndex
		j := conv.Column(leaf.columnIndex)
		if j < 0 {
			columns[i] = &missingColumnChunk{
				typ:    leaf.node.Type(),
				column: i,
				// TODO: we assume the number of values is the same as the
				// number of rows, which may not be accurate when the column is
				// part of a repeated group; neighbor columns may be repeated in
				// which case it would be impossible for this chunk not to be.
				numRows:   numRows,
				numValues: numRows,
				numNulls:  numRows,
			}
		} else {
			columns[i] = rowGroup.Column(j)
		}
	})

	// Sorting columns must exist on the conversion schema in order to be
	// advertised on the converted row group otherwise the resulting rows
	// would not be in the right order.
	sorting := []SortingColumn{}
	for _, col := range rowGroup.SortingColumns() {
		if !hasColumnPath(schema, col.Path()) {
			break
		}
		sorting = append(sorting, col)
	}

	return &convertedRowGroup{
		// The pair of rowGroup+conv is retained to construct a converted row
		// reader by wrapping the underlying row reader of the row group because
		// it allows proper reconstruction of the repetition and definition
		// levels.
		//
		// TODO: can we figure out how to set the repetition and definition
		// levels when reading values from missing column pages? At first sight
		// it appears complex to do, however:
		//
		// * It is possible that having these levels when reading values of
		//   missing column pages is not necessary in some scenarios (e.g. when
		//   merging row groups).
		//
		// * We may be able to assume the repetition and definition levels at
		//   the call site (e.g. in the functions reading rows from columns).
		//
		rowGroup: rowGroup,
		columns:  columns,
		sorting:  sorting,
		conv:     conv,
	}
}

type missingColumnChunk struct {
	typ       Type
	column    int
	numRows   int
	numValues int
	numNulls  int
}

func (c *missingColumnChunk) Type() Type {
	return c.typ
}

func (c *missingColumnChunk) Column() int {
	return c.column
}

func (c *missingColumnChunk) Pages() PageReader {
	return onePage(missingPage{c})
}

func (c *missingColumnChunk) ColumnIndex() ColumnIndex {
	return &columnIndex{
		NullPages:  []bool{true},
		MinValues:  [][]byte{nil},
		MaxValues:  [][]byte{nil},
		NullCounts: []int64{int64(c.numNulls)},
	}
}

func (c *missingColumnChunk) OffsetIndex() OffsetIndex { return missingOffsetIndex{} }

type missingOffsetIndex struct{}

func (missingOffsetIndex) NumPages() int                          { return 0 }
func (missingOffsetIndex) PageLocation(int) (int64, int64, int64) { return 0, 0, 0 }

type missingPage struct{ *missingColumnChunk }

func (p missingPage) Column() int              { return p.column }
func (p missingPage) Dictionary() Dictionary   { return nil }
func (p missingPage) NumRows() int             { return p.numRows }
func (p missingPage) NumValues() int           { return p.numValues }
func (p missingPage) NumNulls() int            { return p.numNulls }
func (p missingPage) Bounds() (min, max Value) { return }
func (p missingPage) Size() int64              { return 0 }
func (p missingPage) Values() ValueReader      { return &missingValues{page: p} }

type missingValues struct {
	page missingPage
	read int
}

func (r *missingValues) ReadValues(values []Value) (int, error) {
	remain := r.page.numValues - r.read
	if len(values) > remain {
		values = values[:remain]
	}
	for i := range values {
		// TODO: how do we set the repetition and definition levels here?
		values[i] = Value{columnIndex: ^int8(r.page.column)}
	}
	if r.read += len(values); r.read == r.page.numValues {
		return len(values), io.EOF
	}
	return len(values), nil
}

type convertedRowGroup struct {
	rowGroup RowGroup
	columns  []ColumnChunk
	sorting  []SortingColumn
	conv     Conversion
}

func (c *convertedRowGroup) NumRows() int                    { return c.rowGroup.NumRows() }
func (c *convertedRowGroup) NumColumns() int                 { return len(c.columns) }
func (c *convertedRowGroup) Column(i int) ColumnChunk        { return c.columns[i] }
func (c *convertedRowGroup) SortingColumns() []SortingColumn { return c.sorting }
func (c *convertedRowGroup) Rows() RowReader                 { return ConvertRowReader(c.rowGroup.Rows(), c.conv) }
func (c *convertedRowGroup) Schema() *Schema                 { return c.conv.Schema() }

// ConvertRowReader constructs a wrapper of the given row reader which applies
// the given schema conversion to the rows.
func ConvertRowReader(rows RowReader, conv Conversion) RowReaderWithSchema {
	return &convertedRowReader{rows: rows, conv: conv}
}

type convertedRowReader struct {
	rows RowReader
	buf  Row
	conv Conversion
}

func (c *convertedRowReader) ReadRow(row Row) (Row, error) {
	defer func() {
		clearValues(c.buf)
	}()
	var err error
	c.buf, err = c.rows.ReadRow(c.buf[:0])
	if err != nil {
		return row, err
	}
	return c.conv.Convert(row, c.buf)
}

func (c *convertedRowReader) Schema() *Schema {
	return c.conv.Schema()
}
