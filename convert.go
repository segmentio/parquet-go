package parquet

import (
	"fmt"
	"sort"
)

// ConvertErorr is an error type returned by calls to Convert when the conversion
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
	return fmt.Sprintf("parquet conversion error: %s %q", e.Reason, join(e.Path))
}

// Conversion is an interface implemented by types that provide conversion of
// parquet rows from one schema to another.
//
// Conversion instances must be safe to use concurrently from multiple goroutines.
type Conversion interface {
	// Applies the conversion logic on the src row, returning the result
	// appended to dst.
	Convert(dst, src Row) (Row, error)
	// Returns the target schema of the conversion.
	Schema() *Schema
}

type conversion struct {
	convert convertFunc
	schema  *Schema
}

func (c *conversion) Convert(dst, src Row) (Row, error) {
	dst, src, err := c.convert(dst, src, levels{})
	if len(src) != 0 && err == nil {
		err = fmt.Errorf("%d values remain unused after converting parquet row", len(src))
	}
	return dst, err
}

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

	_, _, convertFunc := convert(convertNode{node: to}, convertNode{node: from})

	c := &conversion{
		convert: convertFunc,
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
	path        []string
}

func (c convertNode) child(name string) convertNode {
	c.node = c.node.ChildByName(name)
	c.path = appendPath(c.path, name)
	return c
}

func appendPath(path []string, name string) []string {
	return append(path[:len(path):len(path)], name)
}

func convert(to, from convertNode) (int, int, convertFunc) {
	switch {
	case from.node.Optional():
		if to.node.Optional() {
			return convertFuncOfOptional(to, from)
		}
		panic(convertError(to, from, "cannot convert from optional to required column"))

	case from.node.Repeated():
		if to.node.Repeated() {
			return convertFuncOfRepeated(to, from)
		}
		panic(convertError(to, from, "cannot convert from repeated to required column"))

	case to.node.Optional():
		panic(convertError(to, from, "cannot convert from required to optional column"))

	case to.node.Repeated():
		panic(convertError(to, from, "cannot convert from required to repeated column"))

	case isLeaf(from.node):
		if isLeaf(to.node) {
			return convertFuncOfLeaf(to, from)
		}
		panic(convertError(to, from, "cannot convert from leaf to group column"))

	case isLeaf(to.node):
		panic(convertError(to, from, "cannot convert from group to leaf column"))

	default:
		return convertFuncOfGroup(to, from)
	}
}

func convertError(to, from convertNode, reason string) *ConvertError {
	return &ConvertError{Reason: reason, Path: from.path, From: from.node, To: to.node}
}

//go:noinline
func convertFuncOfOptional(to, from convertNode) (int, int, convertFunc) {
	to.node = Required(to.node)
	from.node = Required(from.node)

	toColumnIndex, fromColumnIndex, conv := convert(to, from)
	return toColumnIndex, fromColumnIndex, func(dst, src Row, levels levels) (Row, Row, error) {
		levels.definitionLevel++
		return conv(dst, src, levels)
	}
}

//go:noinline
func convertFuncOfRepeated(to, from convertNode) (int, int, convertFunc) {
	to.node = Required(to.node)
	from.node = Required(from.node)
	srcColumnIndex := ^int8(from.columnIndex)

	toColumnIndex, fromColumnIndex, conv := convert(to, from)
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
func convertFuncOfLeaf(to, from convertNode) (int, int, convertFunc) {
	if !typesAreEqual(to.node, from.node) {
		panic(convertError(to, from, fmt.Sprintf("unsupported type conversion from %s to %s for parquet column", from.node.Type(), to.node.Type())))
	}

	srcColumnIndex := ^int8(from.columnIndex)
	dstColumnIndex := ^int8(to.columnIndex)

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
func convertFuncOfGroup(to, from convertNode) (int, int, convertFunc) {
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
			to.columnIndex, from.columnIndex, conv = convert(to.child(name), from.child(name))
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

func stringsAreEqual(strings1, strings2 []string) bool {
	if len(strings1) != len(strings2) {
		return false
	}

	for i := range strings1 {
		if strings1[i] != strings2[i] {
			return false
		}
	}

	return true
}

func stringsAreOrdered(strings1, strings2 []string) bool {
	n := min(len(strings1), len(strings2))

	for i := 0; i < n; i++ {
		if strings1[i] >= strings2[i] {
			return false
		}
	}

	return len(strings1) <= len(strings2)
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
	return &convertedRowGroup{RowGroup: rowGroup, conv: conv}
}

type convertedRowGroup struct {
	RowGroup
	conv Conversion
}

func (c *convertedRowGroup) Schema() *Schema {
	return c.conv.Schema()
}

func (c *convertedRowGroup) Rows() RowReader {
	return ConvertRowReader(c.RowGroup.Rows(), c.conv)
}

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
