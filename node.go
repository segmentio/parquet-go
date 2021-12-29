package parquet

import (
	"reflect"
	"sort"

	"github.com/segmentio/parquet/compress"
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
)

// Node values represent nodes of a parquet schema.
//
// Nodes carry the type of values, as well as properties like whether the values
// are optional or repeat. Nodes with one or more children represent parquet
// groups and therefore do not have a logical type.
//
// Nodes are immutable values and therefore safe to use concurrently from
// multiple goroutines.
type Node interface {
	// For leaf nodes, returns the type of values of the parquet column.
	//
	// Calling this method on non-leaf nodes will panic.
	Type() Type

	// Returns whether the parquet column is optional.
	Optional() bool

	// Returns whether the parquet column is repeated.
	Repeated() bool

	// Returns whether the parquet column is required.
	Required() bool

	// Returns the number of child nodes.
	//
	// The method returns zero on leaf nodes.
	NumChildren() int

	// Returns the sorted list of child node namees.
	//
	// The method returns an empty slice on leaf nodes.
	//
	// As an optimization, the returned slice may be the same across calls to
	// this method. Applications should treat the return value as immutable.
	ChildNames() []string

	// Returns the child node associated with the given name, or nil if the
	// name did not exist.
	//
	// The method panics if it is called on a leaf node.
	ChildByName(name string) Node

	// ValueByName is returns the sub-value with the givne name in base.
	ValueByName(base reflect.Value, name string) reflect.Value

	// Returns the list of encodings used by the node and its children.
	//
	// The method may return an empty slice to indicate that only the plain
	// encoding is used.
	//
	// As an optimization, the returned slice may be the same across calls to
	// this method. Applications should treat the return value as immutable.
	Encoding() []encoding.Encoding

	// Returns the list of compression codecs used by the node and its children.
	//
	// The method may return an empty slice to indicate that no compression was
	// configured on the node.
	//
	// As an optimization, the returned slice may be the same across calls to
	// this method. Applications should treat the return value as immutable.
	Compression() []compress.Codec
}

// IndexedNode is an extension of the Node interface implemented by types which
// support indexing child nodes by their position.
type IndexedNode interface {
	Node

	// ChildByIndex returns the child node at the given index.
	ChildByIndex(index int) Node

	// ValueByIndex returns the sub-value of base at the given index.
	ValueByIndex(base reflect.Value, index int) reflect.Value
}

// Match returns true if the two nodes have the same structure. The comparison
// is performed recursively on children nodes.
//
// On leaf nodes, the type, optional, repeated, and required properties are
// tested for equality.
func Match(node1, node2 Node) bool {
	if node1 == nil || node2 == nil {
		return node1 == nil && node2 == nil
	}

	n1 := node1.NumChildren()
	n2 := node2.NumChildren()
	if n1 != n2 {
		return false
	}
	if n1 == 0 {
		return node1.Type().Kind() == node2.Type().Kind() &&
			node1.Optional() == node2.Optional() &&
			node1.Repeated() == node2.Repeated() &&
			node1.Required() == node2.Required()
	}

	var childAt1 func(int) Node
	var childAt2 func(int) Node

	if indexedNode, ok := unwrap(node1).(IndexedNode); ok {
		childAt1 = indexedNode.ChildByIndex
	} else {
		names := node1.ChildNames()
		childAt1 = func(i int) Node { return node1.ChildByName(names[i]) }
	}

	if indexedNode, ok := unwrap(node2).(IndexedNode); ok {
		childAt2 = indexedNode.ChildByIndex
	} else {
		names := node2.ChildNames()
		childAt2 = func(i int) Node { return node2.ChildByName(names[i]) }
	}

	for i := 0; i < n1; i++ {
		if !Match(childAt1(i), childAt2(i)) {
			return false
		}
	}

	return true
}

// WrappedNode is an extension of the Node interface implemented by types which
// wrap another underlying node.
type WrappedNode interface {
	Node
	// Unwrap returns the underlying base node.
	//
	// Note that Unwrap is not intended to recursively unwrap multple layers of
	// wrappers, it returns the immediate next layer.
	Unwrap() Node
}

type wrappedNode struct{ Node }

func (w wrappedNode) Unwrap() Node { return w.Node }

func wrap(node Node) wrappedNode { return wrappedNode{node} }

func unwrap(node Node) Node {
	for {
		if w, ok := node.(WrappedNode); ok {
			node = w.Unwrap()
		} else {
			break
		}
	}
	return node
}

// Encoded wraps the node passed as argument to add the given list of encodings.
//
// The function panics if it is called on a non-leaf node, or if one of the
// encodings is not able to encode the node type.
func Encoded(node Node, encodings ...encoding.Encoding) Node {
	if len(encodings) == 0 {
		return node
	}
	if !isLeaf(node) {
		panic("cannot add encodings to a non-leaf node")
	}
	kind := node.Type().Kind()
	for _, e := range encodings {
		if !e.CanEncode(format.Type(kind)) {
			panic("cannot apply " + e.Encoding().String() + " to node of type " + kind.String())
		}
	}
	encodings = append([]encoding.Encoding{}, encodings...)
	encodings = append(encodings, node.Encoding()...)
	sortEncodings(encodings)
	encodings = dedupeSortedEncodings(encodings)
	return &encodedNode{
		wrappedNode: wrap(node),
		encodings:   encodings[:len(encodings):len(encodings)],
	}
}

type encodedNode struct {
	wrappedNode
	encodings []encoding.Encoding
}

func (n *encodedNode) Encoding() []encoding.Encoding {
	if isLeaf(n) {
		return n.encodings
	}
	childNames := n.ChildNames()
	encodings := make([]encoding.Encoding, 0, len(childNames))
	for _, name := range childNames {
		encodings = append(encodings, n.ChildByName(name).Encoding()...)
	}
	sortEncodings(encodings)
	return dedupeSortedEncodings(encodings)
}

// Compressed wraps the node passed as argument to add the given list of
// compression codecs.
//
// The function panics if it is called on a non-leaf node.
func Compressed(node Node, codecs ...compress.Codec) Node {
	if len(codecs) == 0 {
		return node
	}
	if !isLeaf(node) {
		panic("cannot add compression codecs to a non-leaf node")
	}
	codecs = append([]compress.Codec{}, codecs...)
	codecs = append(codecs, node.Compression()...)
	codecs = codecs[:len(codecs):len(codecs)]
	sortCodecs(codecs)
	codecs = dedupeSortedCodecs(codecs)
	return &compressedNode{
		wrappedNode: wrap(node),
		codecs:      codecs[:len(codecs):len(codecs)],
	}
}

type compressedNode struct {
	wrappedNode
	codecs []compress.Codec
}

func (n *compressedNode) Compression() []compress.Codec {
	if isLeaf(n) {
		return n.codecs
	}
	childNames := n.ChildNames()
	compression := make([]compress.Codec, 0, len(childNames))
	for _, name := range childNames {
		compression = append(compression, n.ChildByName(name).Compression()...)
	}
	sortCodecs(compression)
	return dedupeSortedCodecs(compression)
}

// Optional wraps the given node to make it optional.
func Optional(node Node) Node {
	if node.Optional() {
		return node
	}
	return &optionalNode{wrap(node)}
}

type optionalNode struct{ wrappedNode }

func (opt *optionalNode) Optional() bool { return true }
func (opt *optionalNode) Repeated() bool { return false }
func (opt *optionalNode) Required() bool { return false }

// Repeated wraps the given node to make it repeated.
func Repeated(node Node) Node {
	if node.Repeated() {
		return node
	}
	return &repeatedNode{wrap(node)}
}

type repeatedNode struct{ wrappedNode }

func (rep *repeatedNode) Optional() bool { return false }
func (rep *repeatedNode) Repeated() bool { return true }
func (rep *repeatedNode) Required() bool { return false }

// Required wraps the given node to make it required.
func Required(node Node) Node {
	if node.Required() {
		return node
	}
	return &requiredNode{wrap(node)}
}

type requiredNode struct{ wrappedNode }

func (req *requiredNode) Optional() bool { return false }
func (req *requiredNode) Repeated() bool { return false }
func (req *requiredNode) Required() bool { return true }

type node struct{}

func (node) Optional() bool                { return false }
func (node) Repeated() bool                { return false }
func (node) Required() bool                { return true }
func (node) Encoding() []encoding.Encoding { return nil }
func (node) Compression() []compress.Codec { return nil }

// Leaf returns a leaf node of the given type.
func Leaf(typ Type) Node {
	return &leafNode{typ: typ}
}

type leafNode struct {
	node
	typ Type
}

func (n *leafNode) Type() Type { return n.typ }

func (n *leafNode) NumChildren() int { return 0 }

func (n *leafNode) ChildNames() []string { return nil }

func (n *leafNode) ChildByName(string) Node {
	panic("cannot call ChildByName on leaf parquet node")
}

func (n *leafNode) ValueByName(reflect.Value, string) reflect.Value {
	panic("cannot call ValueByName on leaf parquet node")
}

var repetitionTypes = [...]format.FieldRepetitionType{
	0: format.Required,
	1: format.Optional,
	2: format.Repeated,
}

func fieldRepetitionTypeOf(node Node) *format.FieldRepetitionType {
	switch {
	case node.Required():
		return &repetitionTypes[format.Required]
	case node.Optional():
		return &repetitionTypes[format.Optional]
	case node.Repeated():
		return &repetitionTypes[format.Repeated]
	default:
		return nil
	}
}

type Group map[string]Node

func (g Group) Type() Type { return groupType{} }

func (g Group) Optional() bool { return false }

func (g Group) Repeated() bool { return false }

func (g Group) Required() bool { return true }

func (g Group) NumChildren() int { return len(g) }

func (g Group) ChildNames() []string {
	names := make([]string, 0, len(g))
	for name := range g {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (g Group) ChildByName(name string) Node {
	return g[name]
}

func (g Group) ValueByName(base reflect.Value, name string) reflect.Value {
	return base.MapIndex(reflect.ValueOf(name))
}

func (g Group) Encoding() []encoding.Encoding {
	encodings := make([]encoding.Encoding, 0, len(g))
	for _, node := range g {
		encodings = append(encodings, node.Encoding()...)
	}
	sortEncodings(encodings)
	return dedupeSortedEncodings(encodings)
}

func (g Group) Compression() []compress.Codec {
	codecs := make([]compress.Codec, 0, len(g))
	for _, node := range g {
		codecs = append(codecs, node.Compression()...)
	}
	sortCodecs(codecs)
	return dedupeSortedCodecs(codecs)
}

func isLeaf(node Node) bool {
	return node.NumChildren() == 0
}

func isList(node Node) bool {
	logicalType := node.Type().LogicalType()
	return logicalType != nil && logicalType.List != nil
}

func numColumnsOf(node Node) int {
	return maxColumnIndexOf(node, 0)
}

func maxColumnIndexOf(node Node, columnIndex int) int {
	if isLeaf(node) {
		return columnIndex + 1
	}

	if indexedNode, ok := unwrap(node).(IndexedNode); ok {
		for i, n := 0, indexedNode.NumChildren(); i < n; i++ {
			columnIndex = maxColumnIndexOf(indexedNode.ChildByIndex(i), columnIndex)
		}
	} else {
		for _, name := range node.ChildNames() {
			columnIndex = maxColumnIndexOf(node.ChildByName(name), columnIndex)
		}
	}

	return columnIndex
}

func listElementOf(node Node) Node {
	if !isLeaf(node) {
		if list := node.ChildByName("list"); list != nil {
			if elem := list.ChildByName("element"); elem != nil {
				return elem
			}
		}
	}
	panic("node with logical type LIST is not composed of a .list.element")
}

func encodingAndCompressionOf(node Node) (encoding.Encoding, compress.Codec) {
	// TODO: we pick the first encoding and compression algorithm configured
	// on the node. An amelioration we could bring to this model is to
	// generate a matrix of encoding x codec and generate multiple
	// representations of the pages, picking the one with the smallest space
	// footprint; keep it simple for now.
	encoding := encoding.Encoding(&Plain)
	compression := compress.Codec(&Uncompressed)
	// The parquet-format documentation states that the
	// DELTA_LENGTH_BYTE_ARRAY is always preferred to PLAIN when
	// encoding BYTE_ARRAY values. We apply it as a default if
	// none were explicitly specified, which gives the application
	// the opportunity to override this behavior if needed.
	//
	// https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-length-byte-array-delta_length_byte_array--6
	if node.Type().Kind() == ByteArray {
		encoding = &DeltaLengthByteArray
	}

	for _, e := range node.Encoding() {
		encoding = e
		break
	}

	for _, c := range node.Compression() {
		compression = c
		break
	}

	return encoding, compression
}
