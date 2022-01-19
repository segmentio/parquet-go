package parquet

import (
	"reflect"
	"sort"
	"unicode"
	"unicode/utf8"

	"github.com/segmentio/parquet-go/compress"
	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/format"
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
	// Returns a human-readable representation of the parquet node.
	String() string

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

	// Returns the Go type that best represents the parquet node.
	//
	// For leaf nodes, this will be one of bool, int32, int64, deprecated.Int96,
	// float32, float64, string, []byte, or [N]byte.
	//
	// For groups, the method returns a struct type.
	//
	// If the method is called on a repeated node, the method returns a slice of
	// the underlying type.
	//
	// For optional nodes, the method returns a pointer of the underlying type.
	//
	// For nodes that were constructed from Go values (e.g. using SchemaOf), the
	// method returns the original Go type.
	GoType() reflect.Type
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
func Optional(node Node) Node { return &optionalNode{wrap(node)} }

type optionalNode struct{ wrappedNode }

func (opt *optionalNode) Optional() bool       { return true }
func (opt *optionalNode) Repeated() bool       { return false }
func (opt *optionalNode) Required() bool       { return false }
func (opt *optionalNode) GoType() reflect.Type { return reflect.PtrTo(unwrap(opt).GoType()) }

// Repeated wraps the given node to make it repeated.
func Repeated(node Node) Node { return &repeatedNode{wrap(node)} }

type repeatedNode struct{ wrappedNode }

func (rep *repeatedNode) Optional() bool       { return false }
func (rep *repeatedNode) Repeated() bool       { return true }
func (rep *repeatedNode) Required() bool       { return false }
func (rep *repeatedNode) GoType() reflect.Type { return reflect.SliceOf(unwrap(rep).GoType()) }

// Required wraps the given node to make it required.
func Required(node Node) Node { return &requiredNode{wrap(node)} }

type requiredNode struct{ wrappedNode }

func (req *requiredNode) Optional() bool       { return false }
func (req *requiredNode) Repeated() bool       { return false }
func (req *requiredNode) Required() bool       { return true }
func (req *requiredNode) GoType() reflect.Type { return unwrap(req).GoType() }

type node struct{}

// Leaf returns a leaf node of the given type.
func Leaf(typ Type) Node {
	return &leafNode{typ: typ}
}

type leafNode struct{ typ Type }

func (n *leafNode) String() string { return sprint("", n) }

func (n *leafNode) Type() Type { return n.typ }

func (n *leafNode) Optional() bool { return false }

func (n *leafNode) Repeated() bool { return false }

func (n *leafNode) Required() bool { return true }

func (n *leafNode) Encoding() []encoding.Encoding { return nil }

func (n *leafNode) Compression() []compress.Codec { return nil }

func (n *leafNode) NumChildren() int { return 0 }

func (n *leafNode) ChildNames() []string { return nil }

func (n *leafNode) ChildByName(string) Node {
	panic("cannot call ChildByName on leaf parquet node")
}

func (n *leafNode) ValueByName(reflect.Value, string) reflect.Value {
	panic("cannot call ValueByName on leaf parquet node")
}

func (n *leafNode) GoType() reflect.Type { return goTypeOfLeaf(n) }

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

func (g Group) String() string { return sprint("", g) }

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

func (g Group) GoType() reflect.Type { return goTypeOfGroup(g) }

func goTypeOf(node Node) reflect.Type {
	switch {
	case node.Optional():
		return goTypeOfOptional(node)
	case node.Repeated():
		return goTypeOfRepeated(node)
	default:
		return goTypeOfRequired(node)
	}
}

func goTypeOfOptional(node Node) reflect.Type {
	return reflect.PtrTo(goTypeOfRequired(node))
}

func goTypeOfRepeated(node Node) reflect.Type {
	return reflect.SliceOf(goTypeOfRequired(node))
}

func goTypeOfRequired(node Node) reflect.Type {
	if isLeaf(node) {
		return goTypeOfLeaf(node)
	} else {
		return goTypeOfGroup(node)
	}
}

func goTypeOfLeaf(node Node) reflect.Type {
	t := node.Type()
	if convertibleType, ok := t.(interface{ GoType() reflect.Type }); ok {
		return convertibleType.GoType()
	}
	switch t.Kind() {
	case Boolean:
		return reflect.TypeOf(false)
	case Int32:
		return reflect.TypeOf(int32(0))
	case Int64:
		return reflect.TypeOf(int64(0))
	case Int96:
		return reflect.TypeOf(deprecated.Int96{})
	case Float:
		return reflect.TypeOf(float32(0))
	case Double:
		return reflect.TypeOf(float64(0))
	case ByteArray:
		return reflect.TypeOf(([]byte)(nil))
	case FixedLenByteArray:
		return reflect.ArrayOf(t.Length(), reflect.TypeOf(byte(0)))
	default:
		panic("BUG: parquet type returned an unsupported kind")
	}
}

func goTypeOfGroup(node Node) reflect.Type {
	names := node.ChildNames()
	fields := make([]reflect.StructField, len(names))
	for i, name := range names {
		child := node.ChildByName(name)
		fields[i].Name = exportedStructFieldName(name)
		fields[i].Type = child.GoType()
		// TODO: can we reconstruct a struct tag that would be valid if a value
		// of this type were passed to SchemaOf?
	}
	return reflect.StructOf(fields)
}

func exportedStructFieldName(name string) string {
	firstRune, size := utf8.DecodeRuneInString(name)
	return string([]rune{unicode.ToUpper(firstRune)}) + name[size:]
}

func isLeaf(node Node) bool {
	return node.NumChildren() == 0
}

func isList(node Node) bool {
	logicalType := node.Type().LogicalType()
	return logicalType != nil && logicalType.List != nil
}

func isMap(node Node) bool {
	logicalType := node.Type().LogicalType()
	return logicalType != nil && logicalType.Map != nil
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
	panic("node with logical type LIST is not composed of a repeated .list.element")
}

func mapKeyValueOf(node Node) Node {
	if !isLeaf(node) && (node.Required() || node.Optional()) {
		if keyValue := node.ChildByName("key_value"); keyValue != nil && !isLeaf(keyValue) && keyValue.Repeated() {
			k := keyValue.ChildByName("key")
			v := keyValue.ChildByName("value")
			if k != nil && v != nil && k.Required() {
				return keyValue
			}
		}
	}
	panic("node with logical type MAP is not composed of a repeated .key_value group with key and value fields")
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
