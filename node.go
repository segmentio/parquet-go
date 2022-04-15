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

	// Returns true if this a leaf node.
	Leaf() bool

	// Returns a mapping of the node's fields.
	//
	// As an optimization, the same slices may be returned by multiple calls to
	// this method, programs must treat the returned values as immutable.
	//
	// This method returns an empty mapping when called on leaf nodes.
	Fields() []Field

	// Returns the list of encodings used by the node.
	//
	// The method may return an empty slice to indicate that only the plain
	// encoding is used.
	//
	// As an optimization, the returned slice may be the same across calls to
	// this method. Applications should treat the return value as immutable.
	Encoding() []encoding.Encoding

	// Returns the list of compression codecs used by the node.
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

// Field instances represent fields of a parquet node, which associate a node to
// their name in their parent node.
type Field interface {
	Node

	// Returns the name of this field in its parent node.
	Name() string

	// Given a reference to the Go value matching the structure of the parent
	// node, eturns the Go value of the field.
	Value(base reflect.Value) reflect.Value
}

// WrappedNode is an extension of the Node interface implemented by types which
// wrap another underlying node.
type WrappedNode interface {
	Node
	// Unwrap returns the underlying base node.
	//
	// Note that Unwrap is not intended to recursively unwrap multiple layers of
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
	if !node.Leaf() {
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
	return n.encodings
}

// Compressed wraps the node passed as argument to add the given list of
// compression codecs.
//
// The function panics if it is called on a non-leaf node.
func Compressed(node Node, codecs ...compress.Codec) Node {
	if len(codecs) == 0 {
		return node
	}
	if !node.Leaf() {
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
	return n.codecs
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

func (n *leafNode) Leaf() bool { return true }

func (n *leafNode) Fields() []Field { return nil }

func (n *leafNode) Encoding() []encoding.Encoding { return nil }

func (n *leafNode) Compression() []compress.Codec { return nil }

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

func (g Group) Leaf() bool { return false }

func (g Group) Fields() []Field {
	groupFields := make([]groupField, 0, len(g))
	for name, node := range g {
		groupFields = append(groupFields, groupField{
			Node: node,
			name: name,
		})
	}
	sort.Slice(groupFields, func(i, j int) bool {
		return groupFields[i].name < groupFields[j].name
	})
	fields := make([]Field, len(groupFields))
	for i := range groupFields {
		fields[i] = &groupFields[i]
	}
	return fields
}

func (g Group) Encoding() []encoding.Encoding { return nil }

func (g Group) Compression() []compress.Codec { return nil }

func (g Group) GoType() reflect.Type { return goTypeOfGroup(g) }

type groupField struct {
	Node
	name string
}

func (f *groupField) Unwrap() Node { return f.Node }

func (f *groupField) Name() string { return f.name }

func (f *groupField) Value(base reflect.Value) reflect.Value {
	return base.MapIndex(reflect.ValueOf(&f.name).Elem())
}

var (
	_ WrappedNode = (*groupField)(nil)
)

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
	if node.Leaf() {
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
	fields := node.Fields()
	structFields := make([]reflect.StructField, len(fields))
	for i, field := range fields {
		structFields[i].Name = exportedStructFieldName(field.Name())
		structFields[i].Type = field.GoType()
		// TODO: can we reconstruct a struct tag that would be valid if a value
		// of this type were passed to SchemaOf?
	}
	return reflect.StructOf(structFields)
}

func exportedStructFieldName(name string) string {
	firstRune, size := utf8.DecodeRuneInString(name)
	return string([]rune{unicode.ToUpper(firstRune)}) + name[size:]
}

func isList(node Node) bool {
	logicalType := node.Type().LogicalType()
	return logicalType != nil && logicalType.List != nil
}

func isMap(node Node) bool {
	logicalType := node.Type().LogicalType()
	return logicalType != nil && logicalType.Map != nil
}

func numLeafColumnsOf(node Node) int16 {
	return makeColumnIndex(numLeafColumns(node, 0))
}

func numLeafColumns(node Node, columnIndex int) int {
	if node.Leaf() {
		return columnIndex + 1
	}
	for _, field := range node.Fields() {
		columnIndex = numLeafColumns(field, columnIndex)
	}
	return columnIndex
}

func listElementOf(node Node) Node {
	if !node.Leaf() {
		if list := childByName(node, "list"); list != nil {
			if elem := childByName(list, "element"); elem != nil {
				return elem
			}
		}
	}
	panic("node with logical type LIST is not composed of a repeated .list.element")
}

func mapKeyValueOf(node Node) Node {
	if !node.Leaf() && (node.Required() || node.Optional()) {
		if keyValue := childByName(node, "key_value"); keyValue != nil && !keyValue.Leaf() && keyValue.Repeated() {
			k := childByName(keyValue, "key")
			v := childByName(keyValue, "value")
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
	nodeEncoding := node.Encoding()
	compression := compress.Codec(&Uncompressed)
	// The parquet-format documentation states that the
	// DELTA_LENGTH_BYTE_ARRAY is always preferred to PLAIN when
	// encoding BYTE_ARRAY values. We apply it as a default if
	// none were explicitly specified, which gives the application
	// the opportunity to override this behavior if needed.
	//
	// https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-length-byte-array-delta_length_byte_array--6
	if node.Type().Kind() == ByteArray && len(nodeEncoding) == 0 {
		encoding = &DeltaLengthByteArray
	}

	for _, e := range nodeEncoding {
		encoding = e
		break
	}

	for _, c := range node.Compression() {
		compression = c
		break
	}

	return encoding, compression
}

func forEachNodeOf(name string, node Node, do func(string, Node)) {
	do(name, node)

	for _, f := range node.Fields() {
		forEachNodeOf(f.Name(), f, do)
	}
}

func childByName(node Node, name string) Node {
	for _, f := range node.Fields() {
		if f.Name() == name {
			return f
		}
	}
	return nil
}

func nodesAreEqual(node1, node2 Node) bool {
	if node1.Leaf() {
		return node2.Leaf() && leafNodesAreEqual(node1, node2)
	} else {
		return !node2.Leaf() && groupNodesAreEqual(node1, node2)
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
	fields1 := node1.Fields()
	fields2 := node2.Fields()

	if len(fields1) != len(fields2) {
		return false
	}

	for i := range fields1 {
		f1 := fields1[i]
		f2 := fields2[i]

		if f1.Name() != f2.Name() {
			return false
		}

		if !nodesAreEqual(f1, f2) {
			return false
		}
	}

	return true
}

type repetition int

const (
	required repetition = iota
	optional
	repeated
)

func (rep repetition) String() string {
	switch rep {
	case optional:
		return "optional"
	case repeated:
		return "repeated"
	default:
		return "required"
	}
}

func repetitionOf(node Node) repetition {
	switch {
	case node.Optional():
		return optional
	case node.Repeated():
		return repeated
	default:
		return required
	}
}
