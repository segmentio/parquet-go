package parquet

import (
	"fmt"
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
	ChildNames() []string

	// Returns the child node associated with the given name.
	//
	// The method panics if it is called on a leaf node.
	ChildByName(name string) Node

	// Returns the list of encodings used by the node and its children.
	//
	// The method may return an empty slice to indicate that only the plain
	// encoding is used.
	Encoding() []encoding.Encoding

	// Returns the list of compression codecs used by the node and its children.
	//
	// The method may return an empty slice to indicate that no compression was
	// configured on the node.
	Compression() []compress.Codec
}

// Encoded wraps the node passed as argument to add the given list of encodings.
//
// The function panics if it is called on a non-leaf node, or if one of the
// encodings is not able to encode the node type.
func Encoded(node Node, encodings ...encoding.Encoding) Node {
	if len(encodings) == 0 {
		return node
	}
	if node.NumChildren() != 0 {
		panic("cannot add encodings to a non-leaf node")
	}
	kind := node.Type().Kind()
	for _, e := range encodings {
		if !e.CanEncode(format.Type(kind)) {
			panic("cannot apply " + e.Encoding().String() + " to node of type " + kind.String())
		}
	}
	encodings = append([]encoding.Encoding{}, encodings...)
	return &encodedNode{
		Node:      node,
		encodings: encodings[:len(encodings):len(encodings)],
	}
}

type encodedNode struct {
	Node
	encodings []encoding.Encoding
}

func (n *encodedNode) Encoding() []encoding.Encoding {
	encodings := append(n.encodings, n.Node.Encoding()...)
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
	if node.NumChildren() != 0 {
		panic("cannot add compression codecs to a non-leaf node")
	}
	codecs = append([]compress.Codec{}, codecs...)
	return &compressedNode{
		Node:   node,
		codecs: codecs[:len(codecs):len(codecs)],
	}
}

type compressedNode struct {
	Node
	codecs []compress.Codec
}

func (n *compressedNode) Compression() []compress.Codec {
	compression := append(n.codecs, n.Node.Compression()...)
	sortCodecs(compression)
	return dedupeSortedCodecs(compression)
}

// Optional wraps the given node to make it optional.
func Optional(node Node) Node {
	if node.Optional() {
		return node
	}
	return &optionalNode{node}
}

type optionalNode struct{ Node }

func (opt *optionalNode) Optional() bool { return true }
func (opt *optionalNode) Repeated() bool { return false }
func (opt *optionalNode) Required() bool { return false }

// Repeated wraps the given node to make it repeated.
func Repeated(node Node) Node {
	if node.Repeated() {
		return node
	}
	return &repeatedNode{node}
}

type repeatedNode struct{ Node }

func (rep *repeatedNode) Optional() bool { return false }
func (rep *repeatedNode) Repeated() bool { return true }
func (rep *repeatedNode) Required() bool { return false }

// Required wraps the given node to make it required.
func Required(node Node) Node {
	if node.Required() {
		return node
	}
	return &requiredNode{node}
}

type requiredNode struct{ Node }

func (req *requiredNode) Optional() bool { return false }
func (req *requiredNode) Repeated() bool { return false }
func (req *requiredNode) Required() bool { return true }

type node struct{}

func (node) Optional() bool                { return false }
func (node) Repeated() bool                { return false }
func (node) Required() bool                { return true }
func (node) NumChildren() int              { return 0 }
func (node) ChildNames() []string          { return nil }
func (node) ChildByName(string) Node       { panic("cannot call ChildByName in leaf parquet node") }
func (node) Encoding() []encoding.Encoding { return nil }
func (node) Compression() []compress.Codec { return nil }

type leafNode struct {
	node
	typ Type
}

func (n *leafNode) Type() Type { return n.typ }

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

// Traverse implements the parquet node traversal algorithm, which produces the
// sequence of parquet values for each column of the node schema by calling the
// given traveral's Traverse method.
//
// This function is a generic impelementation, it works with any Node value and
// dynamically matches the schema by introspecting the given Go value. As a
// result, it has a higher overhead than using a generated parquet schema.
// In contexts where the compute footprint of the program should be minimized,
// creating a parquet schema from a Go struct and using the
// parquet.(*Schema).Traverse method will yield must better results.
func Traverse(node Node, value interface{}, traversal Traversal) error {
	_, err := traverse(node, levels{}, reflect.ValueOf(value), traversal)
	return err
}

type levels struct {
	columnIndex     int32
	repetitionDepth int8
	repetitionLevel int8
	definitionLevel int8
}

func traverse(node Node, levels levels, value reflect.Value, traversal Traversal) (int32, error) {
	var err error

	optional := node.Optional()
	repeated := node.Repeated()

	if optional {
		if value.IsValid() {
			if value.IsZero() {
				value = reflect.Value{}
			} else {
				levels.definitionLevel++
			}
		}
	}

	for value.IsValid() && value.Kind() == reflect.Ptr {
		if !value.IsNil() {
			value = value.Elem()
		} else {
			value = reflect.Value{}
		}
	}

	if logicalType := node.Type().LogicalType(); logicalType != nil {
		switch {
		case logicalType.List != nil:
			node = node.ChildByName("list").ChildByName("element")
			repeated = true
		}
	}

	if repeated {
		if value.IsValid() && value.Kind() != reflect.Slice {
			return levels.columnIndex, fmt.Errorf("cannot traverse non-repeated node with value of type " + value.Type().String())
		}

		numValues := 0
		if value.IsValid() {
			numValues = value.Len()
			levels.repetitionDepth++
			if !value.IsNil() {
				levels.definitionLevel++
			}
		}

		if numValues == 0 {
			value = reflect.Value{}
		} else {
			columnIndex := int32(0)
			// Remove the `repeated` attribute of the node so the recusrive call
			// does not re-enter this branch.
			node = Required(node)

			for i := 0; i < numValues; i++ {
				columnIndex, err = traverse(node, levels, value.Index(i), traversal)
				levels.repetitionLevel = levels.repetitionDepth
			}

			return columnIndex, err
		}
	}

	if node.NumChildren() == 0 {
		if levels.repetitionLevel > 127 {
			panic("cannot represent parquet schema with more than 127 repetition levels")
		}
		if levels.definitionLevel > 127 {
			panic("cannot represent parquet schema with more than 127 definition levels")
		}
		v := makeValue(node.Type().Kind(), value)
		v.repetitionLevel = int8(levels.repetitionLevel)
		v.definitionLevel = int8(levels.definitionLevel)
		err = traversal.Traverse(int(levels.columnIndex), v)
		levels.columnIndex++
	} else {
		index := reflect.Value.MapIndex

		if !value.IsValid() {
			index = func(reflect.Value, reflect.Value) reflect.Value { return reflect.Value{} }
		} else if value.Kind() == reflect.Struct {
			fields := structFieldsOf(value.Type())

			index = func(structValue, fieldName reflect.Value) reflect.Value {
				f := fieldName.String()
				i := sort.Search(len(fields), func(i int) bool { return fields[i].Name >= f })

				if i < len(fields) {
					return structValue.FieldByIndex(fields[i].Index)
				}

				return reflect.Value{}
			}
		}

		names := node.ChildNames()
		for i := range names {
			k := reflect.ValueOf(&names[i]).Elem()
			levels.columnIndex, err = traverse(node.ChildByName(names[i]), levels, index(value, k), traversal)
			if err != nil {
				break
			}
		}
	}

	return levels.columnIndex, err
}
