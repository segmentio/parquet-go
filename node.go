package parquet

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/segmentio/parquet/compress"
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
)

type Node interface {
	Type() Type

	Optional() bool

	Repeated() bool

	Required() bool

	NumChildren() int

	ChildNames() []string

	ChildByName(name string) Node

	Encoding() []encoding.Encoding

	Compression() []compress.Codec
}

func Encoded(node Node, encodings ...encoding.Encoding) Node {
	if len(encodings) == 0 {
		return node
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

func Compressed(node Node, codecs ...compress.Codec) Node {
	if len(codecs) == 0 {
		return node
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
// creating a parquet schema from a Go strut and using the parquet.(*Schema).Traverse
// method will yield must better results.
func Traverse(node Node, value interface{}, traversal Traversal) error {
	_, err := traverse(node, levels{}, reflect.ValueOf(value), traversal)
	return err
}

type levels struct {
	columnIndex     int32
	repetitionDepth int32
	repetitionLevel int32
	definitionLevel int32
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
		v := makeValue(node.Type().Kind(), value)
		v.repetitionLevel = levels.repetitionLevel
		v.definitionLevel = levels.definitionLevel
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
