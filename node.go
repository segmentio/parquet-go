package parquet

import (
	"fmt"
	"reflect"
	"sort"

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

type leafNode struct{ typ Type }

func (n *leafNode) Type() Type           { return n.typ }
func (n *leafNode) Optional() bool       { return false }
func (n *leafNode) Repeated() bool       { return false }
func (n *leafNode) Required() bool       { return true }
func (n *leafNode) NumChildren() int     { return 0 }
func (n *leafNode) ChildNames() []string { return nil }
func (n *leafNode) ChildByName(string) Node {
	panic("cannot lookup child by name in leaf parquet node")
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
	_, err := traverse(node, 0, 0, 0, reflect.ValueOf(value), traversal)
	return err
}

func traverse(node Node, columnIndex, repetitionLevel, definitionLevel int, value reflect.Value, traversal Traversal) (int, error) {
	var err error

	optional := node.Optional()
	repeated := node.Repeated()

	if optional {
		if value.IsValid() {
			if value.IsZero() {
				value = reflect.Value{}
			} else {
				definitionLevel++
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
			return columnIndex, fmt.Errorf("cannot traverse non-repeated node with value of type " + value.Type().String())
		}

		numValues := 0
		if value.IsValid() {
			numValues = value.Len()
			definitionLevel++
		}

		baseColumnIndex := columnIndex
		if numValues == 0 {
			// When the repeated column is not a group, no need to continue; the
			// column index is incremented to represent the leaf column that was
			// reached.
			if node.NumChildren() == 0 {
				return columnIndex + 1, nil
			}
			// Continue through the rest of the function to increment the column
			// index but don't send values to the traversal callback.
			traversal = TraversalFunc(func(int, Value) error { return nil })
		} else {
			// Remove the `repeated` attribute of the node so the recusrive call
			// does not re-enter this branch.
			node = Required(node)

			for i := 0; i < numValues; i++ {
				columnIndex, err = traverse(node, baseColumnIndex, repetitionLevel, definitionLevel, value.Index(i), traversal)
				if i == 0 {
					repetitionLevel++
				}
			}

			return columnIndex, err
		}
	}

	if node.NumChildren() == 0 {
		err = traversal.Traverse(columnIndex, makeValue(node.Type().Kind(), value).Level(repetitionLevel, definitionLevel))
		columnIndex++
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
			columnIndex, err = traverse(node.ChildByName(names[i]), columnIndex, repetitionLevel, definitionLevel, index(value, k), traversal)
			if err != nil {
				break
			}
		}
	}

	return columnIndex, err
}
