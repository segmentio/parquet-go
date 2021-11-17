package parquet

import (
	"reflect"

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

	Construct(value reflect.Value) Object
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
func (opt *optionalNode) Construct(value reflect.Value) Object {
	definitionLevel := int32(1)

	if !value.IsValid() {
		definitionLevel = 0
	} else {
		switch value.Kind() {
		case reflect.Ptr:
			if value.IsNil() {
				definitionLevel = 0
				value = reflect.Value{}
			} else {
				value = value.Elem()
			}

		case reflect.Slice, reflect.Map:
			if value.IsNil() {
				definitionLevel = 0
			}
		}
	}

	return &optionalObject{
		node:            opt,
		definitionLevel: definitionLevel,
		Object:          opt.Node.Construct(value),
	}
}

type optionalObject struct {
	node            Node
	definitionLevel int32
	Object
}

func (opt *optionalObject) Node() Node { return opt.node }

func (opt *optionalObject) Index(index int) Object {
	object := opt.Object.Index(index)
	return &optionalObject{
		node:            object.Node(),
		definitionLevel: opt.definitionLevel,
		Object:          object,
	}
}

func (opt *optionalObject) Value() (value Value) {
	value = opt.Object.Value()
	value.definitionLevel += opt.definitionLevel
	return
}

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
func (rep *repeatedNode) Construct(value reflect.Value) Object {
	return &repeatedElement{elem: rep, slice: value}
}

type repeatedElement struct {
	elem  *repeatedNode
	slice reflect.Value
}

func (rep *repeatedElement) Node() Node {
	return rep.elem
}

func (rep *repeatedElement) Len() int {
	if rep.slice.IsValid() {
		return rep.slice.Len()
	}
	return 0
}

func (rep *repeatedElement) Index(index int) Object {
	repetitionLevel := int32(1)
	definitionLevel := int32(1)

	if index == 0 {
		repetitionLevel = 0
	}

	return &repeatedObject{
		repetitionLevel: repetitionLevel,
		definitionLevel: definitionLevel,
		Object:          rep.elem.Node.Construct(rep.slice.Index(index)),
	}
}

func (rep *repeatedElement) Value() Value {
	panic("cannot call Value on repeated element")
}

func (rep *repeatedElement) Reset(value reflect.Value) { rep.slice = value }

type repeatedObject struct {
	repetitionLevel int32
	definitionLevel int32
	Object
}

func (rep *repeatedObject) Value() (value Value) {
	value = rep.Object.Value()
	value.repetitionLevel += rep.repetitionLevel
	value.definitionLevel += rep.definitionLevel
	return
}

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
func (n *leafNode) RowOf(reflect.Value) Row {
	panic("cannot create row from leaf parquet node")
}
func (n *leafNode) Construct(value reflect.Value) Object {
	return &leafObject{node: n, value: makeValue(n.typ.Kind(), value)}
}

type leafObject struct {
	node  *leafNode
	value Value
}

func (obj *leafObject) Len() int                  { return 0 }
func (obj *leafObject) Index(int) Object          { panic("cannot call Index on leaf object") }
func (obj *leafObject) Node() Node                { return obj.node }
func (obj *leafObject) Value() Value              { return obj.value }
func (obj *leafObject) Reset(value reflect.Value) { obj.value = makeValue(obj.node.typ.Kind(), value) }

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
