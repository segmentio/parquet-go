package parquet

import "reflect"

type Node interface {
	Type() Type

	Optional() bool

	Repeated() bool

	Required() bool

	NumChildren() int

	ChildNames() []string

	ChildByName(name string) Node

	Object(value reflect.Value) Object
}

func Optional(node Node) Node {
	if node.Optional() {
		return node
	}
	return &optional{node}
}

type optional struct{ Node }

func (opt *optional) Optional() bool { return true }
func (opt *optional) Repeated() bool { return false }
func (opt *optional) Required() bool { return false }

func Repeated(node Node) Node {
	if node.Repeated() {
		return node
	}
	return &repeated{node}
}

type repeated struct{ Node }

func (opt *repeated) Optional() bool { return false }
func (opt *repeated) Repeated() bool { return true }
func (opt *repeated) Required() bool { return false }

func Required(node Node) Node {
	if node.Required() {
		return node
	}
	return &required{node}
}

type required struct{ Node }

func (opt *required) Optional() bool { return false }
func (opt *required) Repeated() bool { return false }
func (opt *required) Required() bool { return true }

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
func (n *leafNode) Object(value reflect.Value) Object {
	return &leafObject{node: n, value: makeValue(n.typ.Kind(), value)}
}

type leafObject struct {
	node  *leafNode
	value Value
}

func (obj *leafObject) Len() int                  { return 0 }
func (obj *leafObject) Index(int) Object          { panic("cannot call Index on leaf object") }
func (obj *leafObject) Value() Value              { return obj.value }
func (obj *leafObject) Reset(value reflect.Value) { obj.value = makeValue(obj.node.typ.Kind(), value) }
