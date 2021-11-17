package parquet

import (
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
