package parquet

import (
	"errors"
	"fmt"

	pthrift "github.com/segmentio/parquet/internal/gen-go/parquet"
)

// Schema represents a node in the schema tree of Parquet.
type Schema struct {
	ID              int32
	Name            string
	Kind            Kind
	PhysicalType    pthrift.Type
	ConvertedType   *pthrift.ConvertedType
	LogicalType     *pthrift.LogicalType
	Repetition      pthrift.FieldRepetitionType
	Scale           int32
	Precision       int32
	RepetitionLevel uint32
	DefinitionLevel uint32
	Path            []string
	Root            bool
	parent          *Schema
	Children        []*Schema
}

// Kind indicates the kind of structure a schema node (and its descendent in
// the tree) represent.
type Kind int

const (
	// PrimitiveKind nodes are terminal nodes that map to actual columns in
	// the parquet files.
	PrimitiveKind Kind = iota
	// GroupKind nodes represent compound objects mad of multiple nodes.
	GroupKind
	// MapKind nodes are follow the MAP structure.
	MapKind
	// RepeatedKind nodes are nodes that are expected to be repeated and not
	// following the MAP structure.
	RepeatedKind
)

func (sn *Schema) Parent() *Schema {
	return sn.parent
}

// At walks the tree following Names to retrieve the node the end of the path.
// Returns nil if no node is found.
func (sn *Schema) At(path ...string) *Schema {
	if len(path) == 0 {
		return sn
	}
	for _, child := range sn.Children {
		if child.Name == path[0] {
			return child.At(path[1:]...)
		}
	}
	return nil
}

// Remove the node from the tree. It also removes all the nodes that depend on
// it.
//
// Panics if called on the root of the schema.
func (sn *Schema) Remove() {
	if sn.Root {
		panic("tried to remove the root of the schema")
	}
	p := sn.Parent()
	for i := range p.Children {
		if p.Children[i] == sn {
			copy(p.Children[i:], p.Children[i+1:])
			p.Children[len(p.Children)-1] = nil
			p.Children = p.Children[:len(p.Children)-1]
			break
		}
	}

	if p.Kind != GroupKind {
		p.Remove()
	}
}

// Leaves return the graph's leaves in "in the order in which they appear in
// the schema", which seems to mean depth-first.
func (sn *Schema) Leaves() []*Schema {
	return sn.addLeavesTo(nil)
}

func (sn *Schema) addLeavesTo(leaves []*Schema) []*Schema {
	if len(sn.Children) == 0 {
		return append(leaves, sn)
	}
	for _, child := range sn.Children {
		leaves = child.addLeavesTo(leaves)
	}
	return leaves
}

var errNodeNotFound = errors.New("node not found")

// traverse the tree following Names along path. Calls f on every visited node.
// Returns non-nil error if the path cannot be fully walked through.
func (sn *Schema) traverse(path []string, f func(node *Schema)) error {
	n := sn
	p := path
	for len(p) > 0 {
		f(n)

		var next *Schema
		for _, child := range n.Children {
			if child.Name == p[0] {
				next = child
				p = p[1:]
				break
			}
		}
		if next == nil {
			return errNodeNotFound
		}
		n = next
	}
	return nil
}

// Add a node as a direct child of this node.
// It updates the parent/children relationships, and the path of the provided
// node.
func (sn *Schema) Add(node *Schema) {
	sn.Children = append(sn.Children, node)
	node.parent = sn
	node.Path = make([]string, len(sn.Path), len(sn.Path)+1)
	copy(node.Path, sn.Path)
	node.Path = append(node.Path, node.Name)
}

// Walk the schema tree depth-first, calling walkFn for every node visited.
// If walk fn returns an error, the walk stops.
// It is recommended to not modify the tree while walking it.
func Walk(n *Schema, walkFn WalkFn) error {
	err := walkFn(n)
	if err != nil {
		return err
	}
	for _, c := range n.Children {
		err := Walk(c, walkFn)
		if err != nil {
			return err
		}
	}

	return nil
}

// WalkFn is the type of functions called for each node visited by Walk.
type WalkFn func(n *Schema) error

var errEmptySchema = errors.New("empty schema")

// NewFromFlatSchema builds a schema tree from a list of SchemaElements. Parquet
// serializes the schema tree by laying out its nodes in a depth-first order.
func NewFromFlatSchema(elements []*pthrift.SchemaElement) (*Schema, error) {
	if len(elements) == 0 {
		return nil, errEmptySchema
	}

	root := &Schema{
		Root: true,
	}

	consumed := flatThriftSchemaToTreeRecurse(root, elements)
	if consumed != len(elements) {
		panic(fmt.Errorf("should have consumed %d elements but got %d instead", len(elements), consumed))
	}

	return root, nil
}

func flatThriftSchemaToTreeRecurse(current *Schema, left []*pthrift.SchemaElement) int {
	if len(left) == 0 {
		panic("should be at least one left")
	}
	if current == nil {
		panic("nil ptr")
	}

	el := left[0]

	current.ID = el.GetFieldID()
	current.Name = el.GetName()
	current.PhysicalType = el.GetType()
	current.ConvertedType = el.ConvertedType
	current.LogicalType = el.GetLogicalType()
	current.Repetition = el.GetRepetitionType()
	current.Scale = el.GetScale()
	current.Precision = el.GetPrecision()
	current.Children = make([]*Schema, el.GetNumChildren())
	current.Kind = kindFromSchemaElement(el)
	if !current.Root {
		current.Path = append(current.Path, el.GetName())
	}

	if current.Repetition == pthrift.FieldRepetitionType_REPEATED {
		current.RepetitionLevel++
	}

	if current.Repetition != pthrift.FieldRepetitionType_REQUIRED {
		current.DefinitionLevel++
	}

	offset := 1
	for i := int32(0); i < el.GetNumChildren(); i++ {
		current.Children[i] = &Schema{
			DefinitionLevel: current.DefinitionLevel,
			RepetitionLevel: current.RepetitionLevel,
			Path:            make([]string, len(current.Path), len(current.Path)+1),
			parent:          current,
		}
		copy(current.Children[i].Path, current.Path)
		offset += flatThriftSchemaToTreeRecurse(current.Children[i], left[offset:])
	}

	return offset
}

func kindFromSchemaElement(el *pthrift.SchemaElement) Kind {
	if el.GetNumChildren() == 0 {
		return PrimitiveKind
	}

	if el.ConvertedType != nil {
		switch *el.ConvertedType {
		case pthrift.ConvertedType_MAP, pthrift.ConvertedType_MAP_KEY_VALUE:
			return MapKind
		case pthrift.ConvertedType_LIST:
			return RepeatedKind
		}
	}

	if el.GetRepetitionType() == pthrift.FieldRepetitionType_REPEATED {
		return RepeatedKind
	}

	return GroupKind
}
