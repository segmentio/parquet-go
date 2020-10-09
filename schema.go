package parquet

import (
	"errors"
	"fmt"

	pthrift "github.com/segmentio/parquet/internal/gen-go/parquet"
)

// Schema represents a node in the schema tree of Parquet.
type Schema struct {
	// User-definable attributes
	ID            int32
	Name          string
	PhysicalType  pthrift.Type
	ConvertedType *pthrift.ConvertedType
	LogicalType   *pthrift.LogicalType
	Repetition    pthrift.FieldRepetitionType
	Scale         int32
	Precision     int32

	// Computed attributes (parquets spec)
	RepetitionLevel uint32
	DefinitionLevel uint32
	Path            []string
	// Computed attributes (segmentio/parquet)
	Root bool
	Kind kind

	// Tree structure
	parent   *Schema
	Children []*Schema
}

// kind indicates the kind of structure a schema node (and its descendent in
// the tree) represent.
type kind int

const (
	// primitiveKind nodes are terminal nodes that map to actual columns in
	// the parquet files.
	primitiveKind kind = iota
	// groupKind nodes represent compound objects made of multiple nodes.
	groupKind
	// mapKind nodes are follow the MAP structure.
	mapKind
	// repeatedKind nodes are nodes that are expected to be repeated and not
	// following the MAP structure.
	repeatedKind
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
		panic("tried to remove the root of the s")
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

	if p.Kind != groupKind {
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
// It updates the parent/children relationships.
func (sn *Schema) Add(node *Schema) {
	sn.Children = append(sn.Children, node)
	node.parent = sn
}

// Compute walks the Schema and update all computed attributes.
func (sn *Schema) Compute() {
	if sn.parent == nil {
		sn.Root = true
	}

	if sn.parent != nil {
		sn.RepetitionLevel = sn.parent.RepetitionLevel
		sn.DefinitionLevel = sn.parent.DefinitionLevel
		sn.Path = newPath(sn.parent.Path, sn.Name)
	}

	if sn.Repetition == pthrift.FieldRepetitionType_REPEATED {
		sn.RepetitionLevel++
	}
	if sn.Repetition != pthrift.FieldRepetitionType_REQUIRED {
		sn.DefinitionLevel++
	}

	sn.Kind = computeKind(sn)

	for _, c := range sn.Children {
		c.Compute()
	}
}

func computeKind(s *Schema) kind {
	if len(s.Children) == 0 {
		return primitiveKind
	}

	if s.ConvertedType != nil {
		switch *s.ConvertedType {
		case pthrift.ConvertedType_MAP, pthrift.ConvertedType_MAP_KEY_VALUE:
			return mapKind
		case pthrift.ConvertedType_LIST:
			return repeatedKind
		}
	}

	if s.Repetition == pthrift.FieldRepetitionType_REPEATED {
		return repeatedKind
	}

	return groupKind
}

var errEmptySchema = errors.New("empty schema")

// schemaFromFlatElements builds a schema tree from a list of SchemaElements. Parquet
// serializes the schema tree by laying out its nodes in a depth-first order.
func schemaFromFlatElements(elements []*pthrift.SchemaElement) (*Schema, error) {
	if len(elements) == 0 {
		return nil, errEmptySchema
	}

	root := &Schema{
		Root: true,
	}

	consumed := flatThriftSchemaToTreeRecurse(root, elements)
	if consumed != len(elements) {
		return nil, fmt.Errorf("should have consumed %d elements but got %d instead", len(elements), consumed)
	}

	root.Compute()

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

func newPath(path []string, name string) []string {
	newPath := make([]string, len(path)+1)
	copy(newPath, path)
	newPath[len(path)] = name
	return newPath
}
