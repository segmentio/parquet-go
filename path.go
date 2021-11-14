package parquet

/*
import (
	"strings"
)

type Path interface {
	Path() []string

	NumValues(row Row) int

	ValueIndex(row Row, index int) Value
}

func PathsOf(node Node) []Path {
	return appendPathsOf(nil, node, node, nil)
}

func appendPathsOf(dst []Path, root, node Node, path []string) []Path {
	if node.NumChildren() > 0 {
		base := path[:len(path):len(path)]

		for _, name := range node.ChildNames() {
			dst = appendPathsOf(dst, root, node.ChildByName(name), append(base, name))
		}
	} else {
		dst = append(dst, pathTo(root, path, 0))
	}
	return dst
}

func pathTo(node Node, path []string, index int) Path {
	// TODO:
	// * optional
	// * repeated
	if node.NumChildren() > 0 {
		return &groupPath{
			path: path[:index+1],
			next: pathTo(node.ChildByName(path[index]), path, index+1),
		}
	} else {
		if index < len(path) {
			panic("cannot create path to " + join(path) + " through parquet value of type " + node.Type().Kind().String())
		}
		return &valuePath{
			path: path,
		}
	}
}

type groupPath struct {
	path []string
	next Path
}

func (g *groupPath) name() string {
	return g.path[len(g.path)-1]
}

func (g *groupPath) Path() []string {
	return g.path
}

func (g *groupPath) NumValues(row Row) int {
	if sub := row.ChildByName(g.name()); sub.IsValid() {
		return g.next.NumValues(sub)
	}
	return 0
}

func (g *groupPath) ValueIndex(row Row, index int) Value {
	if sub := row.ChildByName(g.name()); sub.IsValid() {
		return g.next.ValueIndex(sub, index)
	}
	return Value{}
}

type valuePath struct {
	path []string
}

func (v *valuePath) Path() []string {
	return v.path
}

func (v *valuePath) NumValues(row Row) int {
	// TODO: nullable?
	return 1
}

func (v *valuePath) ValueIndex(row Row, index int) Value {
	if index > 0 {
		panic("cannot get value at non-zero index from leaf parquet node")
	}
	return Value{}
	//return makeValue(v.kind, row.value)
}

type emptyPath struct{}

func (emptyPath) NumValues(Row) int         { return 0 }
func (emptyPath) ValueIndex(Row, int) Value { panic("cannot call ValueIndex on empty parquet path") }

func join(path []string) string {
	return strings.Join(path, ".")
}
*/
