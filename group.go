package parquet

import (
	"sort"

	"github.com/segmentio/parquet/deprecated"
	"github.com/segmentio/parquet/format"
)

type Group map[string]Node

func (g Group) Type() Type { return groupType{} }

func (g Group) Optional() bool { return false }

func (g Group) Repeated() bool { return false }

func (g Group) Required() bool { return true }

func (g Group) NumChildren() int { return len(g) }

func (g Group) ChildNames() []string {
	names := make([]string, 0, len(g))
	for name := range g {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (g Group) ChildByName(name string) Node {
	n, ok := g[name]
	if ok {
		return n
	}
	panic("column not found in parquet group: " + name)
}

/*
func (g Group) PathTo(path []string) Path {
	if len(path) == 0 {
		return emptyPath{}
	}


}
*/

type groupType struct{}

func (groupType) Kind() Kind                              { panic("cannot call Kind on parquet group type") }
func (groupType) Length() int                             { panic("cannot call Length on parquet group type") }
func (groupType) LogicalType() format.LogicalType         { return format.LogicalType{} }
func (groupType) ConvertedType() deprecated.ConvertedType { return -1 }
func (groupType) NewPageBuffer(int) PageBuffer            { panic("cannot create page buffer for parquet group") }
