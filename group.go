package parquet

import (
	"sort"

	"github.com/segmentio/parquet/compress"
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

func (g Group) Compression() []compress.Codec {
	codecs := make([]compress.Codec, 0, len(g))

	for _, node := range g {
		codecs = append(codecs, node.Compression()...)
	}

	sortCodecs(codecs)
	return dedupeSortedCodecs(codecs)
}

type groupType struct{}

func (groupType) Kind() Kind                               { panic("cannot call Kind on parquet GROUP type") }
func (groupType) Length() int                              { return 0 }
func (groupType) Less(Value, Value) bool                   { panic("cannot compare values on parquet GROUP type") }
func (groupType) PhyiscalType() *format.Type               { return nil }
func (groupType) LogicalType() *format.LogicalType         { return nil }
func (groupType) ConvertedType() *deprecated.ConvertedType { return nil }
func (groupType) NewPageBuffer(int) PageBuffer             { panic("cannot create page buffer for parquet group") }
