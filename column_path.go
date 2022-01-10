package parquet

import "strings"

type columnPath []string

func (path columnPath) append(name string) columnPath {
	return append(path[:len(path):len(path)], name)
}

func (path columnPath) equal(other columnPath) bool {
	return stringsAreEqual(path, other)
}

func (path columnPath) less(other columnPath) bool {
	return stringsAreOrdered(path, other)
}

func (path columnPath) String() string {
	return strings.Join(path, ".")
}

func stringsAreEqual(strings1, strings2 []string) bool {
	if len(strings1) != len(strings2) {
		return false
	}

	for i := range strings1 {
		if strings1[i] != strings2[i] {
			return false
		}
	}

	return true
}

func stringsAreOrdered(strings1, strings2 []string) bool {
	n := len(strings1)

	if n > len(strings2) {
		n = len(strings2)
	}

	for i := 0; i < n; i++ {
		if strings1[i] >= strings2[i] {
			return false
		}
	}

	return len(strings1) <= len(strings2)
}

type leafColumn struct {
	node               Node
	path               columnPath
	columnIndex        int
	maxRepetitionLevel int8
	maxDefinitionLevel int8
}

func forEachLeafColumnOf(node Node, do func(leafColumn)) {
	forEachLeafColumn(node, nil, 0, 0, 0, do)
}

func forEachLeafColumn(node Node, path columnPath, columnIndex int, maxRepetitionLevel, maxDefinitionLevel int8, do func(leafColumn)) int {
	switch {
	case node.Optional():
		maxDefinitionLevel++
	case node.Repeated():
		maxRepetitionLevel++
		maxDefinitionLevel++
	}

	if isLeaf(node) {
		do(leafColumn{
			node:               node,
			path:               path,
			columnIndex:        columnIndex,
			maxRepetitionLevel: maxRepetitionLevel,
			maxDefinitionLevel: maxDefinitionLevel,
		})
		return columnIndex + 1
	}

	for _, name := range node.ChildNames() {
		columnIndex = forEachLeafColumn(
			node.ChildByName(name),
			path.append(name),
			columnIndex,
			maxRepetitionLevel,
			maxDefinitionLevel,
			do,
		)
	}

	return columnIndex
}
