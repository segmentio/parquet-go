package parquet

func columnMappingOf(schema Node) (mapping columnMappingGroup, columns [][]string) {
	mapping = make(columnMappingGroup)
	columns = make([][]string, 0, 16)

	forEachLeafColumnOf(schema, func(leaf leafColumn) {
		column := make([]string, len(leaf.path))
		copy(column, leaf.path)
		columns = append(columns, column)

		group, path := mapping, leaf.path

		for len(path) > 1 {
			columnName := path[0]
			g, ok := group[columnName].(columnMappingGroup)
			if !ok {
				g = make(columnMappingGroup)
				group[columnName] = g
			}
			group, path = g, path[1:]
		}

		group[path[0]] = &columnMappingLeaf{
			columnIndex: leaf.columnIndex,
			columnNode:  leaf.node,
		}
	})

	return mapping, columns
}

type columnMapping interface {
	lookup(path columnPath) (columnIndex int16, columnNode Node)
}

type columnMappingGroup map[string]columnMapping

func (group columnMappingGroup) lookup(path columnPath) (int16, Node) {
	if len(path) > 0 {
		c, ok := group[path[0]]
		if ok {
			return c.lookup(path[1:])
		}
	}
	return -1, nil
}

type columnMappingLeaf struct {
	columnIndex int16
	columnNode  Node
}

func (leaf *columnMappingLeaf) lookup(path columnPath) (int16, Node) {
	if len(path) == 0 {
		return leaf.columnIndex, leaf.columnNode
	}
	return -1, nil
}
