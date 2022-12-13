package parquet

// RowBuilder is a type which helps build parquet rows incrementally by adding
// values to columns.
type RowBuilder struct {
	columns [][]Value
	kinds   []int8
	levels  []columnLevel
	groups  []*columnGroup
}

type columnLevel struct {
	repetitionLevel byte
	definitionLevel byte
}

type columnGroup struct {
	baseColumn []Value
	members    []int16
	startIndex int16
	endIndex   int16
	columnLevel
}

// NewRowBuilder constructs a RowBuilder which builds rows for the parquet
// schema passed as argument.
func NewRowBuilder(schema Node) *RowBuilder {
	if schema.Leaf() {
		panic("schema of row builder must be a group")
	}
	n := numLeafColumnsOf(schema)
	b := &RowBuilder{
		columns: make([][]Value, n),
		kinds:   make([]int8, n),
		levels:  make([]columnLevel, n),
	}
	buffers := make([]Value, len(b.columns))
	for i := range b.columns {
		b.columns[i] = buffers[i : i : i+1]
	}
	topGroup := &columnGroup{baseColumn: []Value{{}}}
	endIndex := b.configureGroup(schema, 0, columnLevel{}, topGroup)
	topGroup.endIndex = endIndex
	b.groups = append(b.groups, topGroup)
	return b
}

func (b *RowBuilder) configure(node Node, columnIndex int16, level columnLevel, group *columnGroup) int16 {
	switch {
	case node.Optional():
		return b.configureOptional(node, columnIndex, level, group)
	case node.Repeated():
		return b.configureRepeated(node, columnIndex, level, group)
	default:
		return b.configureRequired(node, columnIndex, level, group)
	}
}

func (b *RowBuilder) configureOptional(node Node, columnIndex int16, level columnLevel, group *columnGroup) int16 {
	level.definitionLevel++
	endIndex := b.configureRequired(node, columnIndex, level, group)

	for i := columnIndex; i < endIndex; i++ {
		b.kinds[i] = 0 // null if not set
	}

	return endIndex
}

func (b *RowBuilder) configureRepeated(node Node, columnIndex int16, level columnLevel, group *columnGroup) int16 {
	level.repetitionLevel++
	level.definitionLevel++

	group = &columnGroup{startIndex: columnIndex, columnLevel: level}
	endIndex := b.configureRequired(node, columnIndex, level, group)

	for i := columnIndex; i < endIndex; i++ {
		b.kinds[i] = 0 // null if not set
	}

	group.endIndex = endIndex
	b.groups = append(b.groups, group)
	return endIndex
}

func (b *RowBuilder) configureRequired(node Node, columnIndex int16, level columnLevel, group *columnGroup) int16 {
	switch {
	case node.Leaf():
		return b.configureLeaf(node, columnIndex, level, group)
	default:
		return b.configureGroup(node, columnIndex, level, group)
	}
}

func (b *RowBuilder) configureGroup(node Node, columnIndex int16, level columnLevel, group *columnGroup) int16 {
	endIndex := columnIndex
	for _, field := range node.Fields() {
		endIndex = b.configure(field, endIndex, level, group)
	}
	return endIndex
}

func (b *RowBuilder) configureLeaf(node Node, columnIndex int16, level columnLevel, group *columnGroup) int16 {
	group.members = append(group.members, columnIndex)
	b.kinds[columnIndex] = ^int8(node.Type().Kind())
	b.levels[columnIndex] = level
	return columnIndex + 1
}

// Add adds columnValue to the column at columnIndex.
func (b *RowBuilder) Add(columnIndex int, columnValue Value) {
	level := b.levels[columnIndex]
	columnValue.repetitionLevel = level.repetitionLevel
	columnValue.definitionLevel = level.definitionLevel
	b.columns[columnIndex] = append(b.columns[columnIndex], columnValue)
}

// Set sets the values at columnIndex to a copy of columnValues.
func (b *RowBuilder) Set(columnIndex int, columnValues ...Value) {
	column := b.columns[columnIndex][:0]

	i := len(column)
	column = append(column, columnValues...)

	level := b.levels[columnIndex]
	for i < len(column) {
		v := &column[i]
		v.repetitionLevel = level.repetitionLevel
		v.definitionLevel = level.definitionLevel
		i++
	}

	b.columns[columnIndex] = column
}

// Reset clears the internal state of b, making it possible to reuse while
// retaining the internal buffers.
func (b *RowBuilder) Reset() {
	for i, column := range b.columns {
		clearValues(column)
		b.columns[i] = column[:0]
	}
}

// Row materializes the current state of b into a parquet row.
func (b *RowBuilder) Row() Row {
	numValues := 0
	for _, column := range b.columns {
		numValues += len(column)
	}
	return b.AppendRow(make(Row, 0, numValues))
}

// AppendRow appends the current state of b to row and returns it.
func (b *RowBuilder) AppendRow(row Row) Row {
	for _, group := range b.groups {
		maxColumn := group.baseColumn

		for _, columnIndex := range group.members {
			if column := b.columns[columnIndex]; len(column) > len(maxColumn) {
				maxColumn = column
			}
		}

		if len(maxColumn) != 0 {
			columns := b.columns[group.startIndex:group.endIndex]

			for i, column := range columns {
				if len(column) < len(maxColumn) {
					n := len(column)
					column = append(column, maxColumn[n:]...)

					kind := b.kinds[group.startIndex+int16(i)]
					for n < len(column) {
						v := &column[n]
						v.kind = kind
						v.ptr = nil
						v.u64 = 0
						v.repetitionLevel = group.repetitionLevel
						v.definitionLevel = group.definitionLevel
						n++
					}

					columns[i] = column
				}
			}
		}
	}

	for i, column := range b.columns {
		columnIndex := ^int16(i)
		column[0].repetitionLevel = 0
		column[0].columnIndex = columnIndex

		for j := 1; j < len(column); j++ {
			column[j].columnIndex = columnIndex
		}

		row = append(row, column...)
	}

	return row
}
