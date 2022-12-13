package parquet

// RowBuilder is a type which helps build parquet rows incrementally by adding
// values to columns.
type RowBuilder struct {
	columns [][]Value
	levels  []columnLevel
	groups  []*columnGroup
}

type columnLevel struct {
	repetitionLevel byte
	definitionLevel byte
}

type columnGroup struct {
	members []columnGroupMember
	columnLevel
}

type columnGroupMember struct {
	columnKind  Kind
	columnIndex int16
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
		levels:  make([]columnLevel, n),
	}
	buffers := make([]Value, len(b.columns))
	for i := range b.columns {
		b.columns[i] = buffers[i : i : i+1]
	}
	topGroup := new(columnGroup)
	b.configureGroup(schema, 0, columnLevel{}, topGroup)
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
	group.definitionLevel++
	level.definitionLevel++
	return b.configureRequired(node, columnIndex, level, group)
}

func (b *RowBuilder) configureRepeated(node Node, columnIndex int16, level columnLevel, group *columnGroup) int16 {
	level.repetitionLevel++
	level.definitionLevel++

	group = &columnGroup{columnLevel: level}
	endIndex := b.configureRequired(node, columnIndex, level, group)

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
	group.members = append(group.members, columnGroupMember{
		columnKind:  node.Type().Kind(),
		columnIndex: columnIndex,
	})
	b.levels[columnIndex] = level
	return columnIndex + 1
}

// Add adds columnValue to the column at columnIndex.
func (b *RowBuilder) Add(columnIndex int, columnValue Value) {
	level := b.levels[columnIndex]
	columnValue.repetitionLevel = level.repetitionLevel
	columnValue.definitionLevel = level.definitionLevel
	if columnValue.kind == 0 && columnValue.definitionLevel != 0 {
		columnValue.definitionLevel--
	}
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
		if v.kind == 0 && v.definitionLevel != 0 {
			v.definitionLevel--
		}
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
		maxColumn := []Value{{
			repetitionLevel: group.repetitionLevel,
			definitionLevel: group.definitionLevel,
		}}

		for _, member := range group.members {
			if column := b.columns[member.columnIndex]; len(column) > len(maxColumn) {
				maxColumn = column
			}
		}

		for _, member := range group.members {
			if column := b.columns[member.columnIndex]; len(column) < len(maxColumn) {
				n := len(column)
				column = append(column, maxColumn[n:]...)

				for n < len(column) {
					v := &column[n]
					v.kind = ^int8(member.columnKind)
					v.ptr = nil
					v.u64 = 0
					if v.definitionLevel != 0 && v.definitionLevel == group.definitionLevel {
						v.definitionLevel--
					}
					n++
				}

				b.columns[member.columnIndex] = column
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
