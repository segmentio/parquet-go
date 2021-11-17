package parquet

import (
	"fmt"
	"reflect"
)

type Object interface {
	Len() int

	Index(int) Object

	Node() Node

	Value() Value

	Reset(reflect.Value)
}

type Row struct {
	stack  []iterator
	column int
	value  Value
}

func NewRow(object Object) *Row {
	row := new(Row)
	row.Reset(object)
	return row
}

func (row *Row) Reset(object Object) {
	for i := range row.stack {
		row.stack[i] = iterator{}
	}

	row.stack = row.stack[:0]
	row.column = -1
	row.value = Value{}

	if object != nil {
		row.stack = append(row.stack, makeIterator(object, 0))
	}
}

func (row *Row) Next() bool {
	for {
		if len(row.stack) == 0 {
			row.value = Value{}
			return false
		}

		it := &row.stack[len(row.stack)-1]

		if it.index++; it.index < it.limit {
			elem := it.object.Index(it.index)
			node := elem.Node()

			fmt.Printf("%#v (repeated=%t)\n", node, node.Repeated())

			if node.NumChildren() > 0 || node.Repeated() {
				row.stack = append(row.stack, makeIterator(elem, row.column))
				continue
			}

			if it.index == 0 && it.repeated {
				row.column = it.column
			} else {
				row.column++
			}

			row.value = elem.Value()
			return true
		}

		i := len(row.stack) - 1
		row.stack[i] = iterator{}
		row.stack = row.stack[:i]
	}
}

func (row *Row) ColumnIndex() int {
	return int(row.column)
}

func (row *Row) Value() Value {
	return row.value
}

type iterator struct {
	object   Object
	column   int
	index    int
	limit    int
	repeated bool
}

func makeIterator(object Object, column int) iterator {
	return iterator{
		object:   object,
		column:   column,
		index:    -1,
		limit:    object.Len(),
		repeated: object.Node().Repeated(),
	}
}
