package parquet

import "reflect"

type Object interface {
	Len() int

	Index(int) Object

	Value() Value

	Reset(reflect.Value)
}

type Row struct {
	stack []iterator
	value Value
}

type iterator struct {
	object Object
	index  int
	limit  int
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
	row.value = Value{}

	if object != nil {
		limit := object.Len()

		if limit == 0 {
			panic("cannot initialize parquet row with object of length zero")
		}

		row.stack = append(row.stack, iterator{
			object: object,
			index:  -1,
			limit:  limit,
		})
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
			child := it.object.Index(it.index)
			limit := child.Len()

			if limit > 0 {
				row.stack = append(row.stack, iterator{
					object: child,
					index:  -1,
					limit:  limit,
				})
				continue
			}

			row.value = child.Value()
			return true
		}

		i := len(row.stack) - 1
		row.stack[i] = iterator{}
		row.stack = row.stack[:i]
	}
}

func (row *Row) Value() Value {
	return row.value
}

func (row *Row) ColumnIndex() int {
	return len(row.stack) - 1
}
