package parquet

import "reflect"

type Row struct {
	value reflect.Value
}

func RowOf(v interface{}) Row {
	return rowOf(reflect.ValueOf(v))
}

func rowOf(v reflect.Value) Row {
	switch v.Kind() {
	// TODO
	}

	return Row{value: v}
}

func (row Row) IsValid() bool {
	return row.value.IsValid()
}

func (row Row) NumChildren() int {
	return row.value.Len()
}

func (row Row) ChildByName(name string) Row {
	return rowOf(row.value.MapIndex(reflect.ValueOf(name)))
}
