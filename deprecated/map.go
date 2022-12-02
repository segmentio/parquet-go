package deprecated

import "reflect"

type MapGroup[K comparable, V any] map[K]V

func (m MapGroup[K, V]) IsMapGroup() {}

type IsMapGroup interface {
	IsMapGroup()
}

var MapGroupI = reflect.TypeOf((*IsMapGroup)(nil)).Elem()
