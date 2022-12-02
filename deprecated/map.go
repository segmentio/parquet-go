package deprecated

import "reflect"

type MapGroup[K comparable, V any] map[K]V

func (m MapGroup[K, V]) IsMapGroup() {}

type isMapGroup interface {
	IsMapGroup()
}

var MapGroupI = reflect.TypeOf((*isMapGroup)(nil)).Elem()
