package deprecated

import "reflect"

// MapGroup is an implementation of the deprecated `repeated group map`
// syntax that older parquet writers emit.
type MapGroup[K comparable, V any] map[K]V

func (m MapGroup[K, V]) isMapGroup() {}

// This interface allows us to use reflection to determine if our
// map is a MapGroup. Unfortuntely there's no easy way to use
// reflect.TypeOf on a generic map.
type isMapGroup interface {
	isMapGroup()
}

var mapGroupI = reflect.TypeOf((*isMapGroup)(nil)).Elem()

func IsMap(t reflect.Type) bool {
	return t.Implements(mapGroupI)
}
