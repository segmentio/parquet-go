package parquet

import (
	"fmt"
	"reflect"

	"github.com/iancoleman/strcase"
	pthrift "github.com/segmentio/centrifuge-traces/parquet/internal/gen-go/parquet"
)

// FromValue builds a schema tree by reflecting on a provided value v.
//
// It uses the following mapping between Go values and Parquet
// schema:
//
//   TODO
//
// Optionals are decoded as pointers of the type they wrap.
//
// By default, it maps a field's name to its snake_case equivalent. You can
// overwrite this behavior on a per-field basis using the `parquet:"..."` field
// annotation.
//
// Only exported fields are considered.
func FromValue(v interface{}) *Schema {
	t := reflect.TypeOf(v)
	t = unwrap(t)

	root := fromStruct(t)
	root.Name = "root"
	root.Root = true

	return root
}

// fromStruct creates a schema tree from the provided struct type.
func fromStruct(t reflect.Type) *Schema {
	assertKind(t, reflect.Struct)

	node := &Schema{
		Kind: GroupKind,
	}

	n := t.NumField()
	for i := 0; i < n; i++ {
		field := t.Field(i)
		if field.PkgPath != "" {
			// ignore non-exported fields
			continue
		}
		child := fromAny(field.Type)
		child.Name = normalizeName(field.Name)
		node.Add(child)
	}

	return node
}

// fromAny creates a schema tree for any type (does the dispatch to the right
// from* method).
func fromAny(t reflect.Type) *Schema {
	t = unwrap(t)

	switch t.Kind() {
	case reflect.Struct:
		return fromStruct(t)
	case reflect.Int32:
		return fromPrimitive(t)
	default:
		panic(fmt.Errorf("unhandled kind: %s", t.Kind()))
	}
}

// fromPrimitive creates a schema leaf for a Go type that maps directly to a
// Parquet primitive type.
func fromPrimitive(t reflect.Type) *Schema {
	var physicalType pthrift.Type
	switch t.Kind() {
	case reflect.Int32:
		physicalType = pthrift.Type_INT32
	default:
		panic(fmt.Errorf("unhandled kind: %s", t.Kind()))
	}

	node := &Schema{
		Kind:          PrimitiveKind,
		PhysicalType:  physicalType,
		ConvertedType: nil,
		LogicalType:   nil,
	}

	return node
}

func assertKind(t reflect.Type, expected reflect.Kind) {
	if t.Kind() != expected {
		panic(fmt.Errorf("kind should be %s, not %s", expected, t.Kind()))
	}
}

// recursively unwrap a pointer type to a non pointer type
func unwrap(t reflect.Type) reflect.Type {
	if t.Kind() != reflect.Ptr {
		return t
	}
	return unwrap(t.Elem())
}

func normalizeName(name string) string {
	return strcase.ToSnake(name)
}
