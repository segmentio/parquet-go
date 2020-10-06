package parquet

import (
	"fmt"
	"reflect"

	"github.com/iancoleman/strcase"
	pthrift "github.com/segmentio/parquet/internal/gen-go/parquet"
)

// StructPlanner acts a factory for StructBuilder, and generates a Plan for
// RowBuilder that maps to the struct.
type StructPlanner struct {
	blueprint *blueprint
	index     map[*Schema]*blueprint
}

// StructPlanOf builds a StructPlanner using v's type as a basis.
// v is expected to be a pointer to a struct.
//
// Mapping of Go primitive types to Parquet schema:
// (format: Go -> parquet primitive type [| annotation])
//
//   bool    -> BOOLEAN
//   int     -> INT64 | INT(bits=64, signed=true)
//   int8    -> INT32 | INT(bits=8,  signed=true)
//   int16   -> INT32 | INT(bits=16, signed=true)
//   int32   -> INT32 | INT(bits=32, signed=true)
//   int64   -> INT64 | INT(bits=64, signed=true)
//   uint    -> INT32 | INT(bits=64, signed=false)
//   uint8   -> INT32 | INT(bits=8,  signed=false)
//   uint16  -> INT32 | INT(bits=16, signed=false)
//   uint32  -> INT32 | INT(bits=32, signed=false)
//   uint64  -> INT64 | INT(bits=64, signed=false)
//   float32 -> FLOAT
//   float64 -> DOUBLE
//   string  -> BYTE_ARRAY | STRING
//
// Go composite types:
//
//   Array/slice: []T ->
//     <list-repetition> group <name> (LIST) {
//       repeated group list {
//         <element-repetition> <element-type> element;
//       }
//     }
//
//   Maps: map[K]V ->
//     <map-repetition> group <name> (MAP) {
//       repeated group key_value {
//         required <key-type> key;
//         <value-repetition> <value-type> value;
//       }
//     }
//
//   Structs: ->
//     [optional] group <name> {
//        fields...
//     }
//
// Special cases:
//
//   Byte array/slice: []byte -> BYTE_ARRAY
//   Timestamp: time.Time -> INT64 | TIMESTAMP(isAdjustedToUTC=true, precision=NANOS)
//
// All pointers are treated as optional.
//
// Types not listed here are not supported.
func StructPlannerOf(v interface{}) *StructPlanner {
	t := reflect.TypeOf(v)
	t = derefence(t)

	root := &blueprint{
		schema: &Schema{},
	}
	bpFromStruct(root, t)
	root.schema.Name = "root"
	root.schema.Root = true

	index := map[*Schema]*blueprint{}
	root.register(index)

	return &StructPlanner{
		blueprint: root,
		index:     index,
	}
}

func (sp *StructPlanner) Builder() *StructBuilder {
	return &StructBuilder{
		index: sp.index,
	}
}

func (sp *StructPlanner) Plan() *Plan {
	return &Plan{
		s: sp.blueprint.schema,
	}
}

// blueprint is a structure that parallels a schema, providing the information
// to build the actual Go types.
type blueprint struct {
	schema   *Schema
	t        reflect.Type
	parent   *blueprint
	children []*blueprint
	idx      int // field index
}

func (bp *blueprint) add(child *blueprint) {
	bp.children = append(bp.children, child)
	child.parent = bp
}

func (bp *blueprint) register(index map[*Schema]*blueprint) {
	index[bp.schema] = bp
	for _, c := range bp.children {
		c.register(index)
	}
}

func bpFromStruct(p *blueprint, t reflect.Type) {
	p.schema.Kind = GroupKind
	p.t = t

	n := t.NumField()
	for i := 0; i < n; i++ {
		field := t.Field(i)
		if field.PkgPath != "" {
			// ignore non-exported fields
			continue
		}
		name := normalizeName(field.Name)
		child := &blueprint{
			idx: i,
			schema: &Schema{
				Name: name,
				Path: newPath(p.schema.Path, name),
			},
		}
		bpFromAny(child, field.Type)
		p.schema.Add(child.schema)
		p.add(child)
	}
}

func bpFromSlice(p *blueprint, t reflect.Type) {
	p.t = t
	p.schema.Kind = RepeatedKind
	p.schema.ConvertedType = pthrift.ConvertedTypePtr(pthrift.ConvertedType_LIST)
	p.schema.Repetition = pthrift.FieldRepetitionType_REQUIRED

	list := &Schema{
		Name:            "list",
		Kind:            RepeatedKind,
		Repetition:      pthrift.FieldRepetitionType_REPEATED,
		RepetitionLevel: p.schema.RepetitionLevel + 1,
		DefinitionLevel: p.schema.DefinitionLevel + 1,
		Path:            newPath(p.schema.Path, "list"),
	}
	p.schema.Add(list)

	element := &Schema{
		Name:            "element",
		Path:            newPath(list.Path, "element"),
		RepetitionLevel: list.RepetitionLevel,
		DefinitionLevel: list.DefinitionLevel,
	}
	list.Add(element)

	ebp := &blueprint{
		schema: element,
	}
	p.add(ebp)

	bpFromAny(ebp, t.Elem())
}

func newPath(path []string, name string) []string {
	newPath := make([]string, len(path)+1)
	copy(newPath, path)
	newPath[len(path)] = name
	return newPath
}

func bpFromAny(p *blueprint, t reflect.Type) {
	t = derefence(t)

	switch t.Kind() {
	case reflect.Struct:
		bpFromStruct(p, t)
	case reflect.Int32, reflect.String:
		bpFromPrimitive(p, t)
	case reflect.Slice:
		bpFromSlice(p, t)
	default:
		panic(fmt.Errorf("unhandled kind: %s", t.Kind()))
	}
}

// fromPrimitive creates a schema leaf for a Go type that maps directly to a
// Parquet primitive type.
func bpFromPrimitive(p *blueprint, t reflect.Type) {
	// TODO: having to repeat the same case as in fromAny is not great.
	p.t = t
	p.schema.Kind = PrimitiveKind
	switch t.Kind() {
	case reflect.Int32:
		p.schema.PhysicalType = pthrift.Type_INT32
	case reflect.String:
		p.schema.PhysicalType = pthrift.Type_BYTE_ARRAY
		p.schema.ConvertedType = pthrift.ConvertedTypePtr(pthrift.ConvertedType_UTF8)
		p.schema.LogicalType = pthrift.NewLogicalType()
		p.schema.LogicalType.STRING = pthrift.NewStringType()
	default:
		panic(fmt.Errorf("unhandled kind: %s", t.Kind()))
	}
}

// recursively derefence a pointer type to a non pointer type
func derefence(t reflect.Type) reflect.Type {
	if t.Kind() != reflect.Ptr {
		return t
	}
	return derefence(t.Elem())
}

func normalizeName(name string) string {
	return strcase.ToSnake(name)
}
