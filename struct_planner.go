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
	t = dereference(t)

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

type setFn func(stack *valueStack, value reflect.Value)

// blueprint is a structure that parallels a schema, providing the information
// to build the actual Go types.
type blueprint struct {
	schema *Schema
	t      reflect.Type
	// Call this function to decode the value from readers.
	read func(d Decoder) (reflect.Value, error)
	// Call this function to set the value on the parent container.
	set setFn
	// Call this function to enter a new container. The function is responsible
	// to create and initialize any intermediate structure if necessary.
	enter func(stack *valueStack)
	// Call this function to leave a container. Likely just a stack update.
	leave func(stack *valueStack)

	parent   *blueprint
	children []*blueprint

	fieldPath []int // path to a field in nested strings
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
	p.enter = func(stack *valueStack) {
		newStruct := reflect.Zero(t)
		p.parent.set(stack, newStruct)
	}

	n := t.NumField()
	for i := 0; i < n; i++ {
		field := t.Field(i)
		if field.PkgPath != "" {
			// ignore non-exported fields
			continue
		}
		name := normalizeName(field.Name)
		child := &blueprint{
			schema: &Schema{
				Name: name,
				Path: newPath(p.schema.Path, name),
			},
			fieldPath: newFieldPath(p.fieldPath, i),
		}
		child.set = makeSetStructFieldFn(child)

		bpFromAny(child, field.Type)
		p.schema.Add(child.schema)
		p.add(child)
	}
}

func makeSetStructFieldFn(p *blueprint) setFn {
	return func(stack *valueStack, value reflect.Value) {
		// The expectation is that the top of the stack contains the struct.
		stack.view(len(p.fieldPath) - 1).top().FieldByIndex(p.fieldPath).Set(value)
	}
}

func bpFromSlice(p *blueprint, t reflect.Type) {
	p.t = t
	p.schema.Kind = RepeatedKind
	p.schema.ConvertedType = pthrift.ConvertedTypePtr(pthrift.ConvertedType_LIST)
	p.schema.Repetition = pthrift.FieldRepetitionType_REQUIRED

	list := &Schema{
		Name:       "list",
		Kind:       RepeatedKind,
		Repetition: pthrift.FieldRepetitionType_REPEATED,
		// TODO: write some functions to manipulate Schema that keep these invariants.
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
	ebp.set = makeSetSliceFn(ebp)
}

func makeSetSliceFn(p *blueprint) setFn {
	return func(stack *valueStack, value reflect.Value) {
		// Expect top of stack to be the slice.
		slice := reflect.Append(stack.top(), value)
		stack.replace(slice)
		// Re-set the stack on the parent as it may have been reallocated.
		view := stack.view(1)
		p.parent.set(view, slice)
	}
}

func newPath(path []string, name string) []string {
	newPath := make([]string, len(path)+1)
	copy(newPath, path)
	newPath[len(path)] = name
	return newPath
}

func newFieldPath(path []int, i int) []int {
	newPath := make([]int, len(path)+1)
	copy(newPath, path)
	newPath[len(path)] = i
	return newPath
}

func bpFromAny(p *blueprint, t reflect.Type) {
	t = dereference(t)

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
		p.read = func(d Decoder) (reflect.Value, error) {
			v, err := d.Int32()
			if err != nil {
				return reflect.Value{}, err
			}
			return reflect.ValueOf(v), nil
		}
	case reflect.String:
		p.schema.PhysicalType = pthrift.Type_BYTE_ARRAY
		p.schema.ConvertedType = pthrift.ConvertedTypePtr(pthrift.ConvertedType_UTF8)
		p.schema.LogicalType = pthrift.NewLogicalType()
		p.schema.LogicalType.STRING = pthrift.NewStringType()
		p.read = func(d Decoder) (reflect.Value, error) {
			b, err := d.ByteArray(nil)
			if err != nil {
				return reflect.Value{}, err
			}
			return reflect.ValueOf(string(b)), nil
		}
	default:
		panic(fmt.Errorf("unhandled kind: %s", t.Kind()))
	}
}

// recursively dereference a pointer type to a non pointer type
func dereference(t reflect.Type) reflect.Type {
	if t.Kind() != reflect.Ptr {
		return t
	}
	return dereference(t.Elem())
}

func normalizeName(name string) string {
	return strcase.ToSnake(name)
}

type valueStack struct {
	stack []reflect.Value
}

func (s *valueStack) push(v reflect.Value) {
	s.stack = append(s.stack, v)
}

func (s *valueStack) pop() {
	s.stack = s.stack[:len(s.stack)-1]
}

func (s *valueStack) replace(value reflect.Value) {
	s.stack[len(s.stack)-1] = value
}

func (s *valueStack) top() reflect.Value {
	return s.stack[len(s.stack)-1]
}

// view(0) is the same as the stack itself
func (s *valueStack) view(offset int) *valueStack {
	// TODO: that's on the hot path. not great
	return &valueStack{stack: s.stack[:len(s.stack)-offset]}
}
