package parquet

import (
	"reflect"
	"sort"
	"strings"
)

type Schema struct {
	name string
	root Node
}

func NewSchema(name string, root Node) *Schema {
	return &Schema{
		name: name,
		root: root,
	}
}

func SchemaOf(model interface{}) *Schema {
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return NamedSchemaOf(t.Name(), model)
}

func NamedSchemaOf(name string, model interface{}) *Schema {
	return namedSchemaOf(name, reflect.ValueOf(model))
}

func namedSchemaOf(name string, model reflect.Value) *Schema {
	switch t := model.Type(); t.Kind() {
	case reflect.Struct:
		return NewSchema(name, structNodeOf(t))

	case reflect.Ptr:
		if elem := t.Elem(); elem.Kind() == reflect.Struct {
			return NewSchema(name, structNodeOf(elem))
		}

	case reflect.Map:

	}

	panic("cannot construct parquet schema from value of type " + model.Type().String())
}

func (s *Schema) Name() string { return s.name }

func (s *Schema) Type() Type { return s.root.Type() }

func (s *Schema) Optional() bool { return s.root.Optional() }

func (s *Schema) Repeated() bool { return s.root.Repeated() }

func (s *Schema) Required() bool { return s.root.Required() }

func (s *Schema) NumChildren() int { return s.root.NumChildren() }

func (s *Schema) ChildNames() []string { return s.root.ChildNames() }

func (s *Schema) ChildByName(name string) Node { return s.root.ChildByName(name) }

func (s *Schema) Object(value reflect.Value) Object { return s.root.Object(value) }

func (s *Schema) String() string {
	b := new(strings.Builder)
	Print(b, s.name, s.root)
	return b.String()
}

type structNode struct {
	fields []structField
	names  []string
}

func structNodeOf(t reflect.Type) *structNode {
	s := &structNode{fields: make([]structField, 0, t.NumField())}
	s.init(t, nil)

	sort.Slice(s.fields, func(i, j int) bool {
		return s.fields[i].name < s.fields[j].name
	})

	s.names = make([]string, len(s.fields))
	for i := range s.fields {
		s.names[i] = s.fields[i].name
	}
	return s
}

func (s *structNode) init(t reflect.Type, index []int) {
	for i, n := 0, t.NumField(); i < n; i++ {
		fieldIndex := index[:len(index):len(index)]
		fieldIndex = append(fieldIndex, i)

		if f := t.Field(i); f.Anonymous {
			s.init(f.Type, fieldIndex)
		} else if f.IsExported() {
			s.fields = append(s.fields, makeStructField(f, fieldIndex))
		}
	}
}

func (s *structNode) Type() Type           { return groupType{} }
func (s *structNode) Optional() bool       { return false }
func (s *structNode) Repeated() bool       { return false }
func (s *structNode) Required() bool       { return true }
func (s *structNode) NumChildren() int     { return len(s.fields) }
func (s *structNode) ChildNames() []string { return s.names }
func (s *structNode) ChildByName(name string) Node {
	i := sort.Search(len(s.fields), func(i int) bool {
		return s.fields[i].name >= name
	})
	if i >= 0 && i < len(s.fields) {
		return &s.fields[i]
	}
	panic("column not found in parquet schema: " + name)
}

func (s *structNode) Object(value reflect.Value) Object {
	obj := &structObject{
		node:   s,
		fields: make([]Object, len(s.fields)),
	}

	structFields := s.fields
	if value = dereference(value); value.IsValid() {
		for i := range obj.fields {
			f := &structFields[i]
			obj.fields[i] = f.Object(value.FieldByIndex(f.index))
		}
	} else {
		for i := range obj.fields {
			f := &structFields[i]
			obj.fields[i] = f.Object(reflect.Value{})
		}
	}

	return obj
}

func dereference(value reflect.Value) reflect.Value {
	if value.IsValid() && value.Kind() == reflect.Ptr && !value.IsNil() {
		return value.Elem()
	}
	return value
}

type structObject struct {
	node   *structNode
	fields []Object
}

func (obj *structObject) Len() int {
	return len(obj.fields)
}

func (obj *structObject) Index(index int) Object {
	return obj.fields[index]
}

func (obj *structObject) Value() Value {
	panic("cannot call Value on parquet struct object")
}

func (obj *structObject) Reset(value reflect.Value) {
	if value = dereference(value); value.IsValid() {
		structFields := obj.node.fields
		for i, field := range obj.fields {
			field.Reset(value.FieldByIndex(structFields[i].index))
		}
	} else {
		for _, field := range obj.fields {
			field.Reset(reflect.Value{})
		}
	}
}

func (obj *structObject) reset() {
	for _, field := range obj.fields {
		field.Reset(reflect.Value{})
	}
}

type structField struct {
	node     Node
	name     string
	optional bool
	repeated bool
	index    []int
}

func makeStructField(f reflect.StructField, index []int) structField {
	field := structField{
		name:  f.Name,
		index: index,
	}

	if tag := f.Tag.Get("parquet"); tag != "" {
		field.name, tag = split(tag)

		for tag != "" {
			opt := ""
			opt, tag = split(tag)

			switch opt {
			case "optional":
				field.optional = true

			case "list":

			case "enum":
				switch f.Type.Kind() {
				case reflect.String:
					field.node = Enum()
				default:
					panic("struct has invalid 'enum' parquet tag on field of type " + f.Type.String())
				}

			case "uuid":

			default:
				panic("struct field contains unknown parquet tag: " + opt)
			}
		}
	}

	if field.node == nil {
		field.node = nodeOf(f.Type)
	}

	return field
}

func (f *structField) Type() Type                        { return f.node.Type() }
func (f *structField) Optional() bool                    { return f.optional }
func (f *structField) Repeated() bool                    { return f.repeated }
func (f *structField) Required() bool                    { return !f.optional && !f.repeated }
func (f *structField) NumChildren() int                  { return f.node.NumChildren() }
func (f *structField) ChildNames() []string              { return f.node.ChildNames() }
func (f *structField) ChildByName(name string) Node      { return f.node.ChildByName(name) }
func (f *structField) Object(value reflect.Value) Object { return f.node.Object(value) }

func nodeOf(t reflect.Type) Node {
	switch t.Kind() {
	case reflect.Int:
		return Int(64)
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return Int(t.Bits())
	case reflect.Uint:
		return Uint(64)
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return Uint(t.Bits())
	case reflect.Float32:
		return Decimal(0, 9, FloatType)
	case reflect.Float64:
		return Decimal(0, 18, DoubleType)
	case reflect.String:
		return String()
	case reflect.Ptr:
		return nodeOf(t.Elem())
	case reflect.Struct:
		return structNodeOf(t)
	default:
		panic("cannot create parquet node from go value of type " + t.String())
	}
}

func split(s string) (head, tail string) {
	if i := strings.IndexByte(s, ','); i < 0 {
		head = s
	} else {
		head, tail = s[:i], s[i+1:]
	}
	return
}
