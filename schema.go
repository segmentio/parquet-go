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

func (s *Schema) Construct(value reflect.Value) Object { return s.root.Construct(dereference(value)) }

func (s *Schema) String() string {
	b := new(strings.Builder)
	Print(b, s.name, s.root)
	return b.String()
}

func dereference(value reflect.Value) reflect.Value {
	if value.IsValid() {
		if value.Kind() != reflect.Ptr {
			return value
		}
		if !value.IsNil() {
			return value.Elem()
		}
	}
	return reflect.Value{}
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

func (s *structNode) Construct(value reflect.Value) Object {
	obj := &structObject{
		node:   s,
		fields: make([]Object, len(s.fields)),
	}

	structFields := s.fields
	if value.IsValid() {
		for i := range obj.fields {
			f := &structFields[i]
			obj.fields[i] = f.Construct(value.FieldByIndex(f.index))
		}
	} else {
		for i := range obj.fields {
			f := &structFields[i]
			obj.fields[i] = f.Construct(reflect.Value{})
		}
	}

	return obj
}

type structObject struct {
	node   *structNode
	fields []Object
}

func (obj *structObject) Len() int               { return len(obj.fields) }
func (obj *structObject) Index(index int) Object { return obj.fields[index] }
func (obj *structObject) Node() Node             { return obj.node }
func (obj *structObject) Value() Value           { panic("cannot call Value on parquet struct object") }
func (obj *structObject) Reset(value reflect.Value) {
	if value.IsValid() {
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

type structField struct {
	Node
	name  string
	index []int
}

func structFieldString(f reflect.StructField) string {
	return f.Name + " " + f.Type.String() + " " + string(f.Tag)
}

func throwInvalidFieldTag(f reflect.StructField, tag string) {
	panic("struct has invalid '" + tag + "' parquet tag: " + structFieldString(f))
}

func throwUnknownFieldTag(f reflect.StructField, tag string) {
	panic("struct has unrecognized '" + tag + "' parquet tag: " + structFieldString(f))
}

func makeStructField(f reflect.StructField, index []int) structField {
	field := structField{index: index}
	optional := false

	setNode := func(node Node) {
		if field.Node != nil {
			panic("struct field has multiple logical parquet types declared: " + structFieldString(f))
		}
		field.Node = node
	}

	if tag := f.Tag.Get("parquet"); tag != "" {
		field.name, tag = split(tag)

		for tag != "" {
			opt := ""
			opt, tag = split(tag)

			switch opt {
			case "optional":
				optional = true

			case "list":
				switch f.Type.Kind() {
				case reflect.Slice:
					setNode(List(nodeOf(f.Type.Elem())))
				default:
					throwInvalidFieldTag(f, opt)
				}

			case "enum":
				switch f.Type.Kind() {
				case reflect.String:
					setNode(Enum())
				default:
					throwInvalidFieldTag(f, opt)
				}

			case "uuid":
				switch f.Type.Kind() {
				case reflect.String:
					setNode(UUID())

				case reflect.Array:
					if f.Type.Elem().Kind() != reflect.Uint8 || f.Type.Len() != 16 {
						throwInvalidFieldTag(f, opt)
					}
				}

			default:
				throwUnknownFieldTag(f, tag)
			}
		}
	}

	if field.name == "" {
		field.name = f.Name
	}
	if field.Node == nil {
		field.Node = nodeOf(f.Type)
	}
	if optional {
		field.Node = Optional(field.Node)
	}
	return field
}

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
		return Optional(nodeOf(t.Elem()))
	case reflect.Struct:
		return structNodeOf(t)
	case reflect.Slice:
		return Repeated(nodeOf(t.Elem()))
	case reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 && t.Len() == 16 {
			return UUID()
		}
	}
	panic("cannot create parquet node from go value of type " + t.String())
}

func split(s string) (head, tail string) {
	if i := strings.IndexByte(s, ','); i < 0 {
		head = s
	} else {
		head, tail = s[:i], s[i+1:]
	}
	return
}
