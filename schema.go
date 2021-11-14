package parquet

import (
	"reflect"
	"sort"
	"strings"
)

type Schema struct {
	name string
	node Node
}

func SchemaOf(v interface{}) *Schema {
	return schemaOf(reflect.TypeOf(v))
}

func schemaOf(t reflect.Type) *Schema {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		panic("cannot construct parquet schema from value of type " + t.String())
	}
	return &Schema{
		name: t.Name(),
		node: structNodeOf(t),
	}
}

func (s *Schema) Name() string { return s.name }

func (s *Schema) Type() Type { return s.node.Type() }

func (s *Schema) Optional() bool { return s.node.Optional() }

func (s *Schema) Repeated() bool { return s.node.Repeated() }

func (s *Schema) Required() bool { return s.node.Required() }

func (s *Schema) NumChildren() int { return s.node.NumChildren() }

func (s *Schema) ChildNames() []string { return s.node.ChildNames() }

func (s *Schema) ChildByName(name string) Node { return s.node.ChildByName(name) }

func (s *Schema) String() string {
	b := new(strings.Builder)
	Print(b, s.name, s.node)
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
		if f := t.Field(i); f.Anonymous {
			subindex := index[:len(index):len(index)]
			subindex = append(subindex, i)
			s.init(f.Type, subindex)
		} else if f.IsExported() {
			s.fields = append(s.fields, makeStructField(f, index))
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

func (f *structField) Type() Type                   { return f.node.Type() }
func (f *structField) Optional() bool               { return f.optional }
func (f *structField) Repeated() bool               { return f.repeated }
func (f *structField) Required() bool               { return !f.optional && !f.repeated }
func (f *structField) NumChildren() int             { return f.node.NumChildren() }
func (f *structField) ChildNames() []string         { return f.node.ChildNames() }
func (f *structField) ChildByName(name string) Node { return f.node.ChildByName(name) }

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
		return UTF8()
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
