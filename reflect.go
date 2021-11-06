package parquet

import (
	"io"
	"reflect"
	"strings"

	"github.com/segmentio/parquet/schema"
)

type Kind int

const (
	Boolean Kind = iota
	Int32
	Int64
	Int96
	Float
	Double
	ByteArray
	FixedLenByteArray
)

func (k Kind) String() string {
	return schema.Type(k).String()
}

type Type interface {
	Kind() Kind
	Length() int
}

type schemaElementType struct {
	*schema.SchemaElement
}

func (t schemaElementType) Kind() Kind  { return Kind(t.Type) }
func (t schemaElementType) Length() int { return int(t.TypeLength) }

type primitiveType struct {
	kind   Kind
	length int
}

func (t *primitiveType) Kind() Kind  { return t.kind }
func (t *primitiveType) Length() int { return t.length }

type MessageType interface {
	Name() string

	NumField() int

	Field(index int) FieldType
}

type FieldType interface {
	Name() string

	NumField() int

	Field(index int) FieldType

	Optional() bool

	Repeated() bool

	Required() bool

	Group() bool

	Type() Type
}

func Format(m MessageType) string {
	s := &strings.Builder{}
	formatMessageType(s, m)
	return s.String()
}

func formatMessageType(s io.StringWriter, m MessageType) {
	s.WriteString("message ")
	s.WriteString(m.Name())
	s.WriteString(" {")

	n := m.NumField()
	if n > 0 {
		s.WriteString("\n")
	}

	for i := 0; i < n; i++ {
		formatFieldType(s, m.Field(i), 2)
		s.WriteString("\n")
	}

	s.WriteString("}")
}

func formatFieldType(s io.StringWriter, f FieldType, indent int) {
	spaces := "                "
	if indent > len(spaces) {
		spaces = strings.Repeat(" ", indent)
	}
	s.WriteString(spaces[:indent])

	switch {
	case f.Optional():
		s.WriteString("optional ")
	case f.Repeated():
		s.WriteString("repeated ")
	default:
		s.WriteString("required ")
	}

	if f.Group() {
		s.WriteString("group ")
	} else {
		switch f.Type().Kind() {
		case Boolean:
			s.WriteString("boolean ")
		case Int32:
			s.WriteString("int32 ")
		case Int64:
			s.WriteString("int64 ")
		case Int96:
			s.WriteString("int96 ")
		case Float:
			s.WriteString("float ")
		case Double:
			s.WriteString("double ")
		case ByteArray:
			s.WriteString("binary ")
		case FixedLenByteArray:
			s.WriteString("fixed_len_byte_array ")
		default:
			s.WriteString("<?> ")
		}
	}

	s.WriteString(f.Name())

	if f.Group() {
		s.WriteString(" {")

		n := f.NumField()
		if n > 0 {
			s.WriteString("\n")
		}

		for i := 0; i < n; i++ {
			formatFieldType(s, f.Field(i), indent+2)
		}

		s.WriteString("}")
	} else {
		s.WriteString(";")
	}
}

func MessageTypeOf(t reflect.Type) MessageType {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		panic("cannot create a parquet message from a go type " + t.String())
	}
	fields := make([]FieldType, t.NumField())
	return &messageType{
		name:   t.Name(),
		fields: appendStructFieldTypes(fields[:0], t, nil, make(map[reflect.Type]FieldType)),
	}
}

func appendStructFieldTypes(fields []FieldType, t reflect.Type, index []int, seen map[reflect.Type]FieldType) []FieldType {
	for i, n := 0, t.NumField(); i < n; i++ {
		if f := t.Field(i); f.IsExported() {
			if f.Anonymous {
				fieldIndex := index[:len(index):len(index)]
				fieldIndex = append(fieldIndex, i)
				fields = appendStructFieldTypes(fields, f.Type, fieldIndex, seen)
			} else {
				fields = append(fields, makeStructFieldType(f, index, seen))
			}
		}
	}
	return fields
}

func makeStructFieldType(f reflect.StructField, index []int, seen map[reflect.Type]FieldType) FieldType {
	name, optional := f.Name, false

	s := &structFieldType{
		typ:   f.Type,
		index: index,
	}

	if tag := f.Tag.Get("parquet"); tag != "" {
		name, tag = split(tag)
		for tag != "" {
			opt := ""
			opt, tag = split(tag)
			switch opt {
			case "optional":
				optional = true
			default:
				panic("struct field contains unknown parquet tag: " + opt)
			}
		}
	}

	s.FieldType = makeFieldType(s.typ, name, optional, false, seen)
	return s
}

func makeFieldType(t reflect.Type, name string, optional, repeated bool, seen map[reflect.Type]FieldType) FieldType {
	if f := seen[t]; f != nil {
		return f
	}
	switch t.Kind() {
	case reflect.Struct:
		f := &groupType{
			name:     name,
			optional: optional,
			repeated: repeated,
			fields:   make([]FieldType, 0, t.NumField()),
		}
		seen[t] = f
		f.fields = appendStructFieldTypes(f.fields, t, nil, seen)
		return f
	case reflect.Slice:
		return makeFieldType(t.Elem(), name, false, true, seen)
	case reflect.Ptr:
		return makeFieldType(t.Elem(), name, true, false, seen)
	case reflect.Bool:
		return makePrimitiveFieldType(name, optional, repeated, &primitiveType{
			kind:   Boolean,
			length: 1,
		})
	case reflect.Int32:
		return makePrimitiveFieldType(name, optional, repeated, &primitiveType{
			kind:   Int32,
			length: 32,
		})
	case reflect.Int64:
		return makePrimitiveFieldType(name, optional, repeated, &primitiveType{
			kind:   Int64,
			length: 64,
		})
	case reflect.Float32:
		return makePrimitiveFieldType(name, optional, repeated, &primitiveType{
			kind:   Float,
			length: 32,
		})
	case reflect.Float64:
		return makePrimitiveFieldType(name, optional, repeated, &primitiveType{
			kind:   Double,
			length: 64,
		})
	case reflect.String:
		return makePrimitiveFieldType(name, optional, repeated, &primitiveType{
			kind: ByteArray,
		})
	}
	panic("cannot construct parquet field from go type " + t.Name())
}

func makePrimitiveFieldType(name string, optional, repeated bool, typ Type) FieldType {
	return &fieldType{
		name:     name,
		typ:      typ,
		optional: optional,
		repeated: repeated,
	}
}

type fieldType struct {
	name     string
	typ      Type
	optional bool
	repeated bool
}

func (t *fieldType) Name() string        { return t.name }
func (t *fieldType) NumField() int       { panic("NumField called on parquet field: " + t.name) }
func (t *fieldType) Field(int) FieldType { panic("NumField called on parquet field: " + t.name) }
func (t *fieldType) Optional() bool      { return t.optional }
func (t *fieldType) Repeated() bool      { return t.repeated }
func (t *fieldType) Required() bool      { return !t.optional && !t.repeated }
func (t *fieldType) Group() bool         { return false }
func (t *fieldType) Type() Type          { return t.typ }

type groupType struct {
	name     string
	fields   []FieldType
	optional bool
	repeated bool
}

func (t *groupType) Name() string          { return t.name }
func (t *groupType) NumField() int         { return len(t.fields) }
func (t *groupType) Field(i int) FieldType { return t.fields[i] }
func (t *groupType) Optional() bool        { return t.optional }
func (t *groupType) Repeated() bool        { return t.repeated }
func (t *groupType) Required() bool        { return !t.optional && !t.repeated }
func (t *groupType) Group() bool           { return false }
func (t *groupType) Type() Type            { panic("Type called on parquet group: " + t.name) }

type messageType struct {
	name   string
	fields []FieldType
}

func (t *messageType) Name() string          { return t.name }
func (t *messageType) NumField() int         { return len(t.fields) }
func (t *messageType) Field(i int) FieldType { return t.fields[i] }

type structFieldType struct {
	FieldType
	typ   reflect.Type
	index []int
}

func split(s string) (head, tail string) {
	if i := strings.IndexByte(s, ','); i < 0 {
		head = s
	} else {
		head, tail = s[:i], s[i+1:]
	}
	return
}
