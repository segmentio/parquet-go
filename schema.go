package parquet

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

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
	Annotation() string
}

type schemaElementType struct {
	*schema.SchemaElement
}

func (t schemaElementType) Kind() Kind  { return Kind(t.Type) }
func (t schemaElementType) Length() int { return int(t.TypeLength) }
func (t schemaElementType) Annotation() string {
	switch x := t.LogicalType.Union.(type) {
	case schema.StringType:
		return "UTF8"
	case schema.MapType:
		return "MAP"
	case schema.ListType:
		return "LIST"
	case schema.EnumType:
		return "ENUM"
	case schema.DecimalType:
		//
	case schema.DateType:
		//
	case schema.TimeType:
		//
	case schema.TimestampType:
		//
	case schema.IntType:
		return fmt.Sprintf("INT(%d, %t)", x.BitWidth, x.IsSigned)
	case schema.NullType:
		return "NULL"
	case schema.JsonType:
		return "JSON"
	case schema.BsonType:
		return "BSON"
	case schema.UUIDType:
		return "UUID"
	}
	return ""
}

type primitiveType struct {
	kind   Kind
	length int
}

func (t *primitiveType) Kind() Kind         { return t.kind }
func (t *primitiveType) Length() int        { return t.length }
func (t *primitiveType) Annotation() string { return "" }

type stringType struct{}

func (t *stringType) Kind() Kind         { return ByteArray }
func (t *stringType) Length() int        { panic("cannot call Length on parquet type STRING") }
func (t *stringType) Annotation() string { return "UTF8" }

type enumType struct{}

func (t *enumType) Kind() Kind         { return ByteArray }
func (t *enumType) Length() int        { panic("cannot call Length on parquet type ENUM") }
func (t *enumType) Annotation() string { return "ENUM" }

type uuidType struct{}

func (t *uuidType) Kind() Kind         { return FixedLenByteArray }
func (t *uuidType) Length() int        { return 16 }
func (t *uuidType) Annotation() string { return "UUID" }

type jsonType struct{}

func (t *jsonType) Kind() Kind         { return ByteArray }
func (t *jsonType) Length() int        { panic("cannot call Length on parquet type JSON") }
func (t *jsonType) Annotation() string { return "JSON" }

type bsonType struct{}

func (t *bsonType) Kind() Kind         { return ByteArray }
func (t *bsonType) Length() int        { panic("cannot call Length on parquet type BSON") }
func (t *bsonType) Annotation() string { return "BSON" }

type intType struct {
	kind     Kind
	bitWidth int
	isSigned bool
}

func (t *intType) Kind() Kind         { return t.kind }
func (t *intType) Length() int        { return t.bitWidth }
func (t *intType) Annotation() string { return fmt.Sprintf("INT(%d, %t)", t.bitWidth, t.isSigned) }

type nullType struct{}

func (t *nullType) Kind() Kind         { panic("cannot call Kind on logical parquet type NULL") }
func (t *nullType) Length() int        { panic("cannot call Length on logical parquet type NULL") }
func (t *nullType) Annotation() string { return "" }

type listType struct{}

func (t *listType) Kind() Kind         { panic("cannot call Kind on logical parquet type LIST") }
func (t *listType) Length() int        { panic("cannot call Length on logical parquet type LIST") }
func (t *listType) Annotation() string { return "LIST" }

type mapType struct{}

func (t *mapType) Kind() Kind         { panic("cannot call Kind on logical parquet type MAP") }
func (t *mapType) Length() int        { panic("cannot call Length on logical parquet type MAP") }
func (t *mapType) Annotation() string { return "MAP" }

type timestampType struct {
	isAdjustedToUTC bool
	unit            timestampUnit
}

func (t *timestampType) Kind() Kind  { return Int64 }
func (t *timestampType) Length() int { return 64 }
func (t *timestampType) Annotation() string {
	return fmt.Sprintf("TIMESTAMP(isAdjustedToUTC=%t, unit=%s)", t.isAdjustedToUTC, t.unit)
}

type timestampUnit int

const (
	millisecond timestampUnit = iota
	microsecond
	nanosecond
)

func (u timestampUnit) String() string {
	switch u {
	case millisecond:
		return "MILLIS"
	case microsecond:
		return "MICROS"
	case nanosecond:
		return "NANOS"
	default:
		return "UNKNOWN"
	}
}

type Schema interface {
	Name() string

	NumField() int

	Field(index int) SchemaElement
}

type SchemaElement interface {
	Schema

	Path() []string

	Optional() bool

	Repeated() bool

	Required() bool

	Group() bool

	Type() Type
}

func Format(m Schema) string {
	s := &strings.Builder{}
	formatSchema(s, m)
	return s.String()
}

func formatSchema(s io.StringWriter, m Schema) {
	s.WriteString("message ")
	s.WriteString(m.Name())
	s.WriteString(" {")

	n := m.NumField()
	if n > 0 {
		s.WriteString("\n")
	}

	for i := 0; i < n; i++ {
		formatSchemaElement(s, m.Field(i), 2)
		s.WriteString("\n")
	}

	s.WriteString("}")
}

func formatSchemaElement(s io.StringWriter, e SchemaElement, indent int) {
	writeIndent(s, indent)

	switch {
	case e.Optional():
		s.WriteString("optional ")
	case e.Repeated():
		s.WriteString("repeated ")
	default:
		s.WriteString("required ")
	}

	if e.Group() {
		s.WriteString("group ")
	} else {
		switch e.Type().Kind() {
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

	s.WriteString(e.Name())

	if a := e.Type().Annotation(); a != "" {
		s.WriteString(" (")
		s.WriteString(a)
		s.WriteString(")")
	}

	if e.Group() {
		s.WriteString(" {")

		n := e.NumField()
		if n > 0 {
			s.WriteString("\n")
		}

		indent += 2
		for i := 0; i < n; i++ {
			formatSchemaElement(s, e.Field(i), indent)
			s.WriteString("\n")
		}

		writeIndent(s, indent-2)
		s.WriteString("}")
	} else {
		s.WriteString(";")
	}
}

func writeIndent(s io.StringWriter, indent int) {
	spaces := "                "
	if indent > len(spaces) {
		spaces = strings.Repeat(" ", indent)
	}
	s.WriteString(spaces[:indent])
}

func SchemaOf(t reflect.Type) Schema {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		panic("cannot create a parquet message from a go type " + t.String())
	}
	fields := make([]SchemaElement, t.NumField())
	return &messageSchema{
		name:   t.Name(),
		fields: appendStructSchemaElements(fields[:0], t, nil, nil, make(map[reflect.Type]struct{})),
	}
}

func appendStructSchemaElements(fields []SchemaElement, t reflect.Type, path []string, index []int, seen map[reflect.Type]struct{}) []SchemaElement {
	for i, n := 0, t.NumField(); i < n; i++ {
		if f := t.Field(i); f.Anonymous {
			fieldIndex := index[:len(index):len(index)]
			fieldIndex = append(fieldIndex, i)
			fields = appendStructSchemaElements(fields, f.Type, path, fieldIndex, seen)
		} else if f.IsExported() {
			fields = append(fields, makeStructSchemaElement(f, path, index, seen))
		}
	}
	return fields
}

func makeStructSchemaElement(f reflect.StructField, path []string, index []int, seen map[reflect.Type]struct{}) SchemaElement {
	name := f.Name
	elem := schemaElement{
		typ:   f.Type,
		index: index,
	}

	setAnnotation := func(a string) {
		// TODO: validate that the type annotation is compatible with the
		// underlying Go type.
		if elem.annotation != "" {
			panic("struct field has too many type annotations: " + f.Type.String())
		}
		elem.annotation = a
	}

	if tag := f.Tag.Get("parquet"); tag != "" {
		name, tag = split(tag)

		for tag != "" {
			opt := ""
			opt, tag = split(tag)

			switch opt {
			case "optional":
				elem.optional = true
			case "list":
				setAnnotation(opt)
			case "enum":
				setAnnotation(opt)
			case "uuid":
				setAnnotation(opt)
			default:
				panic("struct field contains unknown parquet tag: " + opt)
			}
		}
	}

	elem.path = appendPath(path, name)
	return makeSchemaElement(elem, seen)
}

var (
	jsonRawMessage = reflect.TypeOf((*json.RawMessage)(nil)).Elem()
	timeTimeType   = reflect.TypeOf((*time.Time)(nil)).Elem()
)

type schemaElement struct {
	typ        reflect.Type
	path       []string
	index      []int
	optional   bool
	repeated   bool
	annotation string
}

func makeSchemaElement(elem schemaElement, seen map[reflect.Type]struct{}) SchemaElement {
	switch elem.typ {
	case jsonRawMessage:
		return makeFieldElement(elem, &jsonType{})
	case timeTimeType:
		return makeFieldElement(elem, &timestampType{
			isAdjustedToUTC: true,
			unit:            nanosecond,
		})
	}

	switch elem.typ.Kind() {
	case reflect.Struct:
		if _, alreadySeen := seen[elem.typ]; alreadySeen {
			panic("recursive data types cannot be represented in parquet: " + elem.typ.String())
		}
		seen[elem.typ] = struct{}{}
		f := &groupElement{
			path:     elem.path,
			optional: elem.optional,
			repeated: elem.repeated,
			typ:      &nullType{},
			fields:   make([]SchemaElement, 0, elem.typ.NumField()),
		}
		f.fields = appendStructSchemaElements(f.fields, elem.typ, elem.path, elem.index, seen)
		return f

	case reflect.Slice:
		switch elem.annotation {
		case "list":
			subElem := elem
			subElem.typ = elem.typ.Elem()
			subElem.path = appendPath(subElem.path, "list", "element")
			subElem.optional = false
			subElem.repeated = false
			return &groupElement{
				path:     elem.path,
				optional: elem.optional,
				repeated: elem.repeated,
				typ:      &listType{},
				fields: []SchemaElement{
					&groupElement{
						path:     appendPath(elem.path, "list"),
						repeated: true,
						typ:      &nullType{},
						fields:   []SchemaElement{makeSchemaElement(subElem, seen)},
					},
				},
			}
		default:
			elem.typ = elem.typ.Elem()
			elem.repeated = true
			return makeSchemaElement(elem, seen)
		}

	case reflect.Ptr:
		elem.typ = elem.typ.Elem()
		elem.optional = true
		return makeSchemaElement(elem, seen)

	case reflect.Bool:
		return makeFieldElement(elem, &primitiveType{
			kind:   Boolean,
			length: 1,
		})

	case reflect.Int32, reflect.Int16, reflect.Int8:
		return makeFieldElement(elem, &intType{
			kind:     Int32,
			bitWidth: elem.typ.Bits(),
			isSigned: true,
		})

	case reflect.Int64:
		return makeFieldElement(elem, &intType{
			kind:     Int64,
			bitWidth: elem.typ.Bits(),
			isSigned: true,
		})

	case reflect.Int:
		return makeFieldElement(elem, &intType{
			kind:     Int32,
			bitWidth: 32,
			isSigned: true,
		})

	case reflect.Uint32, reflect.Uint16, reflect.Uint8:
		return makeFieldElement(elem, &intType{
			kind:     Int32,
			bitWidth: elem.typ.Bits(),
			isSigned: false,
		})

	case reflect.Uint64, reflect.Uintptr:
		return makeFieldElement(elem, &intType{
			kind:     Int64,
			bitWidth: elem.typ.Bits(),
			isSigned: false,
		})

	case reflect.Uint:
		return makeFieldElement(elem, &intType{
			kind:     Int32,
			bitWidth: 32,
			isSigned: false,
		})

	case reflect.Float32:
		return makeFieldElement(elem, &primitiveType{
			kind:   Float,
			length: 32,
		})

	case reflect.Float64:
		return makeFieldElement(elem, &primitiveType{
			kind:   Double,
			length: 64,
		})

	case reflect.String:
		var typ Type
		switch elem.annotation {
		case "enum":
			typ = &enumType{}
		case "uuid":
			typ = &uuidType{}
		default:
			typ = &stringType{}
		}
		return makeFieldElement(elem, typ)

	case reflect.Array:
		if elem.typ.Elem().Kind() == reflect.Uint8 {
			var typ Type
			switch elem.annotation {
			case "uuid":
				typ = &uuidType{}
			default:
				typ = &primitiveType{
					kind:   FixedLenByteArray,
					length: elem.typ.Len(),
				}
			}
			return makeFieldElement(elem, typ)
		}
	}

	panic("cannot construct parquet field from go type " + elem.typ.Name())
}

func makeFieldElement(elem schemaElement, typ Type) SchemaElement {
	return &fieldElement{
		path:     elem.path,
		index:    elem.index,
		typ:      typ,
		optional: elem.optional,
		repeated: elem.repeated,
	}
}

type fieldElement struct {
	path     []string
	index    []int
	optional bool
	repeated bool
	typ      Type
}

func (t *fieldElement) Name() string            { return t.path[len(t.path)-1] }
func (t *fieldElement) Path() []string          { return t.path }
func (t *fieldElement) NumField() int           { return 0 }
func (t *fieldElement) Field(int) SchemaElement { panic("NumField called on non-group: " + path(t)) }
func (t *fieldElement) Optional() bool          { return t.optional }
func (t *fieldElement) Repeated() bool          { return t.repeated }
func (t *fieldElement) Required() bool          { return !t.optional && !t.repeated }
func (t *fieldElement) Group() bool             { return false }
func (t *fieldElement) Type() Type              { return t.typ }

type groupElement struct {
	path     []string
	fields   []SchemaElement
	optional bool
	repeated bool
	typ      Type
}

func (t *groupElement) Name() string              { return t.path[len(t.path)-1] }
func (t *groupElement) Path() []string            { return t.path }
func (t *groupElement) NumField() int             { return len(t.fields) }
func (t *groupElement) Field(i int) SchemaElement { return t.fields[i] }
func (t *groupElement) Optional() bool            { return t.optional }
func (t *groupElement) Repeated() bool            { return t.repeated }
func (t *groupElement) Required() bool            { return !t.optional && !t.repeated }
func (t *groupElement) Group() bool               { return true }
func (t *groupElement) Type() Type                { return t.typ }

type messageSchema struct {
	name   string
	fields []SchemaElement
}

func (t *messageSchema) Name() string              { return t.name }
func (t *messageSchema) NumField() int             { return len(t.fields) }
func (t *messageSchema) Field(i int) SchemaElement { return t.fields[i] }

func split(s string) (head, tail string) {
	if i := strings.IndexByte(s, ','); i < 0 {
		head = s
	} else {
		head, tail = s[:i], s[i+1:]
	}
	return
}

func path(element SchemaElement) string {
	return strings.Join(element.Path(), ".")
}

func appendPath(path []string, elements ...string) []string {
	return append(path[:len(path):len(path)], elements...)
}
