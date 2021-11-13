package parquet

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/segmentio/parquet/deprecated"
	"github.com/segmentio/parquet/format"
)

type Kind int

const (
	Boolean           Kind = Kind(format.Boolean)
	Int32             Kind = Kind(format.Int32)
	Int64             Kind = Kind(format.Int64)
	Int96             Kind = Kind(format.Int96)
	Float             Kind = Kind(format.Float)
	Double            Kind = Kind(format.Double)
	ByteArray         Kind = Kind(format.ByteArray)
	FixedLenByteArray Kind = Kind(format.FixedLenByteArray)
)

func (k Kind) String() string {
	return format.Type(k).String()
}

type Type interface {
	Kind() Kind

	Length() int

	LogicalType() format.LogicalType

	ConvertedType() deprecated.ConvertedType

	NewPageBuffer(bufferSize int) PageBuffer
}

type primitiveType struct{}

func (t primitiveType) LogicalType() format.LogicalType { return format.LogicalType{} }

func (t primitiveType) ConvertedType() deprecated.ConvertedType { return -1 }

type booleanType struct{ primitiveType }

func (t booleanType) Kind() Kind { return Boolean }

func (t booleanType) Length() int { return 1 }

func (t booleanType) NewPageBuffer(bufferSize int) PageBuffer {
	return newBooleanPageBuffer(t, bufferSize)
}

type int32Type struct{ primitiveType }

func (t int32Type) Kind() Kind { return Int32 }

func (t int32Type) Length() int { return 32 }

func (t int32Type) NewPageBuffer(bufferSize int) PageBuffer { return newInt32PageBuffer(t, bufferSize) }

type int64Type struct{ primitiveType }

func (t int64Type) Kind() Kind { return Int64 }

func (t int64Type) Length() int { return 64 }

func (t int64Type) NewPageBuffer(bufferSize int) PageBuffer { return newInt64PageBuffer(t, bufferSize) }

type int96Type struct{ primitiveType }

func (t int96Type) Kind() Kind { return Int96 }

func (t int96Type) Length() int { return 96 }

func (t int96Type) NewPageBuffer(bufferSize int) PageBuffer { return newInt96PageBuffer(t, bufferSize) }

type floatType struct{ primitiveType }

func (t floatType) Kind() Kind { return Float }

func (t floatType) Length() int { return 32 }

func (t floatType) NewPageBuffer(bufferSize int) PageBuffer { return newFloatPageBuffer(t, bufferSize) }

type doubleType struct{ primitiveType }

func (t doubleType) Kind() Kind { return Double }

func (t doubleType) Length() int { return 64 }

func (t doubleType) NewPageBuffer(bufferSize int) PageBuffer {
	return newDoublePageBuffer(t, bufferSize)
}

type byteArrayType struct{ primitiveType }

func (t byteArrayType) Kind() Kind { return ByteArray }

func (t byteArrayType) Length() int { panic("cannot call Length on parquet binary type") }

func (t byteArrayType) NewPageBuffer(bufferSize int) PageBuffer {
	return newByteArrayPageBuffer(t, bufferSize)
}

type fixedLenByteArrayType struct {
	primitiveType
	length int
}

func (t *fixedLenByteArrayType) Kind() Kind { return FixedLenByteArray }

func (t *fixedLenByteArrayType) Length() int { return t.length }

func (t *fixedLenByteArrayType) NewPageBuffer(bufferSize int) PageBuffer {
	return newFixedLenByteArrayPageBuffer(t, bufferSize)
}

var (
	BooleanType   Type = booleanType{}
	Int32Type     Type = int32Type{}
	Int64Type     Type = int64Type{}
	Int96Type     Type = int96Type{}
	FloatType     Type = floatType{}
	DoubleType    Type = doubleType{}
	ByteArrayType Type = byteArrayType{}
)

func FixedLenByteArrayType(length int) Type {
	return &fixedLenByteArrayType{length: length}
}

type Node interface {
	Type() Type

	Optional() bool

	Repeated() bool

	Required() bool

	NumChildren() int

	Children() []string

	ChildByName(name string) Node
}

type Group map[string]Node

func (g Group) Type() Type { return groupType{} }

func (g Group) Optional() bool { return false }

func (g Group) Repeated() bool { return false }

func (g Group) Required() bool { return true }

func (g Group) NumChildren() int { return len(g) }

func (g Group) Children() []string {
	names := make([]string, 0, len(g))
	for name := range g {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (g Group) ChildByName(name string) Node {
	n, ok := g[name]
	if ok {
		return n
	}
	panic("column not found in parquet group: " + name)
}

type groupType struct{}

func (groupType) Kind() Kind                              { panic("cannot call Kind on parquet group type") }
func (groupType) Length() int                             { panic("cannot call Length on parquet group type") }
func (groupType) LogicalType() format.LogicalType         { return format.LogicalType{} }
func (groupType) ConvertedType() deprecated.ConvertedType { return -1 }
func (groupType) NewPageBuffer(int) PageBuffer            { panic("cannot create page buffer for parquet group") }

func Optional(node Node) Node {
	if node.Optional() {
		return node
	}
	return &optional{node}
}

type optional struct{ Node }

func (opt *optional) Optional() bool { return true }
func (opt *optional) Repeated() bool { return false }
func (opt *optional) Required() bool { return false }

func Repeated(node Node) Node {
	if node.Repeated() {
		return node
	}
	return &repeated{node}
}

type repeated struct{ Node }

func (opt *repeated) Optional() bool { return false }
func (opt *repeated) Repeated() bool { return true }
func (opt *repeated) Required() bool { return false }

func Required(node Node) Node {
	if node.Required() {
		return node
	}
	return &required{node}
}

type required struct{ Node }

func (opt *required) Optional() bool { return false }
func (opt *required) Repeated() bool { return false }
func (opt *required) Required() bool { return true }

func Int(bitWidth int) Node {
	return &leafNode{typ: integerType(bitWidth, &signedIntTypes)}
}

func Uint(bitWidth int) Node {
	return &leafNode{typ: integerType(bitWidth, &unsignedIntTypes)}
}

func integerType(bitWidth int, types *[4]intType) *intType {
	switch bitWidth {
	case 8:
		return &types[0]
	case 16:
		return &types[1]
	case 32:
		return &types[2]
	case 64:
		return &types[3]
	default:
		panic(fmt.Sprintf("cannot create a %d bits parquet integer node", bitWidth))
	}
}

var signedIntTypes = [...]intType{
	{BitWidth: 8, IsSigned: true},
	{BitWidth: 16, IsSigned: true},
	{BitWidth: 32, IsSigned: true},
	{BitWidth: 64, IsSigned: true},
}

var unsignedIntTypes = [...]intType{
	{BitWidth: 8, IsSigned: false},
	{BitWidth: 16, IsSigned: false},
	{BitWidth: 32, IsSigned: false},
	{BitWidth: 64, IsSigned: false},
}

type intType format.IntType

func (t *intType) Kind() Kind {
	if t.BitWidth == 64 {
		return Int64
	} else {
		return Int32
	}
}

func (t *intType) Length() int { return int(t.BitWidth) }

func (t *intType) LogicalType() format.LogicalType {
	return format.LogicalType{Integer: (*format.IntType)(t)}
}

func (t *intType) ConvertedType() deprecated.ConvertedType {
	convertedType := int(deprecated.Uint8) + (int(t.BitWidth) / 8)
	if t.IsSigned {
		convertedType += int(deprecated.Int8)
	}
	return deprecated.ConvertedType(convertedType)
}

func (t *intType) NewPageBuffer(bufferSize int) PageBuffer {
	if t.IsSigned {
		if t.BitWidth == 64 {
			return newInt64PageBuffer(t, bufferSize)
		} else {
			return newInt32PageBuffer(t, bufferSize)
		}
	} else {
		if t.BitWidth == 64 {
			return uint64PageBuffer{newInt64PageBuffer(t, bufferSize)}
		} else {
			return uint32PageBuffer{newInt32PageBuffer(t, bufferSize)}
		}
	}
}

func Decimal(scale, precision int, typ Type) Node {
	return &leafNode{
		typ: &decimalType{
			decimal: format.DecimalType{
				Scale:     int32(scale),
				Precision: int32(precision),
			},
			typ: typ,
		},
	}
}

type decimalType struct {
	decimal format.DecimalType
	typ     Type
}

func (t *decimalType) Kind() Kind { return t.typ.Kind() }

func (t *decimalType) Length() int { return t.typ.Length() }

func (t *decimalType) LogicalType() format.LogicalType {
	return format.LogicalType{Decimal: &t.decimal}
}

func (t *decimalType) ConvertedType() deprecated.ConvertedType { return deprecated.Decimal }

func (t *decimalType) NewPageBuffer(bufferSize int) PageBuffer { panic("NOT IMPLEMENTED") }

func UTF8() Node { return &leafNode{typ: &stringType{}} }

type stringType format.StringType

func (t *stringType) Kind() Kind { return ByteArray }

func (t *stringType) Length() int { panic("cannot call Length on parquet string type") }

func (t *stringType) LogicalType() format.LogicalType {
	return format.LogicalType{UTF8: (*format.StringType)(t)}
}

func (t *stringType) ConvertedType() deprecated.ConvertedType { return deprecated.UTF8 }

func (t *stringType) NewPageBuffer(bufferSize int) PageBuffer {
	return stringPageBuffer{newByteArrayPageBuffer(t, bufferSize)}
}

func UUID() Node { return &leafNode{typ: &uuidType{}} }

type uuidType format.UUIDType

func (t *uuidType) Kind() Kind { return FixedLenByteArray }

func (t *uuidType) Length() int { return 16 }

func (t *uuidType) LogicalType() format.LogicalType {
	return format.LogicalType{UUID: (*format.UUIDType)(t)}
}

func (t *uuidType) ConvertedType() deprecated.ConvertedType { return -1 }

func (t *uuidType) NewPageBuffer(bufferSize int) PageBuffer {
	return uuidPageBuffer{newFixedLenByteArrayPageBuffer(t, bufferSize)}
}

func Enum() Node { return &leafNode{typ: &enumType{}} }

type enumType format.EnumType

func (t *enumType) Kind() Kind { return ByteArray }

func (t *enumType) Length() int { panic("cannot call Length on parquet enum type") }

func (t *enumType) LogicalType() format.LogicalType {
	return format.LogicalType{Enum: (*format.EnumType)(t)}
}

func (t *enumType) ConvertedType() deprecated.ConvertedType { return deprecated.Enum }

func (t *enumType) NewPageBuffer(bufferSize int) PageBuffer {
	return stringPageBuffer{newByteArrayPageBuffer(t, bufferSize)}
}

func JSON() Node { return &leafNode{typ: &jsonType{}} }

type jsonType format.JsonType

func (t *jsonType) Kind() Kind { return ByteArray }

func (t *jsonType) Length() int { panic("cannot call Length on parquet json type") }

func (t *jsonType) LogicalType() format.LogicalType {
	return format.LogicalType{Json: (*format.JsonType)(t)}
}

func (t *jsonType) ConvertedType() deprecated.ConvertedType { return deprecated.Json }

func (t *jsonType) NewPageBuffer(bufferSize int) PageBuffer {
	return newByteArrayPageBuffer(t, bufferSize)
}

func BSON() Node { return &leafNode{typ: &bsonType{}} }

type bsonType format.BsonType

func (t *bsonType) Kind() Kind { return ByteArray }

func (t *bsonType) Length() int { panic("cannot call Length on parquet bson type") }

func (t *bsonType) LogicalType() format.LogicalType {
	return format.LogicalType{Bson: (*format.BsonType)(t)}
}

func (t *bsonType) ConvertedType() deprecated.ConvertedType { return deprecated.Bson }

func (t *bsonType) NewPageBuffer(bufferSize int) PageBuffer {
	return newByteArrayPageBuffer(t, bufferSize)
}

func Date() Node { return &leafNode{typ: &dateType{}} }

type dateType format.DateType

func (t *dateType) Kind() Kind { return Int32 }

func (t *dateType) Length() int { return 32 }

func (t *dateType) LogicalType() format.LogicalType {
	return format.LogicalType{Date: (*format.DateType)(t)}
}

func (t *dateType) ConvertedType() deprecated.ConvertedType { return deprecated.Date }

func (t *dateType) NewPageBuffer(bufferSize int) PageBuffer { return newInt32PageBuffer(t, bufferSize) }

type TimeUnit interface {
	Duration() time.Duration

	TimeUnit() format.TimeUnit
}

var (
	Millisecond TimeUnit = &millisecond{}
	Microsecond TimeUnit = &microsecond{}
	Nanosecond  TimeUnit = &nanosecond{}
)

type millisecond format.MilliSeconds

func (u *millisecond) Duration() time.Duration {
	return time.Millisecond
}

func (u *millisecond) TimeUnit() format.TimeUnit {
	return format.TimeUnit{Millis: (*format.MilliSeconds)(u)}
}

type microsecond format.MicroSeconds

func (u *microsecond) Duration() time.Duration {
	return time.Microsecond
}

func (u *microsecond) TimeUnit() format.TimeUnit {
	return format.TimeUnit{Micros: (*format.MicroSeconds)(u)}
}

type nanosecond format.NanoSeconds

func (u *nanosecond) Duration() time.Duration {
	return time.Nanosecond
}

func (u *nanosecond) TimeUnit() format.TimeUnit {
	return format.TimeUnit{Nanos: (*format.NanoSeconds)(u)}
}

func Time(unit TimeUnit) Node {
	return &leafNode{typ: &timeType{IsAdjustedToUTC: true, Unit: unit.TimeUnit()}}
}

type timeType format.TimeType

func (t *timeType) Kind() Kind {
	if t.Unit.Millis != nil {
		return Int32
	} else {
		return Int64
	}
}

func (t *timeType) Length() int {
	if t.Unit.Millis != nil {
		return 32
	} else {
		return 64
	}
}

func (t *timeType) LogicalType() format.LogicalType {
	return format.LogicalType{Time: (*format.TimeType)(t)}
}

func (t *timeType) ConvertedType() deprecated.ConvertedType {
	switch {
	case t.Unit.Millis != nil:
		return deprecated.TimeMillis
	case t.Unit.Micros != nil:
		return deprecated.TimeMicros
	default:
		return -1
	}
}

func (t *timeType) NewPageBuffer(bufferSize int) PageBuffer {
	if t.Unit.Millis != nil {
		return newInt32PageBuffer(t, bufferSize)
	} else {
		return newInt64PageBuffer(t, bufferSize)
	}
}

func Timestamp(unit TimeUnit) Node {
	return &leafNode{typ: &timestampType{IsAdjustedToUTC: true, Unit: unit.TimeUnit()}}
}

type timestampType format.TimestampType

func (t *timestampType) Kind() Kind { return Int64 }

func (t *timestampType) Length() int { return 64 }

func (t *timestampType) LogicalType() format.LogicalType {
	return format.LogicalType{Timestamp: (*format.TimestampType)(t)}
}

func (t *timestampType) ConvertedType() deprecated.ConvertedType {
	switch {
	case t.Unit.Millis != nil:
		return deprecated.TimestampMillis
	case t.Unit.Micros != nil:
		return deprecated.TimestampMicros
	default:
		return -1
	}
}

func (t *timestampType) NewPageBuffer(bufferSize int) PageBuffer {
	return newInt64PageBuffer(t, bufferSize)
}

func List(of Node) Node {
	return listNode{Group{"list": Repeated(Group{"element": of})}}
}

type listNode struct{ Group }

func (listNode) Type() Type { return &listType{} }

type listType format.ListType

func (t *listType) Kind() Kind { panic("cannot call Kind on parquet list type") }

func (t *listType) Length() int { panic("cannot call Length on parquet list type") }

func (t *listType) LogicalType() format.LogicalType {
	return format.LogicalType{List: (*format.ListType)(t)}
}

func (t *listType) ConvertedType() deprecated.ConvertedType { return deprecated.List }

func (t *listType) NewPageBuffer(bufferSize int) PageBuffer {
	panic("cannot create page buffer for parquet list type")
}

func Map(key, value Node) Node {
	return mapNode{Group{
		"key_value": Repeated(Group{
			"key":   Required(key),
			"value": value,
		}),
	}}
}

type mapNode struct{ Group }

func (mapNode) Type() Type { return &mapType{} }

type mapType format.MapType

func (t *mapType) Kind() Kind { panic("cannot call Kind on parquet map type") }

func (t *mapType) Length() int { panic("cannot call Length on parquet map type") }

func (t *mapType) LogicalType() format.LogicalType {
	return format.LogicalType{Map: (*format.MapType)(t)}
}

func (t *mapType) ConvertedType() deprecated.ConvertedType { return deprecated.Map }

func (t *mapType) NewPageBuffer(bufferSize int) PageBuffer {
	panic("cannot create page buffer for parquet map type")
}

type nullType format.NullType

func (t *nullType) Kind() Kind { panic("cannot call Kind on null parquet type") }

func (t *nullType) Length() int { panic("cannot call Length on null parquet type") }

func (t *nullType) LogicalType() format.LogicalType {
	return format.LogicalType{Unknown: (*format.NullType)(t)}
}

func (t *nullType) ConvertedType() deprecated.ConvertedType { return -1 }

func (t *nullType) NewPageBuffer(bufferSize int) PageBuffer {
	panic("cannot create page buffer for parquet null type")
}

type leafNode struct{ typ Type }

func (n *leafNode) Type() Type         { return n.typ }
func (n *leafNode) Optional() bool     { return false }
func (n *leafNode) Repeated() bool     { return false }
func (n *leafNode) Required() bool     { return true }
func (n *leafNode) NumChildren() int   { return 0 }
func (n *leafNode) Children() []string { return nil }
func (n *leafNode) ChildByName(string) Node {
	panic("cannot lookup child by name in leaf parquet node")
}

var (
	_ Node = Group(nil)
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

func (s *Schema) Children() []string { return s.node.Children() }

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

func (s *structNode) Type() Type         { return groupType{} }
func (s *structNode) Optional() bool     { return false }
func (s *structNode) Repeated() bool     { return false }
func (s *structNode) Required() bool     { return true }
func (s *structNode) NumChildren() int   { return len(s.fields) }
func (s *structNode) Children() []string { return s.names }
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
func (f *structField) Children() []string           { return f.node.Children() }
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

/*
type Value struct {
	ptr   unsafe.Pointer
	u64   uint64
	u32   uint32
	class classIndex
}

func (v Value) Bool() bool { return classes[v.class].bool(v) }

func (v Value) Int32() int32 { return classes[v.class].int32(v) }

func (v Value) Int64() int64 { return classes[v.class].int64(v) }

func (v Value) Int96() [12]byte { return classes[v.class].int96(v) }

func (v Value) Bytes() []byte { return classes[v.class].bytes(v) }

func ValueOf(v interface{}) Value {
	return valueOf(reflect.ValueOf(v))
}

func valueOf(v reflect.Value) Value {
	switch v.Kind() {
	case reflect.Bool:
		return Value{
			u32:   boolToUint32(v.Bool()),
			class: boolClassIndex,
		}

	case reflect.Int8, reflect.Int16, reflect.Int32:
		return Value{
			u32:   uint32(v.Int()),
			class: int32ClassIndex,
		}

	case reflect.Int, reflect.Int64:
		return Value{
			u64:   uint64(v.Int()),
			class: int64ClassIndex,
		}

	case reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return Value{
			u32:   uint32(v.Uint()),
			class: int32ClassIndex,
		}

	case reflect.Uint, reflect.Uintptr, reflect.Uint64:
		return Value{
			u64:   v.Uint(),
			class: int64ClassIndex,
		}

	case reflect.Float32:

	default:
		panic("cannot create parquet value from go value of type " + v.Type().String())
	}
}

type class interface {
	bool(Value) bool
	int32(Value) int32
	int64(Value) int64
	int96(Value) [12]byte
	float32(Value) float32
	float64(Value) float64
	bytes(Value) []byte
}

type classIndex uint32

const (
	boolClassIndex classIndex = iota
	int32ClassIndex
	int64ClassIndex
)

var classes = [...]class{
	boolClassIndex:  boolClass{},
	int32ClassIndex: int32Class{},
	int64ClassIndex: int64Class{},
}

type boolClass struct{}

func (boolClass) bool(v Value) bool       { return v.u32 != 0 }
func (boolClass) int32(v Value) int32     { panic("cannot convert parquet boolean value to int32") }
func (boolClass) int64(v Value) int64     { panic("cannot convert parquet boolean value to int64") }
func (boolClass) int96(v Value) [12]byte  { panic("cannot convert parquet boolean value to int96") }
func (boolClass) float32(v Value) float32 { panic("cannot convert parquet boolean value to float") }
func (boolClass) float64(v Value) float64 { panic("cannot convert parquet boolean value to double") }
func (boolClass) bytes(v Value) []byte    { panic("cannot convert parquet boolean value ta byte array") }

type int32Class struct{}

func (int32Class) bool(v Value) bool       { panic("cannot convert parquet int32 value to boolean") }
func (int32Class) int32(v Value) int32     { return int32(v.u32) }
func (int32Class) int64(v Value) int64     { return int64(int32(v.u32)) }
func (int32Class) int96(v Value) [12]byte  { panic("cannot convert parquet int32 value to int96") }
func (int32Class) float32(v Value) float32 { panic("cannot convert parquet int32 value to float") }
func (int32Class) float64(v Value) float64 { panic("cannot convert parquet int32 value to double") }
func (int32Class) bytes(v Value) []byte    { panic("cannot convert parquet int32 value ta byte array") }

type int64Class struct{}

func (int64Class) bool(v Value) bool       { panic("cannot convert parquet int64 value to boolean") }
func (int64Class) int32(v Value) int32     { panic("cannot convert parquet int64 value to int32") }
func (int64Class) int64(v Value) int64     { return int64(v.u64) }
func (int64Class) int96(v Value) [12]byte  { panic("cannot convert parquet int64 value ot int96") }
func (int64Class) float32(v Value) float32 { panic("cannot convert parquet int64 value to float") }
func (int64Class) float64(v Value) float64 { panic("cannot convert parquet int64 value to double") }
func (int64Class) bytes(v Value) []byte    { panic("cannot convert parquet int64 value ta byte array") }

func boolToUint32(b bool) uint32 {
	if b {
		return 1
	}
	return 0
}
*/
