package parquet

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/segmentio/parquet/compress"
	"github.com/segmentio/parquet/deprecated"
	"github.com/segmentio/parquet/encoding"
)

// Schema represents a parquet schema created from a Go value.
//
// Schema implements the Node interface to represent the root node of a parquet
// schema.
type Schema struct {
	name        string
	root        Node
	deconstruct deconstructFunc
}

// SchemaOf constructs a parquet schema from a Go value.
//
// The function can construct parquet schemas from struct or pointer-to-struct
// values only. A panic is raised if a Go value of a different type is passed
// to this function.
//
// When creating a parquet Schema from a Go value, the struct fields may contain
// a "parquet" tag to describe properties of the parquet node. The "parquet" tag
// follows the conventional format of Go struct tags: a comma-separated list of
// values describe the options, with the first one defining the name of the
// parquet column.
//
// The following options are also supported in the "parquet" struct tag:
//
//	optional | make the parquet column optional
//	snappy   | sets the parquet column compression codec to snappy
//	gzip     | sets the parquet column compression codec to gzip
//	brotli   | sets the parquet column compression codec to brotli
//	lz4      | sets the parquet column compression codec to lz4
//	zstd     | sets the parquet column compression codec to zstd
//	plain    | enables the plain encoding (no-op default)
//	dict     | enables dictionary encoding on the parquet column
//	delta    | enables delta encoding on the parquet column
//	list     | for slice types, use the parquet LIST logical type
//	enum     | for string types, use the parquet ENUM logical type
//	uuid     | for string and [16]byte types, use the parquet UUID logical type
//	decimal  | for int32 and int64 types, use the parquet DECIMAL logical type
//
// The decimal tag must be followed by two ineger parameters, the first integer
// representing the scale and the second the precision; for example:
//
//	type Item struct {
//		Cost int64 `parquet:"cost,decimal(0,3)"`
//	}
//
// Invalid combination of struct tags and Go types, or repeating options will
// cause the function to panic.
//
// The schema name is the Go type name of the value.
func SchemaOf(model interface{}) *Schema {
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return NamedSchemaOf(t.Name(), model)
}

// NamedSchemaOf is like SchemaOf but allows the program to customize the name
// of the parquet schema.
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
	}
	panic("cannot construct parquet schema from value of type " + model.Type().String())
}

// NewSchema constructs a new Schema object with the given name and root node.
func NewSchema(name string, root Node) *Schema {
	return &Schema{
		name:        name,
		root:        root,
		deconstruct: makeDeconstructFunc(root),
	}
}

func makeDeconstructFunc(node Node) (deconstruct deconstructFunc) {
	if isLeaf(node) { // message{}
		deconstruct = func(row Row, _ levels, _ reflect.Value) (Row, error) { return row, nil }
	} else {
		_, deconstruct = deconstructFuncOf(0, node)
	}
	return deconstruct
}

// Name returns the name of s.
func (s *Schema) Name() string { return s.name }

// Type returns the parquet type of s.
func (s *Schema) Type() Type { return s.root.Type() }

// Optional returns false since the root node of a parquet schema is always required.
func (s *Schema) Optional() bool { return s.root.Optional() }

// Repeated returns false since the root node of a parquet schema is always required.
func (s *Schema) Repeated() bool { return s.root.Repeated() }

// Required returns true since the root node of a parquet schema is always required.
func (s *Schema) Required() bool { return s.root.Required() }

// NumChildren returns the number of child nodes of s.
func (s *Schema) NumChildren() int { return s.root.NumChildren() }

// ChildNames returns the list of child node names of s.
func (s *Schema) ChildNames() []string { return s.root.ChildNames() }

// ChildByName returns the child node with the given name in s.
func (s *Schema) ChildByName(name string) Node { return s.root.ChildByName(name) }

// Encoding returns the list of encodings in child nodes of s.
func (s *Schema) Encoding() []encoding.Encoding { return s.root.Encoding() }

// Compression returns the list of compression codecs in the child nodes of s.
func (s *Schema) Compression() []compress.Codec { return s.root.Compression() }

// ValueByName is returns the sub-value with the givne name in base.
func (s *Schema) ValueByName(base reflect.Value, name string) reflect.Value {
	return s.root.ValueByName(base, name)
}

// String returns a parquet schema representation of s.
func (s *Schema) String() string {
	b := new(strings.Builder)
	Print(b, s.name, s.root)
	return b.String()
}

// Deconstruct deconstructs a Go value and appends it to a row.
func (s *Schema) Deconstruct(row Row, value interface{}) (Row, error) {
	v := reflect.ValueOf(value)

	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			v = reflect.Value{}
		} else {
			v = v.Elem()
		}
	}

	return s.deconstruct(row, levels{}, v)
}

// Reconstruct reconstructs a Go value from a row.
func (s *Schema) Reconstruct(value interface{}, row Row) error {
	v := reflect.ValueOf(value)

	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("cannot construct row into go value of non-pointer type %s", v.Type())
	}
	if v.IsNil() {
		return fmt.Errorf("cannot reconstruct row into nil pointer of type %s", v.Type())
	}

	return nil
}

type structNode struct {
	node
	fields []structField
	names  []string
}

func structNodeOf(t reflect.Type) *structNode {
	// Collect struct fields first so we can order them before generating the
	// column indexes.
	fields := structFieldsOf(t)

	s := &structNode{
		fields: make([]structField, len(fields)),
		names:  make([]string, len(fields)),
	}

	for i := range fields {
		s.fields[i] = makeStructField(fields[i])
		s.names[i] = fields[i].Name
	}

	return s
}

func structFieldsOf(t reflect.Type) []reflect.StructField {
	fields := appendStructFields(t, nil, nil)

	for i := range fields {
		f := &fields[i]

		if tag := f.Tag.Get("parquet"); tag != "" {
			name, _ := split(tag)
			if name != "" {
				f.Name = name
			}
		}
	}

	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Name < fields[j].Name
	})

	return fields
}

func appendStructFields(t reflect.Type, fields []reflect.StructField, index []int) []reflect.StructField {
	for i, n := 0, t.NumField(); i < n; i++ {
		fieldIndex := index[:len(index):len(index)]
		fieldIndex = append(fieldIndex, i)

		if f := t.Field(i); f.Anonymous {
			fields = appendStructFields(f.Type, fields, fieldIndex)
		} else if f.IsExported() {
			f.Index = fieldIndex
			fields = append(fields, f)
		}
	}
	return fields
}

func (s *structNode) Type() Type           { return groupType{} }
func (s *structNode) NumChildren() int     { return len(s.fields) }
func (s *structNode) ChildNames() []string { return s.names }
func (s *structNode) ChildByName(name string) Node {
	return s.ChildByIndex(s.indexOf(name))
}

func (s *structNode) ChildByIndex(index int) Node {
	return &s.fields[index]
}

func (s *structNode) ValueByName(base reflect.Value, name string) reflect.Value {
	return s.ValueByIndex(base, s.indexOf(name))
}

func (s *structNode) ValueByIndex(base reflect.Value, index int) reflect.Value {
	switch base.Kind() {
	case reflect.Map:
		return base.MapIndex(reflect.ValueOf(&s.names[index]).Elem())
	default:
		// Assume the structure matches, the method will panic if it does not.
		return s.fields[index].lookup(base)
	}
}

func (s *structNode) indexOf(name string) int {
	i := sort.Search(len(s.names), func(i int) bool {
		return s.names[i] >= name
	})
	if i == len(s.names) || s.names[i] != name {
		i = -1
	}
	return i
}

var (
	_ IndexedNode = (*structNode)(nil)
)

type structField struct {
	wrappedNode
	lookup func(reflect.Value) reflect.Value
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

func throwInvalidStructField(msg string, field reflect.StructField) {
	panic(msg + ": " + structFieldString(field))
}

func fieldByIndex(index []int) func(reflect.Value) reflect.Value {
	if len(index) == 1 {
		// Fast path for the common case where the field is not embedded.
		i := index[0]
		return func(v reflect.Value) reflect.Value { return v.Field(i) }
	}
	return func(v reflect.Value) reflect.Value {
		for _, i := range index {
			if v = v.Field(i); v.Kind() == reflect.Ptr {
				if v.IsNil() {
					v = reflect.Value{}
					break
				} else {
					v = v.Elem()
				}
			}
		}
		return v
	}
}

func makeStructField(f reflect.StructField) structField {
	var (
		field     = structField{lookup: fieldByIndex(f.Index)}
		optional  bool
		list      bool
		encodings []encoding.Encoding
		codecs    []compress.Codec
	)

	setNode := func(node Node) {
		if field.Node != nil {
			throwInvalidStructField("struct field has multiple logical parquet types declared", f)
		}
		field.Node = node
	}

	setOptional := func() {
		if optional {
			throwInvalidStructField("struct field has multiple declaration of the optional tag", f)
		}
		optional = true
	}

	setList := func() {
		if list {
			throwInvalidStructField("struct field has multiple declaration of the list tag", f)
		}
		list = true
	}

	setEncoding := func(enc encoding.Encoding) {
		for _, e := range encodings {
			if e.Encoding() == enc.Encoding() {
				throwInvalidStructField("struct field has encoding declared multiple times", f)
			}
		}
		encodings = append(encodings, enc)
	}

	setCompression := func(codec compress.Codec) {
		for _, c := range codecs {
			if c.CompressionCodec() == codec.CompressionCodec() {
				throwInvalidStructField("struct field has compression codecs declared multiple times", f)
			}
		}
		codecs = append(codecs, codec)
	}

	if tag := f.Tag.Get("parquet"); tag != "" {
		var element Node
		_, tag = split(tag) // skip the field name

		for tag != "" {
			option := ""
			option, tag = split(tag)
			option, args := splitOptionArgs(option)

			switch option {
			case "optional":
				setOptional()

			case "snappy":
				setCompression(&Snappy)

			case "gzip":
				setCompression(&Gzip)

			case "brotli":
				setCompression(&Brotli)

			case "lz4":
				setCompression(&Lz4Raw)

			case "zstd":
				setCompression(&Zstd)

			case "plain":
				setEncoding(&Plain)

			case "dict":
				setEncoding(&RLEDictionary)

			case "delta":
				switch f.Type.Kind() {
				case reflect.Int, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint32, reflect.Uint64:
					setEncoding(&DeltaBinaryPacked)
				default:
					throwInvalidFieldTag(f, option)
				}

			case "list":
				switch f.Type.Kind() {
				case reflect.Slice:
					element = nodeOf(f.Type.Elem())
					setNode(element)
					setList()
				default:
					throwInvalidFieldTag(f, option)
				}

			case "enum":
				switch f.Type.Kind() {
				case reflect.String:
					setNode(Enum())
				default:
					throwInvalidFieldTag(f, option)
				}

			case "uuid":
				switch f.Type.Kind() {
				case reflect.String:
					setNode(UUID())
				case reflect.Array:
					if f.Type.Elem().Kind() != reflect.Uint8 || f.Type.Len() != 16 {
						throwInvalidFieldTag(f, option)
					}
				default:
					throwInvalidFieldTag(f, option)
				}

			case "decimal":
				scale, precision, err := parseDecimalArgs(args)
				if err != nil {
					throwInvalidFieldTag(f, option+args)
				}
				var baseType Type
				switch f.Type.Kind() {
				case reflect.Int32:
					baseType = Int32Type
				case reflect.Int64:
					baseType = Int64Type
				default:
					throwInvalidFieldTag(f, option)
				}
				setNode(Decimal(scale, precision, baseType))

			default:
				throwUnknownFieldTag(f, option)
			}
		}
	}

	if field.Node == nil {
		field.Node = nodeOf(f.Type)
	}

	field.Node = Compressed(field.Node, codecs...)
	field.Node = Encoded(field.Node, encodings...)

	if list {
		field.Node = List(field.Node)
	}

	if optional {
		field.Node = Optional(field.Node)
	}

	return field
}

func nodeOf(t reflect.Type) Node {
	switch t {
	case reflect.TypeOf(deprecated.Int96{}):
		return Leaf(Int96Type)
	}

	switch t.Kind() {
	case reflect.Bool:
		return Leaf(BooleanType)

	case reflect.Int, reflect.Int64:
		return Int(64)

	case reflect.Int8, reflect.Int16, reflect.Int32:
		return Int(t.Bits())

	case reflect.Uint, reflect.Uintptr, reflect.Uint64:
		return Uint(64)

	case reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return Uint(t.Bits())

	case reflect.Float32:
		return Leaf(FloatType)

	case reflect.Float64:
		return Leaf(DoubleType)

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

func splitOptionArgs(s string) (option, args string) {
	if i := strings.IndexByte(s, '('); i >= 0 {
		return s[:i], s[i:]
	} else {
		return s, "()"
	}
}

func parseDecimalArgs(args string) (scale, precision int, err error) {
	if !strings.HasPrefix(args, "(") || !strings.HasSuffix(args, ")") {
		return 0, 0, fmt.Errorf("malformed decimal args: %s", args)
	}
	args = strings.TrimPrefix(args, "(")
	args = strings.TrimSuffix(args, ")")
	parts := strings.Split(args, ",")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("malformed decimal args: (%s)", args)
	}
	s, err := strconv.ParseInt(parts[0], 10, 32)
	if err != nil {
		return 0, 0, err
	}
	p, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		return 0, 0, err
	}
	return int(s), int(p), nil
}
