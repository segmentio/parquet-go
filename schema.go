package parquet

import (
	"reflect"
	"sort"
	"strings"

	"github.com/segmentio/parquet/compress"
	"github.com/segmentio/parquet/encoding"
)

// Schema represents a parquet schema created from a Go value.
//
// Schema implements the Node interface to represent the root node of a parquet
// schema.
type Schema struct {
	name     string
	root     Node
	traverse traverseFunc
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
//	dict     | enables dictionary encoding on the parquet column
//	list     | for slice types, use the parquet LIST logical type
//	enum     | for string types, use the parquet ENUM logical type
//	uuid     | for string and [16]byte types, use the parquet UUID logical type
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
		node, _, traverse := structNodeOf(t, 0)
		return newSchema(name, node, traverse)

	case reflect.Ptr:
		if elem := t.Elem(); elem.Kind() == reflect.Struct {
			node, _, traverse := structNodeOf(elem, 0)
			return newSchema(name, node, traverse)
		}
	}

	panic("cannot construct parquet schema from value of type " + model.Type().String())
}

func newSchema(name string, root Node, traverse traverseFunc) *Schema {
	return &Schema{
		name:     name,
		root:     root,
		traverse: traverse,
	}
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

// String returns a parquet schema representation of s.
func (s *Schema) String() string {
	b := new(strings.Builder)
	Print(b, s.name, s.root)
	return b.String()
}

// Traversal is an interface used to implement the parquet schema traversal
// algorithm.
type Traversal interface {
	// The Traverse method is called for each column index and parquet value
	// when traversing a Go value with its parquet schema.
	//
	// The repetition and definition levels of the parquet value will be set
	// according to the structure of the input Go value.
	Traverse(columnIndex int, value Value) error
}

// TraversalFunc is an implementation of the Traverse interface for regular
// Go functions and methods.
type TraversalFunc func(int, Value) error

// Traverse satisfies the Traversal interface.
func (f TraversalFunc) Traverse(columnIndex int, value Value) error { return f(columnIndex, value) }

// Traverse is the implementation of the traversal algorithm which converts
// Go values into a sequence of column index + parquet value pairs by calling
// the given traversal callback.
//
// The type of the Go value must match the parquet schema or the method will
// panic.
//
// The traversal callback must not be nil or the method will panic.
func (s *Schema) Traverse(value interface{}, traversal Traversal) error {
	v := reflect.ValueOf(value)

	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			v = reflect.Value{}
		} else {
			v = v.Elem()
		}
	}

	return s.traverse(levels{}, v, traversal)
}

type structNode struct {
	node
	fields []structField
	names  []string
}

func structNodeOf(t reflect.Type, columnIndex int) (*structNode, int, traverseFunc) {
	// Collect struct fields first so we can order them before generating the
	// column indexes.
	fields := structFieldsOf(t)

	s := &structNode{
		fields: make([]structField, len(fields)),
		names:  make([]string, len(fields)),
	}

	for i := range fields {
		s.fields[i], columnIndex = makeStructField(fields[i], columnIndex)
		s.names[i] = fields[i].Name
	}

	return s, columnIndex, s.traverse
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

func (s *structNode) traverse(levels levels, value reflect.Value, traversal Traversal) error {
	fieldByIndex := reflect.Value.FieldByIndex

	if !value.IsValid() {
		fieldByIndex = func(reflect.Value, []int) reflect.Value { return reflect.Value{} }
	}

	for i := range s.fields {
		f := &s.fields[i]
		if err := f.traverse(levels, fieldByIndex(value, f.index), traversal); err != nil {
			return err
		}
	}

	return nil
}

func (s *structNode) Type() Type           { return groupType{} }
func (s *structNode) NumChildren() int     { return len(s.fields) }
func (s *structNode) ChildNames() []string { return s.names }
func (s *structNode) ChildByName(name string) Node {
	i := sort.Search(len(s.names), func(i int) bool {
		return s.names[i] >= name
	})
	if i >= 0 && i < len(s.fields) {
		return &s.fields[i]
	}
	panic("column not found in parquet schema: " + name)
}

type structField struct {
	Node
	index    []int
	traverse traverseFunc
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

func makeStructField(f reflect.StructField, columnIndex int) (structField, int) {
	var (
		field     = structField{index: f.Index}
		optional  bool
		encodings []encoding.Encoding
		codecs    []compress.Codec
	)

	setNode := func(node Node, traverse traverseFunc) {
		if field.Node != nil {
			panic("struct field has multiple logical parquet types declared: " + structFieldString(f))
		}
		field.Node = node
		field.traverse = traverse
	}

	setOptional := func() {
		if optional {
			panic("struct field has multiple declaration of the optional tag: " + structFieldString(f))
		}
		optional = true
	}

	setEncoding := func(enc encoding.Encoding) {
		for _, e := range encodings {
			if e.Encoding() == enc.Encoding() {
				panic("struct field has encoding declared multiple times: " + structFieldString(f))
			}
		}
		encodings = append(encodings, enc)
	}

	setCompression := func(codec compress.Codec) {
		for _, c := range codecs {
			if c.CompressionCodec() == codec.CompressionCodec() {
				panic("struct field has compression codecs declared multiple times: " + structFieldString(f))
			}
		}
		codecs = append(codecs, codec)
	}

	if tag := f.Tag.Get("parquet"); tag != "" {
		var element Node
		var traverse traverseFunc
		_, tag = split(tag) // skip the field name

		for tag != "" {
			option := ""
			option, tag = split(tag)

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

			case "dict":
				setEncoding(&Dict)

			case "list":
				switch f.Type.Kind() {
				case reflect.Slice:
					element, columnIndex, traverse = nodeOf(f.Type.Elem(), columnIndex)
					setNode(List(element), traverseSlice(traverse))
				default:
					throwInvalidFieldTag(f, option)
				}

			case "enum":
				switch f.Type.Kind() {
				case reflect.String:
					setNode(Enum(), traverseLeaf(ByteArray, f.Type, columnIndex))
					columnIndex++
				default:
					throwInvalidFieldTag(f, option)
				}

			case "uuid":
				switch f.Type.Kind() {
				case reflect.String:
					setNode(UUID(), traverseLeaf(ByteArray, f.Type, columnIndex))
					columnIndex++
				case reflect.Array:
					if f.Type.Elem().Kind() != reflect.Uint8 || f.Type.Len() != 16 {
						throwInvalidFieldTag(f, option)
					}
				default:
					throwInvalidFieldTag(f, option)
				}

			default:
				throwUnknownFieldTag(f, option)
			}
		}
	}

	if field.Node == nil {
		field.Node, columnIndex, field.traverse = nodeOf(f.Type, columnIndex)
	}

	if optional {
		field.Node = Optional(field.Node)
		field.traverse = traverseOptional(field.traverse)
	}

	field.Node = Compressed(field.Node, codecs...)
	field.Node = Encoded(field.Node, encodings...)
	return field, columnIndex
}

func nodeOf(t reflect.Type, columnIndex int) (Node, int, traverseFunc) {
	switch t.Kind() {
	case reflect.Bool:
		return Leaf(BooleanType), columnIndex + 1, traverseLeaf(Boolean, t, columnIndex)

	case reflect.Int, reflect.Int64:
		return Int(64), columnIndex + 1, traverseLeaf(Int64, t, columnIndex)

	case reflect.Int8, reflect.Int16, reflect.Int32:
		return Int(t.Bits()), columnIndex + 1, traverseLeaf(Int32, t, columnIndex)

	case reflect.Uint, reflect.Uintptr, reflect.Uint64:
		return Uint(64), columnIndex + 1, traverseLeaf(Int64, t, columnIndex)

	case reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return Uint(t.Bits()), columnIndex + 1, traverseLeaf(Int32, t, columnIndex)

	case reflect.Float32:
		return Decimal(0, 9, FloatType), columnIndex + 1, traverseLeaf(Float, t, columnIndex)

	case reflect.Float64:
		return Decimal(0, 18, DoubleType), columnIndex + 1, traverseLeaf(Double, t, columnIndex)

	case reflect.String:
		return String(), columnIndex + 1, traverseLeaf(ByteArray, t, columnIndex)

	case reflect.Ptr:
		node, columnIndex, traverse := nodeOf(t.Elem(), columnIndex)
		return Optional(node), columnIndex, traversePointer(traverse)

	case reflect.Struct:
		return structNodeOf(t, columnIndex)

	case reflect.Slice:
		node, columnIndex, traverse := nodeOf(t.Elem(), columnIndex)
		return Repeated(node), columnIndex, traverseSlice(traverse)

	case reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 && t.Len() == 16 {
			return UUID(), columnIndex + 1, traverseLeaf(FixedLenByteArray, t, columnIndex)
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

type traverseFunc func(levels levels, value reflect.Value, traversal Traversal) error

func traverseLeaf(k Kind, t reflect.Type, columnIndex int) traverseFunc {
	var makeValue func(reflect.Value) Value

	// Same rules as implemented in makeValue but using the reflect.Type to
	// precompute the constructors.
	switch k {
	case Boolean:
		switch t.Kind() {
		case reflect.Bool:
			makeValue = func(v reflect.Value) Value { return makeValueBoolean(v.Bool()) }
		}

	case Int32:
		switch t.Kind() {
		case reflect.Int8, reflect.Int16, reflect.Int32:
			makeValue = func(v reflect.Value) Value { return makeValueInt32(int32(v.Int())) }
		case reflect.Uint8, reflect.Uint16, reflect.Uint32:
			makeValue = func(v reflect.Value) Value { return makeValueInt32(int32(v.Uint())) }
		}

	case Int64:
		switch t.Kind() {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			makeValue = func(v reflect.Value) Value { return makeValueInt64(v.Int()) }
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			makeValue = func(v reflect.Value) Value { return makeValueInt64(int64(v.Uint())) }
		}

	case Float:
		switch t.Kind() {
		case reflect.Float32:
			makeValue = func(v reflect.Value) Value { return makeValueFloat(float32(v.Float())) }
		}

	case Double:
		switch t.Kind() {
		case reflect.Float64:
			makeValue = func(v reflect.Value) Value { return makeValueDouble(v.Float()) }
		}

	case ByteArray:
		switch t.Kind() {
		case reflect.String:
			makeValue = func(v reflect.Value) Value { return makeValueString(k, v.String()) }
		case reflect.Slice:
			if t.Elem().Kind() == reflect.Uint8 {
				makeValue = func(v reflect.Value) Value { return makeValueBytes(k, v.Bytes()) }
			}
		}

	case FixedLenByteArray:
		switch t.Kind() {
		case reflect.String:
			makeValue = func(v reflect.Value) Value { return makeValueString(k, v.String()) }
		case reflect.Array:
			if t.Elem().Kind() == reflect.Uint8 {
				makeValue = makeValueFixedLenByteArray
			}
		}
	}

	if makeValue == nil {
		panic("traverseLeaf called with invalid combination of parquet and go types: " + k.String() + "/" + t.String())
	}

	return func(levels levels, value reflect.Value, traversal Traversal) error {
		var v Value

		if value.IsValid() {
			v = makeValue(value)
		}

		v.repetitionLevel = levels.repetitionLevel
		v.definitionLevel = levels.definitionLevel
		return traversal.Traverse(columnIndex, v)
	}
}

func traversePointer(traverse traverseFunc) traverseFunc {
	return func(levels levels, value reflect.Value, traversal Traversal) error {
		if value.IsValid() {
			if value.IsNil() {
				value = reflect.Value{}
			} else {
				levels.definitionLevel++
				value = value.Elem()
			}
		}
		return traverse(levels, value, traversal)
	}
}

func traverseOptional(traverse traverseFunc) traverseFunc {
	return func(levels levels, value reflect.Value, traversal Traversal) error {
		if value.IsValid() {
			if value.IsZero() {
				value = reflect.Value{}
			} else {
				levels.definitionLevel++
			}
		}
		return traverse(levels, value, traversal)
	}
}

func traverseSlice(traverse traverseFunc) traverseFunc {
	return func(levels levels, value reflect.Value, traversal Traversal) error {
		var numValues int
		var err error

		if value.IsValid() {
			numValues = value.Len()
			levels.repetitionDepth++
			if !value.IsNil() {
				levels.definitionLevel++
			}
		}

		if numValues == 0 {
			err = traverse(levels, reflect.Value{}, traversal)
		} else {
			for i := 0; i < numValues && err == nil; i++ {
				err = traverse(levels, value.Index(i), traversal)
				levels.repetitionLevel = levels.repetitionDepth
			}
		}

		return err
	}
}
