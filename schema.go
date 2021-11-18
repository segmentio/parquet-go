package parquet

import (
	"reflect"
	"sort"
	"strings"
)

type Schema struct {
	name     string
	root     Node
	traverse traverseFunc
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

func (s *Schema) Name() string { return s.name }

func (s *Schema) Type() Type { return s.root.Type() }

func (s *Schema) Optional() bool { return s.root.Optional() }

func (s *Schema) Repeated() bool { return s.root.Repeated() }

func (s *Schema) Required() bool { return s.root.Required() }

func (s *Schema) NumChildren() int { return s.root.NumChildren() }

func (s *Schema) ChildNames() []string { return s.root.ChildNames() }

func (s *Schema) ChildByName(name string) Node { return s.root.ChildByName(name) }

func (s *Schema) String() string {
	b := new(strings.Builder)
	Print(b, s.name, s.root)
	return b.String()
}

type Traversal interface {
	Traverse(columnIndex int, value Value) error
}

type TraversalFunc func(int, Value) error

func (f TraversalFunc) Traverse(columnIndex int, value Value) error { return f(columnIndex, value) }

func (s *Schema) Traverse(value interface{}, traversal Traversal) error {
	v := reflect.ValueOf(value)

	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			v = reflect.Value{}
		} else {
			v = v.Elem()
		}
	}

	return s.traverse(0, 0, v, traversal)
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

func (s *structNode) traverse(repetitionLevel, definitionLevel int32, value reflect.Value, traversal Traversal) error {
	fieldByIndex := reflect.Value.FieldByIndex

	if !value.IsValid() {
		fieldByIndex = func(reflect.Value, []int) reflect.Value { return reflect.Value{} }
	}

	for i := range s.fields {
		f := &s.fields[i]
		if err := f.traverse(repetitionLevel, definitionLevel, fieldByIndex(value, f.index), traversal); err != nil {
			return err
		}
	}

	return nil
}

func (s *structNode) Type() Type           { return groupType{} }
func (s *structNode) Optional() bool       { return false }
func (s *structNode) Repeated() bool       { return false }
func (s *structNode) Required() bool       { return true }
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
	field := structField{index: f.Index}
	optional := false

	setNode := func(node Node, traverse traverseFunc) {
		if field.Node != nil {
			panic("struct field has multiple logical parquet types declared: " + structFieldString(f))
		}
		field.Node = node
		field.traverse = traverse
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
				// TODO: this seems wrong
				optional = true

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
	return field, columnIndex
}

func nodeOf(t reflect.Type, columnIndex int) (Node, int, traverseFunc) {
	switch t.Kind() {
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

type traverseFunc func(repetitionLevel, definitionLevel int32, value reflect.Value, traversal Traversal) error

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

	return func(repetitionLevel, definitionLevel int32, value reflect.Value, traversal Traversal) error {
		var v Value

		if value.IsValid() {
			v = makeValue(value)
		}

		v.repetitionLevel = repetitionLevel
		v.definitionLevel = definitionLevel
		return traversal.Traverse(columnIndex, v)
	}
}

func traversePointer(traverse traverseFunc) traverseFunc {
	return func(repetitionLevel, definitionLevel int32, value reflect.Value, traversal Traversal) error {
		if value.IsValid() {
			if value.IsNil() {
				value = reflect.Value{}
			} else {
				definitionLevel++
				value = value.Elem()
			}
		}
		return traverse(repetitionLevel, definitionLevel, value, traversal)
	}
}

func traverseOptional(traverse traverseFunc) traverseFunc {
	return func(repetitionLevel, definitionLevel int32, value reflect.Value, traversal Traversal) error {
		if value.IsValid() {
			if value.IsZero() {
				value = reflect.Value{}
			} else {
				definitionLevel++
			}
		}
		return traverse(repetitionLevel, definitionLevel, value, traversal)
	}
}

func traverseSlice(traverse traverseFunc) traverseFunc {
	return func(repetitionLevel, definitionLevel int32, value reflect.Value, traversal Traversal) error {
		var err error

		if value.IsValid() {
			definitionLevel++
			nextRepetitionLevel := repetitionLevel + 1

			for i, n := 0, value.Len(); i < n && err == nil; i++ {
				err = traverse(repetitionLevel, definitionLevel, value.Index(i), traversal)
				repetitionLevel = nextRepetitionLevel
			}
		}

		return err
	}
}
