package parquet

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/segmentio/parquet/internal/debug"
	pthrift "github.com/segmentio/parquet/internal/gen-go/parquet"
	"github.com/segmentio/parquet/internal/thrift"
)

const magicBoundary uint32 = 0x31524150 // PAR1 in little endian
const magicLength = 4
const footerLengthLength = 4

// File represents a Parquet File ready for reading.
// Use OpenFile to create a File.
// Files are stateful. They should not be shared or concurrently accessed.
type File struct {
	// small buffer used to read things like magic numbers and footer length
	uint32buf [4]byte
	uint64buf [8]byte

	thrift   *thrift.Reader
	metadata *FileMetadata
}

// FileMetadata represents the information present in a File's footer.
type FileMetadata struct {
	Version      int32
	NumRows      int64
	CreatedBy    string
	Schema       *Schema
	rowGroups    []*pthrift.RowGroup
	columnOrders []*pthrift.ColumnOrder
}

// OpenFile creates a File and immediately reads its metadata.
//
// OpenFile (and in general all the read-related methods of File) have a
// tendency to Seek a lot. Make sure the ReadSeeker method provided to this
// method has reasonable performance for that use-case.
func OpenFile(r io.ReadSeeker) (*File, error) {
	t := thrift.NewReader(r)

	err := t.Open()
	if err != nil {
		return nil, err
	}

	f := &File{
		thrift: t,
	}

	// TODO: we might not want to check that the file starts with the magic
	// number in a less safe mode.
	err = f.checkMagicBoundary()
	if err != nil {
		return nil, err
	}

	_, err = t.Seek(-magicLength, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	err = f.checkMagicBoundary()
	if err != nil {
		return nil, err
	}

	_, err = t.Seek(-(magicLength + footerLengthLength), io.SeekEnd)
	if err != nil {
		return nil, err
	}

	footerLength, err := f.uint32()
	if err != nil {
		return nil, err
	}

	_, err = t.Seek(-int64(magicLength+footerLengthLength+footerLength), io.SeekEnd)
	if err != nil {
		return nil, err
	}

	rawMetadata := pthrift.NewFileMetaData()
	err = t.Unmarshal(rawMetadata)
	if err != nil {
		return nil, err
	}

	f.metadata = &FileMetadata{
		Version:   rawMetadata.GetVersion(),
		NumRows:   rawMetadata.GetNumRows(),
		CreatedBy: rawMetadata.GetCreatedBy(),
	}
	f.metadata.rowGroups = rawMetadata.GetRowGroups()
	f.metadata.columnOrders = rawMetadata.GetColumnOrders()
	// TODO: do this lazily
	f.metadata.Schema, err = schemaFromFlatElements(rawMetadata.GetSchema())
	if err != nil {
		return nil, err
	}

	return f, nil
}

// RowGroups returns an iterator over all the File's row groups.
func (f *File) RowGroups() *RowGroupIterator {
	return &RowGroupIterator{r: f}
}

// Return a pointer to the File's metadata.
// Do not modify this struct while reading.
func (f *File) Metadata() *FileMetadata {
	return f.metadata
}

func (f *File) uint32() (uint32, error) {
	n, err := f.thrift.Read(f.uint32buf[:])
	if err != nil {
		return 0, err
	}
	if n != 4 {
		return 0, fmt.Errorf("not enough bytes: %d (required %d)", n, 4)
	}
	return binary.LittleEndian.Uint32(f.uint32buf[:]), nil
}

func (f *File) checkMagicBoundary() error {
	v, err := f.uint32()
	if err != nil {
		return err
	}
	if v != magicBoundary {
		return fmt.Errorf("invalid magic boundary: '0x%X'", v)
	}
	return nil
}

// Field is a value read from a parquet file and a pointer to its schema node.
type Field struct {
	Schema *Schema
	// Value is either a *Raw for a leaf node, or a []Field of any other
	// node.
	Value interface{}
}

type reader interface {
	// Read returns the next value.
	// Expected to return (nil, EOF) when all values have been consumed.
	Read(b RowBuilder) error

	// Advance the reader without calling back a builder.
	Skip() error

	// Peek returns repetition and definition levels for the next value.
	Peek() (Levels, error)

	// Bind resets the state of the root and adjust any relevant parameter
	// based on the RowGroup header.
	Bind(rg *RowGroup)
}

// RowBuilder interface needs to be implemented to interpret the results of the
// RowReader.
type RowBuilder interface {
	// Called when a new row starts being read.
	Begin()

	// Called when a primitive field is read.
	// It is up to this function to call the right method on the decoder based
	// on the the schema of the field.
	//
	// Returning an error stops the parsing of the whole file.
	Primitive(s *Schema, d Decoder) error
	PrimitiveNil(s *Schema) error

	// Called when a group starts
	GroupBegin(s *Schema)
	GroupEnd(node *Schema)

	// Called when a repeated value starts
	RepeatedBegin(s *Schema)
	RepeatedEnd(node *Schema)

	// Called when a repeated KV starts
	KVBegin(node *Schema)
	KVEnd(node *Schema)

	// Called when the row has been fully read.
	End()
}

type voidBuilder struct {
	// TODO: remove alloc
	scratch []byte
}

func (v *voidBuilder) Begin()                   { panic("should not be called") }
func (v *voidBuilder) End()                     { panic("should not be called") }
func (v *voidBuilder) GroupBegin(s *Schema)     { panic("should not be called") }
func (v *voidBuilder) GroupEnd(node *Schema)    { panic("should not be called") }
func (v *voidBuilder) RepeatedBegin(s *Schema)  { panic("should not be called") }
func (v *voidBuilder) RepeatedEnd(node *Schema) { panic("should not be called") }
func (v *voidBuilder) KVBegin(node *Schema)     { panic("should not be called") }
func (v *voidBuilder) KVEnd(node *Schema)       { panic("should not be called") }
func (v *voidBuilder) PrimitiveNil(s *Schema) error {
	return nil // no-op
}
func (v *voidBuilder) Primitive(s *Schema, d Decoder) error {
	var err error
	switch s.PhysicalType {
	case pthrift.Type_BYTE_ARRAY:
		v.scratch, err = d.ByteArray(v.scratch)
	case pthrift.Type_INT32:
		_, err = d.Int32()
	case pthrift.Type_INT64:
		_, err = d.Int64()
	default:
		panic(fmt.Errorf("unsupported type: %s", s.PhysicalType.String()))
	}
	return err
}

var theVoidBuilder = &voidBuilder{}

// RowReader reads a parquet file, assembling rows as it goes.
//
// This is the high-level interface for reading parquet files. RowReader
// presents an iterator interface. Call Next to advance the reader, Value to
// retrieve the current value, and finally check for errors with Error when you
// are done.
//
// Example:
//
// 	rowReader := parquet.NewRowReader(file)
//	for rowReader.Next() {
//		v := rowReader.Value()
//		fmt.Println(v)
//	}
//	err = rowReader.Error()
//	if err != nil {
//  	panic(err)
//	}
type RowReader struct {
	reader    *File
	schema    *Schema
	root      reader
	rowGroups *RowGroupIterator

	// state
	rowGroup *RowGroup
}

// Next assembles the next row, calling back the RowBuilder.
// Returns true if a row was assembled.
// Use Value to retrieve the row, and Error to check whether something went
// wrong.
func (r *RowReader) Read(b RowBuilder) error {
	if r.rowGroup == nil {
		if !r.nextRowGroup() {
			return EOF
		}
	}

	_, err := r.root.Peek()
	if err == EOF {
		if !r.nextRowGroup() {
			return EOF
		}
		_, err = r.root.Peek()
	}
	if err != nil {
		return err
	}

	b.Begin()

	err = r.root.Read(b)
	if err != nil {
		return err
	}

	b.End()

	return nil
}

// WithSchema instructs the RowReader to follow the provided schema instead of
// the one described in the Parquet file.
func (r *RowReader) WithSchema(s *Schema) {
	r.schema = s
}

func (r *RowReader) nextRowGroup() bool {
	if !r.rowGroups.Next() {
		return false
	}
	debug.Format("-- New row group")
	r.rowGroup = r.rowGroups.Value()
	r.root.Bind(r.rowGroup)
	return true
}

// NewRowReader builds a RowReader using data from r and calling back b.
func NewRowReader(r *File) *RowReader {
	return &RowReader{
		// The root of a schema has to define a group.
		root:      newGroupReader(r.metadata.Schema),
		reader:    r,
		rowGroups: r.RowGroups(),
		schema:    r.metadata.Schema,
	}
}

// newReader builds the appropriate reader for s.
func newReader(s *Schema) reader {
	// TODO: handle optionals
	switch s.Kind {
	case PrimitiveKind:
		return newPrimitiveReader(s)
	case MapKind:
		return newKvReader(s)
	case RepeatedKind:
		return newRepeatedReader(s)
	case GroupKind:
		return newGroupReader(s)
	default:
		panic("unhandled group kind")
	}
}

// primitiveReader reads fields of primitive type.
type primitiveReader struct {
	schema *Schema

	// will be bound at read time
	it   *RowGroupColumnReader
	next *Raw
}

func (r *primitiveReader) String() string          { return "PrimaryReader " + r.schema.Name }
func (r *primitiveReader) Peek() (Levels, error)   { return r.it.Peek() }
func (r *primitiveReader) Bind(rg *RowGroup)       { r.it = rg.Column(r.schema.Path) }
func (r *primitiveReader) Skip() error             { return r.it.Read(theVoidBuilder) }
func (r *primitiveReader) Read(b RowBuilder) error { return r.it.Read(b) }

func newPrimitiveReader(s *Schema) *primitiveReader {
	return &primitiveReader{schema: s}
}

// groupReader reads a group (struct)
type groupReader struct {
	schema  *Schema
	readers []reader
}

func (r *groupReader) String() string { return "GroupReader " + r.schema.Name }
func (r *groupReader) Peek() (Levels, error) {
	return r.readers[0].Peek()
}
func (r *groupReader) Bind(rg *RowGroup) {
	for _, reader := range r.readers {
		reader.Bind(rg)
	}
}

func (r *groupReader) Read(b RowBuilder) error {
	b.GroupBegin(r.schema)
	for _, reader := range r.readers {
		err := reader.Read(b)
		if err != nil {
			return err
		}
	}
	b.GroupEnd(r.schema)
	return nil
}

func (r *groupReader) Skip() error {
	for _, reader := range r.readers {
		err := reader.Skip()
		if err != nil {
			return err
		}
	}
	return nil
}

func newGroupReader(s *Schema) *groupReader {
	readers := make([]reader, len(s.Children))
	for i, child := range s.Children {
		readers[i] = newReader(child)
	}
	reader := &groupReader{
		schema:  s,
		readers: readers,
	}
	return reader
}

// kvReader reads maps.
type kvReader struct {
	schema      *Schema
	keyReader   reader
	valueReader reader
}

func (r *kvReader) String() string { return "KVReader " + r.schema.Name }
func (r *kvReader) Bind(rg *RowGroup) {
	r.keyReader.Bind(rg)
	r.valueReader.Bind(rg)
}

func (r *kvReader) Peek() (Levels, error) {
	return r.keyReader.Peek()
}

func (r *kvReader) Skip() error {
	err := r.keyReader.Skip()
	if err != nil {
		return err
	}
	return r.valueReader.Skip()
}

func (r *kvReader) Read(b RowBuilder) error {
	_, err := r.Peek()
	if err != nil {
		return err
	}

	b.RepeatedBegin(r.schema)

	for {
		levels, err := r.Peek()
		if err != nil {
			return err
		}

		if levels.Definition > r.schema.DefinitionLevel {
			b.KVBegin(r.schema.Children[0])
			err = r.keyReader.Read(b)
			if err != nil {
				return err
			}
			err = r.valueReader.Read(b)
			if err != nil {
				return err
			}
			b.KVEnd(r.schema.Children[0])
		} else {
			err = r.keyReader.Skip()
			if err != nil {
				return err
			}
			err = r.valueReader.Skip()
			if err != nil {
				return err
			}
			break
		}

		levels, err = r.Peek()
		if err == EOF {
			break
		}
		if err != nil {
			return err
		}
		if levels.Repetition <= r.schema.RepetitionLevel {
			break
		}
	}

	b.RepeatedEnd(r.schema)

	return nil
}

func newKvReader(s *Schema) *kvReader {
	// TODO: don't panic

	// MAP is used to annotate types that should be interpreted as a map from
	// keys to values. MAP must annotate a 3-level structure:
	//
	// <map-repetition> group <name> (MAP) {
	// 	repeated group key_value {
	// 		required <key-type> key;
	// 		<value-repetition> <value-type> value;
	// 	}
	// }
	// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps

	// The outer-most level must be a group annotated with MAP that contains a
	// single field named key_value. The repetition of this level must be
	// either optional or required and determines whether the list is nullable.

	if len(s.Children) != 1 {
		panic(fmt.Errorf("MAP should have exactly 1 child"))
	}
	keyValue := s.Children[0]

	// Purposefully don't check the name to handle the legacy cases.

	if keyValue.Repetition != pthrift.FieldRepetitionType_REPEATED {
		panic(fmt.Errorf("MAP's child should be repeated, not %s", keyValue.Repetition))
	}

	if len(keyValue.Children) != 2 {
		panic(fmt.Errorf("MAP's key_value should have exactly 2 children"))
	}

	// The middle level, named key_value, must be a repeated group with a key
	// field for map keys and, optionally, a value field for map values.

	// The key field encodes the map's key type. This field must have
	// repetition required and must always be present.
	key := keyValue.At("key")
	if key == nil {
		panic("MAP's key_value must have a child named 'key'")
	}

	if key.Repetition != pthrift.FieldRepetitionType_REQUIRED {
		panic(fmt.Errorf("MAP's key field must be required, not %s", key.Repetition))
	}

	// The value field encodes the map's value type and repetition. This field
	// can be required, optional, or omitted.

	value := keyValue.At("value")
	if value == nil {
		panic("MAP's key_value must have a child named 'value")
	}

	return &kvReader{
		schema:      s,
		keyReader:   newReader(key),
		valueReader: newReader(value),
	}
}

// repeatedReader reads a repeated group
type repeatedReader struct {
	schema *Schema
	reader reader
}

func (r *repeatedReader) String() string        { return "RepeatedReader " + r.schema.Name }
func (r *repeatedReader) Bind(rg *RowGroup)     { r.reader.Bind(rg) }
func (r *repeatedReader) Peek() (Levels, error) { return r.reader.Peek() }
func (r *repeatedReader) Skip() error           { return r.reader.Skip() }
func (r *repeatedReader) Read(b RowBuilder) error {
	// check for EOF
	_, err := r.Peek()
	if err != nil {
		return err
	}

	b.RepeatedBegin(r.schema)

	for {
		levels, err := r.Peek()
		if err != nil {
			return err
		}

		if levels.Definition > r.schema.DefinitionLevel {
			err = r.reader.Read(b)
			if err != nil {
				return err
			}
		} else {
			// this is an empty list
			err = r.reader.Skip()
			if err != nil {
				return err
			}
			break
		}

		levels, err = r.Peek()
		if err == EOF {
			break
		}
		if err != nil {
			return err
		}

		// next level will be part of a different row
		if levels.Repetition <= r.schema.RepetitionLevel {
			break
		}
	}

	b.RepeatedEnd(r.schema)

	return nil
}

func newRepeatedReader(s *Schema) *repeatedReader {
	// TODO: don't panic
	// TODO: support the legacy list formats

	//	LIST must always annotate a 3-level structure:
	//
	//	<list-repetition> group <name> (LIST) {
	//	repeated group list {
	//		<element-repetition> <element-type> element;
	//	}interface{}
	//}
	//	The outer-most level must be a group annotated with LIST that
	//	contains a single field named list. The repetition of this level
	//	must be either optional or required and determines whether the list
	//	is nullable.
	if s.Repetition != pthrift.FieldRepetitionType_OPTIONAL && s.Repetition != pthrift.FieldRepetitionType_REQUIRED {
		panic(fmt.Errorf("LIST repetition is %s", s.Repetition))
	}
	if len(s.Children) != 1 {
		panic("LIST must have exactly one child")
	}

	//	The middle level, named list, must be a repeated group with a
	//	single field named element.
	list := s.Children[0]
	if list.Name != "list" {
		panic(fmt.Errorf("LIST's child must be named 'list' (not '%s')", list.Name))
	}
	if list.Kind != RepeatedKind {
		panic("LIST's list child must be a group")
	}
	if list.Repetition != pthrift.FieldRepetitionType_REPEATED {
		panic(fmt.Errorf("LIST's list child but be repeated, not %s", list.Repetition))
	}
	if len(list.Children) != 1 {
		panic("LIST's list child must have exactly one child")
	}

	//	The element field encodes the list's element type and repetition.
	//	Element repetition must be required or optional.
	element := list.Children[0]
	if element.Repetition != pthrift.FieldRepetitionType_OPTIONAL && s.Repetition != pthrift.FieldRepetitionType_REQUIRED {
		panic(fmt.Errorf("LIST element child must be either optional or required, not %s", element.Repetition))
	}

	return &repeatedReader{
		schema: s,
		reader: newReader(element),
	}
}
