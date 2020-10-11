package parquet

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	pthrift "github.com/segmentio/parquet/internal/gen-go/parquet"
	"github.com/segmentio/parquet/internal/stats"
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

// rowGroups returns an iterator over all the File's row groups.
func (f *File) rowGroups() *rowGroupIterator {
	return &rowGroupIterator{r: f}
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

type reader interface {
	// read returns the next value.
	// Expected to return (nil, EOF) when all values have been consumed.
	read(b RowBuilder) error

	// Advance the reader without calling back a builder.
	skip() error

	// Peek returns repetition and definition levels for the next value.
	peek() (levels, error)

	// Bind resets the state of the root and adjust any relevant parameter
	// based on the rowGroup header.
	bind(rg *rowGroup)
}

// RowBuilder interface needs to be implemented to interpret the results of the
// RowReader.
type RowBuilder interface {
	// Called when a new row starts being read.
	Begin()

	// Called when a primitive field is read.
	// It is up to this function to call the right method on the decoder based
	// on the the s of the field.
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
// calls back methods on a RowBuilder interface as it assemble rows.
// Call Read() to assemble the next row. It returns EOF when the file has been
// fully processed.
//
// Example:
//
// 	rowReader := parquet.NewRowReader(file)
//	for {
//		// builder implements RowBuilder.
// 		err := rowReader.Read(builder)
//		if err == parquet.EOF {
//			break
// 		}
//		// Do something with err or whatever builder does.
//	}
//
// See StructPlanner to create a RowBuilder that constructs Go structures for
// each row.
type RowReader struct {
	reader    *File
	schema    *Schema
	root      reader
	rowGroups *rowGroupIterator

	// state
	rowGroup *rowGroup
	stats    *rowReaderStats
}

type rowReaderStats struct {
	duration stats.Duration
	rows     stats.Counter
}

// Read assembles the next row, calling back the RowBuilder.
// Returns EOF when all rows have been read.
func (r *RowReader) Read(b RowBuilder) error {
	r.stats.duration.Init()
	if r.rowGroup == nil {
		if !r.nextRowGroup() {
			return EOF
		}
	}

	_, err := r.root.peek()
	if err == EOF {
		if !r.nextRowGroup() {
			return EOF
		}
		_, err = r.root.peek()
	}
	if err != nil {
		return err
	}

	b.Begin()

	err = r.root.read(b)
	if err != nil {
		return err
	}
	r.stats.rows.Inc()

	b.End()

	return nil
}

// RowReaderStats is a data structure returned by RowReader.Stats() that
// contains metrics about the Reader's actions.
type RowReaderStats struct {
	// Time duration since the last Stats() call, or beginning for first
	// Read(), whichever is last.
	Duration time.Duration

	// Number of rows that have been read.
	Rows int64

	// Number of bytes read by the underlying reader.
	ReaderBytes int64
	// Number of times the underlying reader was asked to read.
	ReaderReads int64
	// Number of times the underlying reader was asked to seek.
	ReaderSeeks int64
	// Number of times the underlying reader was asked to fork.
	ReaderForks int64
}

// Stats returns a snapshot of the RowReader's stats since the the RowReader
// was created, or since the last time the method was called.
func (r *RowReader) Stats() RowReaderStats {
	readerStats := r.reader.thrift.Stats()
	return RowReaderStats{
		Duration:    r.stats.duration.Snapshot(),
		Rows:        r.stats.rows.Snapshot(),
		ReaderBytes: readerStats.Bytes.Snapshot(),
		ReaderReads: readerStats.Reads.Snapshot(),
		ReaderSeeks: readerStats.Seeks.Snapshot(),
		ReaderForks: readerStats.Forks.Snapshot(),
	}
}

func (r *RowReader) nextRowGroup() bool {
	if !r.rowGroups.next() {
		return false
	}
	r.rowGroup = r.rowGroups.value()
	r.root.bind(r.rowGroup)
	return true
}

// NewRowReaderWithPlan builds a RowReader using data from f and following the plan p.
func NewRowReaderWithPlan(p *Plan, f *File) *RowReader {
	schema := p.schema()
	if schema == nil {
		// use the file's s if the plan is not providing one
		//
		// TODO: do not parse the file's s if the plan already
		// provides one.
		schema = f.metadata.Schema
	}
	return &RowReader{
		// The root of a s has to define a group.
		root:      newGroupReader(schema),
		reader:    f,
		rowGroups: f.rowGroups(),
		schema:    schema,

		stats: &rowReaderStats{},
	}
}

// NewRowReader constructs a RowReader to read rows from f.
//
// Use NewRowReaderWithPlan to process rows more finely with a Plan.
func NewRowReader(f *File) *RowReader {
	return NewRowReaderWithPlan(defaultPlan, f)
}

// newReader builds the appropriate reader for s.
func newReader(s *Schema) reader {
	// TODO: handle optionals
	switch s.Kind {
	case primitiveKind:
		return newPrimitiveReader(s)
	case mapKind:
		return newKvReader(s)
	case repeatedKind:
		return newRepeatedReader(s)
	case groupKind:
		return newGroupReader(s)
	default:
		panic("unhandled group kind")
	}
}

// primitiveReader reads fields of primitive type.
type primitiveReader struct {
	schema *Schema

	// will be bound at read time
	it *rowGroupColumnReader
}

func (r *primitiveReader) String() string        { return "PrimaryReader " + r.schema.Name }
func (r *primitiveReader) peek() (levels, error) { return r.it.peek() }
func (r *primitiveReader) bind(rg *rowGroup) {
	r.it = rg.Column(r.schema)
	if r.it == nil { // TODO: proper error handling
		panic(fmt.Errorf("could not find a column at %s for reader %s", r.schema.Path, r.String()))
	}
}
func (r *primitiveReader) skip() error             { return r.it.read(theVoidBuilder) }
func (r *primitiveReader) read(b RowBuilder) error { return r.it.read(b) }

func newPrimitiveReader(s *Schema) *primitiveReader {
	return &primitiveReader{schema: s}
}

// groupReader reads a group (struct)
type groupReader struct {
	schema  *Schema
	readers []reader
}

func (r *groupReader) String() string { return "GroupReader " + r.schema.Name }
func (r *groupReader) peek() (levels, error) {
	return r.readers[0].peek()
}
func (r *groupReader) bind(rg *rowGroup) {
	for _, reader := range r.readers {
		reader.bind(rg)
	}
}

func (r *groupReader) read(b RowBuilder) error {
	b.GroupBegin(r.schema)
	for _, reader := range r.readers {
		err := reader.read(b)
		if err != nil {
			return err
		}
	}
	b.GroupEnd(r.schema)
	return nil
}

func (r *groupReader) skip() error {
	for _, reader := range r.readers {
		err := reader.skip()
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
func (r *kvReader) bind(rg *rowGroup) {
	r.keyReader.bind(rg)
	r.valueReader.bind(rg)
}

func (r *kvReader) peek() (levels, error) {
	return r.keyReader.peek()
}

func (r *kvReader) skip() error {
	err := r.keyReader.skip()
	if err != nil {
		return err
	}
	return r.valueReader.skip()
}

func (r *kvReader) read(b RowBuilder) error {
	_, err := r.peek()
	if err != nil {
		return err
	}

	b.RepeatedBegin(r.schema)

	for {
		levels, err := r.peek()
		if err != nil {
			return err
		}

		if levels.definition > r.schema.DefinitionLevel {
			b.KVBegin(r.schema.Children[0])
			err = r.keyReader.read(b)
			if err != nil {
				return err
			}
			err = r.valueReader.read(b)
			if err != nil {
				return err
			}
			b.KVEnd(r.schema.Children[0])
		} else {
			err = r.keyReader.skip()
			if err != nil {
				return err
			}
			err = r.valueReader.skip()
			if err != nil {
				return err
			}
			break
		}

		levels, err = r.peek()
		if err == EOF {
			break
		}
		if err != nil {
			return err
		}
		if levels.repetition <= r.schema.RepetitionLevel {
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
func (r *repeatedReader) bind(rg *rowGroup)     { r.reader.bind(rg) }
func (r *repeatedReader) peek() (levels, error) { return r.reader.peek() }
func (r *repeatedReader) skip() error           { return r.reader.skip() }
func (r *repeatedReader) read(b RowBuilder) error {
	// check for EOF
	_, err := r.peek()
	if err != nil {
		return err
	}

	b.RepeatedBegin(r.schema)

	for {
		levels, err := r.peek()
		if err != nil {
			return err
		}

		if levels.definition > r.schema.DefinitionLevel {
			err = r.reader.read(b)
			if err != nil {
				return err
			}
		} else {
			// this is an empty list
			err = r.reader.skip()
			if err != nil {
				return err
			}
			break
		}

		levels, err = r.peek()
		if err == EOF {
			break
		}
		if err != nil {
			return err
		}

		// next level will be part of a different row
		if levels.repetition <= r.schema.RepetitionLevel {
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
	if list.Kind != repeatedKind {
		panic("LIST's child must be repeated")
	}
	if list.Repetition != pthrift.FieldRepetitionType_REPEATED {
		panic(fmt.Errorf("LIST's child but be repeated, not %s", list.Repetition))
	}
	if len(list.Children) != 1 {
		panic("LIST's child must have exactly one child")
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
