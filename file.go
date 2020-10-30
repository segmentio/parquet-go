package parquet

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	pthrift "github.com/segmentio/parquet/internal/gen-go/parquet"
	"github.com/segmentio/parquet/internal/thrift"
)

const magicBoundary uint32 = 0x31524150 // PAR1 in little endian
const magicLength = 4
const footerLengthLength = 4

// File represents a Parquet File ready for reading.
// Use NewFile to create a File.
// Use OpenFile to generate File based on existing file.
// Files are stateful. They should not be shared or concurrently accessed.
type File struct {
	// small buffer used to read things like magic numbers and footer length
	uint32buf [4]byte
	uint64buf [8]byte

	thrift      *thrift.Reader
	tWriter     *thrift.Writer
	metadata    *FileMetadata
	rawMetaData *pthrift.FileMetaData
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

// Metadata returns a pointer to the File's metadata.
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

// Writer stuff

func (f *File) writeUint32(value uint32) error {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, value)
	n, err := f.tWriter.Write(buf)
	if err != nil {
		return err
	}
	if n != 4 {
		return fmt.Errorf("not enough bytes: %d (required %d)", n, 4)
	}
	return nil
}

func (f *File) writeMagicBoundary() error {
	return f.writeUint32(magicBoundary)
}

// type FileMetadata struct {
// 	Version      int32
// 	NumRows      int64
// 	CreatedBy    string
// 	Schema       *Schema
// 	rowGroups    []*pthrift.RowGroup
// 	columnOrders []*pthrift.ColumnOrder
// }

// NewFile initializes a new parquet file for writing
func NewFile(path string, schema *Schema) (File, error) {
	fd, err := os.Create(path)
	if err != nil {
		return File{}, err
	}
	defer fd.Close()

	file := File{
		tWriter: thrift.NewWriter(fd),
		thrift:  thrift.NewReader(fd),
		metadata: &FileMetadata{
			Schema: schema,
		},
	}

	file.tWriter.Open()
	err = file.writeMagicBoundary()
	if err != nil {
		return File{}, err
	}

	rawMD := pthrift.NewFileMetaData()
	rawMD.Schema, err = schemaTreeToFlatElements(schema)
	if err != nil {
		return File{}, err
	}
	file.rawMetaData = rawMD

	err = file.tWriter.Marshal(rawMD)
	if err != nil {
		return File{}, err
	}
	// f.metadata = &FileMetadata{
	// 	Version:   rawMetadata.GetVersion(),
	// 	NumRows:   rawMetadata.GetNumRows(),
	// 	CreatedBy: rawMetadata.GetCreatedBy(),
	// }
	// f.metadata.rowGroups = rawMetadata.GetRowGroups()
	// f.metadata.columnOrders = rawMetadata.GetColumnOrders()
	// // TODO: do this lazily
	// f.metadata.Schema, err = schemaFromFlatElements(rawMetadata.GetSchema())
	// if err != nil {
	// 	return nil, err
	// }

	_, err = file.tWriter.Seek(-magicLength, io.SeekEnd)
	file.writeMagicBoundary()
	if err != nil {
		return File{}, err
	}

	return file, nil
}
