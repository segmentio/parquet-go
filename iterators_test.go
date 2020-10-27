package parquet

import (
	"fmt"
	"io"
	"os"
	"path"
	"testing"

	pthrift "github.com/segmentio/parquet/internal/gen-go/parquet"
	"github.com/segmentio/parquet/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	localref "github.com/xitongsys/parquet-go-source/local"
	writerref "github.com/xitongsys/parquet-go/writer"
)

type testBuilder struct {
	last interface{}
}

func (v *testBuilder) Begin()                   {}
func (v *testBuilder) End()                     {}
func (v *testBuilder) GroupBegin(s *Schema)     {}
func (v *testBuilder) GroupEnd(node *Schema)    {}
func (v *testBuilder) RepeatedBegin(s *Schema)  {}
func (v *testBuilder) RepeatedEnd(node *Schema) {}
func (v *testBuilder) KVBegin(node *Schema)     {}
func (v *testBuilder) KVEnd(node *Schema)       {}
func (v *testBuilder) PrimitiveNil(s *Schema) error {
	v.last = nil
	return nil
}
func (v *testBuilder) Primitive(s *Schema, d Decoder) error {
	var err error
	switch s.PhysicalType {
	case pthrift.Type_BYTE_ARRAY:
		v.last, err = d.ByteArray(nil)
	case pthrift.Type_INT32:
		v.last, err = d.Int32()
	case pthrift.Type_INT64:
		v.last, err = d.Int64()
	default:
		panic(fmt.Errorf("unsupported type: %s", s.PhysicalType.String()))
	}
	return err
}

func TestPageIterator(t *testing.T) {
	type Record struct {
		A int32 `parquet:"name=a, type=INT32"`
	}

	test.WithTestDir(t, func(dir string) {
		p := path.Join(dir, "simple.parquet")
		dst, err := localref.NewLocalFileWriter(p)
		require.NoError(t, err)
		writer, err := writerref.NewParquetWriter(dst, new(Record), 1)
		require.NoError(t, err)
		require.NoError(t, writer.Write(Record{A: 1}))
		require.NoError(t, writer.Write(Record{A: 2}))
		require.NoError(t, writer.Write(Record{A: 3}))
		require.NoError(t, writer.WriteStop())
		require.NoError(t, dst.Close())

		f, err := os.Open(p)
		require.NoError(t, err)
		defer func() { assert.NoError(t, f.Close()) }()

		reader, err := OpenFile(f)
		require.NoError(t, err)
		_, err = reader.thrift.Seek(reader.metadata.rowGroups[0].Columns[0].GetMetaData().GetDataPageOffset(), io.SeekStart)
		require.NoError(t, err)

		it := pageReader{
			r:                reader.thrift,
			schema:           reader.metadata.Schema.At("a"),
			compressionCodec: &snappyCodec{},
		}
		require.NotNil(t, it.schema)
		b := &testBuilder{}

		err = it.read(b)
		assert.NoError(t, err)
		assert.Equal(t, int32(1), b.last)
		err = it.read(b)
		assert.NoError(t, err)
		assert.Equal(t, int32(2), b.last)
		err = it.read(b)
		assert.NoError(t, err)
		assert.Equal(t, int32(3), b.last)
	})
}

func TestPageIteratorLevels(t *testing.T) {
	type Record struct {
		A *int32 `parquet:"name=a, type=INT32"`
	}

	test.WithTestDir(t, func(dir string) {
		p := path.Join(dir, "simple.parquet")
		dst, err := localref.NewLocalFileWriter(p)
		require.NoError(t, err)
		writer, err := writerref.NewParquetWriter(dst, new(Record), 1)
		require.NoError(t, err)
		one := int32(1)
		three := int32(3)
		require.NoError(t, writer.Write(Record{A: &one}))
		require.NoError(t, writer.Write(Record{A: nil}))
		require.NoError(t, writer.Write(Record{A: &three}))
		require.NoError(t, writer.WriteStop())
		require.NoError(t, dst.Close())

		f, err := os.Open(p)
		require.NoError(t, err)
		defer f.Close()

		reader, err := OpenFile(f)
		require.NoError(t, err)
		_, err = reader.thrift.Seek(reader.metadata.rowGroups[0].Columns[0].GetMetaData().GetDataPageOffset(), io.SeekStart)
		require.NoError(t, err)

		it := pageReader{
			r:                reader.thrift,
			schema:           reader.metadata.Schema.At("a"),
			compressionCodec: &snappyCodec{},
		}
		require.NotNil(t, it.schema)

		b := &testBuilder{}

		err = it.read(b)
		assert.NoError(t, err)
		assert.Equal(t, int32(1), b.last)
		err = it.read(b)
		assert.NoError(t, err)
		assert.Equal(t, nil, b.last)
		err = it.read(b)
		assert.NoError(t, err)
		assert.Equal(t, int32(3), b.last)
	})
}

func TestRowGroupColumnIterator(t *testing.T) {
	type Record struct {
		A int32 `parquet:"name=a, type=INT32"`
	}

	// generates 4 pages
	recordsCount := 5000

	test.WithTestDir(t, func(dir string) {
		p := path.Join(dir, "simple.parquet")
		dst, err := localref.NewLocalFileWriter(p)
		require.NoError(t, err)
		writer, err := writerref.NewParquetWriter(dst, new(Record), 1)
		require.NoError(t, err)
		for i := 0; i < recordsCount; i++ {
			require.NoError(t, writer.Write(Record{A: int32(i)}))
		}
		require.NoError(t, writer.WriteStop())
		require.NoError(t, dst.Close())

		f, err := os.Open(p)
		require.NoError(t, err)
		defer f.Close()

		reader, err := OpenFile(f)
		require.NoError(t, err)
		md := reader.metadata.rowGroups[0].Columns[0].GetMetaData()
		_, err = reader.thrift.Seek(md.GetDataPageOffset(), io.SeekStart)
		require.NoError(t, err)

		it := rowGroupColumnReader{
			r:  reader.thrift,
			md: md,
			s:  reader.metadata.Schema.At("a"),
		}
		require.NotNil(t, it.s)

		b := &testBuilder{}

		for i := 0; i < recordsCount; i++ {
			t.Log("testing record", i)
			err = it.read(b)
			assert.NoError(t, err)
			assert.Equal(t, int32(i), b.last)
		}
	})
}

//
//func TestColumnIterator(t *testing.T) {
//	type Record struct {
//		A int32 `parquet:"name=a, type=INT32"`
//	}
//
//	// Generates 3 row groups:
//	//
//	// row group 0
//	// --------------------------------------------------------------------------------
//	// a:  INT32 SNAPPY DO:0 FPO:4 SZ:10999/10990/1.00 VC:2730 ENC:RLE,BIT_PACKED,PLAIN [more]...
//	//
//	//     a TV=2730 RL=0 DL=0
//	//     ----------------------------------------------------------------------------
//	//     page 0:  DLE:RLE RLE:RLE VLE:PLAIN ST:[min: 0, max: 2048, num_null [more]... SZ:8196
//	//     page 1:  DLE:RLE RLE:RLE VLE:PLAIN ST:[min: 2049, max: 2729, num_n [more]... SZ:2724
//	//
//	// row group 1
//	// --------------------------------------------------------------------------------
//	// a:  INT32 SNAPPY DO:0 FPO:11003 SZ:6595/6590/1.00 VC:1639 ENC:RLE,BIT_ [more]...
//	//
//	//     a TV=1639 RL=0 DL=0
//	//     ----------------------------------------------------------------------------
//	//     page 0:  DLE:RLE RLE:RLE VLE:PLAIN ST:[min: 2730, max: 4368, num_n [more]... SZ:6556
//	//
//	// row group 2
//	// --------------------------------------------------------------------------------
//	// a:  INT32 SNAPPY DO:0 FPO:17598 SZ:6563/6558/1.00 VC:1631 ENC:RLE,BIT_ [more]...
//	//
//	//     a TV=1631 RL=0 DL=0
//	//     ----------------------------------------------------------------------------
//	//     page 0:  DLE:RLE RLE:RLE VLE:PLAIN ST:[min: 4369, max: 5999, num_n [more]... SZ:6524
//
//	recordsCount := 6000
//
//	test.WithTestDir(t, func(dir string) {
//		p := path.Join(dir, "large.parquet")
//
//		dst, err := localref.NewLocalFileWriter(p)
//		require.NoError(t, err)
//		writer, err := writerref.NewParquetWriter(dst, new(Record), 1)
//		require.NoError(t, err)
//		writer.RowGroupSize = 1024 // bytes
//		for i := 0; i < recordsCount; i++ {
//			require.NoError(t, writer.Write(Record{A: int32(i)}))
//		}
//		require.NoError(t, writer.WriteStop())
//		require.NoError(t, dst.Close())
//
//		f, err := os.Open(p)
//		require.NoError(t, err)
//		defer f.Close()
//
//		root := NewReader(f)
//		err = root.Open()
//		require.NoError(t, err)
//		md := root.metadata.rowGroups[0].Columns[0].GetMetaData()
//		_, err = root.thrift.Seek(md.GetDataPageOffset(), io.SeekStart)
//		require.NoError(t, err)
//
//		it := ColumnIterator{
//			r:        root,
//			path:     []string{"a"},
//			callback: func(decoder Decoder) (interface{}, error) { return decoder.Int32() },
//		}
//
//		for i := 0; i < recordsCount; i++ {
//			notLast := it.next()
//			require.NoError(t, it.Error(), "record %d: error", i)
//			require.Equal(t, i < recordsCount-1, notLast, "record %d: not last", i)
//			assert.Equal(t, Raw{
//				value:  int32(i),
//				levels: levels{0, 0},
//			}, *it.value())
//		}
//		require.NoError(t, it.Error())
//	})
//}
//
//// Tests interleaved reading of two simple columns.
//func TestTwoColumnIterators(t *testing.T) {
//	type Record struct {
//		A int32 `parquet:"name=a, type=INT32"`
//		B int32 `parquet:"name=b, type=INT32"`
//	}
//
//	// Generates 3 row groups. See TestColumnIterator.
//	recordsCount := 6000
//
//	test.WithTestDir(t, func(dir string) {
//		p := path.Join(dir, "large.parquet")
//
//		dst, err := localref.NewLocalFileWriter(p)
//		require.NoError(t, err)
//		writer, err := writerref.NewParquetWriter(dst, new(Record), 1)
//		require.NoError(t, err)
//		writer.RowGroupSize = 1024 // bytes
//		for i := 0; i < recordsCount; i++ {
//			require.NoError(t, writer.Write(Record{A: int32(i), B: 10000 + int32(i)}))
//		}
//		require.NoError(t, writer.WriteStop())
//		require.NoError(t, dst.Close())
//
//		f, err := os.Open(p)
//		require.NoError(t, err)
//		defer f.Close()
//
//		root := NewReader(f)
//		err = root.Open()
//		require.NoError(t, err)
//
//		var repetitionLevel uint32
//		var definitionLevel uint32
//		var value1 int32
//		var value2 int32
//
//		it1 := ColumnIterator{
//			r:    root,
//			path: []string{"a"},
//			callback: func(r, d uint32, decoder Decoder) (err error) {
//				repetitionLevel = r
//				definitionLevel = d
//				value1, err = decoder.Int32()
//				return
//			},
//		}
//		it2 := ColumnIterator{
//			r:    root,
//			path: []string{"b"},
//			callback: func(r, d uint32, decoder Decoder) (err error) {
//				repetitionLevel = r
//				definitionLevel = d
//				value2, err = decoder.Int32()
//				return
//			},
//		}
//
//		for i := 0; i < recordsCount; i++ {
//			notLast := it1.next()
//			require.NoError(t, it1.Error(), "record %d: error", i)
//			require.Equal(t, i < recordsCount-1, notLast, "record %d: not last", i)
//			require.Equal(t, int32(i), value1, "record %d: incorrect value", i)
//
//			notLast = it2.next()
//			require.NoError(t, it2.Error(), "record %d: error", i)
//			require.Equal(t, i < recordsCount-1, notLast, "record %d: not last", i)
//			require.Equal(t, 10000+int32(i), value2, "record %d: incorrect value", i)
//
//			require.Equal(t, uint32(0), repetitionLevel, "record %d: incorrect repetition level", i)
//			require.Equal(t, uint32(0), definitionLevel, "record %d: incorrect definition level", i)
//		}
//		require.NoError(t, it1.Error())
//		require.NoError(t, it2.Error())
//	})
//}
