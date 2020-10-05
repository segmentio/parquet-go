package benchmark_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/segmentio/parquet"
	pthrift "github.com/segmentio/parquet/internal/gen-go/parquet"
	parquetgolocal "github.com/xitongsys/parquet-go-source/local"
	parquetgoreader "github.com/xitongsys/parquet-go/reader"
	parquetgosource "github.com/xitongsys/parquet-go/source"
)

type discardBuilder struct {
	scratch []byte
}

func (v *discardBuilder) Begin()                           {}
func (v *discardBuilder) End()                             {}
func (v *discardBuilder) GroupBegin(s *parquet.Schema)     {}
func (v *discardBuilder) GroupEnd(node *parquet.Schema)    {}
func (v *discardBuilder) RepeatedBegin(s *parquet.Schema)  {}
func (v *discardBuilder) RepeatedEnd(node *parquet.Schema) {}
func (v *discardBuilder) KVBegin(node *parquet.Schema)     {}
func (v *discardBuilder) KVEnd(node *parquet.Schema)       {}
func (v *discardBuilder) PrimitiveNil(s *parquet.Schema) error {
	return nil // no-op
}
func (v *discardBuilder) Primitive(s *parquet.Schema, d parquet.Decoder) error {
	var err error
	switch s.PhysicalType {
	case pthrift.Type_BYTE_ARRAY:
		// TODO: remove alloc
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

// stage-small:
// Rows: 1297
// Size: 606530 bytes
func BenchmarkStageSmall(b *testing.B) {
	withParquetFile(b, "../../examples/stage-small.parquet", func(f *parquet.File) {
		builder := &discardBuilder{}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			rowReader := parquet.NewRowReader(parquet.DefaultPlan, f)

			for {
				err := rowReader.Read(builder)
				if err == parquet.EOF {
					break
				}
				if err != nil {
					b.Fatalf("unexpected error: %s", err)
				}
			}
		}
	})
}

func BenchmarkStageSmallParquetGo(b *testing.B) {
	withParquetGoFile(b, "../../examples/stage-small.parquet", func(f parquetgosource.ParquetFile) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			pr, err := parquetgoreader.NewParquetReader(f, nil, 1)
			if err != nil {
				b.Fatalf("can't create parquet reader: %s", err)
			}
			for i := 0; i < int(pr.GetNumRows()); i++ {
				_, err = pr.ReadByNumber(i)
				if err != nil {
					b.Fatalf("parqet go error: %s", err)
				}
			}
		}
	})
}

func withParquetFile(b *testing.B, path string, fn func(*parquet.File)) {
	file, err := os.Open(path)
	if err != nil {
		b.Fatalf("could not open file '%s': %s", path, err)
	}
	defer func() {
		err := file.Close()
		if err != nil {
			b.Fatalf("could not close file: %s", err)
		}
	}()

	f, err := parquet.OpenFile(file)
	if err != nil {
		b.Fatalf("Could parse parquet file '%s': %s", path, err)
	}

	fn(f)
}

func withParquetGoFile(b *testing.B, path string, fn func(parquetgosource.ParquetFile)) {
	fr, err := parquetgolocal.NewLocalFileReader(path)
	if err != nil {
		b.Fatalf("cannot open file %s: %s", path, err)
	}
	defer fr.Close()

	fn(fr)
}
