package parquet_test

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/segmentio/parquet"
	"github.com/segmentio/parquet/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	localref "github.com/xitongsys/parquet-go-source/local"
	writerref "github.com/xitongsys/parquet-go/writer"
)

func TestStructBuilderSimple(t *testing.T) {
	// We have to declare two different structs because the annotations are
	// different between this library and parquet-go. This should all go
	// away when we have our own writer.
	type RecordParquetGo struct {
		Name string `parquet:"name=name, type=UTF8"`
		Foo  int32  `parquet:"name=bar, type=INT_32"`
	}
	type Record struct {
		Name string
		Foo  int32 `parquet:"bar"`
	}

	test.WithTestDir(t, func(dir string) {
		p := path.Join(dir, "test.parquet")
		dst, err := localref.NewLocalFileWriter(p)
		require.NoError(t, err)
		writer, err := writerref.NewParquetWriter(dst, new(RecordParquetGo), 1)
		require.NoError(t, err)
		require.NoError(t, writer.Write(Record{Name: "name1", Foo: 1}))
		require.NoError(t, writer.Write(Record{Name: "name2", Foo: 2}))
		require.NoError(t, writer.Write(Record{Name: "name3", Foo: 3}))
		require.NoError(t, writer.WriteStop())
		require.NoError(t, dst.Close())

		f, err := os.Open(p)
		require.NoError(t, err)
		defer func() { assert.NoError(t, f.Close()) }()

		pf, err := parquet.OpenFile(f)
		require.NoError(t, err)

		// TODO: this is a lot of ceremony
		record := &Record{}
		planner := parquet.StructPlannerOf(record)
		builder := planner.Builder()
		plan := planner.Plan()
		rowReader := parquet.NewRowReader(plan, pf)

		for {
			err := rowReader.Read(builder)
			if err == parquet.EOF {
				break
			}
			require.NoError(t, err)
			fmt.Printf("record: %#v\n", record)
		}
	})
}
