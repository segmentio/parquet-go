package parquet_test

import (
	"os"
	"path"
	"reflect"
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
		Foo  int32  `parquet:"name=foo, type=INT_32"`
	}
	type Record struct {
		Name string
		Foo  int32
	}

	expected := []interface{}{
		&Record{Name: "name1", Foo: 1},
		&Record{Name: "name2", Foo: 2},
		&Record{Name: "name3", Foo: 3},
	}

	structBuilderTest(t, new(RecordParquetGo), new(Record), expected)
}

func structBuilderTest(t *testing.T, recordPgo, record interface{}, expected []interface{}) {
	test.WithTestDir(t, func(dir string) {
		p := path.Join(dir, "test.parquet")
		dst, err := localref.NewLocalFileWriter(p)
		require.NoError(t, err)
		writer, err := writerref.NewParquetWriter(dst, recordPgo, 1)
		require.NoError(t, err)
		for _, record := range expected {
			require.NoError(t, writer.Write(record))
		}
		require.NoError(t, writer.WriteStop())
		require.NoError(t, dst.Close())

		f, err := os.Open(p)
		require.NoError(t, err)
		defer func() { assert.NoError(t, f.Close()) }()

		pf, err := parquet.OpenFile(f)
		require.NoError(t, err)

		// TODO: this is a lot of ceremony
		planner := parquet.StructPlannerOf(record)
		builder := planner.Builder()
		plan := planner.Plan()
		rowReader := parquet.NewRowReader(plan, pf)

		var actual []interface{}

		for {
			target := reflect.New(reflect.TypeOf(record).Elem()).Interface()
			err := rowReader.Read(builder.To(target))
			if err == parquet.EOF {
				break
			}
			require.NoError(t, err)
			actual = append(actual, target)
		}

		require.Equal(t, expected, actual)
	})
}
