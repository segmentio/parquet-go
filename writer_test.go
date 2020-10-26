package parquet_test

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/segmentio/parquet"
	"github.com/segmentio/parquet/internal/test"
	"github.com/stretchr/testify/require"
)

func TestWriter(t *testing.T) {
	type Record struct {
		Value int32
	}

	records := []Record{
		{Value: 1},
		{Value: 2},
		{Value: 3},
	}

	encoder := parquet.NewEncoder(new(Record), parquet.EncoderOptions{})
	planner := parquet.StructPlannerOf(new(Record))
	plan := planner.Plan()

	test.WithTestDir(t, func(dir string) {
		p := path.Join(dir, "test.parquet")
		writer := encoder.To(p)

		for _, r := range records {
			err := writer.Write(r)
			require.NoError(t, err)
		}

		err := writer.Close()
		require.NoError(t, err)

		// validation

		f, err := os.Open(p)
		require.NoError(t, err)
		defer func() { assert.NoError(t, f.Close()) }()

		pf, err := parquet.OpenFile(f)
		require.NoError(t, err)

		builder := planner.Builder()
		rowReader := parquet.NewRowReaderWithPlan(plan, pf)

		var actual []Record

		for {
			record := Record{}
			err := rowReader.Read(builder.To(&record))
			if err == parquet.EOF {
				break
			}
			require.NoError(t, err)
			actual = append(actual, record)
		}

		require.Equal(t, records, actual)
	})
}
