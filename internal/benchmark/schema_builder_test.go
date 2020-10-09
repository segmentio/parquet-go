package benchmark_test

import (
	"log"
	"os"
	"testing"

	"github.com/segmentio/parquet"
	"github.com/segmentio/parquet/internal/benchmark"
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

func BenchmarkReflectReadRow(b *testing.B) {
	p := "../../examples/stage-small.parquet"
	planner := parquet.StructPlannerOf(new(benchmark.Trace))
	builder := planner.Builder()
	plan := planner.Plan()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f, err := os.Open(p)
		require.NoError(b, err)

		pf, err := parquet.OpenFile(f)
		require.NoError(b, err)
		rowReader := parquet.NewRowReaderWithPlan(plan, pf)

		for {
			target := &benchmark.Trace{}
			err := rowReader.Read(builder.To(target))
			if err == parquet.EOF {
				break
			}
			require.NoError(b, err)
		}
		require.NoError(b, f.Close())
	}
}

func BenchmarkReflectReadRowParquetGo(b *testing.B) {
	p := "../../examples/stage-small.parquet"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fr, err := local.NewLocalFileReader(p)
		if err != nil {
			log.Println("Can't open file", err)
			return
		}
		pr, err := reader.NewParquetReader(fr, new(benchmark.Trace), 1)
		require.NoError(b, err)
		num := int(pr.GetNumRows())
		target := make([]benchmark.Trace, 1, 1)
		for i := 0; i < num; i++ {
			err := pr.Read(&target)
			require.NoError(b, err)
		}
		pr.ReadStop()
		require.NoError(b, err, fr.Close())
	}
}
