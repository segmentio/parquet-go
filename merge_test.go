package parquet_test

import (
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/segmentio/parquet-go"
)

func BenchmarkMergeRowGroups(b *testing.B) {
	const numRowGrups = 3
	const rowsPerGroup = 10e3

	rowType := addressBook{}
	rowGroupOptions := []parquet.RowGroupOption{
		parquet.SortingColumns(
			parquet.Ascending("Owner"),
		),
	}
	rowGroups := []parquet.RowGroup{
		sortedRowGroup(rowGroupOptions, rowsOf(rowsPerGroup, rowType)...),
		sortedRowGroup(rowGroupOptions, rowsOf(rowsPerGroup, rowType)...),
		sortedRowGroup(rowGroupOptions, rowsOf(rowsPerGroup, rowType)...),
	}

	for n := 1; n <= len(rowGroups); n++ {
		b.Run(fmt.Sprintf("groups=%d,rows=%d", n, n*rowsPerGroup), func(b *testing.B) {
			mergedRowGroup, err := parquet.MergeRowGroups(rowGroups[:n])
			if err != nil {
				b.Fatal(err)
			}
			start := time.Now()

			rows := mergedRowGroup.Rows()
			rbuf := make(parquet.Row, 0, 16)

			for i := 0; i < b.N; i++ {
				rbuf, err = rows.ReadRow(rbuf[:0])
				if err != nil {
					if !errors.Is(err, io.EOF) {
						b.Fatal(err)
					}
					rows = mergedRowGroup.Rows()
				}
			}

			seconds := time.Since(start).Seconds()
			b.ReportMetric(float64(b.N)/seconds, "row/s")
		})
	}
}
