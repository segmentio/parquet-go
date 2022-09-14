package parquet_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"testing"

	"github.com/segmentio/parquet-go"
)

const (
	numRowGroups = 3
	rowsPerGroup = benchmarkNumRows
)

type wrappedRowGroup struct {
	parquet.RowGroup
	rowsCallback func(parquet.Rows) parquet.Rows
}

func (r wrappedRowGroup) Rows() parquet.Rows {
	return r.rowsCallback(r.RowGroup.Rows())
}

type wrappedRows struct {
	parquet.Rows
	closed bool
}

func (r *wrappedRows) Close() error {
	r.closed = true
	return r.Rows.Close()
}

func TestMergeRowGroupsCursorsAreClosed(t *testing.T) {

	type model struct {
		A int
	}

	schema := parquet.SchemaOf(model{})
	options := []parquet.RowGroupOption{
		parquet.SortingColumns(
			parquet.Ascending(schema.Columns()[0]...),
		),
	}

	prng := rand.New(rand.NewSource(0))
	rowGroups := make([]parquet.RowGroup, numRowGroups)
	rows := make([]*wrappedRows, 0, numRowGroups)

	for i := range rowGroups {
		rowGroups[i] = wrappedRowGroup{
			RowGroup: sortedRowGroup(options, randomRowsOf(prng, rowsPerGroup, model{})...),
			rowsCallback: func(r parquet.Rows) parquet.Rows {
				wrapped := &wrappedRows{Rows: r}
				rows = append(rows, wrapped)
				return wrapped
			},
		}
	}

	m, err := parquet.MergeRowGroups(rowGroups, options...)
	if err != nil {
		t.Fatal(err)
	}
	func() {
		mergedRows := m.Rows()
		defer mergedRows.Close()

		// Add 1 more slot to the buffer to force an io.EOF on the first read.
		rbuf := make([]parquet.Row, (numRowGroups*rowsPerGroup)+1)
		if _, err := mergedRows.ReadRows(rbuf); !errors.Is(err, io.EOF) {
			t.Fatal(err)
		}
	}()

	for i, wrapped := range rows {
		if !wrapped.closed {
			t.Fatalf("RowGroup %d not closed", i)
		}
	}
}

func BenchmarkMergeRowGroups(b *testing.B) {
	for _, test := range readerTests {
		b.Run(test.scenario, func(b *testing.B) {
			schema := parquet.SchemaOf(test.model)

			options := []parquet.RowGroupOption{
				parquet.SortingColumns(
					parquet.Ascending(schema.Columns()[0]...),
				),
			}

			prng := rand.New(rand.NewSource(0))
			rowGroups := make([]parquet.RowGroup, numRowGroups)

			for i := range rowGroups {
				rowGroups[i] = sortedRowGroup(options, randomRowsOf(prng, rowsPerGroup, test.model)...)
			}

			for n := 1; n <= numRowGroups; n++ {
				b.Run(fmt.Sprintf("groups=%d,rows=%d", n, n*rowsPerGroup), func(b *testing.B) {
					mergedRowGroup, err := parquet.MergeRowGroups(rowGroups[:n])
					if err != nil {
						b.Fatal(err)
					}

					rows := mergedRowGroup.Rows()
					rbuf := make([]parquet.Row, benchmarkRowsPerStep)
					defer func() { rows.Close() }()

					benchmarkRowsPerSecond(b, func() int {
						n, err := rows.ReadRows(rbuf)
						if err != nil {
							if !errors.Is(err, io.EOF) {
								b.Fatal(err)
							}
							rows.Close()
							rows = mergedRowGroup.Rows()
						}
						return n
					})
				})
			}
		})
	}
}

func BenchmarkMergeFiles(b *testing.B) {
	rowGroupBuffers := make([]bytes.Buffer, numRowGroups)

	for _, test := range readerTests {
		b.Run(test.scenario, func(b *testing.B) {
			schema := parquet.SchemaOf(test.model)

			buffer := parquet.NewBuffer(
				schema,
				parquet.SortingColumns(
					parquet.Ascending(schema.Columns()[0]...),
				),
			)

			prng := rand.New(rand.NewSource(0))
			files := make([]*parquet.File, numRowGroups)
			rowGroups := make([]parquet.RowGroup, numRowGroups)

			for i := range files {
				for _, row := range randomRowsOf(prng, rowsPerGroup, test.model) {
					buffer.Write(row)
				}
				sort.Sort(buffer)
				rowGroupBuffers[i].Reset()
				writer := parquet.NewWriter(&rowGroupBuffers[i])
				_, err := copyRowsAndClose(writer, buffer.Rows())
				if err != nil {
					b.Fatal(err)
				}
				if err := writer.Close(); err != nil {
					b.Fatal(err)
				}
				r := bytes.NewReader(rowGroupBuffers[i].Bytes())
				f, err := parquet.OpenFile(r, r.Size())
				if err != nil {
					b.Fatal(err)
				}
				files[i], rowGroups[i] = f, f.RowGroups()[0]
			}

			for n := 1; n <= numRowGroups; n++ {
				b.Run(fmt.Sprintf("groups=%d,rows=%d", n, n*rowsPerGroup), func(b *testing.B) {
					mergedRowGroup, err := parquet.MergeRowGroups(rowGroups[:n])
					if err != nil {
						b.Fatal(err)
					}

					rows := mergedRowGroup.Rows()
					rbuf := make([]parquet.Row, benchmarkRowsPerStep)
					defer func() { rows.Close() }()

					benchmarkRowsPerSecond(b, func() int {
						n, err := rows.ReadRows(rbuf)
						if err != nil {
							if !errors.Is(err, io.EOF) {
								b.Fatal(err)
							}
							rows.Close()
							rows = mergedRowGroup.Rows()
						}
						return n
					})

					totalSize := int64(0)
					for _, f := range files[:n] {
						totalSize += f.Size()
					}
				})
			}
		})
	}
}
