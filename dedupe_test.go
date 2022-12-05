//go:build go1.18

package parquet_test

import (
	"sort"
	"testing"

	"github.com/segmentio/parquet-go"
)

func TestDedupeRowReader(t *testing.T) {
	type Row struct {
		Value int32 `parquet:"value"`
	}

	rows := make([]Row, 1000)
	for i := range rows {
		rows[i].Value = int32(i / 3)
	}

	dedupeMap := make(map[Row]struct{}, len(rows))
	for _, row := range rows {
		dedupeMap[row] = struct{}{}
	}

	dedupeRows := make([]Row, 0, len(dedupeMap))
	for row := range dedupeMap {
		dedupeRows = append(dedupeRows, row)
	}

	sort.Slice(dedupeRows, func(i, j int) bool {
		return dedupeRows[i].Value < dedupeRows[j].Value
	})

	buffer1 := parquet.NewRowBuffer[Row]()
	buffer1.Write(rows)

	buffer1Rows := buffer1.Rows()
	defer buffer1Rows.Close()

	buffer2 := parquet.NewRowBuffer[Row]()

	_, err := parquet.CopyRows(buffer2,
		parquet.DedupeRowReader(buffer1Rows,
			buffer1.Schema().Comparator(parquet.Ascending("value")),
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	reader := parquet.NewGenericRowGroupReader[Row](buffer2)
	defer reader.Close()

	n, _ := reader.Read(rows)
	assertRowsEqual(t, dedupeRows, rows[:n])
}

func TestDedupeRowWriter(t *testing.T) {
	type Row struct {
		Value int32 `parquet:"value"`
	}

	rows := make([]Row, 1000)
	for i := range rows {
		rows[i].Value = int32(i / 3)
	}

	dedupeMap := make(map[Row]struct{}, len(rows))
	for _, row := range rows {
		dedupeMap[row] = struct{}{}
	}

	dedupeRows := make([]Row, 0, len(dedupeMap))
	for row := range dedupeMap {
		dedupeRows = append(dedupeRows, row)
	}

	sort.Slice(dedupeRows, func(i, j int) bool {
		return dedupeRows[i].Value < dedupeRows[j].Value
	})

	buffer1 := parquet.NewRowBuffer[Row]()
	buffer1.Write(rows)

	buffer1Rows := buffer1.Rows()
	defer buffer1Rows.Close()

	buffer2 := parquet.NewRowBuffer[Row]()

	_, err := parquet.CopyRows(
		parquet.DedupeRowWriter(buffer2,
			buffer1.Schema().Comparator(parquet.Ascending("value")),
		),
		buffer1Rows,
	)
	if err != nil {
		t.Fatal(err)
	}

	reader := parquet.NewGenericRowGroupReader[Row](buffer2)
	defer reader.Close()

	n, _ := reader.Read(rows)
	assertRowsEqual(t, dedupeRows, rows[:n])
}
