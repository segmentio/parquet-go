//go:build go1.18

package parquet_test

import (
	"bytes"
	"math/rand"
	"sort"
	"testing"

	"github.com/segmentio/parquet-go"
)

func TestSortingWriter(t *testing.T) {
	type Row struct {
		Value int32 `parquet:"value"`
	}

	rows := make([]Row, 1000)
	for i := range rows {
		rows[i].Value = int32(i)
	}

	prng := rand.New(rand.NewSource(0))
	prng.Shuffle(len(rows), func(i, j int) {
		rows[i], rows[j] = rows[j], rows[i]
	})

	buffer := bytes.NewBuffer(nil)
	writer := parquet.NewSortingWriter[Row](buffer, 99,
		parquet.SortingColumns(
			parquet.Ascending("value"),
		),
	)

	_, err := writer.Write(rows)
	if err != nil {
		t.Fatal(err)
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	read, err := parquet.Read[Row](bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
	if err != nil {
		t.Fatal(err)
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Value < rows[j].Value
	})

	assertRowsEqual(t, rows, read)
}
