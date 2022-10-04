//go:build go1.18

package parquet_test

import (
	"bytes"
	"fmt"
	"log"
	"reflect"
	"testing"

	"github.com/segmentio/parquet-go"
)

func ExampleReadFile() {
	type Row struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name,zstd"`
	}

	ExampleWriteFile()

	rows, err := parquet.ReadFile[Row]("/tmp/file.parquet")
	if err != nil {
		log.Fatal(err)
	}

	for _, row := range rows {
		fmt.Printf("%d: %q\n", row.ID, row.Name)
	}

	// Output:
	// 0: "Bob"
	// 1: "Alice"
	// 2: "Franky"
}

func ExampleWriteFile() {
	type Row struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name,zstd"`
	}

	if err := parquet.WriteFile("/tmp/file.parquet", []Row{
		{ID: 0, Name: "Bob"},
		{ID: 1, Name: "Alice"},
		{ID: 2, Name: "Franky"},
	}); err != nil {
		log.Fatal(err)
	}

	// Output:
}

func ExampleRead_any() {
	type Row struct{ FirstName, LastName string }

	buf := new(bytes.Buffer)
	err := parquet.Write(buf, []Row{
		{FirstName: "Luke", LastName: "Skywalker"},
		{FirstName: "Han", LastName: "Solo"},
		{FirstName: "R2", LastName: "D2"},
	})
	if err != nil {
		log.Fatal(err)
	}

	file := bytes.NewReader(buf.Bytes())

	rows, err := parquet.Read[any](file, file.Size())
	if err != nil {
		log.Fatal(err)
	}

	for _, row := range rows {
		fmt.Printf("%q\n", row)
	}

	// Output:
	// map["FirstName":"Luke" "LastName":"Skywalker"]
	// map["FirstName":"Han" "LastName":"Solo"]
	// map["FirstName":"R2" "LastName":"D2"]
}

func ExampleWrite_any() {
	schema := parquet.SchemaOf(struct {
		FirstName string
		LastName  string
	}{})

	buf := new(bytes.Buffer)
	err := parquet.Write[any](
		buf,
		[]any{
			map[string]string{"FirstName": "Luke", "LastName": "Skywalker"},
			map[string]string{"FirstName": "Han", "LastName": "Solo"},
			map[string]string{"FirstName": "R2", "LastName": "D2"},
		},
		schema,
	)
	if err != nil {
		log.Fatal(err)
	}

	file := bytes.NewReader(buf.Bytes())

	rows, err := parquet.Read[any](file, file.Size())
	if err != nil {
		log.Fatal(err)
	}

	for _, row := range rows {
		fmt.Printf("%q\n", row)
	}

	// Output:
	// map["FirstName":"Luke" "LastName":"Skywalker"]
	// map["FirstName":"Han" "LastName":"Solo"]
	// map["FirstName":"R2" "LastName":"D2"]
}

func TestIssue360(t *testing.T) {
	type TestType struct {
		Key []int
	}

	schema := parquet.SchemaOf(TestType{})
	buffer := parquet.NewGenericBuffer[any](schema)

	data := make([]any, 1)
	data[0] = TestType{Key: []int{1}}
	_, err := buffer.Write(data)
	if err != nil {
		fmt.Println("Exiting with error: ", err)
		return
	}

	var out bytes.Buffer
	writer := parquet.NewGenericWriter[any](&out, schema)

	_, err = parquet.CopyRows(writer, buffer.Rows())
	if err != nil {
		fmt.Println("Exiting with error: ", err)
		return
	}
	writer.Close()

	br := bytes.NewReader(out.Bytes())
	rows, _ := parquet.Read[any](br, br.Size())

	expect := []any{
		map[string]any{
			"Key": []any{
				int64(1),
			},
		},
	}

	if !reflect.DeepEqual(rows, expect) {
		t.Error("rows mismatch")

		for _, row := range rows {
			t.Logf("%#v", row)
		}
	}
}
