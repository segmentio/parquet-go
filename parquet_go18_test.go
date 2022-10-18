//go:build go1.18

package parquet_test

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
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

func ExampleSearch() {
	type Row struct{ FirstName, LastName string }

	buf := new(bytes.Buffer)
	// The column being searched should be sorted to avoid a full scan of the
	// column. See the section of the readme on sorting for how to sort on
	// insertion into the parquet file using parquet.SortingColumns
	rows := []Row{
		{FirstName: "C", LastName: "3PO"},
		{FirstName: "Han", LastName: "Solo"},
		{FirstName: "Leia", LastName: "Organa"},
		{FirstName: "Luke", LastName: "Skywalker"},
		{FirstName: "R2", LastName: "D2"},
	}
	// The tiny page buffer size ensures we get multiple pages out of the example above.
	w := parquet.NewGenericWriter[Row](buf, parquet.PageBufferSize(20), parquet.WriteBufferSize(0))
	// Need to write 1 row at a time here as writing many at once disregards PageBufferSize option.
	for _, row := range rows {
		_, err := w.Write([]Row{row})
		if err != nil {
			log.Fatal(err)
		}
	}
	err := w.Close()
	if err != nil {
		log.Fatal(err)
	}

	reader := bytes.NewReader(buf.Bytes())
	file, err := parquet.OpenFile(reader, reader.Size())
	if err != nil {
		log.Fatal(err)
	}

	// Search is scoped to a single RowGroup/ColumnChunk
	rowGroup := file.RowGroups()[0]
	firstNameColChunk := rowGroup.ColumnChunks()[0]

	found := parquet.Search(firstNameColChunk.ColumnIndex(), parquet.ValueOf("Luke"), parquet.ByteArrayType)
	offsetIndex := firstNameColChunk.OffsetIndex()
	fmt.Printf("numPages: %d\n", offsetIndex.NumPages())
	fmt.Printf("result found in page: %d\n", found)
	if found < offsetIndex.NumPages() {
		r := parquet.NewGenericReader[Row](file)
		defer r.Close()
		// Seek to the first row in the page the result was found
		r.SeekToRow(offsetIndex.FirstRowIndex(found))
		result := make([]Row, 2)
		_, _ = r.Read(result)
		// Leia is in index 0 for the page.
		for _, row := range result {
			if row.FirstName == "Luke" {
				fmt.Printf("%q\n", row)
			}
		}
	}

	// Output:
	// numPages: 3
	// result found in page: 1
	// {"Luke" "Skywalker"}
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

	assertRowsEqual(t, expect, rows)
}

func TestIssue362ParquetReadFromGenericReaders(t *testing.T) {
	path := "testdata/dms_test_table_LOAD00000001.parquet"
	fp, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer fp.Close()

	r1 := parquet.NewGenericReader[any](fp)
	rows1 := make([]any, r1.NumRows())
	_, err = r1.Read(rows1)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}

	r2 := parquet.NewGenericReader[any](fp)
	rows2 := make([]any, r2.NumRows())
	_, err = r2.Read(rows2)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
}

func TestIssue362ParquetReadFile(t *testing.T) {
	rows1, err := parquet.ReadFile[any]("testdata/dms_test_table_LOAD00000001.parquet")
	if err != nil {
		t.Fatal(err)
	}

	rows2, err := parquet.ReadFile[any]("testdata/dms_test_table_LOAD00000001.parquet")
	if err != nil {
		t.Fatal(err)
	}

	assertRowsEqual(t, rows1, rows2)
}

func TestIssue368(t *testing.T) {
	f, err := os.Open("testdata/issue368.parquet")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}

	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		t.Fatal(err)
	}

	reader := parquet.NewGenericReader[any](pf)
	defer reader.Close()

	trs := make([]any, 1)
	for {
		_, err := reader.Read(trs)
		if err != nil {
			break
		}
	}
}

func TestIssue377(t *testing.T) {
	type People struct {
		Name string
		Age  int
	}

	type Nested struct {
		P  []People
		F  string
		GF string
	}
	row1 := Nested{P: []People{
		{
			Name: "Bob",
			Age:  10,
		}}}
	ods := []Nested{
		row1,
	}
	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[Nested](buf)
	_, err := w.Write(ods)
	if err != nil {
		t.Fatal("write error: ", err)
	}
	w.Close()

	file := bytes.NewReader(buf.Bytes())
	rows, err := parquet.Read[Nested](file, file.Size())
	if err != nil {
		t.Fatal("read error: ", err)
	}

	assertRowsEqual(t, rows, ods)
}

func assertRowsEqual[T any](t *testing.T, rows1, rows2 []T) {
	if !reflect.DeepEqual(rows1, rows2) {
		t.Error("rows mismatch")

		t.Log("want:")
		logRows(t, rows1)

		t.Log("got:")
		logRows(t, rows2)
	}
}

func logRows[T any](t *testing.T, rows []T) {
	for _, row := range rows {
		t.Logf(". %#v\n", row)
	}
}
