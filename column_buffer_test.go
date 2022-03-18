package parquet_test

import (
	"bytes"
	"testing"

	"github.com/segmentio/parquet-go"
)

type TestStruct struct {
	A *string `parquet:"a,optional,dict"`
}

func TestOptionalDictWriteRowGroup(t *testing.T) {
	s := parquet.SchemaOf(&TestStruct{})

	str1 := "test1"
	str2 := "test2"
	records := []*TestStruct{
		{A: nil},
		{A: &str1},
		{A: nil},
		{A: &str2},
		{A: nil},
	}

	buf := parquet.NewBuffer(s)
	for _, rec := range records {
		row := s.Deconstruct(nil, rec)
		err := buf.WriteRow(row)
		if err != nil {
			t.Fatal(err)
		}
	}

	b := bytes.NewBuffer(nil)
	w := parquet.NewWriter(b)
	_, err := w.WriteRowGroup(buf)
	if err != nil {
		t.Fatal(err)
	}
}
