package format_test

import (
	"reflect"
	"testing"

	"github.com/1712n/parquet-go/format"
	"github.com/segmentio/encoding/thrift"
)

func TestMarshalUnmarshalSchemaMetadata(t *testing.T) {
	protocol := &thrift.CompactProtocol{}
	metadata := &format.FileMetaData{
		Version: 1,
		Schema: []format.SchemaElement{
			{
				Name: "hello",
			},
		},
		RowGroups: []format.RowGroup{},
	}

	b, err := thrift.Marshal(protocol, metadata)
	if err != nil {
		t.Fatal(err)
	}

	decoded := &format.FileMetaData{}
	if err := thrift.Unmarshal(protocol, b, &decoded); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(metadata, decoded) {
		t.Error("values mismatch:")
		t.Logf("expected:\n%#v", metadata)
		t.Logf("found:\n%#v", decoded)
	}
}
