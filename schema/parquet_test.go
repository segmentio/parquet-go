package schema_test

import (
	"reflect"
	"testing"

	"github.com/segmentio/encoding/thrift"
	"github.com/segmentio/parquet/schema"
)

func TestMarshalUnmarshalSchemaMetadata(t *testing.T) {
	protocol := &thrift.CompactProtocol{}
	metadata := &schema.FileMetaData{
		Version: 1,
		Schema: []schema.SchemaElement{
			{
				Name: "hello",
			},
		},
		RowGroups: []schema.RowGroup{},
	}

	b, err := thrift.Marshal(protocol, metadata)
	if err != nil {
		t.Fatal(err)
	}

	decoded := &schema.FileMetaData{}
	if err := thrift.Unmarshal(protocol, b, &decoded); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(metadata, decoded) {
		t.Error("values mismatch:")
		t.Logf("expected:\n%#v", metadata)
		t.Logf("found:\n%#v", decoded)
	}
}
