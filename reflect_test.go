package parquet_test

import (
	"testing"

	"github.com/segmentio/parquet"

	pthrift "github.com/segmentio/parquet/internal/gen-go/parquet"
	"github.com/stretchr/testify/assert"
)

func TestFromValue(t *testing.T) {
	type Record struct {
		Col     int32
		ignored int32
	}

	expected := &parquet.Schema{
		Name: "root",
		Kind: parquet.GroupKind,
		Root: true,
	}
	expected.Add(&parquet.Schema{
		Name:         "col",
		Kind:         parquet.PrimitiveKind,
		PhysicalType: pthrift.Type_INT32,
		Path:         []string{"col"},
	})

	result := parquet.FromValue(new(Record))

	assert.Equal(t, expected, result)
}
