package encoding_test

import (
	"testing"
	"unsafe"

	"github.com/yonesko/parquet-go/encoding"
)

func TestValuesSize(t *testing.T) {
	t.Log(unsafe.Sizeof(encoding.Values{}))
}
