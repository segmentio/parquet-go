package plain_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/segmentio/parquet/encoding/plain"
)

func TestSplitByteArrayList(t *testing.T) {
	tests := []struct {
		values [][]byte
	}{
		{values: [][]byte{}},
		{values: [][]byte{{}, {}, {}}},
		{values: [][]byte{[]byte("Hello"), []byte("World"), []byte("!!!")}},
		{values: [][]byte{bytes.Repeat([]byte("1234567890"), 10)}},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			buffer := plain.JoinByteArrayList(test.values)
			values, err := plain.SplitByteArrayList(buffer)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(values, test.values) {
				t.Errorf("values mismatch:\nwant = %q\ngot  = %q", test.values, values)
			}
		})
	}
}
