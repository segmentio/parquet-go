package parquet_test

import (
	"testing"

	"github.com/segmentio/parquet-go"
)

func TestBinaryColumnIndexMinMax(t *testing.T) {
	testCases := [][]interface{}{
		// kind, type, page min, page max, size limit, [value to search, expected result]...
		{parquet.ByteArray, parquet.ByteArrayType,
			[]byte{0, 0, 0, 0, 0, 0}, []byte{1, 2, 3, 4, 5, 6}, 4,
			[]byte{0, 0, 0, 0, 0, 0}, true,
			[]byte{0, 1, 2, 3, 4, 5}, true,
			[]byte{1, 2, 3, 4}, true,
			[]byte{1, 2, 3, 4, 5, 6}, true, // the page max value should be a hit
			[]byte{1, 2, 3, 4, 5, 7}, true, // false positive due to size limit
			[]byte{1, 2, 3, 5}, true, // false positive due to size limit
			[]byte{1, 2, 3, 5, 6, 7}, false, // should be no hit since it definitely exceeds page max
			[]byte{2, 3, 4, 5}, false, // should be no hit since it definitely exceeds page max
		},
		{parquet.FixedLenByteArray, parquet.FixedLenByteArrayType(6),
			[]byte{0, 0, 0, 0, 0, 0}, []byte{1, 2, 3, 4, 5, 6}, 4,
			[]byte{0, 0, 0, 0, 0, 0}, true,
			[]byte{0, 1, 2, 3, 4, 5}, true,
			[]byte{1, 2, 3, 4, 0, 0}, true,
			[]byte{1, 2, 3, 4, 5, 6}, true, // the page max value should be a hit
			[]byte{1, 2, 3, 4, 5, 7}, true, // false positive due to size limit
			[]byte{1, 2, 3, 4, 0xFF, 0xFF}, true, // false positive due to size limit
			[]byte{1, 2, 3, 5, 0, 0}, false, // should be no hit since it definitely exceeds page max
			[]byte{1, 2, 3, 5, 6, 7}, false, // should be no hit since it definitely exceeds page max
			[]byte{2, 3, 4, 5, 0, 0}, false, // should be no hit since it definitely exceeds page max
		},
	}
	for _, testCase := range testCases {
		kind := testCase[0].(parquet.Kind)
		typ := testCase[1].(parquet.Type)
		min := testCase[2].([]byte)
		max := testCase[3].([]byte)
		sizeLimit := testCase[4].(int)
		indexer := typ.NewColumnIndexer(sizeLimit)
		indexer.IndexPage(100, 0,
			parquet.ValueOf(min),
			parquet.ValueOf(max),
		)
		formatIndex := indexer.ColumnIndex()
		columnIndex := parquet.NewColumnIndex(kind, &formatIndex)
		for i := 5; i < len(testCase); i += 2 {
			value := testCase[i].([]byte)
			expected := testCase[i+1].(bool)

			v := parquet.ValueOf(value)
			actual := parquet.Search(columnIndex, v, typ) == 0
			if actual != expected {
				t.Errorf("checkByteArrayMinMax(%v, %v, %v, %v) = %v, want %v", min, max, value, sizeLimit, actual, expected)
			}
		}
	}
}
