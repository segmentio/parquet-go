package parquet

// test copied from parquet-go
//func TestDecodeRLE(t *testing.T) {
//	testData := [][]interface{}{
//		{int64(1), int64(2), int64(3), int64(4)},
//		{int64(0), int64(0), int64(0), int64(0), int64(0)},
//		{int64(0)},
//		{int64(1)},
//	}
//	for _, data := range testData {
//		maxVal := uint64(data[len(data)-1].(int64))
//		bitwidth := bits.Len64(maxVal)
//
//		b := encodingref.WriteRLEBitPackedHybrid(data, int32(bitwidth), parquetref.Type_INT64)
//
//		fmt.Print("data:", data)
//		fmt.Println("OUTPUT BYTES:", b)
//
//		values := make([]uint32, len(data))
//		err := decodeRLE(bitwidth, values, bytes.NewReader(b))
//		assert.NoError(t, err)
//		assert.Equal(t, len(data), len(values))
//		for i, expected := range data {
//			assert.Equal(t, expected, int64(values[i]))
//		}
//	}
//}
