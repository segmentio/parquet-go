package parquet_test

//func TestStructBuilderSimple(t *testing.T) {
//	type MyRecord struct {
//		Name string
//		Foo  int32 `parquet:"bar"`
//	}
//
//	f, err := parquet.OpenFile()
//	require.NoError(t, err)
//
//	record := &MyRecord{}
//	builder := parquet.NewStructBuilder(record)
//	rowReader := parquet.NewRowReader(f)
//
//	for {
//		err := rowReader.Read(builder.To(record))
//		if err == parquet.EOF {
//			break
//		}
//		require.NoError(t, err)
//		fmt.Printf("record: %#v\n", record)
//	}
//}
