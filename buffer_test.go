package parquet_test

import (
	"bytes"
	"errors"
	"io"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/segmentio/parquet-go"
	"github.com/segmentio/parquet-go/encoding"
)

var bufferTests = [...]struct {
	scenario string
	typ      parquet.Type
	values   [][]interface{}
}{
	{
		scenario: "boolean",
		typ:      parquet.BooleanType,
		values: [][]interface{}{
			{},
			{false},
			{true},
			{
				false, true, false, false, true, true,
				false, false, false, true, false, true,
			},
		},
	},

	{
		scenario: "int32",
		typ:      parquet.Int32Type,
		values: [][]interface{}{
			{},
			{int32(0)},
			{int32(1)},
			{
				int32(1), int32(2), int32(3), int32(4), int32(5), int32(6),
				int32(math.MaxInt8), int32(math.MaxInt16), int32(math.MaxInt32),
				int32(7), int32(9), int32(9), int32(0),
			},
		},
	},

	{
		scenario: "int64",
		typ:      parquet.Int64Type,
		values: [][]interface{}{
			{},
			{int64(0)},
			{int64(1)},
			{
				int64(1), int64(2), int64(3), int64(4), int64(5), int64(6),
				int64(math.MaxInt8), int64(math.MaxInt16), int64(math.MaxInt64), int64(7),
				int64(9), int64(9), int64(0),
			},
		},
	},

	{
		scenario: "float",
		typ:      parquet.FloatType,
		values: [][]interface{}{
			{},
			{float32(0)},
			{float32(1)},
			{
				float32(1), float32(2), float32(3), float32(4), float32(5), float32(6),
				float32(0.5), float32(math.SmallestNonzeroFloat32), float32(math.MaxFloat32), float32(7),
				float32(9), float32(9), float32(0),
			},
		},
	},

	{
		scenario: "double",
		typ:      parquet.DoubleType,
		values: [][]interface{}{
			{},
			{float64(0)},
			{float64(1)},
			{
				float64(1), float64(2), float64(3), float64(4), float64(5), float64(6),
				float64(0.5), float64(math.SmallestNonzeroFloat64), float64(math.MaxFloat64), float64(7),
				float64(9), float64(9), float64(0),
			},
		},
	},

	{
		scenario: "string",
		typ:      parquet.ByteArrayType,
		values: [][]interface{}{
			{},
			{""},
			{"Hello World!"},
			{
				"ABCDEFG", "HIJKLMN", "OPQRSTU", "VWXZY01", "2345678",
				"90!@#$%", "^&*()_+", "Hello World!", "Answer=42", "ABCEDFG",
				"HIJKLMN", "OPQRSTU", "VWXYZ",
			},
		},
	},

	{
		scenario: "fixed length byte array",
		typ:      parquet.FixedLenByteArrayType(10),
		values: [][]interface{}{
			{},
			{[10]byte{}},
			{[10]byte{0: 1}},
			{
				[10]byte{0: 0}, [10]byte{0: 2}, [10]byte{0: 1}, [10]byte{0: 4}, [10]byte{0: 3},
				[10]byte{0: 6}, [10]byte{0: 5}, [10]byte{0: 8}, [10]byte{0: 7}, [10]byte{0: 10},
				[10]byte{0: 11}, [10]byte{0: 12}, [10]byte{9: 0xFF},
			},
		},
	},

	{
		scenario: "uuid",
		typ:      parquet.UUID().Type(),
		values: [][]interface{}{
			{},
			{[16]byte{}},
			{[16]byte{0: 1}},
			{
				[16]byte{0: 0}, [16]byte{0: 2}, [16]byte{0: 1}, [16]byte{0: 4}, [16]byte{0: 3},
				[16]byte{0: 6}, [16]byte{0: 5}, [16]byte{0: 8}, [16]byte{0: 7}, [16]byte{0: 10},
				[16]byte{0: 11}, [16]byte{0: 12}, [16]byte{15: 0xFF},
			},
		},
	},

	{
		scenario: "uint32",
		typ:      parquet.Uint(32).Type(),
		values: [][]interface{}{
			{},
			{uint32(0)},
			{uint32(1)},
			{
				uint32(1), uint32(2), uint32(3), uint32(4), uint32(5), uint32(6),
				uint32(math.MaxInt8), uint32(math.MaxInt16), uint32(math.MaxUint32), uint32(7),
				uint32(9), uint32(9), uint32(0),
			},
		},
	},

	{
		scenario: "uint64",
		typ:      parquet.Uint(64).Type(),
		values: [][]interface{}{
			{},
			{uint64(0)},
			{uint64(1)},
			{
				uint64(1), uint64(2), uint64(3), uint64(4), uint64(5), uint64(6),
				uint64(math.MaxInt8), uint64(math.MaxInt16), uint64(math.MaxUint64),
				uint64(7), uint64(9), uint64(9), uint64(0),
			},
		},
	},
}

func TestBuffer(t *testing.T) {
	for _, test := range bufferTests {
		t.Run(test.scenario, func(t *testing.T) {
			for _, config := range [...]struct {
				scenario string
				typ      parquet.Type
			}{
				{scenario: "plain", typ: test.typ},
				{scenario: "indexed", typ: test.typ.NewDictionary(0, 0, test.typ.NewValues(nil, nil)).Type()},
			} {
				t.Run(config.scenario, func(t *testing.T) {
					for _, mod := range [...]struct {
						scenario string
						function func(parquet.Node) parquet.Node
					}{
						{scenario: "optional", function: parquet.Optional},
						{scenario: "repeated", function: parquet.Repeated},
						{scenario: "required", function: parquet.Required},
					} {
						t.Run(mod.scenario, func(t *testing.T) {
							for _, ordering := range [...]struct {
								scenario string
								sorting  parquet.SortingColumn
								sortFunc func(parquet.Type, []parquet.Value)
							}{
								{scenario: "unordered", sorting: nil, sortFunc: unordered},
								{scenario: "ascending", sorting: parquet.Ascending("data"), sortFunc: ascending},
								{scenario: "descending", sorting: parquet.Descending("data"), sortFunc: descending},
							} {
								t.Run(ordering.scenario, func(t *testing.T) {
									schema := parquet.NewSchema("test", parquet.Group{
										"data": mod.function(parquet.Leaf(config.typ)),
									})

									options := []parquet.RowGroupOption{
										schema,
										parquet.ColumnBufferCapacity(100),
									}
									if ordering.sorting != nil {
										options = append(options, parquet.SortingColumns(ordering.sorting))
									}

									content := new(bytes.Buffer)
									buffer := parquet.NewBuffer(options...)

									for _, values := range test.values {
										t.Run("", func(t *testing.T) {
											defer content.Reset()
											defer buffer.Reset()
											fields := schema.Fields()
											testBuffer(t, fields[0], buffer, &parquet.Plain, values, ordering.sortFunc)
										})
									}
								})
							}
						})
					}
				})
			}
		})
	}
}

type sortFunc func(parquet.Type, []parquet.Value)

func unordered(typ parquet.Type, values []parquet.Value) {}

func ascending(typ parquet.Type, values []parquet.Value) {
	sort.Slice(values, func(i, j int) bool { return typ.Compare(values[i], values[j]) < 0 })
}

func descending(typ parquet.Type, values []parquet.Value) {
	sort.Slice(values, func(i, j int) bool { return typ.Compare(values[i], values[j]) > 0 })
}

func testBuffer(t *testing.T, node parquet.Node, buffer *parquet.Buffer, encoding encoding.Encoding, values []interface{}, sortFunc sortFunc) {
	repetitionLevel := 0
	definitionLevel := 0
	if !node.Required() {
		definitionLevel = 1
	}

	minValue := parquet.Value{}
	maxValue := parquet.Value{}
	batch := make([]parquet.Value, len(values))
	for i := range values {
		batch[i] = parquet.ValueOf(values[i]).Level(repetitionLevel, definitionLevel, 0)
	}

	for i := range batch {
		_, err := buffer.WriteRows([]parquet.Row{batch[i : i+1]})
		if err != nil {
			t.Fatalf("writing value to row group: %v", err)
		}
	}

	numRows := buffer.NumRows()
	if numRows != int64(len(batch)) {
		t.Fatalf("number of rows mismatch: want=%d got=%d", len(batch), numRows)
	}

	typ := node.Type()
	for _, value := range batch {
		if minValue.IsNull() || typ.Compare(value, minValue) < 0 {
			minValue = value
		}
		if maxValue.IsNull() || typ.Compare(value, maxValue) > 0 {
			maxValue = value
		}
	}

	sortFunc(typ, batch)
	sort.Sort(buffer)

	page := buffer.ColumnBuffers()[0].Page()
	numValues := page.NumValues()
	if numValues != int64(len(batch)) {
		t.Fatalf("number of values mistmatch: want=%d got=%d", len(batch), numValues)
	}

	numNulls := page.NumNulls()
	if numNulls != 0 {
		t.Fatalf("number of nulls mismatch: want=0 got=%d", numNulls)
	}

	min, max, hasBounds := page.Bounds()
	if !hasBounds && numRows > 0 {
		t.Fatal("page bounds are missing")
	}
	if !parquet.Equal(min, minValue) {
		t.Fatalf("min value mismatch: want=%v got=%v", minValue, min)
	}
	if !parquet.Equal(max, maxValue) {
		t.Fatalf("max value mismatch: want=%v got=%v", maxValue, max)
	}

	// We write a single value per row, so num values = num rows for all pages
	// including repeated ones, which makes it OK to slice the pages using the
	// number of values as a proxy for the row indexes.
	halfValues := numValues / 2

	for _, test := range [...]struct {
		scenario string
		values   []parquet.Value
		reader   parquet.ValueReader
	}{
		{"page", batch, page.Values()},
		{"head", batch[:halfValues], page.Slice(0, halfValues).Values()},
		{"tail", batch[halfValues:], page.Slice(halfValues, numValues).Values()},
	} {
		v := [1]parquet.Value{}
		i := 0

		for {
			n, err := test.reader.ReadValues(v[:])
			if n > 0 {
				if n != 1 {
					t.Fatalf("reading value from %q reader returned the wrong count: want=1 got=%d", test.scenario, n)
				}
				if i < len(test.values) {
					if !parquet.Equal(v[0], test.values[i]) {
						t.Fatalf("%q value at index %d mismatches: want=%v got=%v", test.scenario, i, test.values[i], v[0])
					}
				}
				i++
			}
			if err != nil {
				if err == io.EOF {
					break
				}
				t.Fatalf("reading value from %q reader: %v", test.scenario, err)
			}
		}

		if i != len(test.values) {
			t.Errorf("wrong number of values read from %q reader: want=%d got=%d", test.scenario, len(test.values), i)
		}
	}
}

func TestBufferGenerateBloomFilters(t *testing.T) {
	type Point3D struct {
		X float64
		Y float64
		Z float64
	}

	f := func(rows []Point3D) bool {
		if len(rows) == 0 { // TODO: support writing files with no rows
			return true
		}

		output := new(bytes.Buffer)
		buffer := parquet.NewBuffer()
		writer := parquet.NewWriter(output,
			parquet.BloomFilters(
				parquet.SplitBlockFilter("X"),
				parquet.SplitBlockFilter("Y"),
				parquet.SplitBlockFilter("Z"),
			),
		)
		for i := range rows {
			buffer.Write(&rows[i])
		}
		_, err := copyRowsAndClose(writer, buffer.Rows())
		if err != nil {
			t.Error(err)
			return false
		}
		if err := writer.Close(); err != nil {
			t.Error(err)
			return false
		}

		reader := bytes.NewReader(output.Bytes())
		f, err := parquet.OpenFile(reader, reader.Size())
		if err != nil {
			t.Error(err)
			return false
		}
		rowGroup := f.RowGroups()[0]
		columns := rowGroup.ColumnChunks()
		x := columns[0]
		y := columns[1]
		z := columns[2]

		for i, col := range []parquet.ColumnChunk{x, y, z} {
			if col.BloomFilter() == nil {
				t.Errorf("column %d has no bloom filter despite being configured to have one", i)
				return false
			}
		}

		fx := x.BloomFilter()
		fy := y.BloomFilter()
		fz := z.BloomFilter()

		test := func(f parquet.BloomFilter, v float64) bool {
			if ok, err := f.Check(parquet.ValueOf(v)); err != nil {
				t.Errorf("unexpected error checking bloom filter: %v", err)
				return false
			} else if !ok {
				t.Errorf("bloom filter does not contain value %g", v)
				return false
			}
			return true
		}

		for _, row := range rows {
			if !test(fx, row.X) || !test(fy, row.Y) || !test(fz, row.Z) {
				return false
			}
		}

		return true
	}

	if err := quickCheck(f); err != nil {
		t.Error(err)
	}
}

func TestBufferRoundtripNestedRepeated(t *testing.T) {
	type C struct {
		D int
	}
	type B struct {
		C []C
	}
	type A struct {
		B []B
	}

	// Write enough objects to exceed first page
	buffer := parquet.NewBuffer()
	var objs []A
	for i := 0; i < 6; i++ {
		o := A{[]B{{[]C{
			{i},
			{i},
		}}}}
		buffer.Write(&o)
		objs = append(objs, o)
	}

	buf := new(bytes.Buffer)
	w := parquet.NewWriter(buf, parquet.PageBufferSize(100))
	w.WriteRowGroup(buffer)
	w.Flush()
	w.Close()

	file := bytes.NewReader(buf.Bytes())
	r := parquet.NewReader(file)
	for i := 0; ; i++ {
		o := new(A)
		err := r.Read(o)
		if errors.Is(err, io.EOF) {
			if i < len(objs) {
				t.Errorf("too few rows were read: %d<%d", i, len(objs))
			}
			break
		}
		if !reflect.DeepEqual(*o, objs[i]) {
			t.Errorf("points mismatch at row index %d: want=%v got=%v", i, objs[i], o)
		}
	}
}

func TestBufferRoundtripNestedRepeatedPointer(t *testing.T) {
	type C struct {
		D *int
	}
	type B struct {
		C []C
	}
	type A struct {
		B []B
	}

	// Write enough objects to exceed first page
	buffer := parquet.NewBuffer()
	var objs []A
	for i := 0; i < 6; i++ {
		j := i
		o := A{[]B{{[]C{
			{&j},
			{nil},
		}}}}
		buffer.Write(&o)
		objs = append(objs, o)
	}

	buf := new(bytes.Buffer)
	w := parquet.NewWriter(buf, parquet.PageBufferSize(100))
	w.WriteRowGroup(buffer)
	w.Flush()
	w.Close()

	file := bytes.NewReader(buf.Bytes())
	r := parquet.NewReader(file)
	for i := 0; ; i++ {
		o := new(A)
		err := r.Read(o)
		if err == io.EOF {
			break
		}
		if !reflect.DeepEqual(*o, objs[i]) {
			t.Errorf("points mismatch at row index %d: want=%v got=%v", i, objs[i], o)
		}
	}
}

func TestRoundtripNestedRepeatedBytes(t *testing.T) {
	type B struct {
		C []byte
	}
	type A struct {
		A string
		B []B
	}

	var objs []A
	for i := 0; i < 2; i++ {
		o := A{
			"test" + strconv.Itoa(i),
			[]B{
				{[]byte{byte(i)}},
			},
		}
		objs = append(objs, o)
	}

	buf := new(bytes.Buffer)
	w := parquet.NewWriter(buf, parquet.PageBufferSize(100))
	for _, o := range objs {
		w.Write(&o)
	}
	w.Close()

	file := bytes.NewReader(buf.Bytes())

	r := parquet.NewReader(file)
	for i := 0; ; i++ {
		o := new(A)
		err := r.Read(o)
		if errors.Is(err, io.EOF) {
			if i < len(objs) {
				t.Errorf("too few rows were read: %d<%d", i, len(objs))
			}
			break
		}
		if !reflect.DeepEqual(*o, objs[i]) {
			t.Errorf("points mismatch at row index %d: want=%v got=%v", i, objs[i], o)
		}
	}
}

func TestBufferSeekToRow(t *testing.T) {
	type B struct {
		I int
		C []string
	}
	type A struct {
		B []B
	}

	buffer := parquet.NewBuffer()
	var objs []A
	for i := 0; i < 2; i++ {
		o := A{
			B: []B{
				{I: i, C: []string{"foo", strconv.Itoa(i)}},
				{I: i + 1, C: []string{"bar", strconv.Itoa(i + 1)}},
			},
		}
		buffer.Write(&o)
		objs = append(objs, o)
	}

	buf := new(bytes.Buffer)
	w := parquet.NewWriter(buf)
	w.WriteRowGroup(buffer)
	w.Flush()
	w.Close()

	file := bytes.NewReader(buf.Bytes())
	r := parquet.NewReader(file)

	i := 1
	o := new(A)
	if err := r.SeekToRow(int64(i)); err != nil {
		t.Fatal(err)
	}
	if err := r.Read(o); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*o, objs[i]) {
		t.Errorf("points mismatch at row index %d: want=%v got=%v", i, objs[i], o)
	}
}

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
		_, err := buf.WriteRows([]parquet.Row{row})
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

func generateBenchmarkBufferRows(n int) (*parquet.Schema, []parquet.Row) {
	model := new(benchmarkRowType)
	schema := parquet.SchemaOf(model)
	prng := rand.New(rand.NewSource(0))
	rows := make([]parquet.Row, n)

	for i := range rows {
		io.ReadFull(prng, model.ID[:])
		model.Value = prng.Float64()
		rows[i] = make(parquet.Row, 0, 2)
		rows[i] = schema.Deconstruct(rows[i], model)
	}

	return schema, rows
}

func BenchmarkBufferReadRows100x(b *testing.B) {
	schema, rows := generateBenchmarkBufferRows(benchmarkNumRows)
	buffer := parquet.NewBuffer(schema)

	for i := 0; i < len(rows); i += benchmarkRowsPerStep {
		j := i + benchmarkRowsPerStep
		if _, err := buffer.WriteRows(rows[i:j]); err != nil {
			b.Fatal(err)
		}
	}

	bufferRows := buffer.Rows()
	defer bufferRows.Close()

	benchmarkRowsPerSecond(b, func() int {
		n, err := bufferRows.ReadRows(rows[:benchmarkRowsPerStep])
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = bufferRows.SeekToRow(0)
			}
			if err != nil {
				b.Fatal(err)
			}
		}
		return n
	})
}

func BenchmarkBufferWriteRows100x(b *testing.B) {
	schema, rows := generateBenchmarkBufferRows(benchmarkNumRows)
	buffer := parquet.NewBuffer(schema)

	i := 0
	benchmarkRowsPerSecond(b, func() int {
		n, err := buffer.WriteRows(rows[i : i+benchmarkRowsPerStep])
		if err != nil {
			b.Fatal(err)
		}

		i += benchmarkRowsPerStep
		i %= benchmarkNumRows

		if i == 0 {
			buffer.Reset()
		}
		return n
	})
}
