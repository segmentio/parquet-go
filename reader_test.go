package parquet_test

import (
	"log"
	"path"

	"github.com/segmentio/centrifuge-traces/parquet"
)

type actionType int

const (
	StartColumn actionType = iota
	Int32Value
	Int64Value
	ByteArray
	Empty
)

func (a actionType) String() string {
	return [...]string{
		"StartColumn",
		"Int32Value",
		"Int64Value",
		"ByteArray",
		"Empty",
	}[a]
}

type action struct {
	t actionType
	v interface{}
}

type value struct {
	d uint32
	r uint32
	v interface{}
}

type testBuilder struct {
	actions []action
	print   bool
}

func (t *testBuilder) actionsByColumn() map[string]uint64 {
	result := map[string]uint64{}
	currentCol := ""
	for _, a := range t.actions {
		switch a.t {
		case StartColumn:
			currentCol = path.Join(a.v.([]string)...)
		default:
			n, ok := result[currentCol]
			if !ok {
				n = 0
			}
			n++
			result[currentCol] = n
		}
	}
	return result
}

func (t *testBuilder) Start(schema *parquet.Schema) {
}

func (t *testBuilder) printf(format string, args ...interface{}) {
	if t.print {
		log.Printf(format, args...)
	}
}

func (t *testBuilder) append(a action) {
	t.actions = append(t.actions, a)
	t.printf("=> (%d)\t%s: %v", len(t.actions), a.t, a.v)
}

func (t *testBuilder) Empty(d, r uint32) {
	t.append(action{
		t: Empty,
		v: value{d, r, nil},
	})
}

func (t *testBuilder) StartColumn(path []string) {
	t.append(action{
		t: StartColumn,
		v: path,
	})
}

func (t *testBuilder) Int32Value(d, r uint32, v int32) {
	t.append(action{
		t: Int32Value,
		v: value{d, r, v},
	})
}

func (t *testBuilder) Int64Value(d, r uint32, v int64) {
	t.append(action{
		t: Int64Value,
		v: value{d, r, v},
	})
}

func (t *testBuilder) ByteArrayValue(d, r uint32, v []byte) {
	t.append(action{
		t: ByteArray,
		v: value{d, r, v},
	})
}

// func TestSimpleRepetition(t *testing.T) {
// 	type Record struct {
// 		Names []string `parquet:"name=names, type=LIST, valuetype=UTF8"`
// 	}

// 	builder := &testBuilder{}

// 	test.WithTestDir(t, func(dir string) {
// 		p := path.Join(dir, "simple.parquet")
// 		dst, err := localref.NewLocalFileWriter(p)
// 		require.NoError(t, err)
// 		writer, err := writerref.NewParquetWriter(dst, new(Record), 1)
// 		require.NoError(t, err)
// 		require.NoError(t, writer.Write(Record{Names: []string{"one"}}))
// 		require.NoError(t, writer.Write(Record{Names: []string{}}))
// 		require.NoError(t, writer.Write(Record{Names: []string{"two", "three"}}))
// 		require.NoError(t, writer.WriteStop())
// 		require.NoError(t, dst.Close())

// 		f, err := os.Open(p)
// 		require.NoError(t, err)
// 		defer f.Close()

// 		root := parquet.NewReader(f)
// 		err = root.Parse(builder, f)
// 		require.NoError(t, err)
// 	})

// 	expected := []action{
// 		{
// 			t: StartColumn,
// 			v: []string{"names", "list", "element"},
// 		},
// 		{
// 			t: ByteArray,
// 			v: value{1, 0, []byte("one")},
// 		},
// 		{
// 			t: Empty,
// 			v: value{0, 0, nil},
// 		},
// 		{
// 			t: ByteArray,
// 			v: value{1, 0, []byte("two")},
// 		},
// 		{
// 			t: ByteArray,
// 			v: value{1, 1, []byte("three")},
// 		},
// 	}

// 	assert.Equal(t, expected, builder.actions)
// }

// func TestRead(t *testing.T) {
// 	b, err := ioutil.ReadFile("examples/stage-trace.snappy.parquet")
// 	assert.NoError(t, err)
// 	root := parquet.NewReader()
// 	builder := &testBuilder{}
// 	err = root.Parse(builder, bytes.NewReader(b))
// 	assert.NoError(t, err)

// 	totalRows := uint64(10240)

// 	for p, count := range builder.actionsByColumn() {
// 		// TODO: handle nested objects
// 		if strings.Contains(p, "/") {
// 			continue
// 		}
// 		assert.Equal(t, totalRows, count)
// 	}
// }
