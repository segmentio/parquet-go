package parquet_test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/parquet-go"
	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/internal/quick"
)

const (
	benchmarkNumRows     = 10_000
	benchmarkRowsPerStep = 1000
)

type benchmarkRowType struct {
	ID    [16]byte `parquet:"id,uuid"`
	Value float64  `parquet:"value"`
}

func (row benchmarkRowType) generate(prng *rand.Rand) benchmarkRowType {
	prng.Read(row.ID[:])
	row.Value = prng.Float64()
	return row
}

type paddedBooleanColumn struct {
	Value bool
	_     [3]byte
}

func (row paddedBooleanColumn) generate(prng *rand.Rand) paddedBooleanColumn {
	return paddedBooleanColumn{Value: prng.Int()%2 == 0}
}

type booleanColumn struct {
	Value bool
}

func (row booleanColumn) generate(prng *rand.Rand) booleanColumn {
	return booleanColumn{Value: prng.Int()%2 == 0}
}

type int32Column struct {
	Value int32 `parquet:",delta"`
}

func (row int32Column) generate(prng *rand.Rand) int32Column {
	return int32Column{Value: prng.Int31n(100)}
}

type int64Column struct {
	Value int64 `parquet:",delta"`
}

func (row int64Column) generate(prng *rand.Rand) int64Column {
	return int64Column{Value: prng.Int63n(100)}
}

type int96Column struct {
	Value deprecated.Int96
}

func (row int96Column) generate(prng *rand.Rand) int96Column {
	row.Value[0] = prng.Uint32()
	row.Value[1] = prng.Uint32()
	row.Value[2] = prng.Uint32()
	return row
}

type floatColumn struct {
	Value float32
}

func (row floatColumn) generate(prng *rand.Rand) floatColumn {
	return floatColumn{Value: prng.Float32()}
}

type doubleColumn struct {
	Value float64
}

func (row doubleColumn) generate(prng *rand.Rand) doubleColumn {
	return doubleColumn{Value: prng.Float64()}
}

type byteArrayColumn struct {
	Value []byte
}

func (row byteArrayColumn) generate(prng *rand.Rand) byteArrayColumn {
	row.Value = make([]byte, prng.Intn(10))
	prng.Read(row.Value)
	return row
}

type fixedLenByteArrayColumn struct {
	Value [10]byte
}

func (row fixedLenByteArrayColumn) generate(prng *rand.Rand) fixedLenByteArrayColumn {
	prng.Read(row.Value[:])
	return row
}

type stringColumn struct {
	Value string
}

func (row stringColumn) generate(prng *rand.Rand) stringColumn {
	return stringColumn{Value: generateString(prng, 10)}
}

type indexedStringColumn struct {
	Value string `parquet:",dict"`
}

func (row indexedStringColumn) generate(prng *rand.Rand) indexedStringColumn {
	return indexedStringColumn{Value: generateString(prng, 10)}
}

type uuidColumn struct {
	Value uuid.UUID `parquet:",delta"`
}

func (row uuidColumn) generate(prng *rand.Rand) uuidColumn {
	prng.Read(row.Value[:])
	return row
}

type decimalColumn struct {
	Value int64 `parquet:",decimal(0:3)"`
}

func (row decimalColumn) generate(prng *rand.Rand) decimalColumn {
	return decimalColumn{Value: prng.Int63()}
}

type mapColumn struct {
	Value map[utf8string]int
}

func (row mapColumn) generate(prng *rand.Rand) mapColumn {
	n := prng.Intn(10)
	row.Value = make(map[utf8string]int, n)
	for i := 0; i < n; i++ {
		row.Value[utf8string(generateString(prng, 8))] = prng.Intn(100)
	}
	return row
}

type addressBook struct {
	Owner             utf8string   `parquet:",plain"`
	OwnerPhoneNumbers []utf8string `parquet:",plain"`
	Contacts          []contact
}

type contact struct {
	Name        utf8string `parquet:",plain"`
	PhoneNumber utf8string `parquet:",plain"`
}

func (row contact) generate(prng *rand.Rand) contact {
	return contact{
		Name:        utf8string(generateString(prng, 16)),
		PhoneNumber: utf8string(generateString(prng, 10)),
	}
}

type optionalInt32Column struct {
	Value int32 `parquet:",optional"`
}

func (row optionalInt32Column) generate(prng *rand.Rand) optionalInt32Column {
	return optionalInt32Column{Value: prng.Int31n(100)}
}

type repeatedInt32Column struct {
	Values []int32
}

func (row repeatedInt32Column) generate(prng *rand.Rand) repeatedInt32Column {
	row.Values = make([]int32, prng.Intn(10))
	for i := range row.Values {
		row.Values[i] = prng.Int31n(10)
	}
	return row
}

type listColumn2 struct {
	Value utf8string `parquet:",optional"`
}

type listColumn1 struct {
	List2 []listColumn2 `parquet:",list"`
}

type listColumn0 struct {
	List1 []listColumn1 `parquet:",list"`
}

type nestedListColumn1 struct {
	Level3 []utf8string `parquet:"level3"`
}

type nestedListColumn struct {
	Level1 []nestedListColumn1 `parquet:"level1"`
	Level2 []utf8string        `parquet:"level2"`
}

type utf8string string

func (utf8string) Generate(rand *rand.Rand, size int) reflect.Value {
	const characters = "abcdefghijklmnopqrstuvwxyz1234567890"
	const maxSize = 10
	if size > maxSize {
		size = maxSize
	}
	n := rand.Intn(size)
	b := make([]byte, n)
	for i := range b {
		b[i] = characters[rand.Intn(len(characters))]
	}
	return reflect.ValueOf(utf8string(b))
}

type Contact struct {
	Name        string `parquet:"name"`
	PhoneNumber string `parquet:"phoneNumber,optional,zstd"`
}

type AddressBook struct {
	Owner             string    `parquet:"owner,zstd"`
	OwnerPhoneNumbers []string  `parquet:"ownerPhoneNumbers,gzip"`
	Contacts          []Contact `parquet:"contacts"`
}

func forEachLeafColumn(col *parquet.Column, do func(*parquet.Column) error) error {
	children := col.Columns()

	if len(children) == 0 {
		return do(col)
	}

	for _, child := range children {
		if err := forEachLeafColumn(child, do); err != nil {
			return err
		}
	}

	return nil
}

func forEachPage(pages parquet.PageReader, do func(parquet.Page) error) error {
	for {
		p, err := pages.ReadPage()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return err
		}
		if err := do(p); err != nil {
			return err
		}
	}
}

func forEachValue(values parquet.ValueReader, do func(parquet.Value) error) error {
	buffer := [3]parquet.Value{}
	for {
		n, err := values.ReadValues(buffer[:])
		for _, v := range buffer[:n] {
			if err := do(v); err != nil {
				return err
			}
		}
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return err
		}
	}
}

func forEachColumnPage(col *parquet.Column, do func(*parquet.Column, parquet.Page) error) error {
	return forEachLeafColumn(col, func(leaf *parquet.Column) error {
		pages := leaf.Pages()
		defer pages.Close()
		return forEachPage(pages, func(page parquet.Page) error { return do(leaf, page) })
	})
}

func forEachColumnValue(col *parquet.Column, do func(*parquet.Column, parquet.Value) error) error {
	return forEachColumnPage(col, func(leaf *parquet.Column, page parquet.Page) error {
		return forEachValue(page.Values(), func(value parquet.Value) error { return do(leaf, value) })
	})
}

func forEachColumnChunk(file *parquet.File, do func(*parquet.Column, parquet.ColumnChunk) error) error {
	return forEachLeafColumn(file.Root(), func(leaf *parquet.Column) error {
		for _, rowGroup := range file.RowGroups() {
			if err := do(leaf, rowGroup.ColumnChunks()[leaf.Index()]); err != nil {
				return err
			}
		}
		return nil
	})
}

func createParquetFile(rows rows, options ...parquet.WriterOption) (*parquet.File, error) {
	buffer := new(bytes.Buffer)

	if err := writeParquetFile(buffer, rows, options...); err != nil {
		return nil, err
	}

	reader := bytes.NewReader(buffer.Bytes())
	return parquet.OpenFile(reader, reader.Size())
}

func writeParquetFile(w io.Writer, rows rows, options ...parquet.WriterOption) error {
	writer := parquet.NewWriter(w, options...)

	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	return writer.Close()
}

func writeParquetFileWithBuffer(w io.Writer, rows rows, options ...parquet.WriterOption) error {
	buffer := parquet.NewBuffer()
	for _, row := range rows {
		if err := buffer.Write(row); err != nil {
			return err
		}
	}

	writer := parquet.NewWriter(w, options...)
	numRows, err := copyRowsAndClose(writer, buffer.Rows())
	if err != nil {
		return err
	}
	if numRows != int64(len(rows)) {
		return fmt.Errorf("wrong number of rows written from buffer to file: want=%d got=%d", len(rows), numRows)
	}
	return writer.Close()
}

type rows []interface{}

func makeRows(any interface{}) rows {
	if v, ok := any.([]interface{}); ok {
		return rows(v)
	}
	value := reflect.ValueOf(any)
	slice := make([]interface{}, value.Len())
	for i := range slice {
		slice[i] = value.Index(i).Interface()
	}
	return rows(slice)
}

func randValueFuncOf(t parquet.Type) func(*rand.Rand) parquet.Value {
	switch k := t.Kind(); k {
	case parquet.Boolean:
		return func(r *rand.Rand) parquet.Value {
			return parquet.ValueOf(r.Float64() < 0.5)
		}

	case parquet.Int32:
		return func(r *rand.Rand) parquet.Value {
			return parquet.ValueOf(r.Int31())
		}

	case parquet.Int64:
		return func(r *rand.Rand) parquet.Value {
			return parquet.ValueOf(r.Int63())
		}

	case parquet.Int96:
		return func(r *rand.Rand) parquet.Value {
			return parquet.ValueOf(deprecated.Int96{
				0: r.Uint32(),
				1: r.Uint32(),
				2: r.Uint32(),
			})
		}

	case parquet.Float:
		return func(r *rand.Rand) parquet.Value {
			return parquet.ValueOf(r.Float32())
		}

	case parquet.Double:
		return func(r *rand.Rand) parquet.Value {
			return parquet.ValueOf(r.Float64())
		}

	case parquet.ByteArray:
		return func(r *rand.Rand) parquet.Value {
			n := r.Intn(49) + 1
			b := make([]byte, n)
			const characters = "1234567890qwertyuiopasdfghjklzxcvbnm "
			for i := range b {
				b[i] = characters[r.Intn(len(characters))]
			}
			return parquet.ValueOf(b)
		}

	case parquet.FixedLenByteArray:
		arrayType := reflect.ArrayOf(t.Length(), reflect.TypeOf(byte(0)))
		return func(r *rand.Rand) parquet.Value {
			b := make([]byte, arrayType.Len())
			r.Read(b)
			v := reflect.New(arrayType).Elem()
			reflect.Copy(v, reflect.ValueOf(b))
			return parquet.ValueOf(v.Interface())
		}

	default:
		panic("NOT IMPLEMENTED")
	}
}

func copyRowsAndClose(w parquet.RowWriter, r parquet.Rows) (int64, error) {
	defer r.Close()
	return parquet.CopyRows(w, r)
}

func benchmarkRowsPerSecond(b *testing.B, f func() int) {
	b.ResetTimer()
	start := time.Now()
	numRows := int64(0)

	for i := 0; i < b.N; i++ {
		n := f()
		numRows += int64(n)
	}

	seconds := time.Since(start).Seconds()
	b.ReportMetric(float64(numRows)/seconds, "row/s")
}

func generateString(r *rand.Rand, n int) string {
	const characters = "1234567890qwertyuiopasdfghjklzxcvbnm"
	b := new(strings.Builder)
	for i := 0; i < n; i++ {
		b.WriteByte(characters[r.Intn(len(characters))])
	}
	return b.String()
}

var quickCheckConfig = quick.Config{
	Sizes: []int{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		10, 20, 30, 40, 50, 123,
	},
}

func quickCheck(f interface{}) error {
	return quickCheckConfig.Check(f)
}
