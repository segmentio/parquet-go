package parquet_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/hexops/gotextdiff"
	"github.com/hexops/gotextdiff/myers"
	"github.com/hexops/gotextdiff/span"
	"github.com/segmentio/parquet-go"
	"github.com/segmentio/parquet-go/compress"
)

const (
	v1 = 1
	v2 = 2
)

func scanParquetFile(f *os.File) error {
	s, err := f.Stat()
	if err != nil {
		return err
	}

	p, err := parquet.OpenFile(f, s.Size())
	if err != nil {
		return err
	}

	return scanParquetValues(p.Root())
}

func scanParquetValues(col *parquet.Column) error {
	return forEachColumnValue(col, func(leaf *parquet.Column, value parquet.Value) error {
		fmt.Printf("%s > %+v\n", strings.Join(leaf.Path(), "."), value)
		return nil
	})
}

func generateParquetFile(rows rows, options ...parquet.WriterOption) ([]byte, error) {
	tmp, err := os.CreateTemp("/tmp", "*.parquet")
	if err != nil {
		return nil, err
	}
	defer tmp.Close()
	path := tmp.Name()
	defer os.Remove(path)
	//fmt.Println(path)

	writerOptions := []parquet.WriterOption{parquet.PageBufferSize(20)}
	writerOptions = append(writerOptions, options...)

	if err := writeParquetFile(tmp, rows, writerOptions...); err != nil {
		return nil, err
	}

	if err := scanParquetFile(tmp); err != nil {
		return nil, err
	}

	return parquetTools("dump", path)
}

type firstAndLastName struct {
	FirstName string `parquet:"first_name,dict,zstd"`
	LastName  string `parquet:"last_name,delta,zstd"`
}

type timeseries struct {
	Name      string  `parquet:"name,dict"`
	Timestamp int64   `parquet:"timestamp,delta"`
	Value     float64 `parquet:"value"`
}

type event struct {
	Name     string  `parquet:"name,dict"`
	Type     string  `parquet:"-"`
	Value    float64 `parquet:"value"`
	Category string  `parquet:"-"`
}

var writerTests = []struct {
	scenario string
	version  int
	codec    compress.Codec
	rows     []interface{}
	dump     string
}{
	{
		scenario: "page v1 with dictionary encoding",
		version:  v1,
		rows: []interface{}{
			&firstAndLastName{FirstName: "Han", LastName: "Solo"},
			&firstAndLastName{FirstName: "Leia", LastName: "Skywalker"},
			&firstAndLastName{FirstName: "Luke", LastName: "Skywalker"},
		},
		dump: `row group 0
--------------------------------------------------------------------------------
first_name:  BINARY ZSTD DO:4 FPO:55 SZ:123/96/0.78 VC:3 ENC:PLAIN,RLE_DICTIONARY ST:[no stats for this column]
last_name:   BINARY ZSTD DO:0 FPO:127 SZ:127/121/0.95 VC:3 ENC:DELTA_BYTE_ARRAY ST:[no stats for this column]

    first_name TV=3 RL=0 DL=0 DS: 3 DE:PLAIN
    ----------------------------------------------------------------------------
    page 0:                        DLE:RLE RLE:RLE VLE:RLE_DICTIONARY ST:[no stats for this column] CRC:[PAGE CORRUPT] SZ:5 VC:2
    page 1:                        DLE:RLE RLE:RLE VLE:RLE_DICTIONARY ST:[no stats for this column] CRC:[PAGE CORRUPT] SZ:3 VC:1

    last_name TV=3 RL=0 DL=0
    ----------------------------------------------------------------------------
    page 0:                        DLE:RLE RLE:RLE VLE:DELTA_BYTE_ARRAY ST:[no stats for this column] CRC:[PAGE CORRUPT] SZ:56 VC:2
    page 1:                        DLE:RLE RLE:RLE VLE:DELTA_BYTE_ARRAY ST:[no stats for this column] CRC:[PAGE CORRUPT] SZ:19 VC:1

BINARY first_name
--------------------------------------------------------------------------------
*** row group 1 of 1, values 1 to 3 ***
value 1: R:0 D:0 V:Han
value 2: R:0 D:0 V:Leia
value 3: R:0 D:0 V:Luke

BINARY last_name
--------------------------------------------------------------------------------
*** row group 1 of 1, values 1 to 3 ***
value 1: R:0 D:0 V:Solo
value 2: R:0 D:0 V:Skywalker
value 3: R:0 D:0 V:Skywalker
`,
	},

	{ // same as the previous test but uses page v2 where data pages aren't compressed
		scenario: "page v2 with dictionary encoding",
		version:  v2,
		rows: []interface{}{
			&firstAndLastName{FirstName: "Han", LastName: "Solo"},
			&firstAndLastName{FirstName: "Leia", LastName: "Skywalker"},
			&firstAndLastName{FirstName: "Luke", LastName: "Skywalker"},
		},
		dump: `row group 0
--------------------------------------------------------------------------------
first_name:  BINARY ZSTD DO:4 FPO:55 SZ:115/106/0.92 VC:3 ENC:RLE_DICTIONARY,PLAIN ST:[no stats for this column]
last_name:   BINARY ZSTD DO:0 FPO:119 SZ:137/131/0.96 VC:3 ENC:DELTA_BYTE_ARRAY ST:[no stats for this column]

    first_name TV=3 RL=0 DL=0 DS: 3 DE:PLAIN
    ----------------------------------------------------------------------------
    page 0:                        DLE:RLE RLE:RLE VLE:RLE_DICTIONARY ST:[no stats for this column] SZ:5 VC:2
    page 1:                        DLE:RLE RLE:RLE VLE:RLE_DICTIONARY ST:[no stats for this column] SZ:3 VC:1

    last_name TV=3 RL=0 DL=0
    ----------------------------------------------------------------------------
    page 0:                        DLE:RLE RLE:RLE VLE:DELTA_BYTE_ARRAY ST:[no stats for this column] SZ:56 VC:2
    page 1:                        DLE:RLE RLE:RLE VLE:DELTA_BYTE_ARRAY ST:[no stats for this column] SZ:19 VC:1

BINARY first_name
--------------------------------------------------------------------------------
*** row group 1 of 1, values 1 to 3 ***
value 1: R:0 D:0 V:Han
value 2: R:0 D:0 V:Leia
value 3: R:0 D:0 V:Luke

BINARY last_name
--------------------------------------------------------------------------------
*** row group 1 of 1, values 1 to 3 ***
value 1: R:0 D:0 V:Solo
value 2: R:0 D:0 V:Skywalker
value 3: R:0 D:0 V:Skywalker
`,
	},

	{
		scenario: "timeseries with delta encoding",
		version:  v2,
		codec:    &parquet.Gzip,
		rows: []interface{}{
			timeseries{Name: "http_request_total", Timestamp: 1639444033, Value: 100},
			timeseries{Name: "http_request_total", Timestamp: 1639444058, Value: 0},
			timeseries{Name: "http_request_total", Timestamp: 1639444085, Value: 42},
			timeseries{Name: "http_request_total", Timestamp: 1639444093, Value: 1},
			timeseries{Name: "http_request_total", Timestamp: 1639444101, Value: 2},
			timeseries{Name: "http_request_total", Timestamp: 1639444108, Value: 5},
			timeseries{Name: "http_request_total", Timestamp: 1639444133, Value: 4},
			timeseries{Name: "http_request_total", Timestamp: 1639444137, Value: 5},
			timeseries{Name: "http_request_total", Timestamp: 1639444141, Value: 6},
			timeseries{Name: "http_request_total", Timestamp: 1639444144, Value: 10},
		},
		dump: `row group 0
--------------------------------------------------------------------------------
name:       BINARY GZIP DO:4 FPO:70 SZ:216/191/0.88 VC:10 ENC:PLAIN,RLE_DICTIONARY ST:[no stats for this column]
timestamp:  INT64 GZIP DO:0 FPO:220 SZ:299/550/1.84 VC:10 ENC:DELTA_BINARY_PACKED ST:[no stats for this column]
value:      DOUBLE GZIP DO:0 FPO:519 SZ:292/192/0.66 VC:10 ENC:PLAIN ST:[no stats for this column]

    name TV=10 RL=0 DL=0 DS: 1 DE:PLAIN
    ----------------------------------------------------------------------------
    page 0:                   DLE:RLE RLE:RLE VLE:RLE_DICTIONARY ST:[no stats for this column] SZ:2 VC:2
    page 1:                   DLE:RLE RLE:RLE VLE:RLE_DICTIONARY ST:[no stats for this column] SZ:2 VC:2
    page 2:                   DLE:RLE RLE:RLE VLE:RLE_DICTIONARY ST:[no stats for this column] SZ:2 VC:2
    page 3:                   DLE:RLE RLE:RLE VLE:RLE_DICTIONARY ST:[no stats for this column] SZ:2 VC:2
    page 4:                   DLE:RLE RLE:RLE VLE:RLE_DICTIONARY ST:[no stats for this column] SZ:2 VC:2

    timestamp TV=10 RL=0 DL=0
    ----------------------------------------------------------------------------
    page 0:                   DLE:RLE RLE:RLE VLE:DELTA_BINARY_PACKED ST:[no stats for this column] SZ:142 VC:3
    page 1:                   DLE:RLE RLE:RLE VLE:DELTA_BINARY_PACKED ST:[no stats for this column] SZ:142 VC:3
    page 2:                   DLE:RLE RLE:RLE VLE:DELTA_BINARY_PACKED ST:[no stats for this column] SZ:142 VC:3
    page 3:                   DLE:RLE RLE:RLE VLE:DELTA_BINARY_PACKED ST:[no stats for this column] SZ:9 VC:1

    value TV=10 RL=0 DL=0
    ----------------------------------------------------------------------------
    page 0:                   DLE:RLE RLE:RLE VLE:PLAIN ST:[no stats for this column] SZ:24 VC:3
    page 1:                   DLE:RLE RLE:RLE VLE:PLAIN ST:[no stats for this column] SZ:24 VC:3
    page 2:                   DLE:RLE RLE:RLE VLE:PLAIN ST:[no stats for this column] SZ:24 VC:3
    page 3:                   DLE:RLE RLE:RLE VLE:PLAIN ST:[no stats for this column] SZ:8 VC:1

BINARY name
--------------------------------------------------------------------------------
*** row group 1 of 1, values 1 to 10 ***
value 1:  R:0 D:0 V:http_request_total
value 2:  R:0 D:0 V:http_request_total
value 3:  R:0 D:0 V:http_request_total
value 4:  R:0 D:0 V:http_request_total
value 5:  R:0 D:0 V:http_request_total
value 6:  R:0 D:0 V:http_request_total
value 7:  R:0 D:0 V:http_request_total
value 8:  R:0 D:0 V:http_request_total
value 9:  R:0 D:0 V:http_request_total
value 10: R:0 D:0 V:http_request_total

INT64 timestamp
--------------------------------------------------------------------------------
*** row group 1 of 1, values 1 to 10 ***
value 1:  R:0 D:0 V:1639444033
value 2:  R:0 D:0 V:1639444058
value 3:  R:0 D:0 V:1639444085
value 4:  R:0 D:0 V:1639444093
value 5:  R:0 D:0 V:1639444101
value 6:  R:0 D:0 V:1639444108
value 7:  R:0 D:0 V:1639444133
value 8:  R:0 D:0 V:1639444137
value 9:  R:0 D:0 V:1639444141
value 10: R:0 D:0 V:1639444144

DOUBLE value
--------------------------------------------------------------------------------
*** row group 1 of 1, values 1 to 10 ***
value 1:  R:0 D:0 V:100.0
value 2:  R:0 D:0 V:0.0
value 3:  R:0 D:0 V:42.0
value 4:  R:0 D:0 V:1.0
value 5:  R:0 D:0 V:2.0
value 6:  R:0 D:0 V:5.0
value 7:  R:0 D:0 V:4.0
value 8:  R:0 D:0 V:5.0
value 9:  R:0 D:0 V:6.0
value 10: R:0 D:0 V:10.0
`,
	},

	{
		scenario: "example from the twitter blog (v1)",
		version:  v1,
		rows: []interface{}{
			AddressBook{
				Owner: "Julien Le Dem",
				OwnerPhoneNumbers: []string{
					"555 123 4567",
					"555 666 1337",
				},
				Contacts: []Contact{
					{
						Name:        "Dmitriy Ryaboy",
						PhoneNumber: "555 987 6543",
					},
					{
						Name: "Chris Aniszczyk",
					},
				},
			},
			AddressBook{
				Owner:             "A. Nonymous",
				OwnerPhoneNumbers: nil,
			},
		},

		dump: `row group 0
--------------------------------------------------------------------------------
owner:              BINARY ZSTD DO:0 FPO:4 SZ:81/73/0.90 VC:2 ENC:DELTA_LENGTH_BYTE_ARRAY ST:[no stats for this column]
ownerPhoneNumbers:  BINARY GZIP DO:0 FPO:85 SZ:179/129/0.72 VC:3 ENC:RLE,DELTA_LENGTH_BYTE_ARRAY ST:[no stats for this column]
contacts:
.name:              BINARY UNCOMPRESSED DO:0 FPO:264 SZ:138/138/1.00 VC:3 ENC:RLE,DELTA_LENGTH_BYTE_ARRAY ST:[no stats for this column]
.phoneNumber:       BINARY ZSTD DO:0 FPO:402 SZ:113/95/0.84 VC:3 ENC:RLE,DELTA_LENGTH_BYTE_ARRAY ST:[no stats for this column]

    owner TV=2 RL=0 DL=0
    ----------------------------------------------------------------------------
    page 0:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats for this column] CRC:[PAGE CORRUPT] SZ:50 VC:2

    ownerPhoneNumbers TV=3 RL=1 DL=1
    ----------------------------------------------------------------------------
    page 0:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats for this column] CRC:[PAGE CORRUPT] SZ:64 VC:2
    page 1:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats for this column] CRC:[PAGE CORRUPT] SZ:17 VC:1

    contacts.name TV=3 RL=1 DL=1
    ----------------------------------------------------------------------------
    page 0:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats for this column] CRC:[verified] SZ:73 VC:2
    page 1:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats for this column] CRC:[verified] SZ:17 VC:1

    contacts.phoneNumber TV=3 RL=1 DL=2
    ----------------------------------------------------------------------------
    page 0:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats for this column] CRC:[PAGE CORRUPT] SZ:33 VC:2
    page 1:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats for this column] CRC:[PAGE CORRUPT] SZ:17 VC:1

BINARY owner
--------------------------------------------------------------------------------
*** row group 1 of 1, values 1 to 2 ***
value 1: R:0 D:0 V:Julien Le Dem
value 2: R:0 D:0 V:A. Nonymous

BINARY ownerPhoneNumbers
--------------------------------------------------------------------------------
*** row group 1 of 1, values 1 to 3 ***
value 1: R:0 D:1 V:555 123 4567
value 2: R:1 D:1 V:555 666 1337
value 3: R:0 D:0 V:<null>

BINARY contacts.name
--------------------------------------------------------------------------------
*** row group 1 of 1, values 1 to 3 ***
value 1: R:0 D:1 V:Dmitriy Ryaboy
value 2: R:1 D:1 V:Chris Aniszczyk
value 3: R:0 D:0 V:<null>

BINARY contacts.phoneNumber
--------------------------------------------------------------------------------
*** row group 1 of 1, values 1 to 3 ***
value 1: R:0 D:2 V:555 987 6543
value 2: R:1 D:1 V:<null>
value 3: R:0 D:0 V:<null>
`,
	},

	{
		scenario: "example from the twitter blog (v2)",
		version:  v2,
		rows: []interface{}{
			AddressBook{
				Owner: "Julien Le Dem",
				OwnerPhoneNumbers: []string{
					"555 123 4567",
					"555 666 1337",
				},
				Contacts: []Contact{
					{
						Name:        "Dmitriy Ryaboy",
						PhoneNumber: "555 987 6543",
					},
					{
						Name: "Chris Aniszczyk",
					},
				},
			},
			AddressBook{
				Owner:             "A. Nonymous",
				OwnerPhoneNumbers: nil,
			},
		},

		dump: `row group 0
--------------------------------------------------------------------------------
owner:              BINARY ZSTD DO:0 FPO:4 SZ:86/78/0.91 VC:2 ENC:DELTA_LENGTH_BYTE_ARRAY ST:[no stats for this column]
ownerPhoneNumbers:  BINARY GZIP DO:0 FPO:90 SZ:172/122/0.71 VC:3 ENC:RLE,DELTA_LENGTH_BYTE_ARRAY ST:[no stats for this column]
contacts:
.name:              BINARY UNCOMPRESSED DO:0 FPO:262 SZ:132/132/1.00 VC:3 ENC:RLE,DELTA_LENGTH_BYTE_ARRAY ST:[no stats for this column]
.phoneNumber:       BINARY ZSTD DO:0 FPO:394 SZ:108/90/0.83 VC:3 ENC:RLE,DELTA_LENGTH_BYTE_ARRAY ST:[no stats for this column]

    owner TV=2 RL=0 DL=0
    ----------------------------------------------------------------------------
    page 0:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats for this column] SZ:50 VC:2

    ownerPhoneNumbers TV=3 RL=1 DL=1
    ----------------------------------------------------------------------------
    page 0:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats for this column] SZ:56 VC:2
    page 1:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats for this column] SZ:9 VC:1

    contacts.name TV=3 RL=1 DL=1
    ----------------------------------------------------------------------------
    page 0:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats for this column] SZ:65 VC:2
    page 1:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats for this column] SZ:9 VC:1

    contacts.phoneNumber TV=3 RL=1 DL=2
    ----------------------------------------------------------------------------
    page 0:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats for this column] SZ:25 VC:2
    page 1:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats for this column] SZ:9 VC:1

BINARY owner
--------------------------------------------------------------------------------
*** row group 1 of 1, values 1 to 2 ***
value 1: R:0 D:0 V:Julien Le Dem
value 2: R:0 D:0 V:A. Nonymous

BINARY ownerPhoneNumbers
--------------------------------------------------------------------------------
*** row group 1 of 1, values 1 to 3 ***
value 1: R:0 D:1 V:555 123 4567
value 2: R:1 D:1 V:555 666 1337
value 3: R:0 D:0 V:<null>

BINARY contacts.name
--------------------------------------------------------------------------------
*** row group 1 of 1, values 1 to 3 ***
value 1: R:0 D:1 V:Dmitriy Ryaboy
value 2: R:1 D:1 V:Chris Aniszczyk
value 3: R:0 D:0 V:<null>

BINARY contacts.phoneNumber
--------------------------------------------------------------------------------
*** row group 1 of 1, values 1 to 3 ***
value 1: R:0 D:2 V:555 987 6543
value 2: R:1 D:1 V:<null>
value 3: R:0 D:0 V:<null>
`,
	},

	{
		scenario: "omit `-` fields",
		version:  v1,
		rows: []interface{}{
			&event{Name: "customer1", Type: "request", Value: 42.0},
			&event{Name: "customer2", Type: "access", Value: 1.0},
		},
		dump: `row group 0
--------------------------------------------------------------------------------
name:   BINARY UNCOMPRESSED DO:4 FPO:49 SZ:73/73/1.00 VC:2 ENC:PLAIN,RLE_DICTIONARY ST:[no stats for this column]
value:  DOUBLE UNCOMPRESSED DO:0 FPO:77 SZ:39/39/1.00 VC:2 ENC:PLAIN ST:[no stats for this column]

    name TV=2 RL=0 DL=0 DS: 2 DE:PLAIN
    ----------------------------------------------------------------------------
    page 0:                  DLE:RLE RLE:RLE VLE:RLE_DICTIONARY ST:[no stats for this column] CRC:[verified] SZ:5 VC:2

    value TV=2 RL=0 DL=0
    ----------------------------------------------------------------------------
    page 0:                  DLE:RLE RLE:RLE VLE:PLAIN ST:[no stats for this column] CRC:[verified] SZ:16 VC:2

BINARY name
--------------------------------------------------------------------------------
*** row group 1 of 1, values 1 to 2 ***
value 1: R:0 D:0 V:customer1
value 2: R:0 D:0 V:customer2

DOUBLE value
--------------------------------------------------------------------------------
*** row group 1 of 1, values 1 to 2 ***
value 1: R:0 D:0 V:42.0
value 2: R:0 D:0 V:1.0
`,
	},
}

func TestWriter(t *testing.T) {
	if !hasParquetTools() {
		t.Skip("parquet-tools are not installed")
	}

	for _, test := range writerTests {
		dataPageVersion := test.version
		codec := test.codec
		rows := test.rows
		dump := test.dump

		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()

			b, err := generateParquetFile(makeRows(rows),
				parquet.DataPageVersion(dataPageVersion),
				parquet.Compression(codec),
			)
			if err != nil {
				t.Logf("\n%s", string(b))
				t.Fatal(err)
			}

			if string(b) != dump {
				edits := myers.ComputeEdits(span.URIFromPath("want.txt"), dump, string(b))
				diff := fmt.Sprint(gotextdiff.ToUnified("want.txt", "got.txt", dump, edits))
				t.Errorf("\n%s", diff)
			}
		})
	}
}

func hasParquetTools() bool {
	_, err := exec.LookPath("parquet-tools")
	return err == nil
}

func parquetTools(cmd, path string) ([]byte, error) {
	p := exec.Command("parquet-tools", cmd, "--debug", "--disable-crop", path)

	output, err := p.CombinedOutput()
	if err != nil {
		return output, err
	}

	// parquet-tools has trailing spaces on some lines
	lines := bytes.Split(output, []byte("\n"))

	for i, line := range lines {
		lines[i] = bytes.TrimRight(line, " ")
	}

	return bytes.Join(lines, []byte("\n")), nil
}

func TestWriterGenerateBloomFilters(t *testing.T) {
	type Person struct {
		FirstName utf8string `parquet:"first_name"`
		LastName  utf8string `parquet:"last_name"`
	}

	err := quickCheck(func(rows []Person) bool {
		if len(rows) == 0 { // TODO: support writing files with no rows
			return true
		}

		buffer := new(bytes.Buffer)
		writer := parquet.NewWriter(buffer,
			parquet.BloomFilters(
				parquet.SplitBlockFilter("last_name"),
			),
		)
		for i := range rows {
			if err := writer.Write(&rows[i]); err != nil {
				t.Error(err)
				return false
			}
		}
		if err := writer.Close(); err != nil {
			t.Error(err)
			return false
		}

		reader := bytes.NewReader(buffer.Bytes())
		f, err := parquet.OpenFile(reader, reader.Size())
		if err != nil {
			t.Error(err)
			return false
		}
		rowGroup := f.RowGroups()[0]
		columns := rowGroup.ColumnChunks()
		firstName := columns[0]
		lastName := columns[1]

		if firstName.BloomFilter() != nil {
			t.Errorf(`"first_name" column has a bloom filter even though none were configured`)
			return false
		}

		bloomFilter := lastName.BloomFilter()
		if bloomFilter == nil {
			t.Error(`"last_name" column has no bloom filter despite being configured to have one`)
			return false
		}

		for i, row := range rows {
			if ok, err := bloomFilter.Check(parquet.ValueOf(row.LastName)); err != nil {
				t.Errorf("unexpected error checking bloom filter: %v", err)
				return false
			} else if !ok {
				t.Errorf("bloom filter does not contain value %q of row %d", row.LastName, i)
				return false
			}
		}

		return true
	})
	if err != nil {
		t.Error(err)
	}
}

func TestBloomFilterForDict(t *testing.T) {
	type testStruct struct {
		A string `parquet:"a,dict"`
	}

	schema := parquet.SchemaOf(&testStruct{})

	b := bytes.NewBuffer(nil)
	w := parquet.NewWriter(
		b,
		schema,
		parquet.BloomFilters(parquet.SplitBlockFilter("a")),
	)

	err := w.Write(&testStruct{A: "test"})
	if err != nil {
		t.Fatal(err)
	}

	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(b.Bytes()), int64(b.Len()))
	if err != nil {
		t.Fatal(err)
	}

	ok, err := f.RowGroups()[0].ColumnChunks()[0].BloomFilter().Check(parquet.ValueOf("test"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("bloom filter should have contained 'test'")
	}
}

func TestWriterRepeatedUUIDDict(t *testing.T) {
	inputID := uuid.MustParse("123456ab-0000-0000-0000-000000000000")
	records := []struct {
		List []uuid.UUID `parquet:"list,dict"`
	}{{
		[]uuid.UUID{inputID},
	}}
	schema := parquet.SchemaOf(&records[0])
	b := bytes.NewBuffer(nil)
	w := parquet.NewWriter(b, schema)
	if err := w.Write(records[0]); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(b.Bytes()), int64(b.Len()))
	if err != nil {
		t.Fatal(err)
	}

	rowbuf := make([]parquet.Row, 1)
	rows := f.RowGroups()[0].Rows()
	defer rows.Close()
	n, err := rows.ReadRows(rowbuf)
	if n == 0 {
		t.Fatalf("reading row from parquet file: %v", err)
	}
	if len(rowbuf[0]) != 1 {
		t.Errorf("expected 1 value in row, got %d", len(rowbuf[0]))
	}
	if !bytes.Equal(inputID[:], rowbuf[0][0].Bytes()) {
		t.Errorf("expected to get UUID %q back out, got %q", inputID, rowbuf[0][0].Bytes())
	}
}

func TestWriterResetWithBloomFilters(t *testing.T) {
	type Test struct {
		Value string `parquet:"value,dict"`
	}

	writer := parquet.NewWriter(new(bytes.Buffer),
		parquet.BloomFilters(
			parquet.SplitBlockFilter("value"),
		),
	)

	if err := writer.Write(&Test{Value: "foo"}); err != nil {
		t.Fatal(err)
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	writer.Reset(new(bytes.Buffer))

	if err := writer.Write(&Test{Value: "bar"}); err != nil {
		t.Fatal(err)
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}
