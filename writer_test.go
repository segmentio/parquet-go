package parquet_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/hexops/gotextdiff"
	"github.com/hexops/gotextdiff/myers"
	"github.com/hexops/gotextdiff/span"
	"github.com/segmentio/parquet"
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

func generateParquetFile(dataPageVersion int, rows rows) ([]byte, error) {
	tmp, err := os.CreateTemp("/tmp", "*.parquet")
	if err != nil {
		return nil, err
	}
	defer tmp.Close()
	path := tmp.Name()
	defer os.Remove(path)
	//fmt.Println(path)

	if err := writeParquetFile(tmp, rows, parquet.DataPageVersion(dataPageVersion), parquet.PageBufferSize(20)); err != nil {
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

var writerTests = []struct {
	scenario string
	version  int
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
first_name:  BINARY ZSTD DO:4 FPO:55 SZ:90/72/0.80 VC:3 ENC:PLAIN,RLE_DICTIONARY [more]...
last_name:   BINARY ZSTD DO:0 FPO:94 SZ:148/121/0.82 VC:3 ENC:DELTA_BYTE_ARRAY [more]...

    first_name TV=3 RL=0 DL=0 DS: 3 DE:PLAIN
    ----------------------------------------------------------------------------
    page 0:                        DLE:RLE RLE:RLE VLE:RLE_DICTIONARY  [more]... SZ:7

    last_name TV=3 RL=0 DL=0
    ----------------------------------------------------------------------------
    page 0:                        DLE:RLE RLE:RLE VLE:DELTA_BYTE_ARRAY [more]... SZ:14
    page 1:                        DLE:RLE RLE:RLE VLE:DELTA_BYTE_ARRAY [more]... SZ:19
    page 2:                        DLE:RLE RLE:RLE VLE:DELTA_BYTE_ARRAY [more]... SZ:19

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
first_name:  BINARY ZSTD DO:4 FPO:55 SZ:86/77/0.90 VC:3 ENC:RLE_DICTIONARY,PLAIN [more]...
last_name:   BINARY ZSTD DO:0 FPO:90 SZ:163/136/0.83 VC:3 ENC:DELTA_BYTE_ARRAY [more]...

    first_name TV=3 RL=0 DL=0 DS: 3 DE:PLAIN
    ----------------------------------------------------------------------------
    page 0:                        DLE:RLE RLE:RLE VLE:RLE_DICTIONARY  [more]... VC:3

    last_name TV=3 RL=0 DL=0
    ----------------------------------------------------------------------------
    page 0:                        DLE:RLE RLE:RLE VLE:DELTA_BYTE_ARRAY [more]... VC:1
    page 1:                        DLE:RLE RLE:RLE VLE:DELTA_BYTE_ARRAY [more]... VC:1
    page 2:                        DLE:RLE RLE:RLE VLE:DELTA_BYTE_ARRAY [more]... VC:1

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
name:       BINARY UNCOMPRESSED DO:4 FPO:45 SZ:101/101/1.00 VC:10 ENC: [more]...
timestamp:  INT64 UNCOMPRESSED DO:0 FPO:105 SZ:278/278/1.00 VC:10 ENC: [more]...
value:      DOUBLE UNCOMPRESSED DO:0 FPO:383 SZ:220/220/1.00 VC:10 ENC:PLAIN [more]...

    name TV=10 RL=0 DL=0 DS: 1 DE:PLAIN
    ----------------------------------------------------------------------------
    page 0:                   DLE:RLE RLE:RLE VLE:RLE_DICTIONARY ST:[n [more]... VC:5
    page 1:                   DLE:RLE RLE:RLE VLE:RLE_DICTIONARY ST:[n [more]... VC:5

    timestamp TV=10 RL=0 DL=0
    ----------------------------------------------------------------------------
    page 0:                   DLE:RLE RLE:RLE VLE:DELTA_BINARY_PACKED  [more]... VC:2
    page 1:                   DLE:RLE RLE:RLE VLE:DELTA_BINARY_PACKED  [more]... VC:2
    page 2:                   DLE:RLE RLE:RLE VLE:DELTA_BINARY_PACKED  [more]... VC:2
    page 3:                   DLE:RLE RLE:RLE VLE:DELTA_BINARY_PACKED  [more]... VC:2
    page 4:                   DLE:RLE RLE:RLE VLE:DELTA_BINARY_PACKED  [more]... VC:2

    value TV=10 RL=0 DL=0
    ----------------------------------------------------------------------------
    page 0:                   DLE:RLE RLE:RLE VLE:PLAIN ST:[no stats f [more]... VC:2
    page 1:                   DLE:RLE RLE:RLE VLE:PLAIN ST:[no stats f [more]... VC:2
    page 2:                   DLE:RLE RLE:RLE VLE:PLAIN ST:[no stats f [more]... VC:2
    page 3:                   DLE:RLE RLE:RLE VLE:PLAIN ST:[no stats f [more]... VC:2
    page 4:                   DLE:RLE RLE:RLE VLE:PLAIN ST:[no stats f [more]... VC:2

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
contacts:
.name:              BINARY UNCOMPRESSED DO:0 FPO:4 SZ:120/120/1.00 VC:3 [more]...
.phoneNumber:       BINARY SNAPPY DO:0 FPO:124 SZ:100/96/0.96 VC:3 ENC [more]...
owner:              BINARY ZSTD DO:0 FPO:224 SZ:98/80/0.82 VC:2 ENC:DE [more]...
ownerPhoneNumbers:  BINARY GZIP DO:0 FPO:322 SZ:166/116/0.70 VC:3 ENC: [more]...

    contacts.name TV=3 RL=1 DL=1
    ----------------------------------------------------------------------------
    page 0:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats  [more]... SZ:57
    page 1:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats  [more]... SZ:17

    contacts.phoneNumber TV=3 RL=1 DL=2
    ----------------------------------------------------------------------------
    page 0:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats  [more]... SZ:33
    page 1:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats  [more]... SZ:17

    owner TV=2 RL=0 DL=0
    ----------------------------------------------------------------------------
    page 0:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats  [more]... SZ:18
    page 1:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats  [more]... SZ:16

    ownerPhoneNumbers TV=3 RL=1 DL=1
    ----------------------------------------------------------------------------
    page 0:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats  [more]... SZ:52
    page 1:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats  [more]... SZ:17

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
contacts:
.name:              BINARY UNCOMPRESSED DO:0 FPO:4 SZ:114/114/1.00 VC:3 [more]...
.phoneNumber:       BINARY SNAPPY DO:0 FPO:118 SZ:94/90/0.96 VC:3 ENC: [more]...
owner:              BINARY ZSTD DO:0 FPO:212 SZ:108/90/0.83 VC:2 ENC:D [more]...
ownerPhoneNumbers:  BINARY GZIP DO:0 FPO:320 SZ:159/109/0.69 VC:3 ENC: [more]...

    contacts.name TV=3 RL=1 DL=1
    ----------------------------------------------------------------------------
    page 0:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats  [more]... VC:2
    page 1:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats  [more]... VC:1

    contacts.phoneNumber TV=3 RL=1 DL=2
    ----------------------------------------------------------------------------
    page 0:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats  [more]... VC:2
    page 1:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats  [more]... VC:1

    owner TV=2 RL=0 DL=0
    ----------------------------------------------------------------------------
    page 0:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats  [more]... VC:1
    page 1:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats  [more]... VC:1

    ownerPhoneNumbers TV=3 RL=1 DL=1
    ----------------------------------------------------------------------------
    page 0:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats  [more]... VC:2
    page 1:  DLE:RLE RLE:RLE VLE:DELTA_LENGTH_BYTE_ARRAY ST:[no stats  [more]... VC:1

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
`,
	},
}

func TestWriter(t *testing.T) {
	if !hasParquetTools() {
		t.Skip("parquet-tools are not installed")
	}

	for _, test := range writerTests {
		dataPageVersion := test.version
		rows := test.rows
		dump := test.dump

		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()

			b, err := generateParquetFile(dataPageVersion, makeRows(rows))
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
	p := exec.Command("parquet-tools", cmd, "--debug", path)

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
