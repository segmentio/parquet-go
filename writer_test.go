package parquet_test

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"

	"github.com/segmentio/parquet"
)

func generateParquetFile(rows ...interface{}) ([]byte, error) {
	tmp, err := os.CreateTemp("/tmp", "*.parquet")
	if err != nil {
		return nil, err
	}
	path := tmp.Name()
	defer os.Remove(path)

	schema := parquet.SchemaOf(rows[0])
	writer := parquet.NewWriter(tmp, schema)

	for _, row := range rows {
		if err := writer.WriteRow(row); err != nil {
			return nil, err
		}
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	return parquetTools("dump", path)
}

type firstAndLastName struct {
	FirstName string `parquet:"first_name"`
	LastName  string `parquet:"last_name"`
}

var writerTests = []struct {
	rows []interface{}
	dump string
}{
	{
		rows: []interface{}{
			&firstAndLastName{FirstName: "Han", LastName: "Solo"},
			&firstAndLastName{FirstName: "Leia", LastName: "Skywalker"},
			&firstAndLastName{FirstName: "Luke", LastName: "Skywalker"},
		},
		dump: `row group 0
--------------------------------------------------------------------------------
first_name:  BINARY UNCOMPRESSED DO:0 FPO:4 SZ:77/77/1.00 VC:3 ENC:PLAIN [more]...
last_name:   BINARY UNCOMPRESSED DO:0 FPO:81 SZ:100/100/1.00 VC:3 ENC:PLAIN [more]...

    first_name TV=3 RL=0 DL=0
    ----------------------------------------------------------------------------
    page 0:  DLE:RLE RLE:RLE VLE:PLAIN ST:[min: Luke, max: Han, num_nu [more]... VC:3

    last_name TV=3 RL=0 DL=0
    ----------------------------------------------------------------------------
    page 0:  DLE:RLE RLE:RLE VLE:PLAIN ST:[min: Solo, max: Skywalker,  [more]... VC:3

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
}

func TestWriter(t *testing.T) {
	if !hasParquetTools() {
		t.Skip("parquet-tools are not installed")
	}

	for _, test := range writerTests {
		t.Run("", func(t *testing.T) {
			t.Parallel()

			dump, err := generateParquetFile(test.rows...)
			if err != nil {
				t.Fatal(err)
			}

			if string(dump) != test.dump {
				t.Errorf("OUTPUT MISMATCH\ngot:\n%s\nwant:\n%s", string(dump), test.dump)
			}
		})
	}
}

type debugWriter struct {
	writer io.Writer
	offset int64
}

func (d *debugWriter) Write(b []byte) (int, error) {
	n, err := d.writer.Write(b)
	fmt.Printf("writing %d bytes at offset %d => %d %v\n", len(b), d.offset, n, err)
	d.offset += int64(n)
	return n, err
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
