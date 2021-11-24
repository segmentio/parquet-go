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

func scanParquetFile(f *os.File) error {
	s, err := f.Stat()
	if err != nil {
		return err
	}

	p, err := parquet.OpenFile(f, s.Size())
	if err != nil {
		return err
	}

	return scanParquetColumns(p.Root())
}

func scanParquetColumns(col *parquet.Column) error {
	const bufferSize = 1024
	chunks := col.Chunks()

	for chunks.Next() {
		pages := chunks.Pages()
		dictionary := (parquet.Dictionary)(nil)

		for pages.Next() {
			switch header := pages.PageHeader().(type) {
			case parquet.DictionaryPageHeader:
				decoder := header.Encoding().NewDecoder(pages.PageData())
				dictionary = col.Type().NewDictionary(0)

				if err := dictionary.ReadFrom(decoder); err != nil {
					return err
				}

			case parquet.DataPageHeader:
				var pageReader parquet.PageReader
				var pageData = header.Encoding().NewDecoder(pages.PageData())

				if dictionary != nil {
					pageReader = parquet.NewIndexedPageReader(pageData, bufferSize, dictionary)
				} else {
					pageReader = col.Type().NewPageReader(pageData, bufferSize)
				}

				dataPageReader := parquet.NewDataPageReader(
					header.RepetitionLevelEncoding().NewDecoder(pages.RepetitionLevels()),
					header.DefinitionLevelEncoding().NewDecoder(pages.DefinitionLevels()),
					header.NumValues(),
					pageReader,
					col.MaxRepetitionLevel(),
					col.MaxDefinitionLevel(),
					bufferSize,
				)

				for {
					v, err := dataPageReader.ReadValue()
					if err != nil {
						if err != io.EOF {
							return err
						}
						break
					}
					fmt.Printf("> %v\n", v)
				}

			default:
				return fmt.Errorf("unsupported page header type: %#v", header)
			}

			if err := pages.Err(); err != nil {
				return err
			}
		}
	}

	for _, child := range col.Columns() {
		if err := scanParquetColumns(child); err != nil {
			return err
		}
	}

	return nil
}

func generateParquetFile(dataPageVersion int, rows ...interface{}) ([]byte, error) {
	tmp, err := os.CreateTemp("/tmp", "*.parquet")
	if err != nil {
		return nil, err
	}
	defer tmp.Close()
	path := tmp.Name()
	defer os.Remove(path)

	schema := parquet.SchemaOf(rows[0])
	writer := parquet.NewWriter(tmp, schema, parquet.DataPageVersion(dataPageVersion))

	for _, row := range rows {
		if err := writer.WriteRow(row); err != nil {
			return nil, err
		}
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	if err := scanParquetFile(tmp); err != nil {
		return nil, err
	}

	return parquetTools("dump", path)
}

type firstAndLastName struct {
	FirstName string `parquet:"first_name,dict"`
	LastName  string `parquet:"last_name,dict"`
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
first_name:  BINARY UNCOMPRESSED DO:0 FPO:4 SZ:74/74/1.00 VC:3 ENC:PLAIN [more]...
last_name:   BINARY UNCOMPRESSED DO:0 FPO:78 SZ:97/97/1.00 VC:3 ENC:PLAIN [more]...

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

	{
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
	},
}

func TestWriter(t *testing.T) {
	if !hasParquetTools() {
		t.Skip("parquet-tools are not installed")
	}

	for _, version := range []int{1, 2} {
		dataPageVersion := version
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			t.Parallel()

			for _, test := range writerTests {
				rows := test.rows
				dump := test.dump
				t.Run("", func(t *testing.T) {
					t.Parallel()

					b, err := generateParquetFile(dataPageVersion, rows...)
					if err != nil {
						t.Logf("\n%s", string(b))
						t.Fatal(err)
					}

					if string(b) != dump {
						t.Errorf("OUTPUT MISMATCH\ngot:\n%s\nwant:\n%s", string(b), dump)
					}
				})
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
