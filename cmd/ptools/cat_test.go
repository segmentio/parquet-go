// Test file for ptools-cat. It effectively runs ptools-cat and parquet-cat
// against the same input and compares stderr, expecting byte-to-byte matches.
//
// At the moment, it uses xitongsys/parquet-go to generate the input files.
// When the implementation of this library matures, it should support a wider
// range of inputs.
package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"

	"github.com/pmezard/go-difflib/difflib"
	"github.com/segmentio/centrifuge-traces/parquet/internal/test"
	"github.com/stretchr/testify/assert"
	localref "github.com/xitongsys/parquet-go-source/local"
	writerref "github.com/xitongsys/parquet-go/writer"
)

const parquetToolsBinary string = "parquet-tools"

func isParquetToolsMissing() bool {
	_, err := exec.LookPath(parquetToolsBinary)
	return err != nil
}

func refcat(p string) {
	cmd := exec.Command(parquetToolsBinary, "cat", p)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
}

func refdump(p string) {
	cmd := exec.Command(parquetToolsBinary, "dump", p)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
}

func refmeta(p string) {
	cmd := exec.Command(parquetToolsBinary, "meta", p)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
}

func captureStdout(t *testing.T, f func()) string {
	// TODO: need a concurrent-safe way of doing that
	old := os.Stdout // keep backup of the real stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	result := make(chan string, 1)

	go func() {
		var buf bytes.Buffer
		defer func() {
			result <- buf.String()
			close(result)
		}()
		for {
			n, err := io.Copy(&buf, r)
			if n == 0 && err == nil {
				// write pipe has been closed.
				return
			}
			if err != nil && err != io.EOF {
				log.Println("Error piping stdout:", err)
				return
			}
		}
	}()

	f()

	test.Close(t, w)
	os.Stdout = old // restoring the real stdout
	out := <-result

	return out
}

func TestCat(t *testing.T) {
	if isParquetToolsMissing() {
		t.Skip("parquet-tools is missing from PATH")
	}

	scenarios := []struct {
		name    string
		records func() (interface{}, []interface{})
	}{
		{
			name: "all required",
			records: func() (interface{}, []interface{}) {
				type Record struct {
					Intval int64 `parquet:"name=intval, type=INT64"`
				}
				return new(Record), []interface{}{
					Record{Intval: 1},
					Record{Intval: 2},
					Record{Intval: 3},
				}
			},
		},
		{
			name: "int32",
			records: func() (interface{}, []interface{}) {
				type Record struct {
					Intval int32 `parquet:"name=intval, type=INT32"`
				}
				return new(Record), []interface{}{
					Record{1},
					Record{2},
					Record{3},
				}
			},
		},
		{
			name: "simple",
			records: func() (interface{}, []interface{}) {
				type Record struct {
					Names  []string `parquet:"name=names, type=LIST, valuetype=UTF8"`
					Intval int64    `parquet:"name=intval, type=INT64"`
				}
				return new(Record), []interface{}{
					Record{Names: []string{"one"}, Intval: 1},
					Record{Names: []string{}, Intval: 2},
					Record{Names: []string{"two", "three"}, Intval: 3},
				}
			},
		},
		{
			name: "tags",
			records: func() (interface{}, []interface{}) {
				type TagParquet struct {
					Name  string `parquet:"name=name, type=UTF8"`
					Value string `parquet:"name=value, type=UTF8"`
				}
				type Record struct {
					Tags []TagParquet `parquet:"name=tags, type=LIST"`
				}
				return new(Record), []interface{}{
					Record{
						Tags: []TagParquet{
							{
								Name:  "name1.1",
								Value: "value1.1",
							},
							{
								Name:  "name1.2",
								Value: "value1.2",
							},
						},
					},
					Record{
						Tags: []TagParquet{
							{
								Name:  "name2.1",
								Value: "value2.1",
							},
						},
					},
					Record{
						Tags: []TagParquet{},
					},
				}
			},
		},
		{
			name: "empty binaries",
			records: func() (interface{}, []interface{}) {
				type Record struct {
					ExchangeRequestBody string `parquet:"name=exchange_request_body, type=BYTE_ARRAY"`
				}
				return new(Record), []interface{}{
					Record{},
					Record{ExchangeRequestBody: "Something"},
					Record{},
					Record{ExchangeRequestBody: "Something else"},
					Record{},
				}
			},
		},
		{
			name: "kv",
			records: func() (interface{}, []interface{}) {
				type Record struct {
					KV map[string]string `parquet:"name=kv, type=MAP, keytype=UTF8, valuetype=UTF8"`
				}
				return new(Record), []interface{}{
					Record{},
					Record{KV: map[string]string{}},
					Record{KV: map[string]string{"one": "un"}},
					Record{KV: map[string]string{"one": "un", "two": "deux"}},
				}
			},
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			test.TempDir(func(dir string) {
				structure, records := scenario.records()
				basename := strings.ReplaceAll(t.Name(), " ", "_")
				basename = strings.ReplaceAll(basename, "/", "_")
				p := path.Join(dir, basename+".parquet")
				dst, _ := localref.NewLocalFileWriter(p)
				writer, _ := writerref.NewParquetWriter(dst, structure, 1)
				for _, record := range records {
					assert.NoError(t, writer.Write(record))
				}
				assert.NoError(t, writer.WriteStop())
				assert.NoError(t, dst.Close())

				diffWithParquetTools(t, p)
			})
		})
	}
}

func TestStageFile(t *testing.T) {
	if isParquetToolsMissing() {
		t.Skip("parquet-tools is missing from PATH")
	}
	diffWithParquetTools(t, "../../examples/stage-small.parquet")
}

func diffWithParquetTools(t *testing.T, path string) {
	mycat := captureStdout(t, func() {
		catCommand(catFlags{Debug: true}, path)
	})

	thecat := captureStdout(t, func() {
		refcat(path)
	})

	out, err := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
		A: difflib.SplitLines(mycat),
		B: difflib.SplitLines(thecat),
	})
	assert.NoError(t, err)
	if len(out) != 0 {
		fmt.Println("-------------------- OUT  --------------------")
		fmt.Println(mycat)
		fmt.Println("-------------------- REF  --------------------")
		fmt.Println(thecat)
		fmt.Println("-------------------- DIFF --------------------")
		fmt.Println(out)
		fmt.Println("-------------------- DUMP --------------------")
		fmt.Println(captureStdout(t, func() { refdump(path) }))
		fmt.Println("-------------------- META --------------------")
		fmt.Println(captureStdout(t, func() { refmeta(path) }))

		t.Fail()
	}
}
