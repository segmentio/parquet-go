package parquet_test

import (
	"bytes"
	"testing"

	"github.com/segmentio/parquet"
)

func TestRowGroupColumn(t *testing.T) {
	for _, test := range pageReadWriteTests {
		t.Run(test.scenario, func(t *testing.T) {
			buf := new(bytes.Buffer)
			dec := parquet.Plain.NewDecoder(buf)
			enc := parquet.Plain.NewEncoder(buf)
			pr := test.typ.NewPageReader(dec, 32)
			pw := test.typ.NewRowGroupColumn(1024)

			for _, values := range test.values {
				t.Run("", func(t *testing.T) {
					defer func() {
						buf.Reset()
						dec.Reset(buf)
						enc.Reset(buf)
						pr.Reset(dec)
						pw.Reset()
					}()
					testPageReadWrite(t, pr, pw, enc, values)
				})
			}
		})
	}
}
