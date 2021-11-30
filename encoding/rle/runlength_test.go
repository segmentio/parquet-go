package rle

import (
	"bytes"
	"io"
	"math"
	. "math/bits"
	"testing"

	"github.com/segmentio/parquet/internal/bits"
)

func TestRunLength(t *testing.T) {
	buf := new(bytes.Buffer)
	enc := runLengthRunEncoder{}
	dec := runLengthRunDecoder{}
	data := [1]int16{}

	for value := 0; value < math.MaxInt16; value += 31 {
		t.Run("", func(t *testing.T) {
			numValues := uint(10)
			bitWidth := uint(Len16(uint16(value)))
			if bitWidth == 0 {
				bitWidth = 1
			}
			enc.reset(buf, bitWidth)
			dec.reset(buf, bitWidth, numValues)

			data[0] = int16(value)

			if err := enc.encode(bits.Int16ToBytes(data[:]), 16); err != nil {
				t.Fatal("encoding:", err)
			}

			for i := uint(0); i < numValues; i++ {
				data[0] = 0
				n, err := dec.decode(bits.Int16ToBytes(data[:]), 16)
				if err != nil {
					t.Fatal("decoding:", err)
				}
				if n != 1 {
					t.Fatal("wrong number of values decoded:", n)
				}
				if data[0] != int16(value) {
					t.Fatal("wrong value decoded:", data[0])
				}
			}

			if n, err := dec.decode(bits.Int16ToBytes(data[:]), 16); err != io.EOF {
				t.Fatal("non-EOF error returned after decoding all the values:", err)
			} else if n != 0 {
				t.Fatal("non-zero number of values decoded at EOF:", n)
			}
		})
	}
}
