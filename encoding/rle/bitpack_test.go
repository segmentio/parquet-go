package rle

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/segmentio/parquet/internal/bits"
)

func TestBitPack(t *testing.T) {
	data := make([]uint64, 4096)
	prng := rand.New(rand.NewSource(0))
	prng.Read(bits.Uint64ToBytes(data))

	buf := new(bytes.Buffer)
	enc := bitPackRunEncoder{}
	dec := bitPackRunDecoder{}
	tmp := [1]uint64{}

	for bitWidth := uint(1); bitWidth <= 64; bitWidth++ {
		t.Run(fmt.Sprintf("bitWidth=%d", bitWidth), func(t *testing.T) {
			enc.reset(buf, bitWidth)
			dec.reset(buf, bitWidth)

			if err := enc.encode(bits.Uint64ToBytes(data), 64); err != nil {
				t.Fatal("encoding:", err)
			}

			mask := uint64((1 << bitWidth) - 1)

			for i, value := range data {
				n, err := dec.decode(bits.Uint64ToBytes(tmp[:]), 64)
				if err != nil {
					t.Fatal("decoding:", err)
				}
				if n != 1 {
					t.Fatalf("wrong number of values decoded at index %d/%d: want=1 got=%d", i, len(data), n)
				}
				v1 := mask & value
				v2 := mask & tmp[0]
				if v1 != v2 {
					t.Fatalf("wrong value at index %d/%d: want=%08b got=%08b (mask=%08b)", i, len(data), v1, v2, mask)
				}
			}
		})
	}
}
