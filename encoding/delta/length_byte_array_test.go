package delta

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/segmentio/parquet-go/encoding/plain"
)

func TestDecodeLengthByteArray(t *testing.T) {
	const characters = "1234567890qwertyuiopasdfghjklzxcvbnm"
	const numValues = 1000
	src := []byte{}
	dst := []byte{}
	lengths := []int32{}

	for i := 0; i < numValues; i++ {
		n := i % len(characters)
		src = append(src, characters[:n]...)
		lengths = append(lengths, int32(n))
	}

	size := plain.ByteArrayLengthSize*numValues + len(src)
	dst = make([]byte, size, size+lengthByteArrayPadding)
	decodeLengthByteArray(dst, src, lengths)

	index := 0
	err := plain.RangeByteArrays(dst, func(got []byte) error {
		want := characters[:index%len(characters)]

		if want != string(got) {
			return fmt.Errorf("wrong value at index %d: want=%q got=%q", index, want, got)
		}

		index++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkDecodeLengthByteArray(b *testing.B) {
	for _, maxLen := range []int{0, 10, 20, 100, 1000} {
		b.Run(fmt.Sprintf("maxLen=%d", maxLen), func(b *testing.B) {
			lengths := make([]int32, 1000)
			totalLength := 0
			prng := rand.New(rand.NewSource(int64(maxLen)))

			if maxLen > 0 {
				for i := range lengths {
					lengths[i] = prng.Int31n(int32(maxLen)) + 1
					totalLength += int(lengths[i])
				}
			}

			size := plain.ByteArrayLengthSize*len(lengths) + totalLength
			dst := make([]byte, size, size+lengthByteArrayPadding)
			src := make([]byte, totalLength)
			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				decodeLengthByteArray(dst, src, lengths)
			}
		})
	}
}
