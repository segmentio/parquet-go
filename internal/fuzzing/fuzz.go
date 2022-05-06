//go:build go1.18
// +build go1.18

package fuzzing

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math/rand"

	"github.com/segmentio/parquet-go/deprecated"
)

func MakeRandBoolean(data []byte, count int) []bool {
	if count < 1 {
		return nil
	}
	src := rand.New(newByteSource(data))
	b := make([]bool, count)
	for i := 0; i < count; i++ {
		b[i] = src.Int63()&0x01 == 1
	}
	return b
}

func MakeRandFloat(data []byte, count int) []float32 {
	if count < 1 {
		return nil
	}
	src := rand.New(newByteSource(data))
	f := make([]float32, count)
	for i := 0; i < count; i++ {
		f[i] = src.Float32()
	}
	return f
}

func MakeRandDouble(data []byte, count int) []float64 {
	if count < 1 {
		return nil
	}
	src := rand.New(newByteSource(data))
	f := make([]float64, count)
	for i := 0; i < count; i++ {
		f[i] = src.Float64()
	}

	return f
}

func MakeRandInt32(data []byte, count int) []int32 {
	if count < 1 {
		return nil
	}

	src := rand.New(newByteSource(data))
	a := make([]int32, count)
	for i := 0; i < count; i++ {
		a[i] = int32(src.Int63())
	}
	return a
}

func MakeRandInt64(data []byte, count int) []int64 {
	if count < 1 {
		return nil
	}

	src := rand.New(newByteSource(data))
	a := make([]int64, count)
	for i := 0; i < count; i++ {
		a[i] = src.Int63()
	}
	return a
}

func MakeRandInt96(data []byte, count int) []deprecated.Int96 {
	if count < 1 {
		return nil
	}

	src := rand.New(newByteSource(data))
	a := make([]deprecated.Int96, count)
	for i := 0; i < count; i++ {
		a[i] = deprecated.Int96{
			uint32(src.Int63()),
			uint32(src.Int63()),
			uint32(src.Int63()),
		}
	}
	return a
}

// byteSource is used to compose fuzz tests from a byte array.
// This is to workaround the current stblib limitations.
type byteSource struct {
	*bytes.Reader
}

func newByteSource(data []byte) *byteSource {
	return &byteSource{
		Reader: bytes.NewReader(data),
	}
}

func (s *byteSource) Uint64() uint64 {
	var bytes [8]byte
	if _, err := s.Read(bytes[:]); err != nil && !errors.Is(err, io.EOF) {
		panic("byteSource: failed to read bytes")
	}
	return binary.BigEndian.Uint64(bytes[:])
}

func (s *byteSource) Int63() int64 {
	return int64(s.Uint64() >> 1)
}

func (s *byteSource) Seed(seed int64) {}
