package bits_test

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/segmentio/parquet-go/internal/bits"
)

func TestNearestPowerOfTwo(t *testing.T) {
	for _, test := range []struct {
		input  uint32
		output uint32
	}{
		{input: 0, output: 0},
		{input: 1, output: 1},
		{input: 2, output: 2},
		{input: 3, output: 4},
		{input: 4, output: 4},
		{input: 5, output: 8},
		{input: 6, output: 8},
		{input: 7, output: 8},
		{input: 8, output: 8},
		{input: 30, output: 32},
	} {
		t.Run(fmt.Sprintf("NearestPowerOfTwo(%d)", test.input), func(t *testing.T) {
			if nextPow2 := bits.NearestPowerOfTwo32(test.input); nextPow2 != test.output {
				t.Errorf("wrong 32 bits value: want=%d got=%d", test.output, nextPow2)
			}
			if nextPow2 := bits.NearestPowerOfTwo64(uint64(test.input)); nextPow2 != uint64(test.output) {
				t.Errorf("wrong 64 bits value: want=%d got=%d", test.output, nextPow2)
			}
		})
	}
}

func TestBitCount(t *testing.T) {
	for _, test := range []struct {
		bytes int
		bits  uint
	}{
		{bytes: 0, bits: 0},
		{bytes: 1, bits: 8},
		{bytes: 2, bits: 16},
		{bytes: 3, bits: 24},
		{bytes: 4, bits: 32},
		{bytes: 5, bits: 40},
		{bytes: 6, bits: 48},
	} {
		t.Run(fmt.Sprintf("BitCount(%d)", test.bytes), func(t *testing.T) {
			if bits := bits.BitCount(test.bytes); bits != test.bits {
				t.Errorf("wrong bit count: want=%d got=%d", test.bits, bits)
			}
		})
	}
}

func TestByteCount(t *testing.T) {
	for _, test := range []struct {
		bits  uint
		bytes int
	}{
		{bits: 0, bytes: 0},
		{bits: 1, bytes: 1},
		{bits: 7, bytes: 1},
		{bits: 8, bytes: 1},
		{bits: 9, bytes: 2},
		{bits: 30, bytes: 4},
		{bits: 63, bytes: 8},
	} {
		t.Run(fmt.Sprintf("ByteCount(%d)", test.bits), func(t *testing.T) {
			if bytes := bits.ByteCount(test.bits); bytes != test.bytes {
				t.Errorf("wrong byte count: want=%d got=%d", test.bytes, bytes)
			}
		})
	}
}

func TestRound(t *testing.T) {
	for _, test := range []struct {
		bits  uint
		round uint
	}{
		{bits: 0, round: 0},
		{bits: 1, round: 8},
		{bits: 8, round: 8},
		{bits: 9, round: 16},
		{bits: 30, round: 32},
		{bits: 63, round: 64},
	} {
		t.Run(fmt.Sprintf("Round(%d)", test.bits), func(t *testing.T) {
			if round := bits.Round(test.bits); round != test.round {
				t.Errorf("wrong rounded bit count: want=%d got=%d", test.round, round)
			}
		})
	}
}

var benchmarkBufferSizes = [...]int{
	4 * 1024,
	256 * 1024,
	2048 * 1024,
}

func forEachBenchmarkBufferSize(b *testing.B, f func(*testing.B, int)) {
	for _, bufferSize := range benchmarkBufferSizes {
		b.Run(fmt.Sprintf("%dKiB", bufferSize/1024), func(b *testing.B) {
			b.SetBytes(int64(bufferSize))
			f(b, bufferSize)
		})
	}
}

// quickCheck is inspired by the standard quick.Check package, but enhances the
// API and tests arrays of larger sizes than the maximum of 50 hardcoded in
// testing/quick.
func quickCheck(f interface{}) error {
	v := reflect.ValueOf(f)
	r := rand.New(rand.NewSource(0))

	var makeArray func(int) interface{}
	switch t := v.Type().In(0); t.Elem().Kind() {
	case reflect.Bool:
		makeArray = func(n int) interface{} {
			v := make([]bool, n)
			for i := range v {
				v[i] = r.Int()%2 != 0
			}
			return v
		}

	case reflect.Int32:
		makeArray = func(n int) interface{} {
			v := make([]int32, n)
			for i := range v {
				v[i] = r.Int31()
			}
			return v
		}

	case reflect.Int64:
		makeArray = func(n int) interface{} {
			v := make([]int64, n)
			for i := range v {
				v[i] = r.Int63()
			}
			return v
		}

	case reflect.Uint32:
		makeArray = func(n int) interface{} {
			v := make([]uint32, n)
			for i := range v {
				v[i] = r.Uint32()
			}
			return v
		}
	case reflect.Uint64:
		makeArray = func(n int) interface{} {
			v := make([]uint64, n)
			for i := range v {
				v[i] = r.Uint64()
			}
			return v
		}

	case reflect.Float32:
		makeArray = func(n int) interface{} {
			v := make([]float32, n)
			for i := range v {
				v[i] = r.Float32()
			}
			return v
		}

	case reflect.Float64:
		makeArray = func(n int) interface{} {
			v := make([]float64, n)
			for i := range v {
				v[i] = r.Float64()
			}
			return v
		}

	case reflect.Uint8:
		makeArray = func(n int) interface{} {
			v := make([]byte, n)
			r.Read(v)
			return v
		}

	case reflect.Array:
		e := t.Elem()
		if e.Elem().Kind() == reflect.Uint8 && e.Len() == 16 {
			makeArray = func(n int) interface{} {
				v := make([]byte, n*16)
				r.Read(v)
				return bits.BytesToUint128(v)
			}
		}
	}

	if makeArray == nil {
		panic("cannot run quick check on function with input of type " + v.Type().In(0).String())
	}

	for _, n := range [...]int{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
		20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
		30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
		99, 100, 101,
		127, 128, 129,
		255, 256, 257,
		1000, 1023, 1024, 1025,
		2000, 2095, 2048, 2049,
		4000, 4095, 4096, 4097,
	} {
		for i := 0; i < 3; i++ {
			in := makeArray(n)
			ok := v.Call([]reflect.Value{reflect.ValueOf(in)})
			if !ok[0].Bool() {
				return fmt.Errorf("test #%d: failed on input of size %d: %#v\n", i+1, n, in)
			}
		}
	}
	return nil
}
