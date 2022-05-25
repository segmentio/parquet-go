package quick

import (
	"fmt"
	"math/rand"
	"reflect"

	"github.com/segmentio/parquet-go/internal/bits"
)

// Check is inspired by the standard quick.Check package, but enhances the
// API and tests arrays of larger sizes than the maximum of 50 hardcoded in
// testing/quick.
func Check(f interface{}) error {
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
