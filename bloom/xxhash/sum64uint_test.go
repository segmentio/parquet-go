package xxhash_test

import (
	"encoding/binary"
	"testing"
	"testing/quick"

	"github.com/segmentio/parquet-go/bloom/xxhash"
)

func TestSumUint8(t *testing.T) {
	b := [1]byte{0: 42}
	h := xxhash.Sum64Uint8(42)
	x := xxhash.Sum64(b[:])
	if h != x {
		t.Errorf("got 0x%x; want 0x%x", h, x)
	}
}

func TestSumUint16(t *testing.T) {
	b := [2]byte{0: 42}
	h := xxhash.Sum64Uint16(42)
	x := xxhash.Sum64(b[:])
	if h != x {
		t.Errorf("got 0x%x; want 0x%x", h, x)
	}
}

func TestSumUint32(t *testing.T) {
	b := [4]byte{0: 42}
	h := xxhash.Sum64Uint32(42)
	x := xxhash.Sum64(b[:])
	if h != x {
		t.Errorf("got 0x%x; want 0x%x", h, x)
	}
}

func TestSumUint64(t *testing.T) {
	b := [8]byte{0: 42}
	h := xxhash.Sum64Uint64(42)
	x := xxhash.Sum64(b[:])
	if h != x {
		t.Errorf("got 0x%x; want 0x%x", h, x)
	}
}

func TestMultiSum64Uint8(t *testing.T) {
	f := func(v []uint8) bool {
		h := make([]uint64, len(v))
		n := xxhash.MultiSum64Uint8(h, v)
		if n != len(v) {
			t.Errorf("return value mismatch: got %d; want %d", n, len(v))
			return false
		}
		for i := range h {
			b := [1]byte{byte(v[i])}
			x := xxhash.Sum64(b[:])
			if h[i] != x {
				t.Errorf("sum at index %d mismatch: got 0x%x; want 0x%x", i, h[i], x)
				return false
			}
		}
		return true
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestMultiSum64Uint16(t *testing.T) {
	f := func(v []uint16) bool {
		h := make([]uint64, len(v))
		n := xxhash.MultiSum64Uint16(h, v)
		if n != len(v) {
			t.Errorf("return value mismatch: got %d; want %d", n, len(v))
			return false
		}
		for i := range h {
			b := [2]byte{}
			binary.LittleEndian.PutUint16(b[:], v[i])
			x := xxhash.Sum64(b[:])
			if h[i] != x {
				t.Errorf("sum at index %d mismatch: got 0x%x; want 0x%x", i, h[i], x)
				return false
			}
		}
		return true
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestMultiSum64Uint32(t *testing.T) {
	f := func(v []uint32) bool {
		h := make([]uint64, len(v))
		n := xxhash.MultiSum64Uint32(h, v)
		if n != len(v) {
			t.Errorf("return value mismatch: got %d; want %d", n, len(v))
			return false
		}
		for i := range h {
			b := [4]byte{}
			binary.LittleEndian.PutUint32(b[:], v[i])
			x := xxhash.Sum64(b[:])
			if h[i] != x {
				t.Errorf("sum at index %d mismatch: got 0x%x; want 0x%x", i, h[i], x)
				return false
			}
		}
		return true
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestMultiSum64Uint64(t *testing.T) {
	f := func(v []uint64) bool {
		h := make([]uint64, len(v))
		n := xxhash.MultiSum64Uint64(h, v)
		if n != len(v) {
			t.Errorf("return value mismatch: got %d; want %d", n, len(v))
			return false
		}
		for i := range h {
			b := [8]byte{}
			binary.LittleEndian.PutUint64(b[:], v[i])
			x := xxhash.Sum64(b[:])
			if h[i] != x {
				t.Errorf("sum at index %d mismatch: got 0x%x; want 0x%x", i, h[i], x)
				return false
			}
		}
		return true
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func BenchmarkMultiSum64Uint64(b *testing.B) {
	for _, bb := range benchmarks {
		in := make([]uint64, bb.n/8)
		for i := range in {
			in[i] = uint64(i)
		}
		out := make([]uint64, len(in))
		b.Run(bb.name, func(b *testing.B) {
			b.SetBytes(bb.n)
			for i := 0; i < b.N; i++ {
				_ = xxhash.MultiSum64Uint64(out, in)
			}
		})
	}
}
