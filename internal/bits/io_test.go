package bits_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/segmentio/parquet/internal/bits"
)

func TestReader(t *testing.T) {
	want := []byte{
		0b10101010, 0b10101010, 0b10101010, 0b10101010,
		0b10101010, 0b10101010, 0b10101010, 0b10101010,

		0b10101010, 0b10101010, 0b10101010, 0b10101010,
		0b10101010, 0b10101010, 0b10101010, 0b00000010,
	}
	data := make([]byte, len(want))

	r := new(bits.Reader)
	r.Reset(bytes.NewReader(want))

	for i := 0; i < 8*len(data); i++ {
		j := i / 8
		k := i % 8

		b, err := r.ReadBit()
		if err != nil {
			t.Fatal(err)
		}

		data[j] |= byte(b) << k
	}

	_, err := r.ReadBit()
	if err != io.EOF {
		t.Errorf("unexpected error returned after reading all the bits: %v", err)
	}

	if !bytes.Equal(data, want) {
		t.Errorf("data = %08b", data)
		t.Errorf("want = %08b", want)
	}
}

func TestWriter(t *testing.T) {
	b := new(bytes.Buffer)
	w := new(bits.Writer)
	w.Reset(b)

	for i := 0; i < 123; i++ {
		w.WriteBit(i & 1)
	}

	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	data := b.Bytes()
	want := []byte{
		0b10101010, 0b10101010, 0b10101010, 0b10101010,
		0b10101010, 0b10101010, 0b10101010, 0b10101010,

		0b10101010, 0b10101010, 0b10101010, 0b10101010,
		0b10101010, 0b10101010, 0b10101010, 0b00000010,
	}

	if !bytes.Equal(data, want) {
		t.Errorf("data = %08b", data)
		t.Errorf("want = %08b", want)
	}
}
