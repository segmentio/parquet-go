package bytestreamsplit

import (
	"bytes"
	"reflect"
	"testing"
)

func TestEncoding(t *testing.T) {
	e := &Encoder{}

	data := []float32{1.0, 2.0, 3.0}

	expected := []byte{0, 0, 0, 0, 0, 0, 128, 0, 64, 63, 64, 64}

	encoded := e.encode32(data)

	if !bytes.Equal(encoded, expected) {
		t.Error("encoding result not expected")
		t.Logf("got: %v", encoded)
		t.Logf("expected: %v", expected)
	}

	d := &Decoder{
		reader: bytes.NewReader(encoded),
	}

	final := make([]float32, 3)

	if err := d.read(); err != nil {
		t.Error(err)
	}

	if _, err := d.decode32(final); err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(data, final) {
		t.Error("decoding result not expected")
		t.Logf("got: %v", final)
		t.Logf("expected: %v", data)
	}
}
