package plain_test

import (
	"bytes"
	"testing"

	"github.com/segmentio/parquet-go/encoding/plain"
)

func TestAppendBoolean(t *testing.T) {
	values := []byte{}

	for i := 0; i < 100; i++ {
		values = plain.AppendBoolean(values, i, (i%2) != 0)
	}

	if !bytes.Equal(values, []byte{
		0b10101010,
		0b10101010,
		0b10101010,
		0b10101010,
		0b10101010,
		0b10101010,
		0b10101010,
		0b10101010,
		0b10101010,
		0b10101010,
		0b10101010,
		0b10101010,
		0b00001010,
	}) {
		t.Errorf("%08b\n", values)
	}
}

func TestValidateByteArray(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		var b []byte
		b = plain.AppendByteArrayString(b, "Hello")
		b = plain.AppendByteArrayString(b, "World")
		b = plain.AppendByteArrayString(b, "!")

		if err := plain.ValidateByteArray(b); err != nil {
			t.Error(err)
		}
	})

	t.Run("errTooShort", func(t *testing.T) {
		var b []byte
		b = plain.AppendByteArrayString(b, "Hello")
		b = plain.AppendByteArrayString(b, "World")
		b = plain.AppendByteArrayString(b, "!")

		if plain.ValidateByteArray(b[:len(b)-1]) == nil {
			t.Error("expected non-nil error")
		}
	})
}
