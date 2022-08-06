package piotest

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/segmentio/parquet-go/pio"
)

func TestReaderAt(t *testing.T, makeFile func([]byte) (io.ReaderAt, func(), error)) {
	data := make([]byte, 1e6)
	prng := rand.New(rand.NewSource(0))
	prng.Read(data)

	file, teardown, err := makeFile(data)
	if err != nil {
		t.Fatal(err)
	}
	defer teardown()

	const bufferSize = 8192
	ops := make([]pio.Op, 219)
	tmp := make([]byte, bufferSize)

	buffers := make([][]byte, len(ops))
	for i := range buffers {
		buffers[i] = make([]byte, bufferSize)
	}

	reader := bytes.NewReader(data)

	for n := 1; n < len(ops); n++ {
		t.Run(fmt.Sprintf("N=%d", n), func(t *testing.T) {
			for i := range ops[:n] {
				buffers[i] = buffers[i][:prng.Intn(bufferSize)]

				ops[i].Data = buffers[i]
				ops[i].Off = prng.Int63n(int64(len(data)))
				ops[i].Err = nil
			}

			pio.MultiReadAt(file, ops[:n])

			for i := range ops[:n] {
				op := &ops[i]
				offset := op.Off
				length := int64(len(buffers[i]))

				rn, err := reader.ReadAt(tmp[:length], offset)
				switch {
				case err != op.Err:
					t.Fatalf("error mismatch for operation at index %d: want=%v got=%v (read=%d/%d offset=%d size=%d)", i, err, op.Err, len(op.Data), rn, offset, reader.Size())
				case rn != len(op.Data):
					t.Fatalf("length mismatch for operation at index %d: want=%d got=%d", i, rn, len(op.Data))
				case !bytes.Equal(tmp[:rn], op.Data):
					t.Fatalf("data mismatch for operation at index %d:\nwant = %q\ngot  = %q\n", i, tmp[:rn], op.Data)
				}
			}
		})
	}
}
