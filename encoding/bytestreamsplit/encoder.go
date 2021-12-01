package bytestreamsplit

import (
	"io"
	"math"

	"github.com/segmentio/parquet/encoding"
)

// Byte Stream Split: (BYTE_STREAM_SPLIT = 9)
// Supported Types: FLOAT DOUBLE
//
// This encoding does not reduce the size of the data but can lead to a significantly better compression ratio and speed when a compression algorithm is used afterwards.
//
// This encoding creates K byte-streams of length N where K is the size in bytes of the data type and N is the number of elements in the data sequence.
// The bytes of each value are scattered to the corresponding streams. The 0-th byte goes to the 0-th stream, the 1-st byte goes to the 1-st stream and so on.
// The streams are concatenated in the following order: 0-th stream, 1-st stream, etc.
//
// Example: Original data is three 32-bit floats and for simplicity we look at their raw representation.
//
//        Element 0      Element 1      Element 2
// Bytes  AA BB CC DD    00 11 22 33    A3 B4 C5 D6
// After applying the transformation, the data has the following representation:
//
// Bytes  AA 00 A3 BB 11 B4 CC 22 C5 DD 33 D6
//
type Encoder struct {
	encoding.NotSupportedEncoder
	writer io.Writer
}

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{writer: w}
}

func (e *Encoder) Write(b []byte) (int, error) {
	return e.writer.Write(b)
}

func (e *Encoder) Reset(w io.Writer) {
	e.writer = w
}

func (e *Encoder) EncodeFloat(data []float32) error {
	_, err := e.writer.Write(e.encode32(data))
	return err
}

func (e *Encoder) EncodeDouble(data []float64) error {
	_, err := e.writer.Write(e.encode64(data))
	return err
}

func (e *Encoder) encode32(data []float32) []byte {
	length := len(data)
	if length == 0 {
		return []byte{}
	}

	buffer := make([]byte, length*4)

	for i, f := range data {
		bits := math.Float32bits(f)
		buffer[i] = byte(bits)
		buffer[i+length] = byte(bits >> 8)
		buffer[i+length*2] = byte(bits >> 16)
		buffer[i+length*3] = byte(bits >> 24)
	}

	return buffer
}

func (e *Encoder) encode64(data []float64) []byte {
	length := len(data)
	if length == 0 {
		return []byte{}
	}

	buffer := make([]byte, length*8)

	for i, f := range data {
		bits := math.Float64bits(f)
		buffer[i] = byte(bits)
		buffer[i+length] = byte(bits >> 8)
		buffer[i+length*2] = byte(bits >> 16)
		buffer[i+length*3] = byte(bits >> 24)
		buffer[i+length*4] = byte(bits >> 32)
		buffer[i+length*5] = byte(bits >> 40)
		buffer[i+length*6] = byte(bits >> 48)
		buffer[i+length*7] = byte(bits >> 56)
	}

	return buffer
}
