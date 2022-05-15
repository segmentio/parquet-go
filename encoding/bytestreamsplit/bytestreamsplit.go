package bytestreamsplit

import (
	"math"

	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/format"
	"github.com/segmentio/parquet-go/internal/bits"
)

// This encoder implements a version of the Byte Stream Split encoding as described
// in https://github.com/apache/parquet-format/blob/master/Encodings.md#byte-stream-split-byte_stream_split--9
type Encoding struct {
	encoding.NotSupported
}

func (e *Encoding) String() string {
	return "BYTE_STREAM_SPLIT"
}

func (e *Encoding) Encoding() format.Encoding {
	return format.ByteStreamSplit
}

func (e *Encoding) EncodeFloat(dst []byte, src []float32) ([]byte, error) {
	n := 4 * len(src)
	if cap(dst) < n {
		dst = make([]byte, n)
	} else {
		dst = dst[:n]
	}

	b0 := dst[0*len(src) : 1*len(src)]
	b1 := dst[1*len(src) : 2*len(src)]
	b2 := dst[2*len(src) : 3*len(src)]
	b3 := dst[3*len(src) : 4*len(src)]

	for i, f := range src {
		bits := math.Float32bits(f)
		b0[i] = byte(bits)
		b1[i] = byte(bits >> 8)
		b2[i] = byte(bits >> 16)
		b3[i] = byte(bits >> 24)
	}

	return dst, nil
}

func (e *Encoding) EncodeDouble(dst, src []byte) ([]byte, error) {
	if (len(src) % 8) != 0 {
		return dst[:0], encoding.ErrInvalidInputSize(e, "DOUBLE", len(src))
	}

	dst = resize(dst, len(src))
	n := len(src) / 8
	b0 := dst[0*n : 1*n]
	b1 := dst[1*n : 2*n]
	b2 := dst[2*n : 3*n]
	b3 := dst[3*n : 4*n]
	b4 := dst[4*n : 5*n]
	b5 := dst[5*n : 6*n]
	b6 := dst[6*n : 7*n]
	b7 := dst[7*n : 8*n]

	for i, v := range bits.BytesToUint64(src) {
		b0[i] = byte(v >> 0)
		b1[i] = byte(v >> 8)
		b2[i] = byte(v >> 16)
		b3[i] = byte(v >> 24)
		b4[i] = byte(v >> 32)
		b5[i] = byte(v >> 40)
		b6[i] = byte(v >> 48)
		b7[i] = byte(v >> 56)
	}

	return dst, nil
}

func (e *Encoding) DecodeFloat(dst []float32, src []byte) ([]float32, error) {
	if (len(src) % 4) != 0 {
		return dst[:0], encoding.ErrInvalidInputSize(e, "FLOAT", len(src))
	}

	n := len(src) / 4
	if cap(dst) < n {
		dst = make([]float32, n)
	} else {
		dst = dst[:n]
	}

	b0 := src[0*n : 1*n]
	b1 := src[1*n : 2*n]
	b2 := src[2*n : 3*n]
	b3 := src[3*n : 4*n]

	for i := range dst {
		dst[i] = math.Float32frombits(
			uint32(b0[i]) |
				uint32(b1[i])<<8 |
				uint32(b2[i])<<16 |
				uint32(b3[i])<<24,
		)
	}

	return dst, nil
}

func (e *Encoding) DecodeDouble(dst, src []byte) ([]byte, error) {
	if (len(src) % 8) != 0 {
		return dst[:0], encoding.ErrInvalidInputSize(e, "DOUBLE", len(src))
	}

	dst = resize(dst, len(src))
	n := len(src) / 8
	b0 := src[0*n : 1*n]
	b1 := src[1*n : 2*n]
	b2 := src[2*n : 3*n]
	b3 := src[3*n : 4*n]
	b4 := src[4*n : 5*n]
	b5 := src[5*n : 6*n]
	b6 := src[6*n : 7*n]
	b7 := src[7*n : 8*n]

	dst64 := bits.BytesToUint64(dst)
	for i := range dst64 {
		dst64[i] = uint64(b0[i]) |
			uint64(b1[i])<<8 |
			uint64(b2[i])<<16 |
			uint64(b3[i])<<24 |
			uint64(b4[i])<<32 |
			uint64(b5[i])<<40 |
			uint64(b6[i])<<48 |
			uint64(b7[i])<<56
	}

	return dst, nil
}

func resize(buf []byte, size int) []byte {
	if cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}
	return buf
}
