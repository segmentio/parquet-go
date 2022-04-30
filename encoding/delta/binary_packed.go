package delta

import (
	"encoding/binary"
	"io"

	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/format"
)

// TODO: figure out better heuristics to determine those values.
const (
	blockSize64     = 128
	numMiniBlock64  = 4 // (blockSize64 / numMiniBlock64) % 32 == 0
	miniBlockSize64 = blockSize64 / numMiniBlock64

	blockSize32     = 2 * blockSize64
	numMiniBlock32  = 2 * numMiniBlock64
	miniBlockSize32 = blockSize32 / numMiniBlock32

	headerBufferSize    = 32
	blockBufferSize     = 8 * blockSize64
	bitWidthsBufferSize = 2 * numMiniBlock64
)

type BinaryPackedEncoding struct {
	encoding.NotSupported
}

func (e *BinaryPackedEncoding) Encoding() format.Encoding {
	return format.DeltaBinaryPacked
}

func (e *BinaryPackedEncoding) CanEncode(t format.Type) bool {
	return t == format.Int32 || t == format.Int64
}

func (e *BinaryPackedEncoding) NewDecoder(r io.Reader) encoding.Decoder {
	return NewBinaryPackedDecoder(r)
}

func (e *BinaryPackedEncoding) NewEncoder(w io.Writer) encoding.Encoder {
	return NewBinaryPackedEncoder(w)
}

func (e *BinaryPackedEncoding) String() string {
	return "DELTA_BINARY_PACKED"
}

func (e *BinaryPackedEncoding) EncodeInt32(dst []byte, src []int32) ([]byte, error) {
	firstValue := int32(0)
	if len(src) > 0 {
		firstValue = src[0]
	}

	dst = appendBinaryPackedHeader(dst[:0], blockSize32, numMiniBlock32, len(src), int64(firstValue))

	if len(src) <= 1 {
		return dst, nil
	}

	src = src[1:]
	lastValue := firstValue

	for len(src) > 0 {

	}
}

func (e *BinaryPackedEncoding) DecodeInt32(dst []int32, src []byte) ([]int32, error) {

}

func appendBinaryPackedHeader(b []byte, blockSize, numMiniBlock, totalValues int, firstValue int64) []byte {
	h := [headerBufferSize]byte{}
	n := 0
	n += binary.PutUvarint(h[n:], uint64(blockSize))
	n += binary.PutUvarint(h[n:], uint64(numMiniBlock))
	n += binary.PutUvarint(h[n:], uint64(totalValues))
	n += binary.PutVarint(h[n:], firstValue)
	return append(b, h[:n]...)
}
