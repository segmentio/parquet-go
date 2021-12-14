package delta

import (
	"encoding/binary"
	"io"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
	"github.com/segmentio/parquet/internal/bits"
)

// TODO: figure out better heuristics to determine those values,
// right now they are optimized for keeping the memory footprint
// of the encoder/decoder at ~8KB.
const (
	blockSize64     = 128
	numMiniBlock64  = 4 // (BlockSize / NumMiniBlock) % 32 == 0
	miniBlockSize64 = blockSize64 / numMiniBlock64

	blockSize32     = 2 * blockSize64
	numMiniBlock32  = 2 * numMiniBlock64
	miniBlockSize32 = blockSize32 / numMiniBlock32

	headerBufferSize    = 32
	blockBufferSize     = 8 * blockSize64
	bitWidthsBufferSize = 2 * numMiniBlock64
	miniBlockBufferSize = 8 * miniBlockSize64
)

type BinaryPackedEncoder struct {
	encoding.NotSupportedEncoder
	writer    io.Writer
	header    [headerBufferSize]byte
	block     [blockBufferSize]byte
	bitWidths [bitWidthsBufferSize]byte
	miniBlock [miniBlockBufferSize]byte
}

func NewBinaryPackedEncoder(w io.Writer) *BinaryPackedEncoder {
	e := &BinaryPackedEncoder{}
	e.Reset(w)
	return e
}

func (e *BinaryPackedEncoder) Reset(w io.Writer) {
	e.writer = w
}

func (e *BinaryPackedEncoder) Encoding() format.Encoding {
	return format.DeltaBinaryPacked
}

func (e *BinaryPackedEncoder) EncodeInt32(data []int32) error {
	firstValue := int32(0)
	if len(data) > 0 {
		firstValue = data[0]
	}

	if err := e.encodeBlockHeader(blockSize32, numMiniBlock32, len(data), int64(firstValue)); err != nil {
		return err
	}

	lastValue := firstValue
	for len(data) > 0 {
		block := bits.BytesToInt32(e.block[:])
		for i := range block {
			block[i] = 0
		}

		n := copy(block, data)
		data = data[n:]

		for i, v := range block {
			block[i], lastValue = v-lastValue, v
		}

		minDelta := bits.MinInt32(block)
		bits.SubInt32(block, minDelta)

		bitWidths := e.bitWidths[:numMiniBlock32]
		for i := range bitWidths {
			bitWidths[i] = byte(bits.MaxLen32(block[i*miniBlockSize32 : (i+1)*miniBlockSize32]))
		}

		if err := e.encodeBlock(int64(minDelta), bitWidths, bits.Int32ToBytes(block), 32, 4*miniBlockSize32); err != nil {
			return err
		}
	}

	return nil
}

func (e *BinaryPackedEncoder) EncodeInt64(data []int64) error {
	firstValue := int64(0)
	if len(data) > 0 {
		firstValue = data[0]
	}

	if err := e.encodeBlockHeader(blockSize64, numMiniBlock64, len(data), firstValue); err != nil {
		return err
	}

	lastValue := firstValue
	for len(data) > 0 {
		block := bits.BytesToInt64(e.block[:])
		for i := range block {
			block[i] = 0
		}

		n := copy(block, data)
		data = data[n:]

		for i, v := range block {
			block[i], lastValue = v-lastValue, v
		}

		minDelta := bits.MinInt64(block)
		bits.SubInt64(block, minDelta)

		bitWidths := e.bitWidths[:numMiniBlock64]
		for i := range bitWidths {
			bitWidths[i] = byte(bits.MaxLen64(block[i*miniBlockSize64 : (i+1)*miniBlockSize64]))
		}

		if err := e.encodeBlock(minDelta, bitWidths, bits.Int64ToBytes(block), 64, 8*miniBlockSize64); err != nil {
			return err
		}
	}

	return nil
}

func (e *BinaryPackedEncoder) encodeBlockHeader(blockSize, numMiniBlock, totalValues int, firstValue int64) error {
	b := e.header[:]
	n := 0
	n += binary.PutUvarint(b[n:], uint64(blockSize))
	n += binary.PutUvarint(b[n:], uint64(numMiniBlock))
	n += binary.PutUvarint(b[n:], uint64(totalValues))
	n += binary.PutVarint(b[n:], firstValue)
	_, err := e.writer.Write(b[:n])
	return err
}

func (e *BinaryPackedEncoder) encodeBlock(minDelta int64, bitWidths []byte, block []byte, blockWidth, miniBlockSize uint) error {
	_ = block[:miniBlockSize*uint(len(bitWidths))]

	b := e.header[:]
	n := binary.PutVarint(b, minDelta)
	if _, err := e.writer.Write(b[:n]); err != nil {
		return err
	}
	if _, err := e.writer.Write(bitWidths); err != nil {
		return err
	}

	for i, bitWidth := range bitWidths {
		if bitWidth == 0 {
			continue
		}
		n := bits.Pack(e.miniBlock[:], uint(bitWidth),
			block[uint(i)*miniBlockSize:(uint(i)+1)*miniBlockSize],
			blockWidth,
		)
		_, err := e.writer.Write(e.miniBlock[:bits.ByteCount(uint(n)*uint(bitWidth))])
		if err != nil {
			return err
		}
	}

	return nil
}
