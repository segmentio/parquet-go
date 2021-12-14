package delta

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
	"github.com/segmentio/parquet/internal/bits"
)

type BinaryPackedDecoder struct {
	encoding.NotSupportedDecoder
	reader        *bufio.Reader
	blockSize     int
	numMiniBlock  int
	miniBlockSize int
	totalValues   int
	lastValue     int64
	bitWidths     []byte
	miniBlocks    []byte
	blockValues   []int64
	valueIndex    int
	blockIndex    int
}

func NewBinaryPackedDecoder(r io.Reader) *BinaryPackedDecoder {
	d := &BinaryPackedDecoder{}
	d.Reset(r)
	return d
}

func (d *BinaryPackedDecoder) Reset(r io.Reader) {
	*d = BinaryPackedDecoder{
		reader:      d.reader,
		bitWidths:   d.bitWidths[:0],
		miniBlocks:  d.miniBlocks[:0],
		blockValues: d.blockValues[:0],
	}
	if d.reader == nil {
		d.reader = bufio.NewReaderSize(r, 4096)
	} else {
		d.reader.Reset(r)
	}
}

func (e *BinaryPackedDecoder) Encoding() format.Encoding {
	return format.DeltaBinaryPacked
}

func (d *BinaryPackedDecoder) DecodeInt32(data []int32) (int, error) {
	decoded := 0

	for len(data) > 0 {
		if err := d.decode(); err != nil {
			return decoded, err
		}

		i := d.blockIndex
		j := len(d.blockValues)
		remain := d.totalValues - d.valueIndex

		if (j - i) > remain {
			j = i + remain
		}

		n := j - i
		if n > len(data) {
			n = len(data)
			j = i + n
		}

		for i, v := range d.blockValues[i:j] {
			data[i] = int32(v)
		}

		data = data[n:]
		decoded += n
		d.valueIndex += n
		d.blockIndex += n
	}

	return decoded, nil
}

func (d *BinaryPackedDecoder) DecodeInt64(data []int64) (int, error) {
	decoded := 0

	for len(data) > 0 {
		if err := d.decode(); err != nil {
			return decoded, err
		}

		i := d.blockIndex
		j := len(d.blockValues)
		remain := d.totalValues - d.valueIndex

		if (j - i) > remain {
			j = i + remain
		}

		n := copy(data, d.blockValues[i:j])
		data = data[n:]
		decoded += n
		d.valueIndex += n
		d.blockIndex += n
	}

	return decoded, nil
}

func (d *BinaryPackedDecoder) decode() error {
	for d.valueIndex == d.totalValues {
		blockSize, numMiniBlock, totalValues, firstValue, err := d.decodeBlockHeader()
		if err != nil {
			return err
		}
		d.blockSize = blockSize
		d.numMiniBlock = numMiniBlock
		d.miniBlockSize = blockSize / numMiniBlock
		d.totalValues = totalValues
		d.lastValue = firstValue
		d.valueIndex = 0
		d.blockIndex = 0
	}

	if d.blockIndex == 0 || d.blockIndex == d.blockSize {
		d.blockIndex = 0
		if err := d.decodeBlock(); err != nil {
			return err
		}
	}

	return nil
}

func (d *BinaryPackedDecoder) decodeBlockHeader() (blockSize, numMiniBlock, totalValues int, firstValue int64, err error) {
	var u uint64

	if u, err = binary.ReadUvarint(d.reader); err != nil {
		if err != io.EOF {
			err = fmt.Errorf("DELTA_BINARY_PACKED: reading block size: %w", err)
		}
		return
	} else {
		blockSize = int(u)
	}
	if u, err = binary.ReadUvarint(d.reader); err != nil {
		err = fmt.Errorf("DELTA_BINARY_PACKED: reading number of mini blocks: %w", dontExpectEOF(err))
		return
	} else {
		numMiniBlock = int(u)
	}
	if u, err = binary.ReadUvarint(d.reader); err != nil {
		err = fmt.Errorf("DELTA_BINARY_PACKED: reading number of values: %w", dontExpectEOF(err))
		return
	} else {
		totalValues = int(u)
	}
	if firstValue, err = binary.ReadVarint(d.reader); err != nil {
		err = fmt.Errorf("DELTA_BINARY_PACKED: reading first value: %w", dontExpectEOF(err))
		return
	}

	if (blockSize <= 0) || (blockSize%128) != 0 {
		err = fmt.Errorf("DELTA_BINARY_PACKED: invalid block size is not a multiple of 128 (%d)", blockSize)
	} else if miniBlockSize := blockSize / numMiniBlock; (numMiniBlock <= 0) || (miniBlockSize%32) != 0 {
		err = fmt.Errorf("DELTA_BINARY_PACKED: invalid mini block size is not a multiple of 32 (%d)", miniBlockSize)
	} else if totalValues < 0 {
		err = fmt.Errorf("DETLA_BINARY_PACKED: invalid total number of values is negative (%d)", totalValues)
	}
	return
}

func (d *BinaryPackedDecoder) decodeBlock() error {
	minDelta, err := binary.ReadVarint(d.reader)
	if err != nil {
		return fmt.Errorf("DELTA_BINARY_PACKED: reading min delta: %w", err)
	}

	d.bitWidths = resize(d.bitWidths, d.numMiniBlock)
	if _, err := io.ReadFull(d.reader, d.bitWidths); err != nil {
		return fmt.Errorf("DELTA_BINARY_PACKED: reading bit widths: %w", err)
	}

	miniBlockSize := uint(d.miniBlockSize)
	miniBlockBytes := 0
	for _, bitWidth := range d.bitWidths {
		miniBlockBytes += bits.ByteCount(uint(bitWidth) * miniBlockSize)
	}

	d.miniBlocks = resize(d.miniBlocks, miniBlockBytes)
	if _, err := io.ReadFull(d.reader, d.miniBlocks); err != nil {
		return fmt.Errorf("DELTA_BINARY_PACKED: reading mini blocks: %w", err)
	}

	d.blockValues = bits.BytesToInt64(resize(bits.Int64ToBytes(d.blockValues), 8*d.blockSize))
	src := d.miniBlocks
	dst := bits.Int64ToBytes(d.blockValues)
	for _, bitWidth := range d.bitWidths {
		if bitWidth == 0 {
			i := bits.ByteCount(miniBlockSize * 64)
			b := dst[:i]
			for j := range b {
				b[j] = 0
			}
			dst = dst[i:]
		} else {
			i := bits.ByteCount(miniBlockSize * 64)
			j := bits.ByteCount(miniBlockSize * uint(bitWidth))
			bits.Pack(dst[:i], 64, src[:j], uint(bitWidth))
			dst = dst[i:]
			src = src[j:]
		}
	}

	blockValues := d.blockValues
	if remain := d.totalValues - d.valueIndex; len(blockValues) > remain {
		blockValues = blockValues[:remain]
	}

	bits.AddInt64(blockValues, minDelta)
	blockValues[0] += d.lastValue
	for i := 1; i < len(blockValues); i++ {
		blockValues[i] += blockValues[i-1]
	}
	d.lastValue = blockValues[len(blockValues)-1]
	return nil
}

func dontExpectEOF(err error) error {
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return err
}

func resize(buf []byte, size int) []byte {
	if cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}
	return buf
}
