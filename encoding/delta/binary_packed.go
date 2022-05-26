package delta

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/format"
	"github.com/segmentio/parquet-go/internal/bits"
)

const (
	blockSize     = 128
	numMiniBlocks = 4
	miniBlockSize = blockSize / numMiniBlocks
	// The parquet spec does not enforce a limit to the block size, but we need
	// one otherwise invalid inputs may result in unbounded memory allocations.
	//
	// 65K+ values should be enough for any valid use case.
	maxSupportedBlockSize = 65536

	maxHeaderLen      = 4 * binary.MaxVarintLen64
	maxBlockHeaderLen = binary.MaxVarintLen64 + numMiniBlocks
)

func maxEncodeInt32Len(numValues int) int {
	return maxEncodeLen(numValues, 4)
}

func maxEncodeInt64Len(numValues int) int {
	return maxEncodeLen(numValues, 8)
}

func maxEncodeLen(numValues, valueSize int) int {
	if numValues--; (numValues % blockSize) != 0 {
		numValues = ((numValues / blockSize) + 1) * blockSize
	}
	return maxHeaderLen + maxBlockHeaderLen + (valueSize * numValues)
}

type BinaryPackedEncoding struct {
	encoding.NotSupported
}

func (e *BinaryPackedEncoding) String() string {
	return "DELTA_BINARY_PACKED"
}

func (e *BinaryPackedEncoding) Encoding() format.Encoding {
	return format.DeltaBinaryPacked
}

func (e *BinaryPackedEncoding) EncodeInt32(dst, src []byte) ([]byte, error) {
	if (len(src) % 4) != 0 {
		return dst[:0], encoding.ErrEncodeInvalidInputSize(e, "INT64", len(src))
	}
	dst = resize(dst, maxEncodeInt32Len(len(src)/4))
	n := encodeInt32(dst, bits.BytesToInt32(src))
	return dst[:n], nil
}

func (e *BinaryPackedEncoding) EncodeInt64(dst, src []byte) ([]byte, error) {
	if (len(src) % 8) != 0 {
		return dst[:0], encoding.ErrEncodeInvalidInputSize(e, "INT64", len(src))
	}
	dst = resize(dst, maxEncodeInt64Len(len(src)/8))
	n := encodeInt64(dst, bits.BytesToInt64(src))
	return dst[:n], nil
}

func (e *BinaryPackedEncoding) DecodeInt32(dst, src []byte) ([]byte, error) {
	dst, _, err := e.decodeInt32(dst[:0], src)
	return dst, e.wrap(err)
}

func (e *BinaryPackedEncoding) DecodeInt64(dst, src []byte) ([]byte, error) {
	dst, _, err := e.decodeInt64(dst[:0], src)
	return dst, e.wrap(err)
}

func (e *BinaryPackedEncoding) decodeInt32(dst, src []byte) ([]byte, []byte, error) {
	src, err := e.decode(src, func(value int64) {
		buf := [4]byte{}
		binary.LittleEndian.PutUint32(buf[:], uint32(value))
		dst = append(dst, buf[:]...)
	})
	return dst, src, err
}

func (e *BinaryPackedEncoding) decodeInt64(dst, src []byte) ([]byte, []byte, error) {
	src, err := e.decode(src, func(value int64) {
		buf := [8]byte{}
		binary.LittleEndian.PutUint64(buf[:], uint64(value))
		dst = append(dst, buf[:]...)
	})
	return dst, src, err
}

func (e *BinaryPackedEncoding) decode(src []byte, observe func(int64)) ([]byte, error) {
	blockSize, numMiniBlocks, totalValues, firstValue, src, err := decodeBinaryPackedHeader(src)
	if err != nil {
		return src, err
	}
	if totalValues == 0 {
		return src, nil
	}

	observe(firstValue)
	totalValues--
	lastValue := firstValue
	numValuesInMiniBlock := blockSize / numMiniBlocks

	block := make([]int64, 128)
	if cap(block) < blockSize {
		block = make([]int64, blockSize)
	} else {
		block = block[:blockSize]
	}

	miniBlockData := make([]byte, 256)

	for totalValues > 0 && len(src) > 0 {
		var minDelta int64
		var bitWidths []byte
		minDelta, bitWidths, src, err = decodeBinaryPackedBlock(src, numMiniBlocks)
		if err != nil {
			return src, err
		}

		blockOffset := 0
		for i := range block {
			block[i] = 0
		}

		for _, bitWidth := range bitWidths {
			if bitWidth == 0 {
				n := numValuesInMiniBlock
				if n > totalValues {
					n = totalValues
				}
				blockOffset += n
				totalValues -= n
			} else {
				miniBlockSize := (numValuesInMiniBlock * int(bitWidth)) / 8
				if cap(miniBlockData) < miniBlockSize {
					miniBlockData = make([]byte, miniBlockSize, miniBlockSize)
				} else {
					miniBlockData = miniBlockData[:miniBlockSize]
				}

				n := copy(miniBlockData, src)
				src = src[n:]
				bitOffset := uint(0)

				for count := numValuesInMiniBlock; count > 0 && totalValues > 0; count-- {
					delta := int64(0)

					for b := uint(0); b < uint(bitWidth); b++ {
						x := (bitOffset + b) / 8
						y := (bitOffset + b) % 8
						delta |= int64((miniBlockData[x]>>y)&1) << b
					}

					block[blockOffset] = delta
					blockOffset++
					totalValues--
					bitOffset += uint(bitWidth)
				}
			}

			if totalValues == 0 {
				break
			}
		}

		bits.AddInt64(block, minDelta)
		block[0] += lastValue
		for i := 1; i < len(block); i++ {
			block[i] += block[i-1]
		}
		if values := block[:blockOffset]; len(values) > 0 {
			for _, v := range values {
				observe(v)
			}
			lastValue = values[len(values)-1]
		}
	}

	if totalValues > 0 {
		return src, fmt.Errorf("%d missing values: %w", totalValues, io.ErrUnexpectedEOF)
	}

	return src, nil
}

func (e *BinaryPackedEncoding) wrap(err error) error {
	if err != nil {
		err = encoding.Error(e, err)
	}
	return err
}

func decodeBinaryPackedHeader(src []byte) (blockSize, numMiniBlocks, totalValues int, firstValue int64, next []byte, err error) {
	u := uint64(0)
	n := 0
	i := 0

	if u, n, err = decodeUvarint(src[i:], "block size"); err != nil {
		return
	}
	i += n
	blockSize = int(u)

	if u, n, err = decodeUvarint(src[i:], "number of mini-blocks"); err != nil {
		return
	}
	i += n
	numMiniBlocks = int(u)

	if u, n, err = decodeUvarint(src[i:], "total values"); err != nil {
		return
	}
	i += n
	totalValues = int(u)

	if firstValue, n, err = decodeVarint(src[i:], "first value"); err != nil {
		return
	}
	i += n

	if numMiniBlocks == 0 {
		err = fmt.Errorf("invalid number of mini block (%d)", numMiniBlocks)
	} else if (blockSize <= 0) || (blockSize%128) != 0 {
		err = fmt.Errorf("invalid block size is not a multiple of 128 (%d)", blockSize)
	} else if blockSize > maxSupportedBlockSize {
		err = fmt.Errorf("invalid block size is too large (%d)", blockSize)
	} else if miniBlockSize := blockSize / numMiniBlocks; (numMiniBlocks <= 0) || (miniBlockSize%32) != 0 {
		err = fmt.Errorf("invalid mini block size is not a multiple of 32 (%d)", miniBlockSize)
	} else if totalValues < 0 {
		err = fmt.Errorf("invalid total number of values is negative (%d)", totalValues)
	} else if totalValues > math.MaxInt32 {
		err = fmt.Errorf("too many values: %d", totalValues)
	}

	return blockSize, numMiniBlocks, totalValues, firstValue, src[i:], err
}

func decodeBinaryPackedBlock(src []byte, numMiniBlocks int) (minDelta int64, bitWidths, next []byte, err error) {
	minDelta, n, err := decodeVarint(src, "min delta")
	if err != nil {
		return 0, nil, src, err
	}
	src = src[n:]
	if len(src) < numMiniBlocks {
		bitWidths, next = src, nil
	} else {
		bitWidths, next = src[:numMiniBlocks], src[numMiniBlocks:]
	}
	return minDelta, bitWidths, next, nil
}

func decodeUvarint(buf []byte, what string) (u uint64, n int, err error) {
	u, n = binary.Uvarint(buf)
	if n == 0 {
		return 0, 0, fmt.Errorf("decoding %s: %w", what, io.ErrUnexpectedEOF)
	}
	if n < 0 {
		return 0, 0, fmt.Errorf("overflow decoding %s (read %d/%d bytes)", what, -n, len(buf))
	}
	return u, n, nil
}

func decodeVarint(buf []byte, what string) (v int64, n int, err error) {
	v, n = binary.Varint(buf)
	if n == 0 {
		return 0, 0, fmt.Errorf("decoding %s: %w", what, io.ErrUnexpectedEOF)
	}
	if n < 0 {
		return 0, 0, fmt.Errorf("overflow decoding %s (read %d/%d bytes)", what, -n, len(buf))
	}
	return v, n, nil
}
