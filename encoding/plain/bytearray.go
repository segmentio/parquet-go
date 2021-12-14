package plain

import (
	"encoding/binary"
	"fmt"
	"math"
)

const (
	// All is a constant to pass to SanByteArrayList to apply no limist to the
	// number of values scanned.
	All = math.MaxInt32
)

// NextByteArrayLength returns the length of the PLAIN byte array starting at
// the beginning of the buffer.
func NextByteArrayLength(buffer []byte) int {
	return int(binary.LittleEndian.Uint32(buffer))
}

// ByteArray returns the PLAIN representation of a byte slice.
func ByteArray(value []byte) []byte {
	return AppendByteArray(make([]byte, 0, 4+len(value)), value)
}

// AppendByteArray appends the PLAIN representation of the given value to the
// buffer and returns it.
func AppendByteArray(buffer, value []byte) []byte {
	length := [4]byte{}
	binary.LittleEndian.PutUint32(length[:], uint32(len(value)))
	buffer = append(buffer, length[:]...)
	buffer = append(buffer, value...)
	return buffer
}

// AppendByteArray appends the PLAIN representation of the given values list to
// the buffer and returns it.
func AppendByteArrayList(buffer []byte, values ...[]byte) []byte {
	numBytes := 0

	for _, value := range values {
		numBytes += 4 + len(value)
	}

	if (cap(buffer) - len(buffer)) < numBytes {
		newBuf := make([]byte, len(buffer)+numBytes)
		buffer = newBuf[:copy(newBuf, buffer)]
	}

	for _, value := range values {
		length := [4]byte{}
		binary.LittleEndian.PutUint32(length[:], uint32(len(value)))
		buffer = append(buffer, length[:]...)
		buffer = append(buffer, value...)
	}

	return buffer
}

// JoinByteArrayList returns a byte slice with the given values joined into
// a plain representation.
func JoinByteArrayList(values [][]byte) []byte {
	bufferSize := 0
	for _, value := range values {
		bufferSize += 4 + len(value)
	}
	buffer := make([]byte, 0, bufferSize)
	for _, value := range values {
		buffer = AppendByteArray(buffer, value)
	}
	return buffer
}

// SplitByteArrayList splits the given buffer into a slice of byte slices where
// each element is one value from the buffer.
//
// The returned slice references sub-slices of the input buffer, no copies of
// the values are made.
func SplitByteArrayList(buffer []byte) ([][]byte, error) {
	n, err := ScanByteArrayList(buffer, All, func(value []byte) error { return nil })
	if err != nil {
		return nil, err
	}
	values := make([][]byte, n)
	offset := 0
	ScanByteArrayList(buffer, All, func(value []byte) error {
		values[offset] = value
		offset++
		return nil
	})
	return values, nil
}

// NextByteArray returns a pair byte slices with the next byte array found in
// the buffer, and the remaining bytes.
//
// The function may panic if the input is not properly encoded as a PLAIN byte
// array.
func NextByteArray(buffer []byte) (value, remain []byte) {
	length := NextByteArrayLength(buffer)
	return buffer[4 : 4+length], buffer[4+length:]
}

// ScanByteArrayList iterates over the sequence of PLAIN encoded byte array
// values in the buffer, calling the scan function on each one.
//
// The function errors if the input is not properly formatted as a sequence
// of PLAIN byte array values.
func ScanByteArrayList(buffer []byte, limit int, scan func([]byte) error) (int, error) {
	var remain = limit
	var err error

	for len(buffer) >= 4 && remain > 0 {
		n := 4 + NextByteArrayLength(buffer)
		if len(buffer) < n {
			err = fmt.Errorf("invalid PLAIN byte array sequence has value of length %d but only %d bytes remain to be read", n-4, len(buffer)-4)
			break
		}
		if err = scan(buffer[4:n:n]); err != nil {
			break
		}
		buffer = buffer[n:]
		remain--
	}

	return limit - remain, err
}
