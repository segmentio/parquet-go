package plain

import (
	"encoding/binary"
	"fmt"
)

// ByteArrayLength returns the length of the PLAIN byte array starting at the
// beginning of the buffer.
func ByteArrayLength(buffer []byte) int {
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

// SplitByteArray returns a pair byte slices with the next byte array found in
// the buffer, and the remaining bytes.
//
// The function may panic if the input is not properly encoded as a PLAIN byte
// array.
func SplitByteArray(buffer []byte) (value, remain []byte) {
	length := ByteArrayLength(buffer)
	return buffer[4 : 4+length], buffer[4+length:]
}

// ScanByteArrayList iterates over the sequence of PLAIN encoded byte array
// values in the buffer, calling the scan function on each one.
//
// The function errors if the input is not properly formatted as a sequence
// of PLAIN byte array values.
func ScanByteArrayList(buffer []byte, limit int, scan func([]byte) error) (int, error) {
	var remain = limit
	for len(buffer) >= 4 && remain > 0 {
		n := 4 + ByteArrayLength(buffer)
		if len(buffer) < n {
			return limit - remain, fmt.Errorf("invalid PLAIN byte array sequence has value of length %d but only %d bytes remain to be read", n-4, len(buffer)-4)
		}
		if err := scan(buffer[4:n]); err != nil {
			return limit - remain, err
		}
		buffer = buffer[n:]
		remain--
	}
	var err error
	if len(buffer) != 0 {
		err = fmt.Errorf("invalid PLAIN byte array sequence has %d trailing bytes", len(buffer))
	}
	return limit - remain, err
}
