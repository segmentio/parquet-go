// Package pio implements "Parallel I/O", allowing programs to operate on
// multiple file locations in parallel.
package pio

import (
	"bytes"
	"io"
	"os"
	"sync"
)

// ReaderAt is an interface which might be implemented by io.ReaderAt values
// passed as first argument to MultiReadAt in order to provide a specialization
// of the mutli-read operation.
type ReaderAt interface {
	MultiReadAt(ops []Op)
}

// Op represents a single operation submitted to MultiReadAt.
type Op struct {
	// For read operations, this field is buffer where bytes will be read from
	// a file. After a call to MultiReadAt returns, the slice length is adjusted
	// to reflect the number of bytes that were read.
	Data []byte

	// The offset at which the operation will be performed in the file.
	Off int64

	// After an operation completes, this field is nil if the operation
	// succeeded, or it holds an error representing the reason for the failure.
	Err error
}

// MultiReadAt uses the given list of operations to read multiple locations in
// a file.
//
// If the file implements pio.ReaderAt, the function delegates to calling the
// file's MultiReadAt method. Otherwise, the implementation emulates parallel
// reads of each file region by submitting multiple calls to ReadAt.
func MultiReadAt(file io.ReaderAt, ops []Op) {
	switch f := file.(type) {
	case *bytes.Reader:
		byteMultiReadAt(f, ops)
	case *os.File:
		fileMultiReadAt(f, ops)
	case ReaderAt:
		f.MultiReadAt(ops)
	default:
		multiReadAt(f, ops)
	}
}

func byteMultiReadAt(file *bytes.Reader, ops []Op) {
	for i := range ops {
		op := &ops[i]
		rn, err := file.ReadAt(op.Data, op.Off)
		op.Data, op.Err = op.Data[:rn], err
	}
}

func multiReadAt(file io.ReaderAt, ops []Op) {
	wg := sync.WaitGroup{}
	wg.Add(len(ops))
	defer wg.Wait()

	for i := range ops {
		go func(op *Op) {
			defer wg.Done()
			n, err := file.ReadAt(op.Data, op.Off)
			op.Data, op.Err = op.Data[:n], err
		}(&ops[i])
	}
}
