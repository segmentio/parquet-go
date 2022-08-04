package pio

import "os"

// File is an adapter for *os.File which implements the ReaderAt interface.
type File struct{ *os.File }

// MultiReadAt satisifies the ReaderAt interface.
func (f File) MultiReadAt(ops []Op) { fileMultiReadAt(f.File, ops) }

var (
	_ ReaderAt = File{}
)
