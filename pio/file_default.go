//go:build !darwin

package pio

import "os"

func fileMultiReadAt(f *os.File, ops []Op) { multiReadAt(f, ops) }
