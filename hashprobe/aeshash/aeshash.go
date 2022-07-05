// Package aeshash implements hashing functions derived from the Go runtime's
// internal hashing based on the support of AES encryption in CPU instructions.
//
// On architecture where the CPU does not provide instructions for AES
// encryption, the aeshash.Enabled function always returns false, and attempting
// to call any other function will trigger a panic.
package aeshash
