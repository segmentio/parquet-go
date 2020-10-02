package readers_test

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/segmentio/centrifuge-traces/parquet/internal/readers"
	"github.com/segmentio/centrifuge-traces/parquet/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSharedRead(t *testing.T) {
	test.WithTestDir(t, func(dir string) {
		p := path.Join(dir, "file.txt")
		err := ioutil.WriteFile(p, []byte{
			0x1, 0x2, // belongs to reader 1
			0x3, 0x4, // belongs to reader 2
		}, 0644)
		require.NoError(t, err)
		f, err := os.Open(p)
		require.NoError(t, err)
		defer test.Close(t, f)

		r1 := readers.NewShared(f)
		r2 := readers.NewShared(f)

		_, err = r2.Seek(2, io.SeekStart)
		assert.NoError(t, err)

		readByteAssert(t, r1, 0x1)
		readByteAssert(t, r2, 0x3)
		readByteAssert(t, r1, 0x2)
		readByteAssert(t, r2, 0x4)
	})
}

func TestSharedFork(t *testing.T) {
	test.WithTestDir(t, func(dir string) {
		p := path.Join(dir, "file.txt")
		err := ioutil.WriteFile(p, []byte{0x1, 0x2, 0x3}, 0644)
		require.NoError(t, err)
		f, err := os.Open(p)
		require.NoError(t, err)
		defer test.Close(t, f)

		r1 := readers.NewShared(f)

		readByteAssert(t, r1, 0x1)

		r2 := r1.Fork()

		readByteAssert(t, r1, 0x2)
		readByteAssert(t, r2, 0x2)

		_, err = r2.Seek(0, io.SeekStart)
		assert.NoError(t, err)

		readByteAssert(t, r1, 0x3)
		readByteAssert(t, r2, 0x1)
	})
}

func readByteAssert(t *testing.T, r io.ReadSeeker, expected byte) {
	b := make([]byte, 1)
	read, err := r.Read(b)
	assert.NoError(t, err)
	assert.Equal(t, 1, read)
	assert.Equal(t, expected, b[0])
}
