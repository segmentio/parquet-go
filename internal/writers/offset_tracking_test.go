package writers_test

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/segmentio/parquet/internal/test"
	"github.com/segmentio/parquet/internal/writers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOffsetTrackingRead(t *testing.T) {
	test.WithTestDir(t, func(dir string) {
		p := path.Join(dir, "file.txt")
		err := ioutil.WriteFile(p, []byte{0x1, 0x2, 0x3}, 0644)
		require.NoError(t, err)
		f, err := os.Open(p)
		require.NoError(t, err)
		defer test.Close(t, f)

		r := writers.NewOffsetTracking(f)
		assert.Zero(t, r.Offset())

		readByteAssert(t, r, 0x1)
		assert.Equal(t, int64(1), r.Offset())

		readByteAssert(t, r, 0x2)
		assert.Equal(t, int64(2), r.Offset())

		readByteAssert(t, r, 0x3)
		assert.Equal(t, int64(3), r.Offset())

		n, err := r.Seek(1, io.SeekStart)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), n)

		readByteAssert(t, r, 0x2)
		assert.Equal(t, int64(2), r.Offset())
	})
}
