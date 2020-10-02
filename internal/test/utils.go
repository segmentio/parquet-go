package test

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Close(t *testing.T, c io.Closer) {
	assert.NoError(t, c.Close())
}

func TempDir(f func(dir string)) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		panic(err)
	}
	defer func() {
		if r := recover(); r != nil {
			log.Println("Temporary directory available at", dir)
			panic(r)
		} else {
			os.RemoveAll(dir)
		}
	}()

	f(dir)
}

func WithTestDir(t *testing.T, f func(dir string)) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer func() {
		if r := recover(); r != nil {
			t.Log("Test directory available at", dir)
			panic(r)
		} else if t.Failed() {
			t.Log("Test directory available at", dir)
		} else {
			os.RemoveAll(dir)
		}
	}()

	f(dir)
}
