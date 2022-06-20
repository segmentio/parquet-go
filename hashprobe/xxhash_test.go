package hashprobe

import "testing"

func TestXXHash32x32bits(t *testing.T) {
	h := xxhash32x32bits(42)

	if h != 0x454235d1 {
		t.Errorf("hash(42): %08x", h)
	}
}

func TestXXHash32x64bits(t *testing.T) {
	h := xxhash32x64bits(42)

	if h != 0x8b06618d {
		t.Errorf("hash(42): %08x", h)
	}
}
