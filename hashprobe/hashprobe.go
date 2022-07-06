// Package hashprobe provides implementations of probing tables for various
// data types.
//
// Probing tables are specialized hash tables supporting only a single
// "probing" operation which behave like a "lookup or insert". When a key
// is probed, either its value is retrieved if it already existed in the table,
// or it is inserted and assigned its index in the insert sequence as value.
//
// Values are represented as signed 32 bits integers, which means that probing
// tables defined in this package may contain at most 2^31-1 entries.
//
// Probing tables have a method named Probe with the following signature:
//
//	func (t *Int64Table) Probe(keys []int64, values []int32) int {
//		...
//	}
//
// The method takes an array of keys to probe as first argument, an array of
// values where the indexes of each key will be written as second argument, and
// returns the number of keys that were inserted during the call.
//
// Applications that need to determine which keys were inserted can capture the
// length of the probing table prior to the call, and scan the list of values
// looking for indexes greater or equal to the captured lenth.
package hashprobe

import (
	"math"
	"math/bits"
	"math/rand"

	"github.com/segmentio/parquet-go/hashprobe/aeshash"
	"github.com/segmentio/parquet-go/hashprobe/wyhash"
	"github.com/segmentio/parquet-go/internal/unsafecast"
)

const (
	// Number of probes tested per iteration. This parameter balances between
	// the amount of memory allocated on the stack to hold the computed hashes
	// of the keys being probed, and amortizing the baseline cost of the probing
	// algorithm.
	//
	// The larger the value, the more memory is required, but lower the baseline
	// cost will be.
	//
	// We chose a value that is somewhat large, resulting in reserving 2KiB of
	// stack but mostly erasing the baseline cost.
	probesPerLoop = 256
)

var (
	hash32  = wyhash.Hash32
	hash64  = wyhash.Hash64
	hash128 = wyhash.Hash128
)

func init() {
	if aeshash.Enabled() {
		hash32 = aeshash.Hash32
		hash64 = aeshash.Hash64
		hash128 = aeshash.Hash128
	}
}

func nextPowerOf2(n int) int {
	return 1 << (64 - bits.LeadingZeros64(uint64(n-1)))
}

func randSeed() uintptr {
	return uintptr(rand.Uint64())
}

type Int32Table struct{ table32 }

func NewInt32Table(cap int, maxLoad float64) *Int32Table {
	return &Int32Table{makeTable32(cap, maxLoad)}
}

func (t *Int32Table) Reset() { t.reset() }

func (t *Int32Table) Len() int { return t.len }

func (t *Int32Table) Cap() int { return t.cap }

func (t *Int32Table) Probe(keys, values []int32) int {
	return t.probe(unsafecast.Int32ToUint32(keys), values)
}

type Float32Table struct{ table32 }

func NewFloat32Table(cap int, maxLoad float64) *Float32Table {
	return &Float32Table{makeTable32(cap, maxLoad)}
}

func (t *Float32Table) Reset() { t.reset() }

func (t *Float32Table) Len() int { return t.len }

func (t *Float32Table) Cap() int { return t.cap }

func (t *Float32Table) Probe(keys []float32, values []int32) int {
	return t.probe(unsafecast.Float32ToUint32(keys), values)
}

type Uint32Table struct{ table32 }

func NewUint32Table(cap int, maxLoad float64) *Uint32Table {
	return &Uint32Table{makeTable32(cap, maxLoad)}
}

func (t *Uint32Table) Reset() { t.reset() }

func (t *Uint32Table) Len() int { return t.len }

func (t *Uint32Table) Cap() int { return t.cap }

func (t *Uint32Table) Probe(keys []uint32, values []int32) int {
	return t.probe(keys, values)
}

type table32 struct {
	len     int
	cap     int
	maxLen  int
	maxLoad float64
	seed    uintptr
	table   []uint32
}

func makeTable32(cap int, maxLoad float64) (t table32) {
	if cap < 32 {
		cap = 32
	}
	t.init(nextPowerOf2(cap), maxLoad)
	return t
}

func (t *table32) init(cap int, maxLoad float64) {
	*t = table32{
		cap:     cap,
		maxLen:  int(math.Ceil(maxLoad * float64(cap))),
		maxLoad: maxLoad,
		seed:    randSeed(),
		table:   make([]uint32, cap/32+2*cap),
	}
}

func (t *table32) grow(totalValues int) {
	cap := 2 * t.cap
	totalValues = nextPowerOf2(totalValues)
	if totalValues > cap {
		cap = totalValues
	}

	tmp := table32{}
	tmp.init(cap, t.maxLoad)
	tmp.len = t.len

	flags, pairs := t.content()

	for i, f := range flags {
		if f != 0 {
			j := 32 * i
			n := bits.TrailingZeros32(f)
			j += n
			f >>= uint(n)

			for {
				n := bits.TrailingZeros32(^f)
				k := j + n
				tmp.insert(pairs[2*j : 2*k : 2*k])
				j += n
				f >>= uint(n)
				if f == 0 {
					break
				}
				n = bits.TrailingZeros32(f)
				j += n
				f >>= uint(n)
			}
		}
	}

	*t = tmp
}

func (t *table32) insert(pairs []uint32) {
	flags, table := t.content()
	mod := uintptr(t.cap) - 1

	for i := 0; i < len(pairs); i += 2 {
		hash := hash32(pairs[i], t.seed)

		for {
			position := hash & mod
			index := position / 32
			shift := position % 32

			if (flags[index] & (1 << shift)) == 0 {
				flags[index] |= 1 << shift
				table[2*position+0] = pairs[i+0]
				table[2*position+1] = pairs[i+1]
				break
			}

			hash++
		}
	}
}

func (t *table32) content() (flags, pairs []uint32) {
	n := t.cap / 32
	return t.table[:n:n], t.table[n:len(t.table):len(t.table)]
}

func (t *table32) reset() {
	for i := range t.table {
		t.table[i] = 0
	}
	t.len = 0
}

func (t *table32) probe(keys []uint32, values []int32) int {
	if totalValues := t.len + len(keys); totalValues > t.maxLen {
		t.grow(totalValues)
	}

	var hashes [probesPerLoop]uintptr
	var baseLength = t.len
	var useAesHash = aeshash.Enabled()

	for i := 0; i < len(keys); {
		j := len(hashes) + i
		n := len(hashes)

		if j > len(keys) {
			j = len(keys)
			n = len(keys) - i
		}

		h := hashes[:n:n]
		k := keys[i:j:j]
		v := values[i:j:j]

		if useAesHash {
			aeshash.MultiHash32(h, k, t.seed)
		} else {
			wyhash.MultiHash32(h, k, t.seed)
		}

		t.len = multiProbe32(t.table, t.len, t.cap, h, k, v)
		i = j
	}

	return t.len - baseLength
}

type Int64Table struct{ table64 }

func NewInt64Table(cap int, maxLoad float64) *Int64Table {
	return &Int64Table{makeTable64(cap, maxLoad)}
}

func (t *Int64Table) Reset() { t.reset() }

func (t *Int64Table) Len() int { return t.len }

func (t *Int64Table) Cap() int { return t.cap }

func (t *Int64Table) Probe(keys []int64, values []int32) int {
	return t.probe(unsafecast.Int64ToUint64(keys), values)
}

type Float64Table struct{ table64 }

func NewFloat64Table(cap int, maxLoad float64) *Float64Table {
	return &Float64Table{makeTable64(cap, maxLoad)}
}

func (t *Float64Table) Reset() { t.reset() }

func (t *Float64Table) Len() int { return t.len }

func (t *Float64Table) Cap() int { return t.cap }

func (t *Float64Table) Probe(keys []float64, values []int32) int {
	return t.probe(unsafecast.Float64ToUint64(keys), values)
}

type Uint64Table struct{ table64 }

func NewUint64Table(cap int, maxLoad float64) *Uint64Table {
	return &Uint64Table{makeTable64(cap, maxLoad)}
}

func (t *Uint64Table) Reset() { t.reset() }

func (t *Uint64Table) Len() int { return t.len }

func (t *Uint64Table) Cap() int { return t.cap }

func (t *Uint64Table) Probe(keys []uint64, values []int32) int {
	return t.probe(keys, values)
}

type table64 struct {
	len     int
	cap     int
	maxLen  int
	maxLoad float64
	seed    uintptr
	table   []uint64
}

func makeTable64(cap int, maxLoad float64) (t table64) {
	if cap < 64 {
		cap = 64
	}
	t.init(nextPowerOf2(cap), maxLoad)
	return t
}

func (t *table64) init(cap int, maxLoad float64) {
	*t = table64{
		cap:     cap,
		maxLen:  int(math.Ceil(maxLoad * float64(cap))),
		maxLoad: maxLoad,
		seed:    randSeed(),
		table:   make([]uint64, cap/64+2*cap),
	}
}

func (t *table64) grow(totalValues int) {
	cap := 2 * t.cap
	totalValues = nextPowerOf2(totalValues)
	if totalValues > cap {
		cap = totalValues
	}

	tmp := table64{}
	tmp.init(cap, t.maxLoad)
	tmp.len = t.len

	flags, pairs := t.content()

	for i, f := range flags {
		if f != 0 {
			j := 64 * i
			n := bits.TrailingZeros64(f)
			j += n
			f >>= uint(n)

			for {
				n := bits.TrailingZeros64(^f)
				k := j + n
				tmp.insert(pairs[2*j : 2*k : 2*k])
				j += n
				f >>= uint(n)
				if f == 0 {
					break
				}
				n = bits.TrailingZeros64(f)
				j += n
				f >>= uint(n)
			}
		}
	}

	*t = tmp
}

func (t *table64) insert(pairs []uint64) {
	flags, table := t.content()
	mod := uintptr(t.cap) - 1

	for i := 0; i < len(pairs); i += 2 {
		hash := hash64(pairs[i], t.seed)

		for {
			position := hash & mod
			index := position / 64
			shift := position % 64

			if (flags[index] & (1 << shift)) == 0 {
				flags[index] |= 1 << shift
				table[2*position+0] = pairs[i+0]
				table[2*position+1] = pairs[i+1]
				break
			}

			hash++
		}
	}
}

func (t *table64) content() (flags, pairs []uint64) {
	n := t.cap / 64
	return t.table[:n:n], t.table[n:len(t.table):len(t.table)]
}

func (t *table64) reset() {
	for i := range t.table {
		t.table[i] = 0
	}
	t.len = 0
}

func (t *table64) probe(keys []uint64, values []int32) int {
	if totalValues := t.len + len(keys); totalValues > t.maxLen {
		t.grow(totalValues)
	}

	var hashes [probesPerLoop]uintptr
	var baseLength = t.len
	var useAesHash = aeshash.Enabled()

	for i := 0; i < len(keys); {
		j := len(hashes) + i
		n := len(hashes)

		if j > len(keys) {
			j = len(keys)
			n = len(keys) - i
		}

		h := hashes[:n:n]
		k := keys[i:j:j]
		v := values[i:j:j]

		if useAesHash {
			aeshash.MultiHash64(h, k, t.seed)
		} else {
			wyhash.MultiHash64(h, k, t.seed)
		}

		t.len = multiProbe64(t.table, t.len, t.cap, h, k, v)
		i = j
	}

	return t.len - baseLength
}

type Uint128Table struct{ table128 }

func NewUint128Table(cap int, maxLoad float64) *Uint128Table {
	return &Uint128Table{makeTable128(cap, maxLoad)}
}

func (t *Uint128Table) Reset() { t.reset() }

func (t *Uint128Table) Len() int { return t.len }

func (t *Uint128Table) Cap() int { return t.cap }

func (t *Uint128Table) Probe(keys [][16]byte, values []int32) int {
	return t.probe(keys, values)
}

type table128 struct {
	len     int
	cap     int
	maxLen  int
	maxLoad float64
	seed    uintptr
	table   []byte
}

func makeTable128(cap int, maxLoad float64) (t table128) {
	if cap < 8 {
		cap = 8
	}
	t.init(nextPowerOf2(cap), maxLoad)
	return t
}

func (t *table128) init(cap int, maxLoad float64) {
	*t = table128{
		cap:     cap,
		maxLen:  int(math.Ceil(maxLoad * float64(cap))),
		maxLoad: maxLoad,
		seed:    randSeed(),
		table:   make([]byte, cap/8+16*cap+4*cap),
	}
}

func (t *table128) grow(totalValues int) {
	cap := 2 * t.cap
	totalValues = nextPowerOf2(totalValues)
	if totalValues > cap {
		cap = totalValues
	}

	tmp := table128{}
	tmp.init(cap, t.maxLoad)
	tmp.len = t.len

	oldFlags, oldKeys, oldValues := t.content()
	newFlags, newKeys, newValues := tmp.content()
	modulo := uintptr(tmp.cap) - 1

	for i := range oldKeys {
		x := i / 8
		y := i % 8

		if (oldFlags[x] & (1 << y)) == 0 {
			continue
		}

		hash := hash128(oldKeys[i], tmp.seed) & modulo
		for {
			index := hash / 8
			shift := hash % 8

			if (newFlags[index] & (1 << shift)) == 0 {
				newFlags[index] |= 1 << shift
				newKeys[hash] = oldKeys[i]
				newValues[hash] = oldValues[i]
				break
			}

			hash = (hash + 1) & modulo
		}
	}

	*t = tmp
}

func (t *table128) content() (flags []byte, keys [][16]byte, values []int32) {
	i := t.cap / 8
	j := 16*t.cap + i
	return t.table[:i], unsafecast.BytesToUint128(t.table[i:j]), unsafecast.BytesToInt32(t.table[j:])
}

func (t *table128) reset() {
	for i := range t.table {
		t.table[i] = 0
	}
	t.len = 0
}

func (t *table128) probe(keys [][16]byte, values []int32) int {
	if totalValues := t.len + len(keys); totalValues > t.maxLen {
		t.grow(totalValues)
	}

	var hashes [probesPerLoop]uintptr
	var baseLength = t.len
	var useAesHash = aeshash.Enabled()

	for i := 0; i < len(keys); {
		j := len(hashes) + i
		n := len(hashes)

		if j > len(keys) {
			j = len(keys)
			n = len(keys) - i
		}

		h := hashes[:n:n]
		k := keys[i:j:j]
		v := values[i:j:j]

		if useAesHash {
			aeshash.MultiHash128(h, k, t.seed)
		} else {
			wyhash.MultiHash128(h, k, t.seed)
		}

		t.len = multiProbe128(t.table, t.len, t.cap, h, k, v)
		i = j
	}

	return t.len - baseLength
}
