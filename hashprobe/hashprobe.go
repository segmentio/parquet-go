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
// looking for indexes greater or equal to the length of the table before the
// call.
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

// table32 is the generic implementation of probing tables for 32 bit types.
//
// The table uses the following memory layout:
//
//		[bitmap][keys][values]
//
// - The bitmap is used to determine which slots of the table are occupied,
//   when a bit is set to 1, the key and value at the corresponding index in
//   the arrays of keys and values have been assigned.
//
// - The array of keys is written right after the bitmap. All keys are 32 bits
//   values.
//
// - Values are written after the keys, with each slot of the keys array mapping
//   to a value at the same slot in the values array.
//
// This memory layout is a combination between simplicity and performance; the
// bitmap is required in order to differentiate between key slots with the zero
// value, and key slots that have not yet been assigned.
//
// The memory layout is also optimized for fast probing of existing keys, in
// which case the lookup can be performed with only 4 memory loads, and 1 store:
// 1. load the bitmap location and test the bit to see that the slot is occupied
// 2. load the key at the corresponding index, compare with the current key
// 3. load the value at the corresponding index, save in the output buffer
//
// Conflicts are resolved by testing the next slot, until a free slot is found.
// This strategy relies on having a good hashing function with fairly strong
// resistance to attacks that would attempt to create a layout where all keys
// conflict. The package uses hashing functions similar to the ones used by the
// Go runtime for this purpose.
//
// https://en.wikipedia.org/wiki/Linear_probing
type table32 struct {
	len     int
	cap     int
	maxLen  int
	maxLoad float64
	seed    uintptr
	table   []uint32
}

func makeTable32(cap int, maxLoad float64) (t table32) {
	if maxLoad < 0 || maxLoad > 1 {
		panic("max load of probing table must be a value between 0 and 1")
	}
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

	flags, keys, values := t.content()

	for i, f := range flags {
		if f != 0 {
			j := 32 * i
			n := bits.TrailingZeros32(f)
			j += n
			f >>= uint(n)

			for {
				n := bits.TrailingZeros32(^f)
				k := j + n
				tmp.insert(keys[j:k:k], values[j:k:k])
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

func (t *table32) insert(keys, values []uint32) {
	tableFlags, tableKeys, tableValues := t.content()
	hashes := make([]uintptr, len(keys), 32)
	modulo := uintptr(t.cap) - 1

	if aeshash.Enabled() {
		aeshash.MultiHash32(hashes, keys, t.seed)
	} else {
		wyhash.MultiHash32(hashes, keys, t.seed)
	}

	for i, hash := range hashes {
		for {
			hash &= modulo
			index := hash / 32
			shift := hash % 32

			if (tableFlags[index] & (1 << shift)) == 0 {
				tableFlags[index] |= 1 << shift
				tableKeys[hash] = keys[i]
				tableValues[hash] = values[i]
				break
			}

			hash++
		}
	}
}

func (t *table32) content() (flags, keys, values []uint32) {
	i := t.cap / 32
	j := t.cap + i
	k := len(t.table)
	return t.table[:i:i], t.table[i:j:j], t.table[j:k:k]
}

func (t *table32) reset() {
	t.len = 0

	for i := range t.table {
		t.table[i] = 0
	}
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

// table64 is the generic implementation of probing tables for 64 bit types.
//
// The datastructure follows the same implementation as the one used by table32
// but the keys are 64 bit values.
type table64 struct {
	len     int
	cap     int
	maxLen  int
	maxLoad float64
	seed    uintptr
	table   []byte
}

func makeTable64(cap int, maxLoad float64) (t table64) {
	if maxLoad < 0 || maxLoad > 1 {
		panic("max load of probing table must be a value between 0 and 1")
	}
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
		table:   make([]byte, cap/8+8*cap+4*cap),
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

	flags, keys, values := t.content()

	for i, f := range flags {
		if f != 0 {
			j := 64 * i
			n := bits.TrailingZeros64(f)
			j += n
			f >>= uint(n)

			for {
				n := bits.TrailingZeros64(^f)
				k := j + n
				tmp.insert(keys[j:k:k], values[j:k:k])
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

func (t *table64) insert(keys []uint64, values []int32) {
	tableFlags, tableKeys, tableValues := t.content()
	hashes := make([]uintptr, len(keys), 64)
	modulo := uintptr(t.cap) - 1

	if aeshash.Enabled() {
		aeshash.MultiHash64(hashes, keys, t.seed)
	} else {
		wyhash.MultiHash64(hashes, keys, t.seed)
	}

	for i, hash := range hashes {
		for {
			hash &= modulo
			index := hash / 64
			shift := hash % 64

			if (tableFlags[index] & (1 << shift)) == 0 {
				tableFlags[index] |= 1 << shift
				tableKeys[hash] = keys[i]
				tableValues[hash] = values[i]
				break
			}

			hash++
		}
	}
}

func (t *table64) content() (flags, keys []uint64, values []int32) {
	i := t.cap / 8
	j := 8*t.cap + i
	k := len(t.table)
	return unsafecast.BytesToUint64(t.table[:i:i]), unsafecast.BytesToUint64(t.table[i:j:j]), unsafecast.BytesToInt32(t.table[j:k:k])
}

func (t *table64) reset() {
	t.len = 0

	for i := range t.table {
		t.table[i] = 0
	}
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

// table128 is the generic implementation of probing tables for 128 bit types.
//
// The datastructure follows the same implementation as the one used by table32
// but the keys are 128 bit values.
type table128 struct {
	len     int
	cap     int
	maxLen  int
	maxLoad float64
	seed    uintptr
	table   []byte
}

func makeTable128(cap int, maxLoad float64) (t table128) {
	if maxLoad < 0 || maxLoad > 1 {
		panic("max load of probing table must be a value between 0 and 1")
	}
	if cap < 64 {
		cap = 64
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

	flags, keys, values := t.content()

	for i, f := range flags {
		if f != 0 {
			j := 64 * i
			n := bits.TrailingZeros64(f)
			j += n
			f >>= uint(n)

			for {
				n := bits.TrailingZeros64(^f)
				k := j + n
				tmp.insert(keys[j:k:k], values[j:k:k])
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

func (t *table128) insert(keys [][16]byte, values []int32) {
	tableFlags, tableKeys, tableValues := t.content()
	hashes := make([]uintptr, len(keys), 64)
	modulo := uintptr(t.cap) - 1

	if aeshash.Enabled() {
		aeshash.MultiHash128(hashes, keys, t.seed)
	} else {
		wyhash.MultiHash128(hashes, keys, t.seed)
	}

	for i, hash := range hashes {
		for {
			hash &= modulo
			index := hash / 64
			shift := hash % 64

			if (tableFlags[index] & (1 << shift)) == 0 {
				tableFlags[index] |= 1 << shift
				tableKeys[hash] = keys[i]
				tableValues[hash] = values[i]
				break
			}

			hash++
		}
	}
}

func (t *table128) content() (flags []uint64, keys [][16]byte, values []int32) {
	i := t.cap / 8
	j := 16*t.cap + i
	k := len(t.table)
	return unsafecast.BytesToUint64(t.table[:i:i]), unsafecast.BytesToUint128(t.table[i:j:j]), unsafecast.BytesToInt32(t.table[j:k:k])
}

func (t *table128) reset() {
	t.len = 0

	for i := range t.table {
		t.table[i] = 0
	}
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
