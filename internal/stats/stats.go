package stats

import (
	"sync"
	"sync/atomic"
	"time"
)

type Counter int64

func (c *Counter) ptr() *int64 {
	return (*int64)(c)
}

func (c *Counter) Inc() {
	atomic.AddInt64(c.ptr(), 1)
}

func (c *Counter) Snapshot() int64 {
	return atomic.SwapInt64(c.ptr(), 0)
}

func (c *Counter) Add(n int64) {
	atomic.AddInt64(c.ptr(), n)
}

type Duration struct {
	t     time.Time
	mutex sync.Mutex
	once  sync.Once
}

func (d *Duration) Init() {
	d.once.Do(func() {
		d.mutex.Lock()
		defer d.mutex.Unlock()
		if !d.t.IsZero() {
			return
		}
		d.t = time.Now()
	})
}

func (d *Duration) Snapshot() time.Duration {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	now := time.Now()
	delta := now.Sub(d.t)
	d.t = now
	return delta
}
