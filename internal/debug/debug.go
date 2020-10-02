package debug

import (
	"log"
	"sync/atomic"
)

var enabled int32 = 0

// Toggle turns on/off debug mode
func Toggle(on bool) {
	val := int32(0)
	if on {
		val = 1
	}
	atomic.StoreInt32(&enabled, val)
}

// Do executes a function if debug is enabled, usually for side effects.
func Do(f func()) {
	if atomic.LoadInt32(&enabled) != 1 {
		return
	}
	f()
}

// Format a log line and writes it to stderr if debug is enabled
func Format(format string, args ...interface{}) {
	if atomic.LoadInt32(&enabled) != 1 {
		return
	}
	log.Printf(format, args...)
}
