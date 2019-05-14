package utils

import (
	"fmt"
	"sync"
)

// Copied from src/thunder/utils/mutex.go and src/thunder/libs/debug/debug.go
type CheckedLock struct {
	lock   sync.Mutex
	locked bool
}

// Lock locks the CheckedLock, and records that it is locked.
func (c *CheckedLock) Lock() {
	c.lock.Lock()
	c.locked = true
}

// Unlock unlocks the CheckedLock, but debug.Bugs if the check fails.
func (c *CheckedLock) Unlock() {
	if !c.locked {
		Bug("Double unlocking sync.Mutex")
	}
	c.locked = false
	c.lock.Unlock()
}

// CheckIsLocked uses lock.CheckIsLocked("message") to check that a lock is held.
//
// It was legal but broken to use if c.IsLocked() { Bug() }
// because if you don't hold the lock, reading c.locked is actually
// a data race.
//
// In most cases, the reason to use locks in the first place was to
// synchronize with otherwise asynchronous events, so it is also
// legal for the lock to be in a locked state if the object is
// already "live".  If you need to check that an object is not
// in a "live" state, the simplest approach is to use a boolean;
// mutual exclusion is not required - and Go's race detector will
// hopefully catch you if you are not correct.
func (c *CheckedLock) CheckIsLocked(msg string) {
	if !c.locked {
		Bug(msg)
	}
}

// This function is used to indicate a logic bug has been found.
// Do not use it for error handling or parse failures, except
// when the result should have been guaranteed to be a success.
//
// Examples: Proper locks are not held, array bounds exceeded,
func Bug(s string, args ...interface{}) {
	panic(fmt.Sprintf("BUG: "+s, args...))
}

// Fatal is used to indicate that a condition or error has been
// encountered that no longer allows making forward progress.
//
// Examples: Filesystem failure, corrupt database, unable to
//           connect or open ports
//
// This is completely different from log.Fatal; do not use log.Fatal
// except in some kind of dire emergency (infinite recursion, deadlock...)
// log.Fatal immediately terminates the process, with no chance for
// clean shutdown.  This is only appropriate during single threaded
// initialization, for simple command line argument parsing, where the
// cause is immediately obvious.
//
// If this is not the case, do not use it.  It terminates without flushing
// logs (even stderr) and gives no goroutine backtrace, making the situation
// complicated to debug.  Bear in mind that many tests use multiple threads
// and initialize things in unexpected ways and so it is unwise to embed
// log.Fatal in deeply nested functions.
func Fatal(s string, args ...interface{}) {
	panic(fmt.Sprintf("FATAL ERROR: "+s, args...))
}

// NotImplemented means the functionality or error handling required
// has not yet been implemented.
//
// Examples: Missing protocol handling, no error handling in caller
//           Possible state transition not yet implemented
func NotImplemented(s string, args ...interface{}) {
	panic(fmt.Sprintf("NOT IMPLEMENTED: "+s, args...))
}
