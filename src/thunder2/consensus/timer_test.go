package consensus

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTimerFake(t *testing.T) {
	req := require.New(t)

	timer := NewTimerFake(1)
	ch := timer.GetChannel()
	select {
	case <-ch:
		req.FailNow("expect blocked")
	default:
	}

	timer.(*TimerFake).AllowAdvancingEpochTo(2, time.Nanosecond)
	// Expect the returned channel is unblocked.
	select {
	case <-ch:
	case <-time.NewTimer(10 * time.Millisecond).C:
		req.FailNow("expect not blocked")
	}
}
