package consensus

import (
	"thunder2/blockchain"
	"time"
)

// TODO(thunder): discuss and set a better value.
const voterWaitingTimeInMS = 6000

type Timer interface {
	GetChannel() <-chan time.Time
	// epoch is used to have a more deterministic timeout in test.
	// We don't expect it's necessary for the production code.
	Reset(epoch blockchain.Epoch)
}

type timerImpl struct {
	timer *time.Timer
}

func NewTimer(epoch blockchain.Epoch) Timer {
	return &timerImpl{time.NewTimer(voterWaitingTimeInMS * time.Millisecond)}
}

func (t *timerImpl) GetChannel() <-chan time.Time {
	return t.timer.C
}

func (t *timerImpl) Reset(epoch blockchain.Epoch) {
	if !t.timer.Stop() {
		<-t.timer.C
	}
	t.timer.Reset(voterWaitingTimeInMS * time.Millisecond)
}
