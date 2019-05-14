package consensus

import (
	"thunder2/blockchain"

	"github.com/pkg/errors"
)

// About goroutine safety: live in the same goroutine of the caller,
// i.e., the worker goroutine of Mediator.
// Note that it will has its worker goroutine in the final version.
//
// TODO(thunder): rewrite ChainSyncer. Here are some design requirements:
// * Ensure there is no stall: Maybe sending heartbeat messages with Status periodically.
// * Catch up efficiently: use batch operations.
// * Catch up efficiently: support proactively push data to proposer/voter candidates.
// * Network hiccups: request again if there is no progress.
// * Identify invalid status from byzantine nodes.
type ChainSyncer struct {
	loggingId string
	client    ChainSyncerClient
	status    Status
	// Instead of only remembering the node who has the latest data, remember all nodes
	// who have newer data. This is useful when we want to fetch data in parallel.
	requests               map[string]*syncRequest
	lastRequestedSn        blockchain.BlockSn
	lastReceivedProposalSn blockchain.BlockSn
}

type CatchUpPolicy int

var (
	CatchUpPolicyMust            = CatchUpPolicy(1)
	CatchUpPolicyIfNotInProgress = CatchUpPolicy(2)
)

type syncRequest struct {
	id string
	// TODO(thunder): sync Status.Epoch
	status Status
}

// Note that all methods do not return error. This allows the client to execute asynchronously.
// If ChainSyncer really needs to know whether the execution succeeds, use some other way
// to get the result.
type ChainSyncerClient interface {
	RequestNotarizedBlock(id string, sn blockchain.BlockSn)
	RequestProposal(id string, sn blockchain.BlockSn)
	OnCaughtUp(id string, s Status)
}

type SyncerDebugState struct {
	Status            Status
	LastRequestId     string
	LastRequestStatus Status
}

//--------------------------------------------------------------------

func NewChainSyncer(
	loggingId string, client ChainSyncerClient) *ChainSyncer {
	return &ChainSyncer{
		loggingId: loggingId,
		client:    client,
		requests:  make(map[string]*syncRequest),
	}
}

func (c *ChainSyncer) SetFreshestNotarizedChainBlockSn(sn blockchain.BlockSn) error {
	logger.Debug("[%s] SetFreshestNotarizedChainBlockSn %s", c.loggingId, sn)
	if c.status.FncBlockSn.Compare(sn) > 0 {
		return errors.Errorf("update smaller BlockSn: %s < %s", sn, c.status.FncBlockSn)
	}
	c.status.FncBlockSn = sn
	c.requestNextBlock(false)
	return nil
}

func (c *ChainSyncer) SetBlockNotExisted(sn blockchain.BlockSn) error {
	logger.Debug("[%s] SetBlockNotExisted %s", c.loggingId, sn)
	c.requestNextBlock(true)
	return nil
}

func (c *ChainSyncer) SetEpoch(epoch blockchain.Epoch) error {
	if c.status.Epoch > epoch {
		return errors.Errorf("update smaller epoch: %d < %d", epoch, c.status.Epoch)
	}
	c.status.Epoch = epoch
	return nil
}

func (c *ChainSyncer) SetReconfFinalizedByBlockSn(sn blockchain.BlockSn) error {
	logger.Debug("[%s] SetReconfFinalizedByBlockSn %s", c.loggingId, sn)
	c.status.ReconfFinalizedByBlockSn = sn
	return nil
}

func (c *ChainSyncer) SetReceivedProposalBlockSn(sn blockchain.BlockSn) error {
	logger.Debug("[%s] SetReceivedProposalBlockSn %s", c.loggingId, sn)
	if sn.Compare(c.lastReceivedProposalSn) > 0 {
		c.lastReceivedProposalSn = sn
	}
	c.requestNextBlock(false)
	return nil
}

func (c *ChainSyncer) IsAllCaughtUp() bool {
	return len(c.requests) == 0
}

func (c *ChainSyncer) Stop() error {
	// TODO(thunder): ChainSyncer will have its worker goroutine. Stop the worker goroutine.
	return nil
}

// TODO(thunder): catching up epoch.
func (c *ChainSyncer) CatchUp(id string, s Status, policy CatchUpPolicy) error {
	if _, ok := c.requests[id]; policy == CatchUpPolicyMust || !ok {
		c.requests[id] = &syncRequest{id, s}
	}
	logger.Info("[%s] request chain syncer to catching up %s -> %s:%s (policy=%s); result=%s",
		c.loggingId, c.status, id, s, policy, c.requests[id])
	c.requestNextBlock(false)
	return nil
}

func (c *ChainSyncer) requestNextBlock(lastRequestNotExisted bool) {
	r := c.findClosestRequest()
	if r == nil {
		return
	}

	// Try to find the next sn to make a request.
	sn := c.status.FncBlockSn
	if lastRequestNotExisted {
		sn.Epoch = c.lastRequestedSn.Epoch + 1
		sn.S = 1
	} else if sn == blockchain.GetGenesisBlockSn() {
		// Assume we already have the genesis block, so request the next one {1, 1}.
		sn = blockchain.BlockSn{Epoch: 1, S: 1}
	} else {
		sn.S++
	}
	c.lastRequestedSn = sn
	if sn.Compare(r.status.FncBlockSn) > 0 && sn.Compare(r.status.ReconfFinalizedByBlockSn) <= 0 {
		if sn.Compare(c.lastReceivedProposalSn) <= 0 {
			sn.S = c.lastReceivedProposalSn.S + 1
		}
		c.client.RequestProposal(r.id, sn)
	} else {
		c.client.RequestNotarizedBlock(r.id, sn)
	}
}

func (c *ChainSyncer) findClosestRequest() *syncRequest {
	for len(c.requests) > 0 {
		var min *syncRequest
		for _, r := range c.requests {
			if min == nil || r.status.FncBlockSn.Compare(min.status.FncBlockSn) < 0 {
				min = r
			}
		}
		if c.isCaughtUp(min) {
			logger.Info("[%s] chain syncer ends catching up %s:%s", c.loggingId, min.id, min.status)
			delete(c.requests, min.id)
			c.client.OnCaughtUp(min.id, min.status)
			continue
		}
		return min
	}
	return nil
}

func (c *ChainSyncer) isCaughtUp(r *syncRequest) bool {
	if c.status.FncBlockSn.Compare(r.status.FncBlockSn) < 0 {
		return false
	}
	if c.status.FncBlockSn.Compare(r.status.ReconfFinalizedByBlockSn) >= 0 {
		return true
	}
	if c.lastReceivedProposalSn.Compare(r.status.ReconfFinalizedByBlockSn) >= 0 {
		return true
	}
	return false
}

// Note that ChainSyncer will have its own goroutine, so any getter is async.
func (c *ChainSyncer) GetDebugState() chan SyncerDebugState {
	ch := make(chan SyncerDebugState, 1)
	var max *syncRequest
	for _, r := range c.requests {
		if max == nil || r.status.FncBlockSn.Compare(max.status.FncBlockSn) > 0 {
			max = r
		}
	}
	if max == nil {
		max = &syncRequest{"n.a.", Status{}}
	}
	ch <- SyncerDebugState{
		Status:            c.status,
		LastRequestId:     max.id,
		LastRequestStatus: max.status,
	}
	return ch
}

//--------------------------------------------------------------------

func (policy CatchUpPolicy) String() string {
	switch policy {
	case CatchUpPolicyMust:
		return "CatchUpPolicyMust"
	case CatchUpPolicyIfNotInProgress:
		return "CatchUpPolicyIfNotInProgress"
	default:
		return "unknown"
	}
}

func (s *syncRequest) String() string {
	return s.id + ":" + s.status.String()
}
