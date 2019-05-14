package consensus

import (
	"testing"
	"thunder2/blockchain"

	"github.com/stretchr/testify/require"
)

type request struct {
	id string
	// Either epoch or sn is set, but both are not set at the same time.
	epoch blockchain.Epoch
	sn    blockchain.BlockSn
	// Used when sn is set.
	isProposal bool
}

type done struct {
	id     string
	status Status
}

type chainSyncerClientFake struct {
	requestChan chan request
	doneChan    chan done
}

func newChainSyncerClientFake() *chainSyncerClientFake {
	return &chainSyncerClientFake{
		requestChan: make(chan request, 1024),
		doneChan:    make(chan done, 1024),
	}
}

func (c *chainSyncerClientFake) RequestEpochProof(id string, epoch blockchain.Epoch) {
	c.requestChan <- request{id, epoch, blockchain.BlockSn{}, false}
}

func (c *chainSyncerClientFake) RequestNotarizedBlock(id string, sn blockchain.BlockSn) {
	c.requestChan <- request{id, 0, sn, false}
}

func (c *chainSyncerClientFake) RequestProposal(id string, sn blockchain.BlockSn) {
	c.requestChan <- request{id, 0, sn, true}
}

func (c *chainSyncerClientFake) OnCaughtUp(id string, s Status) {
	c.doneChan <- done{id, s}
}

func TestChainSync(t *testing.T) {
	t.Run("catch up nodes at the same epoch without timeout", func(t *testing.T) {
		req := require.New(t)

		client := newChainSyncerClientFake()
		c := NewChainSyncer("p1", client)
		epoch := blockchain.Epoch(2)
		c.SetFreshestNotarizedChainBlockSn(blockchain.BlockSn{Epoch: epoch, S: 5})
		c.SetEpoch(epoch)
		targetStatus := []Status{
			Status{
				Epoch:      epoch,
				FncBlockSn: blockchain.BlockSn{Epoch: epoch, S: 10},
			},
			Status{
				Epoch:      epoch,
				FncBlockSn: blockchain.BlockSn{Epoch: epoch, S: 20},
			},
			Status{
				Epoch:      epoch,
				FncBlockSn: blockchain.BlockSn{Epoch: epoch, S: 30},
			},
		}
		// Call CatchUp() with unordered S.
		c.CatchUp("v1", targetStatus[1], CatchUpPolicyMust)
		c.CatchUp("v2", targetStatus[0], CatchUpPolicyMust)
		c.CatchUp("v3", targetStatus[2], CatchUpPolicyMust)

		// Expect ChainSyncer requests BlockSn{epoch,6} from some node whenever CatchUp() is called.
		count := 3
		for r := range client.requestChan {
			req.Equal(blockchain.BlockSn{Epoch: epoch, S: 6}, r.sn)
			req.False(r.isProposal)
			count--
			if count == 0 {
				break
			}
		}
		req.Equal(0, count)
		// Notify ChainSyncer of the progress
		c.SetFreshestNotarizedChainBlockSn(blockchain.BlockSn{Epoch: epoch, S: 6})
		i := uint32(7)
		for r := range client.requestChan {
			// Expect ChainSyncer requests BlockSn{epoch,i} from some node.
			req.Equal(blockchain.BlockSn{Epoch: epoch, S: i}, r.sn)
			req.False(r.isProposal)
			// Notify ChainSyncer of the progress
			c.SetFreshestNotarizedChainBlockSn(blockchain.BlockSn{Epoch: epoch, S: i})
			i++
			if i > 10 {
				break
			}
		}
		req.Equal(uint32(11), i)

		// Expect catching up v2 is done
		d := <-client.doneChan
		req.Equal("v2", d.id)
		req.Equal(targetStatus[0], d.status)

		for r := range client.requestChan {
			// Expect ChainSyncer requests BlockSn{epoch,i} from some node.
			req.Equal(blockchain.BlockSn{Epoch: epoch, S: i}, r.sn)
			req.False(r.isProposal)
			// Notify ChainSyncer of the progress
			c.SetFreshestNotarizedChainBlockSn(blockchain.BlockSn{Epoch: epoch, S: i})
			i++
			if i > 20 {
				break
			}
		}
		req.Equal(uint32(21), i)

		// Expect catching up v1 is done
		d = <-client.doneChan
		req.Equal("v1", d.id)
		req.Equal(targetStatus[1], d.status)

		for r := range client.requestChan {
			// Expect ChainSyncer requests BlockSn{epoch,i} from v3.
			req.Equal("v3", r.id)
			req.Equal(blockchain.BlockSn{Epoch: epoch, S: i}, r.sn)
			req.False(r.isProposal)
			// Notify ChainSyncer of the progress
			c.SetFreshestNotarizedChainBlockSn(blockchain.BlockSn{Epoch: epoch, S: i})
			i++
			if i == uint32(31) {
				break
			}
		}
		req.Equal(uint32(31), i)

		// Expect catching up v3 is done
		d = <-client.doneChan
		req.Equal("v3", d.id)
		req.Equal(targetStatus[2], d.status)
	})

	t.Run("catch up the different epoch without timeout", func(t *testing.T) {
		// Test catching up from (1,5) to (4,5).
		// Assume the chain is (1,1-10) -> (4,1-5). There is no (2,s) and (3,s).
		req := require.New(t)

		client := newChainSyncerClientFake()
		c := NewChainSyncer("p1", client)
		epoch := blockchain.Epoch(4)
		c.SetFreshestNotarizedChainBlockSn(blockchain.BlockSn{Epoch: 1, S: 5})
		targetStatus := Status{
			Epoch:      epoch,
			FncBlockSn: blockchain.BlockSn{Epoch: epoch, S: 3},
		}
		c.CatchUp("v1", targetStatus, CatchUpPolicyMust)

		// Expect ChainSyncer requests ClockNotarization first.
		r := <-client.requestChan
		// Expect ChainSyncer requests epoch=4 from v1.
		req.Equal("v1", r.id)
		req.Equal(epoch, r.epoch)
		// Notify ChainSyncer of the progress.
		c.SetEpoch(epoch)

		// Expect ChainSyncer requests BlockSn{1,6-10}
		i := uint32(6)
		for r := range client.requestChan {
			// Expect ChainSyncer requests BlockSn{1,i} from v1.
			req.Equal("v1", r.id)
			req.Equal(blockchain.BlockSn{Epoch: 1, S: i}, r.sn)
			req.False(r.isProposal)
			// Notify ChainSyncer of the progress
			c.SetFreshestNotarizedChainBlockSn(blockchain.BlockSn{Epoch: 1, S: i})
			i++
			if i > 10 {
				break
			}
		}
		req.Equal(uint32(11), i)

		// Expect ChainSyncer requests BlockSn{1,11}, BlockSn{2,1}.
		sns := []blockchain.BlockSn{
			blockchain.BlockSn{Epoch: 1, S: 11},
			blockchain.BlockSn{Epoch: 2, S: 1},
			blockchain.BlockSn{Epoch: 3, S: 1},
		}
		for _, sn := range sns {
			r := <-client.requestChan
			req.Equal("v1", r.id)
			req.Equal(sn, r.sn)
			req.False(r.isProposal)
			// Notify ChainSyncer the requested block does not exist.
			c.SetBlockNotExisted(sn)
		}

		// Expect ChainSyncer requests BlockSn{4,1-3}
		i = uint32(1)
		for r := range client.requestChan {
			// Expect ChainSyncer requests BlockSn{1,i} from v1.
			req.Equal("v1", r.id)
			req.Equal(blockchain.BlockSn{Epoch: 4, S: i}, r.sn)
			req.False(r.isProposal)
			// Notify ChainSyncer of the progress
			c.SetFreshestNotarizedChainBlockSn(blockchain.BlockSn{Epoch: 4, S: i})
			i++
			if i > 3 {
				break
			}
		}
		req.Equal(uint32(4), i)

		// Expect catching up v1 is done
		d := <-client.doneChan
		req.Equal("v1", d.id)
		req.Equal(targetStatus, d.status)
	})

	t.Run("timeout while catching up", func(t *testing.T) {
		// TODO(thunder): finish this after proposer switch is done.
	})
}

func TestChainSyncWithDifferentPolicies(t *testing.T) {
	epoch := blockchain.Epoch(2)
	targetStatus := []Status{
		Status{
			Epoch:      epoch,
			FncBlockSn: blockchain.BlockSn{Epoch: epoch, S: 5},
		},
		Status{
			Epoch:      epoch,
			FncBlockSn: blockchain.BlockSn{Epoch: epoch, S: 10},
		},
	}

	createSyncerForTest := func() (*ChainSyncer, *chainSyncerClientFake) {
		client := newChainSyncerClientFake()
		syncer := NewChainSyncer("p1", client)
		syncer.SetFreshestNotarizedChainBlockSn(blockchain.BlockSn{Epoch: epoch, S: 1})
		syncer.SetEpoch(epoch)
		return syncer, client
	}

	t.Run("catch up using CatchUpPolicyIfNotInProgress", func(t *testing.T) {
		req := require.New(t)

		c, client := createSyncerForTest()
		c.CatchUp("v1", targetStatus[0], CatchUpPolicyMust)
		c.CatchUp("v1", targetStatus[1], CatchUpPolicyIfNotInProgress)

		// Expect ChainSyncer requests whenever CatchUp() is called.
		// Thus, there is one more call.
		r := <-client.requestChan
		req.Equal(blockchain.BlockSn{Epoch: epoch, S: 2}, r.sn)
		req.False(r.isProposal)

		// Notify ChainSyncer of the progress
		i := uint32(2)
		for r := range client.requestChan {
			// Expect ChainSyncer requests BlockSn{epoch,i} from some node.
			req.Equal(blockchain.BlockSn{Epoch: epoch, S: i}, r.sn)
			req.False(r.isProposal)
			// Notify ChainSyncer of the progress
			c.SetFreshestNotarizedChainBlockSn(blockchain.BlockSn{Epoch: epoch, S: i})
			i++
			if i > targetStatus[0].FncBlockSn.S {
				break
			}
		}
		req.Equal(targetStatus[0].FncBlockSn.S+1, i)

		// Expect catching up v1 is done
		d := <-client.doneChan
		req.Equal("v1", d.id)
		req.Equal(targetStatus[0], d.status)
	})

	t.Run("catch up using CatchUpPolicyMust", func(t *testing.T) {
		req := require.New(t)

		c, client := createSyncerForTest()
		c.CatchUp("v1", targetStatus[0], CatchUpPolicyMust)
		c.CatchUp("v1", targetStatus[1], CatchUpPolicyMust)

		// Expect ChainSyncer requests whenever CatchUp() is called.
		// Thus, there is one more call.
		r := <-client.requestChan
		req.Equal(blockchain.BlockSn{Epoch: epoch, S: 2}, r.sn)
		req.False(r.isProposal)
		// Notify ChainSyncer of the progress
		i := uint32(2)
		for r := range client.requestChan {
			// Expect ChainSyncer requests BlockSn{epoch,i} from some node.
			req.Equal(blockchain.BlockSn{Epoch: epoch, S: i}, r.sn)
			req.False(r.isProposal)
			// Notify ChainSyncer of the progress
			c.SetFreshestNotarizedChainBlockSn(blockchain.BlockSn{Epoch: epoch, S: i})
			i++
			if i > targetStatus[1].FncBlockSn.S {
				break
			}
		}
		req.Equal(targetStatus[1].FncBlockSn.S+1, i)

		// Expect catching up v1 is done
		d := <-client.doneChan
		req.Equal("v1", d.id)
		req.Equal(targetStatus[1], d.status)
	})
}

func TestChainSyncForReconfiguration(t *testing.T) {
	t.Run("catch up different epoch", func(t *testing.T) {
		req := require.New(t)

		client := newChainSyncerClientFake()
		c := NewChainSyncer("p1", client)
		c.SetEpoch(1)
		c.SetFreshestNotarizedChainBlockSn(blockchain.BlockSn{Epoch: 1, S: 4})
		rfbsn := blockchain.BlockSn{Epoch: 1, S: 6}
		targetStatus := Status{
			Epoch:                    2,
			FncBlockSn:               blockchain.BlockSn{Epoch: 1, S: 5},
			ReconfFinalizedByBlockSn: rfbsn,
		}
		c.CatchUp("v1", targetStatus, CatchUpPolicyMust)

		// Expect not requesting the epoch(2).
		// Expect requesting the notarized block(1,5)
		r := <-client.requestChan
		req.Equal("v1", r.id)
		req.Equal(blockchain.BlockSn{Epoch: 1, S: 5}, r.sn)
		req.False(r.isProposal)
		// Notify ChainSyncer of the progress
		c.SetFreshestNotarizedChainBlockSn(r.sn)

		// Expect requesting the proposal(1,6) to start the reconfiguration.
		r = <-client.requestChan
		req.Equal("v1", r.id)
		req.Equal(rfbsn, r.sn)
		req.True(r.isProposal)
		// Notify ChainSyncer of the progress
		c.SetReceivedProposalBlockSn(r.sn)

		// Expect catching up v1 is done
		d := <-client.doneChan
		req.Equal("v1", d.id)
		req.Equal(targetStatus, d.status)
	})

	t.Run("catch up different epoch with a timeout block", func(t *testing.T) {
		req := require.New(t)

		// Simulate catching up from (1,4) to the chain: (1,4) -> (2,1) -> ... (2,5)
		client := newChainSyncerClientFake()
		c := NewChainSyncer("p1", client)
		c.SetEpoch(1)
		c.SetFreshestNotarizedChainBlockSn(blockchain.BlockSn{Epoch: 1, S: 4})
		rfbsn := blockchain.BlockSn{Epoch: 2, S: 6}
		targetStatus := Status{
			Epoch:                    3,
			FncBlockSn:               blockchain.BlockSn{Epoch: 2, S: 5},
			ReconfFinalizedByBlockSn: rfbsn,
		}
		c.CatchUp("v1", targetStatus, CatchUpPolicyMust)

		// Expect requesting the epoch(2)
		r := <-client.requestChan
		req.Equal("v1", r.id)
		req.Equal(blockchain.Epoch(2), r.epoch)
		// Notify ChainSyncer of the progress
		c.SetEpoch(blockchain.Epoch(2))

		// Expect requesting the notarized block(1,5)
		r = <-client.requestChan
		req.Equal("v1", r.id)
		req.Equal(blockchain.BlockSn{Epoch: 1, S: 5}, r.sn)
		req.False(r.isProposal)
		// Notify ChainSyncer of the progress
		c.SetBlockNotExisted(r.sn)

		// Expect requesting the notarized block(2,1-5)
		i := uint32(1)
		for r := range client.requestChan {
			req.Equal("v1", r.id)
			req.Equal(blockchain.BlockSn{Epoch: blockchain.Epoch(2), S: i}, r.sn)
			req.False(r.isProposal)
			// Notify ChainSyncer of the progress
			c.SetFreshestNotarizedChainBlockSn(r.sn)
			i++
			if i > 5 {
				break
			}
		}
		req.Equal(uint32(6), i)

		// Expect requesting the proposal(1,6) to start the reconfiguration.
		r = <-client.requestChan
		req.Equal("v1", r.id)
		req.Equal(rfbsn, r.sn)
		req.True(r.isProposal)
		// Notify ChainSyncer of the progress
		c.SetReceivedProposalBlockSn(r.sn)

		// Expect catching up v1 is done
		d := <-client.doneChan
		req.Equal("v1", d.id)
		req.Equal(targetStatus, d.status)
	})

	t.Run("catch up zero proposal", func(t *testing.T) {
		req := require.New(t)

		client := newChainSyncerClientFake()
		c := NewChainSyncer("p1", client)
		c.SetEpoch(1)
		c.SetFreshestNotarizedChainBlockSn(blockchain.BlockSn{Epoch: 1, S: 4})
		rfbsn := blockchain.BlockSn{Epoch: 1, S: 6}
		targetStatus := Status{
			Epoch:                    1,
			FncBlockSn:               blockchain.BlockSn{Epoch: 1, S: 6},
			ReconfFinalizedByBlockSn: rfbsn,
		}
		c.CatchUp("v1", targetStatus, CatchUpPolicyMust)

		// Expect requesting the notarized block(1,5)
		i := uint32(5)
		for r := range client.requestChan {
			req.Equal("v1", r.id)
			req.Equal(blockchain.BlockSn{Epoch: 1, S: i}, r.sn)
			req.False(r.isProposal)
			// Notify ChainSyncer of the progress
			c.SetFreshestNotarizedChainBlockSn(r.sn)
			i++
			if i > 6 {
				break
			}
		}
		req.Equal(uint32(7), i)

		// Expect catching up v2 is done
		d := <-client.doneChan
		req.Equal("v1", d.id)
		req.Equal(targetStatus, d.status)
	})

	t.Run("catch up many proposals", func(t *testing.T) {
		req := require.New(t)

		client := newChainSyncerClientFake()
		c := NewChainSyncer("p1", client)
		c.SetEpoch(1)
		c.SetFreshestNotarizedChainBlockSn(blockchain.BlockSn{Epoch: 1, S: 4})
		rfbsn := blockchain.BlockSn{Epoch: 1, S: 10}
		targetStatus := Status{
			Epoch:                    1,
			FncBlockSn:               blockchain.BlockSn{Epoch: 1, S: 5},
			ReconfFinalizedByBlockSn: rfbsn,
		}
		c.CatchUp("v1", targetStatus, CatchUpPolicyMust)

		// Expect requesting the notarized block(1,5)
		r := <-client.requestChan
		req.Equal("v1", r.id)
		req.Equal(blockchain.BlockSn{Epoch: 1, S: 5}, r.sn)
		req.False(r.isProposal)
		// Notify ChainSyncer of the progress
		c.SetFreshestNotarizedChainBlockSn(r.sn)

		// Expect requesting the proposal(1,6-10) to start the reconfiguration.
		i := uint32(6)
		for r := range client.requestChan {
			req.Equal("v1", r.id)
			req.Equal(blockchain.BlockSn{Epoch: 1, S: i}, r.sn)
			req.True(r.isProposal)
			// Notify ChainSyncer of the progress
			c.SetReceivedProposalBlockSn(r.sn)
			i++
			if i > rfbsn.S {
				break
			}
		}
		req.Equal(rfbsn.S+1, i)

		// Expect catching up v2 is done
		d := <-client.doneChan
		req.Equal("v1", d.id)
		req.Equal(targetStatus, d.status)
	})
}

// TODO(thunder): test handling contracted status from different nodes.
