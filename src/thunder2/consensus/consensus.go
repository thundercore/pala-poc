// Class Has-A Relations:
// https://docs.google.com/presentation/d/1AY-GiujqkzRdfdleDSrj516d48-3w-z70w4DQiy_3HY/edit?usp=sharing
//
// Data flow:
// https://docs.google.com/presentation/d/1vQ1Kh5O_kNXe0y0GK9c26UTmblPIdx8DDoKmPhrrr3c/edit?usp=sharing

package consensus

import (
	"fmt"
	"sync"
	"thunder2/blockchain"
	"thunder2/lgr"
	"thunder2/network"
	"thunder2/utils"

	"github.com/petar/GoLLRB/llrb"
	"github.com/pkg/errors"
)

var logger = lgr.NewLgr("/consensus")

type Role int

const (
	RoleFullNode = Role(1 << 0)
	RoleProposer = Role(1 << 1)
	RoleVoter    = Role(1 << 2)
)

type NodeConfig struct {
	LoggingId  string
	K          uint32 // The outstanding window
	Chain      blockchain.BlockChain
	NodeClient NodeClient
	Role       RoleAssigner
	Verifier   blockchain.Verifier
	Epoch      blockchain.Epoch
	Timer      Timer
}

type StartStopWaiter struct {
	// mutex protects all members.
	mutex sync.Mutex
	// Notifies the caller of Start() stopping
	stopChan chan interface{}
	// The caller of Start() is responsible to notify the service is stopped via stoppedChan.
	stoppedChan chan interface{}
}

// Note that all methods do not return error. This allows the client to execute asynchronously.
// If Node really needs to know whether the execution succeeds, use some other way
// to get the result.
type NodeClient interface {
	Broadcast(m blockchain.Message)
	// Reply sends |m| to the one who sent |source|.
	Reply(source *network.Message, m blockchain.Message)
	CatchUp(source *network.Message, sn blockchain.BlockSn)
	// UpdateEpoch updates the epoch by clock message notarization.
	UpdateEpoch(cNota blockchain.ClockMsgNota)
}

// Requirement / Design
// * All operations are goroutine-safe.
// * Use the active object pattern to avoid using locks for main data.
//
// Target a simplified version first:
// * one proposer
// * one voter
// * No local epoch.
// * Allow k outstanding unnotarized proposals.
type Node struct {
	StartStopWaiter

	// Immutable after set.
	loggingId string
	k         uint32 // The outstanding window

	// Only used in the worker goroutine.
	chain    blockchain.BlockChain
	client   NodeClient
	role     RoleAssigner
	verifier blockchain.Verifier
	epoch    blockchain.Epoch
	timer    Timer
	// Only used by the primary proposer. Expect there are at most k objects in the maps.
	votes            map[blockchain.BlockSn]map[string]blockchain.Vote
	broadcastedNotas map[blockchain.BlockSn]bool
	// Only used by proposers.
	clockMsgs map[string]blockchain.ClockMsg
	// Only used by voters.
	voted *llrb.LLRB

	// Channels to the worker goroutine
	workChan chan work
}

type BlockCreator int

const (
	BlockCreatedByOther = BlockCreator(0)
	BlockCreatedBySelf  = BlockCreator(1)
)

type work struct {
	// The main data.
	blob interface{}
	// The aux data for blob
	context interface{}
	// Return the result via ch
	ch chan error
}

type notarizedBlock struct {
	notarization blockchain.Notarization
	block        blockchain.Block
}

type proposalContext struct {
	source  *network.Message
	creator BlockCreator
}

// A node can have multiple roles. When doing the proposer/voter reconfiguration,
// need |id| to determine whether to establish/drop the connection.
type RoleAssigner interface {
	// IsProposer returns true if |id| is a proposer at |epoch|.
	// If |id| is an empty string, it means asking whether itself is a proposer.
	IsProposer(id string, epoch blockchain.Epoch) bool
	// IsProposer returns true if |id| is the primary proposer at |epoch|.
	// If |id| is an empty string, it means asking whether itself is a proposer.
	IsPrimaryProposer(id string, epoch blockchain.Epoch) bool
	// IsVoter returns true if |id| is a voter at |epoch|.
	// If |id| is an empty string, it means asking whether itself is a voter.
	IsVoter(id string, epoch blockchain.Epoch) bool
	// IsBootnode() returns true if |id| is a bootnode. Bootnodes are special nodes assigned
	// in the configurations/code without a term of office.
	// If |id| is an empty string, it means asking whether itself is a bootnode.
	IsBootnode(id string) bool
	// GetProposerId returns the proposer id at |epoch| or an empty string.
	GetProposerId(epoch blockchain.Epoch) string
	// GetVoterId returns the voter id at |epoch| or an empty string.
	GetVoterId(epoch blockchain.Epoch) string
	// GetBootnodeId returns the bootnode id or an empty string.
	GetBootnodeId() string
	// GetDefaultProposerId() returns the default proposer id.
	GetDefaultProposerId() string
	// GetDefaultProposerId() returns the default voter id.
	GetDefaultVoterId() string
	// GetNumVoters returns the number of voters at |epoch|.
	GetNumVoters(epoch blockchain.Epoch) int
}

// The item used with the ordered map (LLRB).
type Item struct {
	key   blockchain.BlockSn
	value interface{}
}

//--------------------------------------------------------------------

func init() {
	// NOTE: This package logs detailed steps in debug logs. Uncomment this for debug.
	//lgr.SetLogLevel("/", lgr.LvlDebug)
	// Uncomment this to run faster.
	// Removing verbose logs helps test potentially different race conditions.
	//lgr.SetLogLevel("/", lgr.LvlError)
}

//--------------------------------------------------------------------

// Start calls action() and enters the running state;
// the caller is responsible to use stoppedChan to notify the service started
// by action() is stopped.
func (s *StartStopWaiter) Start(
	action func(chan interface{}) error, stoppedChan chan interface{},
) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.stoppedChan != nil {
		return errors.Errorf("is still running")
	}
	s.stopChan = make(chan interface{})
	if err := action(s.stopChan); err != nil {
		return err
	}
	s.stoppedChan = stoppedChan
	return nil
}

func (s *StartStopWaiter) StopAndWait() error {
	if err := s.Stop(); err != nil {
		return err
	}
	return s.Wait()
}

func (s *StartStopWaiter) Stop() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.stopChan == nil {
		return errors.Errorf("Has called Stop() before; isRunning=%t", s.stoppedChan != nil)
	}
	close(s.stopChan)
	s.stopChan = nil
	return nil
}

func (s *StartStopWaiter) Wait() error {
	s.mutex.Lock()
	ch := s.stoppedChan
	s.mutex.Unlock()
	if ch != nil {
		<-ch
	}

	s.mutex.Lock()
	s.stoppedChan = nil
	s.mutex.Unlock()
	return nil
}

//--------------------------------------------------------------------

func NewNode(cfg NodeConfig) Node {
	return Node{
		loggingId: cfg.LoggingId,
		k:         cfg.K,
		chain:     cfg.Chain,
		client:    cfg.NodeClient,
		role:      cfg.Role,
		verifier:  cfg.Verifier,
		epoch:     cfg.Epoch,
		// Use |clock| to get the time/timer such that we can use a fake clock to speed up tests.
		timer:            cfg.Timer,
		votes:            make(map[blockchain.BlockSn]map[string]blockchain.Vote),
		broadcastedNotas: make(map[blockchain.BlockSn]bool),
		clockMsgs:        make(map[string]blockchain.ClockMsg),
		voted:            llrb.New(),

		workChan: make(chan work, 1024),
	}
}

func (n *Node) Start() error {
	stoppedChan := make(chan interface{})
	action := func(stopChan chan interface{}) error {
		// Enter the event-waiting loop.
		go func() {
			n.handleEventLoop(stopChan, stoppedChan)
		}()

		return nil
	}

	return n.StartStopWaiter.Start(action, stoppedChan)
}

func (n *Node) handleEventLoop(
	stopChan chan interface{}, stoppedChan chan interface{}) {
	// Non-voters will never time out.
	n.timer.Reset(n.epoch)
	for {
		var err error
		select {
		case <-stopChan:
			close(stoppedChan)
			return
		case <-n.timer.GetChannel():
			logger.Info("[%s] timeout at epoch %d", n.loggingId, n.epoch)
			if !n.role.IsVoter("", n.epoch) {
				break
			}
			if err = n.onTimeout(); err != nil {
				logger.Warn("[%s] %s", err)
			}
			// Reset the timer, so voters will resend ClockMsg after timeout happens again.
			// Otherwise, proposers which connect to voters after sending ClockMsg
			// will not receive ClockMsg.
			n.timer.Reset(n.epoch)
		case w := <-n.workChan:
			switch v := w.blob.(type) {
			case blockchain.Epoch:
				if v > n.epoch {
					logger.Info("[%s] update epoch %d -> %d", n.loggingId, n.epoch, v)
					n.epoch = v
					n.timer.Reset(n.epoch)
				} else {
					logger.Info("[%s] skip update epoch %d <= %d", n.loggingId, n.epoch, v)
				}
			case blockchain.Block:
				creator, ok := w.context.(BlockCreator)
				if !ok {
					utils.Bug("context is invalid; type=%T (%v)", w.context, w.context)
				}
				err = n.onReceivedBlock(v, creator)
			case blockchain.Proposal:
				ctx := w.context.(proposalContext)
				err = n.onReceivedProposal(v, ctx)
			case blockchain.Vote:
				err = n.onReceivedVote(v)
			case blockchain.Notarization:
				err = n.onReceivedNotarization(v)
			case blockchain.ClockMsg:
				err = n.onReceivedClockMsg(v)
			case blockchain.ClockMsgNota:
				err = n.onReceivedClockMsgNota(v)
			case blockchain.FreshestNotarizedChainExtendedEvent:
				if v.Sn.Epoch >= n.epoch {
					// There is a progress, so reset the timer.
					n.timer.Reset(n.epoch)
				}
				err = n.onReceivedFreshestNotarizedChainExtendedEvent(v)
			case blockchain.FinalizedChainExtendedEvent:
				err = n.onReceivedFinalizedChainExtendedEvent(v)
			case notarizedBlock:
				err = n.onReceivedNotarizedBlock(v.notarization, v.block)
			default:
				err = errors.Errorf("unknown type %v", w.blob)
			}
			if w.ch != nil {
				if err != nil {
					// TODO(thunder): verify whether we should use different level for different errors.
					logger.Warn("[%s] received err=%s", n.loggingId, err)
				}
				w.ch <- err
			}
		}
	}
}

// Run in the worker goroutine
// This function is only called on voters.
func (n *Node) onTimeout() error {
	// The node may not be a voter after the timeout.
	if !n.role.IsVoter("", n.epoch) {
		return nil
	}

	epoch := n.epoch
	if !n.role.IsVoter("", epoch) {
		return errors.Errorf("non-voter calls onTimeout at epoch %d", epoch)
	}

	epoch++
	if c, err := n.verifier.NewClockMsg(epoch); err != nil {
		logger.Error("[%s] a timeout happened but failed to create clock(%d); err=%s",
			n.loggingId, epoch, err)
		return err
	} else {
		n.client.Broadcast(c)
		logger.Info("[%s] broadcast clock(%d) due to timeout", n.loggingId, epoch)
		return nil
	}
}

// Run in the worker goroutine
func (n *Node) onReceivedBlock(b blockchain.Block, creator BlockCreator) error {
	logger.Debug("[%s] onReceivedBlock: %s, creator=%d",
		n.loggingId, b.GetBlockSn(), creator)

	parentSn := b.GetParentBlockSn()
	sn := b.GetBlockSn()
	if sn.Epoch != parentSn.Epoch {
		if sn.S != 1 || sn.Epoch < parentSn.Epoch {
			return errors.Errorf("invalid block sequence number %s with parent %s", sn, parentSn)
		}
	} else if sn.S != parentSn.S+1 {
		return errors.Errorf("invalid block sequence number %s with parent %s", sn, parentSn)
	}

	if creator == BlockCreatedByOther {
		if err := n.chain.InsertBlock(b); err != nil {
			return err
		}
		for _, nota := range b.GetNotarizations() {
			err := n.onReceivedNotarization(nota)
			if err != nil {
				logger.Warn("%s", err)
			}
		}
		return nil
	}

	if b.GetBlockSn().Epoch != n.epoch || !n.role.IsPrimaryProposer("", b.GetBlockSn().Epoch) {
		logger.Info("[%s] received block %s and tried to stop the blockchain "+
			"creating new blocks (epoch=%d)", n.loggingId, b.GetBlockSn(), n.epoch)
		if err := n.chain.StopCreatingNewBlocks(); err != nil {
			logger.Warn("[%s] received block %s and tried to stop the blockchain "+
				"creating new blocks but failed (epoch=%d); err=%s",
				n.loggingId, b.GetBlockSn(), n.epoch, err)
		}
		return nil
	}

	if n.chain.GetBlock(b.GetBlockSn()) == nil {
		return errors.Errorf("%s does not exist in the blockchain", b.GetBlockSn())
	}

	p, err := n.verifier.Propose(b)
	if err != nil {
		return err
	}
	n.client.Broadcast(p)
	if n.role.IsVoter("", b.GetBlockSn().Epoch) {
		ctx := proposalContext{&network.Message{}, BlockCreatedBySelf}
		return n.onReceivedProposal(p, ctx)
	}
	return nil
}

// Run in the worker goroutine
func (n *Node) onReceivedProposal(p blockchain.Proposal, ctx proposalContext) error {
	logger.Debug("[%s] onReceivedProposal: %s", n.loggingId, p.GetBlockSn())

	if n.epoch != p.GetBlockSn().Epoch {
		msg := fmt.Sprintf("skip proposal %s because local epoch=%d is different",
			p.GetBlockSn(), n.epoch)
		err := errors.Errorf(msg)
		logger.Info("[%s] %s", n.loggingId, msg)
		return err
	}

	isVoter := n.role.IsVoter("", p.GetBlockSn().Epoch)
	if isVoter && n.isVoted(p.GetBlockSn()) {
		return errors.Errorf("have voted %s", p.GetBlockSn())
	}

	if err := n.verifier.VerifyProposal(p); err != nil {
		return err
	}

	b := p.GetBlock()
	if b == nil {
		return errors.Errorf("invalid proposal")
	}

	// If the proposal comes from outside, add it to our local blockchain if needed.
	if ctx.creator == BlockCreatedByOther {
		sn := b.GetBlockSn()
		if n.chain.ContainsBlock(sn) {
			if b2 := n.chain.GetBlock(sn); b2 == nil {
				utils.Bug("BlockChain's ContainsBlock() and GetBlock() are inconsistent at %s", sn)
			} else if !b.GetHash().Equal(b2.GetHash()) {
				// We only store blocks with "proofs", i.e., either a notarized block or a proposal.
				// There is no way to have a different proposal at the same BlockSn.
				return errors.Errorf("two proposal at %s have different hash; new vs. old: "+
					"%s != %s; reject the new one", sn, b.GetHash(), b.GetHash(), b2.GetHash())
			}
		} else {
			// To avoid double voting on the same BlockSn, insert the block before voting on the proposal.
			if err := n.onReceivedBlock(b, ctx.creator); err != nil {
				// TODO(thunder): if the error happens because the node is behind,
				// save the proposal to n.uninsertedProposal
				if te, ok := err.(utils.TemporaryError); ok {
					if te.IsTemporary() {
						n.client.CatchUp(ctx.source, b.GetParentBlockSn())
						return err
					}
				}
				logger.Warn("[%s] onReceivedProposal: failed to insert block %s; err=%s",
					n.loggingId, p.GetBlockSn(), err)
				return err
			}
		}
	}

	if !isVoter {
		return nil
	}

	// TODO(thunder): check whether we need to follow the pseudocode to make it more reliable.
	// TODO(thunder): if b is the first block of this session, check whether
	// its parent is the stop block in the last session instead.
	fnb := n.chain.GetFreshestNotarizedChain()
	fnbs := fnb.GetBlockSn()
	sn := b.GetBlockSn()
	if fnbs.Epoch == sn.Epoch {
		if sn.S > fnbs.S+n.k {
			// TODO(thunder): save the proposal to n.unvotedProposals.
			return errors.Errorf("Exceed the outstanding window (%d): %s > %s",
				n.k, sn, fnbs)
		}
	} else if fnbs.Epoch > sn.Epoch {
		utils.Bug("freshest notarized block %s is newer than proposal %s", fnbs, sn)
	} else {
		if sn.S > n.k {
			return errors.Errorf("skip proposal %s because S > k=%d and epoch=%d is different",
				sn, n.k, n.epoch)
		} else {
			// Ensure blocks before sn.Epoch are notarized.
			first := n.chain.GetBlock(blockchain.BlockSn{Epoch: sn.Epoch, S: 1})
			lastSnInPreviousEpoch := first.GetParentBlockSn()
			r := lastSnInPreviousEpoch.Compare(fnbs)
			if r != 0 {
				if r < 0 {
					return errors.Errorf("skip proposal %s because the last block in previous epoch %s "+
						"is older than freshest notarized block %s", sn, lastSnInPreviousEpoch, fnbs)
				} else {
					n.client.CatchUp(ctx.source, lastSnInPreviousEpoch)
					return errors.Errorf("skip proposal %s because the last block in previous epoch %s "+
						"is newer than freshest notarized block %s", sn, lastSnInPreviousEpoch, fnbs)
				}
			}
		}
	}

	vote, err := n.verifier.Vote(p)
	if err != nil {
		return err
	}
	n.voted.ReplaceOrInsert(&Item{p.GetBlockSn(), true})

	if n.role.IsPrimaryProposer("", p.GetBlockSn().Epoch) {
		return n.onReceivedVote(vote)
	}
	n.client.Reply(ctx.source, vote)
	return nil
}

// Run in the worker goroutine
// This function is only called on proposers.
func (n *Node) onReceivedVote(v blockchain.Vote) error {
	logger.Debug("[%s] onReceivedVote: %s by %s", n.loggingId, v.GetBlockSn(), v.GetVoterId())

	if n.epoch != v.GetBlockSn().Epoch {
		return errors.Errorf("skip vote %s because epoch=%d is different", v.GetBlockSn(), n.epoch)
	}

	if !n.role.IsPrimaryProposer("", v.GetBlockSn().Epoch) {
		return errors.Errorf("received unexpected vote: %s", v.GetBlockSn())
	}

	// Check whether Node has received it before verifying it
	// because the computation cost of verifying is higher than the check.
	votes, ok := n.votes[v.GetBlockSn()]
	if !ok {
		votes = make(map[string]blockchain.Vote)
		n.votes[v.GetBlockSn()] = votes
	}
	if _, ok := votes[v.GetVoterId()]; ok {
		// Have received.
		return nil
	}
	if err := n.verifier.VerifyVote(v); err != nil {
		return err
	}
	votes[v.GetVoterId()] = v
	var vs []blockchain.Vote
	for _, t := range votes {
		vs = append(vs, t)
	}

	nota, err := n.verifier.Notarize(vs)
	if err != nil {
		// Votes are not enough to create the notarization.
		return nil
	}
	if !n.isNotarizationBroadcasted(nota.GetBlockSn()) {
		// Broadcast the notarization to minimize the chance of losing the last K blocks
		// when a propose switch occurs. Otherwise, only the primary proposer knows the
		// notarizations of the last K blocks.
		n.client.Broadcast(nota)
		n.broadcastedNotas[nota.GetBlockSn()] = true
	}
	// To collect late votes, always add the notarization to the chain.
	return n.onReceivedNotarization(nota)
}

// Possible callers:
// * Received enough votes -> a new notarization.
// * Received a new block which contains some notarizations.
// * Actively pull notarization from the other nodes.
//
// Run in the worker goroutine
func (n *Node) onReceivedNotarization(nota blockchain.Notarization) error {
	// Ensure the node stores the block before the notarization. This ensures the freshest
	// notarized chain grows in order. Maybe this is not necessary.
	if !n.chain.ContainsBlock(nota.GetBlockSn()) {
		logger.Debug("[%s] onReceivedNotarization: %s (reject early notarization)",
			n.loggingId, nota.GetBlockSn())
		return nil
	}

	logger.Debug("[%s] onReceivedNotarization: %s", n.loggingId, nota.GetBlockSn())
	existedNota := n.chain.GetNotarization(nota.GetBlockSn())
	if existedNota != nil && existedNota.GetNVote() >= nota.GetNVote() {
		return nil
	}
	if err := n.verifier.VerifyNotarization(nota); err != nil {
		return errors.Errorf("invalid notarization %s; err=%s", nota.GetBlockSn(), err)
	}
	return n.chain.AddNotarization(nota)
}

// Only called by proposer
// This function is only called on proposers.
func (n *Node) onReceivedClockMsg(c blockchain.ClockMsg) error {
	logger.Debug("[%s] onReceivedClockMsg: %d by %s", n.loggingId, c.GetEpoch(), c.GetVoterId())

	nextEpoch := c.GetEpoch()
	if n.epoch > nextEpoch {
		return errors.Errorf("skip clock(%d) by %s because epoch=%d is larger",
			nextEpoch, c.GetVoterId(), n.epoch)
	}

	if !n.role.IsProposer("", n.epoch) &&
		!n.role.IsProposer("", nextEpoch) {
		return errors.Errorf("a node not a proposer received an unexpected clock(%d) by %s "+
			"(local epoch=%d)", nextEpoch, c.GetVoterId(), n.epoch)
	}

	// Check whether Node has received it before verifying it
	// because the computation cost of verifying is high.
	if t, ok := n.clockMsgs[c.GetVoterId()]; ok && t.GetEpoch() >= nextEpoch {
		// Have received.
		return nil
	}
	if err := n.verifier.VerifyClockMsg(c); err != nil {
		return err
	}
	n.clockMsgs[c.GetVoterId()] = c

	var cs []blockchain.ClockMsg
	for _, t := range n.clockMsgs {
		if t.GetEpoch() == nextEpoch {
			cs = append(cs, t)
		}
	}
	if cNota, err := n.verifier.NewClockMsgNota(cs); err != nil {
		// ClockMsgs are not enough to create the notarization.
		logger.Info("[%s] received %d of %d clock(%d) messages "+
			"(not enough to make a clock message notarization)",
			n.loggingId, len(cs), n.role.GetNumVoters(nextEpoch), c.GetEpoch())
		return nil
	} else {
		// NOTE:
		// 1. ClockMsgNota is important. It's okay to broadcast multiple times
		// whenever we have more ClockMsg, although this is not necessary.
		// 2. The proposer should perform reconciliation *after* advancing the epoch,
		// so call Broadcast() before onReceivedClockMsgNota().
		n.client.Broadcast(cNota)
		logger.Info("[%s] broadcast notarization of clock(%d)", n.loggingId, cNota.GetEpoch())
		return n.onReceivedClockMsgNota(cNota)
	}
}

// Run in the worker goroutine
func (n *Node) onReceivedClockMsgNota(cNota blockchain.ClockMsgNota) error {
	logger.Debug("[%s] onReceivedClockMsgNota: %d", n.loggingId, cNota.GetEpoch())
	if err := n.verifier.VerifyClockMsgNota(cNota); err != nil {
		return errors.Errorf("invalid clock message notarization for epoch=%d; err=%s",
			cNota.GetEpoch(), err)
	} else if cNota.GetEpoch() <= n.epoch {
		return nil
	} else {
		n.client.UpdateEpoch(cNota)
		return nil
	}
}

// Run in the worker goroutine
func (n *Node) onReceivedFreshestNotarizedChainExtendedEvent(
	e blockchain.FreshestNotarizedChainExtendedEvent) error {
	logger.Debug("[%s] onReceivedFreshestNotarizedChainExtendedEvent: %s", n.loggingId, e.Sn)

	// TODO(thunder): try voting on proposals which are not voted.

	return nil
}

// Run in the worker goroutine
func (n *Node) onReceivedFinalizedChainExtendedEvent(
	e blockchain.FinalizedChainExtendedEvent) error {
	logger.Debug("[%s] onReceivedFinalizedChainExtendedEvent: %s", n.loggingId, e.Sn)

	// Clean up unnecessary data.
	for sn := range n.votes {
		if sn.Compare(e.Sn) <= 0 {
			delete(n.votes, sn)
		}
	}
	for sn := range n.broadcastedNotas {
		if sn.Compare(e.Sn) <= 0 {
			delete(n.broadcastedNotas, sn)
		}
	}
	for id, c := range n.clockMsgs {
		if c.GetEpoch() <= e.Sn.Epoch {
			delete(n.clockMsgs, id)
		}
	}
	cleanUpOldData(n.voted, e.Sn)

	// TODO(thunder): delete out-of-date data in n.chain.

	return nil
}

func (n *Node) onReceivedNotarizedBlock(nota blockchain.Notarization, b blockchain.Block) error {
	logger.Debug("[%s] onReceivedNotarizedBlock: %s %s", n.loggingId, nota.GetBlockSn(), b.GetBlockSn())

	if nota.GetBlockSn() != b.GetBlockSn() {
		return errors.Errorf("BlockSn mismatched %s != %s", nota.GetBlockSn(), b.GetBlockSn())
	}
	if !nota.GetBlockHash().Equal(b.GetHash()) {
		return errors.Errorf("hash mismatched %s != %s", nota.GetBlockHash(), b.GetHash())
	}
	if err := n.verifier.VerifyNotarization(nota); err != nil {
		return errors.Errorf("invalid notarization %s; err=%s", nota.GetBlockSn(), err)
	}
	if !n.chain.ContainsBlock(b.GetBlockSn()) {
		if err := n.onReceivedBlock(b, BlockCreatedByOther); err != nil {
			return err
		}
	}
	return n.onReceivedNotarization(nota)
}

func (n *Node) SetEpoch(epoch blockchain.Epoch) chan error {
	ch := make(chan error, 1)
	n.workChan <- work{epoch, nil, ch}
	return ch
}

func (n *Node) SetIsInReconfiguration(yes bool) chan error {
	ch := make(chan error, 1)
	n.workChan <- work{yes, nil, ch}
	return ch
}

func (n *Node) AddBlock(b blockchain.Block, creator BlockCreator) chan error {
	ch := make(chan error, 1)
	n.workChan <- work{b, creator, ch}
	return ch
}

func (n *Node) AddProposal(
	p blockchain.Proposal, msg *network.Message, creator BlockCreator) chan error {
	ch := make(chan error, 1)
	n.workChan <- work{p, proposalContext{msg, creator}, ch}
	return ch
}

func (n *Node) AddVote(v blockchain.Vote) chan error {
	ch := make(chan error, 1)
	n.workChan <- work{v, nil, ch}
	return ch
}

func (n *Node) AddNotarization(nota blockchain.Notarization) chan error {
	ch := make(chan error, 1)
	n.workChan <- work{nota, nil, ch}
	return ch
}

func (n *Node) AddClockMsg(c blockchain.ClockMsg) chan error {
	ch := make(chan error, 1)
	n.workChan <- work{c, nil, ch}
	return ch
}

func (n *Node) AddClockMsgNota(cn blockchain.ClockMsgNota) chan error {
	ch := make(chan error, 1)
	n.workChan <- work{cn, nil, ch}
	return ch
}

func (n *Node) AddNotarizedBlock(nota blockchain.Notarization, b blockchain.Block) chan error {
	ch := make(chan error, 1)
	n.workChan <- work{notarizedBlock{nota, b}, nil, ch}
	return ch
}

func (n *Node) AddFreshestNotarizedChainExtendedEvent(
	event blockchain.FreshestNotarizedChainExtendedEvent) chan error {
	ch := make(chan error, 1)
	n.workChan <- work{event, nil, ch}
	return ch
}

func (n *Node) AddFinalizedChainExtendedEvent(
	event blockchain.FinalizedChainExtendedEvent) chan error {
	ch := make(chan error, 1)
	n.workChan <- work{event, nil, ch}
	return ch
}

func (n *Node) isVoted(sn blockchain.BlockSn) bool {
	return LLRBItemToBool(n.voted.Get(ToItem(sn)))
}

func (n *Node) isNotarizationBroadcasted(sn blockchain.BlockSn) bool {
	return n.broadcastedNotas[sn]
}

//--------------------------------------------------------------------

func (i *Item) Less(other llrb.Item) bool {
	return i.key.Compare(other.(*Item).key) < 0
}

func ToItem(sn blockchain.BlockSn) *Item {
	return &Item{sn, nil}
}

func LLRBItemToProposal(item llrb.Item) blockchain.Proposal {
	if item == nil {
		return nil
	}
	return item.(*Item).value.(blockchain.Proposal)
}

func LLRBItemToBool(item llrb.Item) bool {
	if item == nil {
		return false
	}
	return item.(*Item).value.(bool)
}

func cleanUpOldData(tree *llrb.LLRB, sn blockchain.BlockSn) int {
	count := 0
	min := tree.Min()
	for min != nil && min.(*Item).key.Compare(sn) <= 0 {
		count++
		tree.DeleteMin()
		min = tree.Min()
	}
	return count
}
