// Class Has-A Relations:
// https://docs.google.com/presentation/d/1AY-GiujqkzRdfdleDSrj516d48-3w-z70w4DQiy_3HY/edit?usp=sharing
//
// Data flow:
// https://docs.google.com/presentation/d/1vQ1Kh5O_kNXe0y0GK9c26UTmblPIdx8DDoKmPhrrr3c/edit?usp=sharing

package consensus

import (
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
	// TODO(thunder): add the methods related to clock messages.
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
	// Only used by the primary proposer. Expect there are at most k objects in the maps.
	votes            map[blockchain.BlockSn]map[string]blockchain.Vote
	broadcastedNotas map[blockchain.BlockSn]bool
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
		loggingId:        cfg.LoggingId,
		k:                cfg.K,
		chain:            cfg.Chain,
		client:           cfg.NodeClient,
		role:             cfg.Role,
		verifier:         cfg.Verifier,
		votes:            make(map[blockchain.BlockSn]map[string]blockchain.Vote),
		broadcastedNotas: make(map[blockchain.BlockSn]bool),
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
	for {
		var err error
		select {
		case <-stopChan:
			close(stoppedChan)
			return
		case w := <-n.workChan:
			switch v := w.blob.(type) {
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
			case blockchain.FreshestNotarizedChainExtendedEvent:
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
func (n *Node) onReceivedBlock(b blockchain.Block, creator BlockCreator) error {
	logger.Debug("[%s] onReceivedBlock: %s, creator=%d",
		n.loggingId, b.GetBlockSn(), creator)

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

	if !n.role.IsPrimaryProposer("", b.GetBlockSn().Epoch) {
		logger.Info("[%s] received block %s and tried to stop the blockchain "+
			"creating new blocks", n.loggingId, b.GetBlockSn())

		if err := n.chain.StopCreatingNewBlocks(); err != nil {
			logger.Warn("[%s] received block %s and tried to stop the blockchain "+
				"creating new blocks but failed; err=%s", n.loggingId, b.GetBlockSn(), err)
		}
		return nil
	}

	if n.chain.GetBlock(b.GetBlockSn()) == nil {
		return errors.Errorf("%s does not exist in the blockchain", b.GetBlockSn())
	}

	p, err := n.verifier.Propose(b)
	if err == nil {
		n.client.Broadcast(p)
		if n.role.IsVoter("", b.GetBlockSn().Epoch) {
			ctx := proposalContext{&network.Message{}, BlockCreatedBySelf}
			err = n.onReceivedProposal(p, ctx)
		}
	}
	return err
}

// Run in the worker goroutine
func (n *Node) onReceivedProposal(p blockchain.Proposal, ctx proposalContext) error {
	logger.Debug("[%s] onReceivedProposal: %s", n.loggingId, p.GetBlockSn())

	isVoter := n.role.IsVoter("", p.GetBlockSn().Epoch)
	if isVoter && n.isVoted(p.GetBlockSn()) {
		return errors.Errorf("have voted %s", p.GetBlockSn())
	}

	if err := n.verifier.VerifyProposal(p); err != nil {
		return err
	}

	// TODO(thunder): reject p if epoch is mismatched.
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

	fnb := n.chain.GetFreshestNotarizedChain()
	fnbs := fnb.GetBlockSn()
	sn := b.GetBlockSn()
	if fnbs.Epoch == sn.Epoch {
		if sn.S > fnbs.S+n.k {
			// TODO(thunder): save the proposal to n.unvotedProposals.
			return errors.Errorf("Exceed the outstanding window (%d): %s > %s",
				n.k, sn, fnbs)
		}
	} else {
		// TODO(thunder)
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
