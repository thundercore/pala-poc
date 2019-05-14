// Put the fake implementations used by the production code for the integration test.
package blockchain

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"thunder2/utils"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// About goroutine-safety:
// * All public methods hold mutex by themselves.
// * Most private methods assume the caller holds the mutex.
type BlockChainFake struct {
	// Protect all data. All public methods hold the lock.
	mutex utils.CheckedLock
	// All data.
	blocks  map[BlockSn]Block
	genesis Block
	// The last block of the freshest notarized chain.
	freshestNotarizedChain                   Block
	freshestNotarizedChainUsingNotasInBlocks Block
	// The last block of the finalized chain.
	finalizedChain Block
	// The max number of unnotarized blocks. Also used as the finalized parameter.
	k              uint32
	eventChans     []chan interface{}
	finalizedEvent *FinalizedChainExtendedEvent
	// A helpful index.
	notasInMemory map[BlockSn]Notarization
	notasInBlocks map[BlockSn]Notarization
	// The block number to reconfigure proposers/voters.
	stopBlockNumber uint32
	// Worker goroutine
	isRunning bool
	stopChan  chan chan error
	notaChan  chan Notarization
	delay     time.Duration
}

type useNotarization int

const (
	useNotarizationNone     = useNotarization(0)
	useNotarizationInAll    = useNotarization(1)
	useNotarizationInBlocks = useNotarization(2)
)

type BlockFake struct {
	sn       BlockSn
	parentSn BlockSn
	nBlock   uint32
	notas    []Notarization
	body     string
}

type NotarizationFake struct {
	sn       BlockSn
	voterIds []string
}

// myProposerIds/myVoterIds are slices to make it possible to simulate key rotations
// during reconfiguration.
type VerifierFake struct {
	mutex         utils.CheckedLock
	myProposerIds []string          // my ids in the current or future proposers in proposerLists
	myVoterIds    []string          // my ids in the current or future voters in voterLists
	proposerLists []*ElectionResult // history of elected proposers
	voterLists    []*ElectionResult // history of elected voters
}

type ProposalFake struct {
	proposerId string
	block      Block
}

type VoteFake struct {
	sn      BlockSn
	voterId string
}

type ClockMsgFake struct {
	epoch   Epoch
	voterId string
}

type ClockMsgNotaFake struct {
	epoch    Epoch
	voterIds []string
}

type DataUnmarshallerFake struct {
}

// NOTE: Maybe we can move ElectionResult to blockchain.go
// if it's easy to use for the real implementation.
//
// All data are immutable after being set.
type ElectionResult struct {
	consensusIds []string
	// Included.
	begin Epoch
	// Included. Note that we don't know the number in the real implementation.
	end Epoch
}

//--------------------------------------------------------------------

func NewBlockChainFake(k uint32) (BlockChain, error) {
	return NewBlockChainFakeWithDelay(k, 0)
}

func NewBlockChainFakeWithDelay(k uint32, delay time.Duration) (BlockChain, error) {
	sn := GetGenesisBlockSn()
	genesis := NewBlockFake(sn, BlockSn{}, 1, nil, "0")
	bc := BlockChainFake{
		blocks:  make(map[BlockSn]Block),
		genesis: genesis,
		k:       k,
		freshestNotarizedChain:                   genesis,
		freshestNotarizedChainUsingNotasInBlocks: genesis,
		finalizedChain:                           genesis,
		notasInMemory:                            make(map[BlockSn]Notarization),
		notasInBlocks:                            make(map[BlockSn]Notarization),
		stopChan:                                 make(chan chan error),
		notaChan:                                 make(chan Notarization, 1024),
		delay:                                    delay,
	}
	if err := bc.InsertBlock(genesis); err != nil {
		return nil, err
	} else {
		return &bc, nil
	}
}

func (bc *BlockChainFake) ContainsBlock(s BlockSn) bool {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	return bc.doesBlockExist(s)
}

func (bc *BlockChainFake) doesBlockExist(s BlockSn) bool {
	bc.mutex.CheckIsLocked("")

	// We should use a more efficient way to implement this in the real implementation.
	return bc.getBlock(s) != nil
}

func (bc *BlockChainFake) GetBlock(s BlockSn) Block {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	return bc.getBlock(s)
}

func (bc *BlockChainFake) getBlock(s BlockSn) Block {
	bc.mutex.CheckIsLocked("")

	if b, ok := bc.blocks[s]; ok {
		return b
	}
	return nil
}

func (bc *BlockChainFake) GetGenesisBlock() Block {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	return bc.genesis
}

func (bc *BlockChainFake) GetNotarization(s BlockSn) Notarization {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	return bc.getNotarization(s)
}

func (bc *BlockChainFake) getNotarization(s BlockSn) Notarization {
	bc.mutex.CheckIsLocked("")

	if n, ok := bc.notasInBlocks[s]; ok {
		return n
	}
	if n, ok := bc.notasInMemory[s]; ok {
		return n
	}
	return nil
}

func (bc *BlockChainFake) GetLongestChain() Block {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	return bc.getLongestChain(useNotarizationNone)
}

func (bc *BlockChainFake) GetFreshestNotarizedChain() Block {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	return bc.getFreshestNotarizedChain()
}

// Assume blocks and notarizations are inserted in order.
// The time complexity is O(1).
func (bc *BlockChainFake) getFreshestNotarizedChain() Block {
	return bc.freshestNotarizedChain
}

// ComputeFreshestNotarizedChain is the baseline for correctness.
// The time complexity is O(N).
func (bc *BlockChainFake) ComputeFreshestNotarizedChain() Block {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	return bc.computeFreshestNotarizedChain()
}

func (bc *BlockChainFake) computeFreshestNotarizedChain() Block {
	return bc.getLongestChain(useNotarizationInAll)
}

func (bc *BlockChainFake) GetFinalizedChain() Block {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	return bc.getFinalizedChain()
}

func (bc *BlockChainFake) getFinalizedChain() Block {
	bc.mutex.CheckIsLocked("")

	return bc.finalizedChain
}

// ComputeFinalizedChain is the baseline for correctness.
// The time complexity is O(N).
func (bc *BlockChainFake) ComputeFinalizedChain() Block {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	return bc.computeFinalizedChain()
}

func (bc *BlockChainFake) computeFinalizedChain() Block {
	bc.mutex.CheckIsLocked("")

	b := bc.computeFinalizingChain()
	sn := b.GetBlockSn()
	// Ensure there are k normal blocks after the finalizing block.
	if !(sn.S > bc.k && bc.doesBlockExist(BlockSn{sn.Epoch, sn.S + bc.k})) ||
		sn.IsGenesis() {
		return bc.genesis
	}
	// Chop off the last k normal blocks.
	return bc.getBlock(BlockSn{sn.Epoch, sn.S - bc.k})
}

// Return the last block of the freshest notarized chain only using notarizations in blocks.
func (bc *BlockChainFake) computeFinalizingChain() Block {
	bc.mutex.CheckIsLocked("")

	return bc.getLongestChain(useNotarizationInBlocks)
}

// If useNota is useNotarizationNone, return the last block of the longest chain.
// If useNota is useNotarizationInAll, return the last block of the freshest notarized chain.
// If useNota is useNotarizationInBlocks, return the last block of the freshest notarized chain
// using notarizations in blocks only.
func (bc *BlockChainFake) getLongestChain(useNota useNotarization) Block {
	bc.mutex.CheckIsLocked("")

	depths := make(map[BlockSn]int)
	var ms BlockSn
	md := 0
	for s := range bc.blocks {
		t := bc.getDepth(s, depths, useNota)
		if useNota != useNotarizationNone {
			if t > 0 && ms.Compare(s) < 0 {
				ms = s
			}
		} else {
			if md < t {
				md = t
				ms = s
			}
		}
	}
	return bc.blocks[ms]
}

func (bc *BlockChainFake) getDepth(
	s BlockSn, depths map[BlockSn]int, useNota useNotarization) int {
	bc.mutex.CheckIsLocked("")

	if useNota != useNotarizationNone {
		if s.IsGenesis() {
			// There is no notarization of the genesis block. No check.
		} else {
			var nota Notarization
			if useNota == useNotarizationInAll {
				nota = bc.getNotarization(s)
			} else {
				if n, ok := bc.notasInBlocks[s]; ok {
					nota = n
				}
			}
			if nota == nil {
				return -1
			}
		}
	}

	if d, ok := depths[s]; ok {
		return d
	} else {
		b := bc.blocks[s]
		if p := getParentBlock(bc, b); p != nil {
			parentDepth := bc.getDepth(p.GetBlockSn(), depths, useNota)
			if parentDepth < 0 {
				depths[s] = -1
			} else {
				depths[s] = parentDepth + 1
			}
		} else {
			depths[s] = 1
		}
		return depths[s]
	}
}

// TODO(thunder): reject b if it is not extended from b.finalizedChain.
func (bc *BlockChainFake) InsertBlock(b Block) error {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	return bc.insertBlock(b, true)
}

func (bc *BlockChainFake) insertBlock(b Block, fromOutside bool) error {
	bc.mutex.CheckIsLocked("")

	if bc.getBlock(b.GetBlockSn()) != nil {
		return errors.Errorf("%s existed", b.GetBlockSn())
	}
	if !b.GetBlockSn().IsGenesis() {
		p := getParentBlock(bc, b)
		if p == nil {
			err := errors.Errorf("illegal block: parent %s does not exist", b.GetParentBlockSn())
			return utils.NewTemporaryError(err, true)
		}
	}
	sn := b.GetBlockSn()
	parentSn := b.GetParentBlockSn()
	if sn.Epoch != parentSn.Epoch {
		if sn.S != 1 || sn.Epoch < parentSn.Epoch {
			return errors.Errorf("invalid block %s with parent %s", sn, parentSn)
		}
	} else if sn.S != parentSn.S+1 {
		return errors.Errorf("invalid block %s with parent %s", sn, parentSn)
	}

	notas := b.GetNotarizations()
	for _, n := range notas {
		// Ensure the notarization is added in order.
		if err := bc.addNotarization(n); err != nil {
			msg := fmt.Sprintf("invalid block %s with invalid notarization", sn)
			return errors.Wrap(err, msg)
		}
	}
	// Ensure the block stores correct notarizations.
	// Recall the rule for (e,s):
	// * s=1       : contain the notarizations of the previous k blocks.
	// * s in [2,k]: contain no notarization.
	// * s>k       : contain the notarization of (e,s-k).
	if sn.S == 1 {
		if !sn.IsGenesis() && !b.GetParentBlockSn().IsGenesis() && len(notas) == 0 {
			return errors.Errorf("invalid block %s without any notarization", sn)
		}
	} else if sn.S <= bc.k {
		if len(notas) != 0 {
			return errors.Errorf("invalid block %s which contains %d notarization (k=%d)",
				sn, len(notas), bc.k)
		}
	} else {
		if len(notas) != 1 {
			return errors.Errorf("invalid block %s which contains %d notarization (k=%d)",
				sn, len(notas), bc.k)
		}
		notaSn := notas[0].GetBlockSn()
		if sn.Epoch != notaSn.Epoch || sn.S != notaSn.S+bc.k {
			return errors.Errorf("invalid block %s which contains invalid notarization %s (k=%d)",
				sn, notaSn, bc.k)
		}
	}

	bc.blocks[b.GetBlockSn()] = b
	// Update the handy index.
	fncSn := bc.freshestNotarizedChainUsingNotasInBlocks.GetBlockSn()
	for _, n := range notas {
		bc.notasInBlocks[n.GetBlockSn()] = n
		delete(bc.notasInMemory, n.GetBlockSn())

		// Since the notarizations are included in order, we can assume the new notarization
		// just extends a notarized chain.
		if n.GetBlockSn().Compare(fncSn) > 0 {
			fncSn = n.GetBlockSn()
		}
	}
	if bc.freshestNotarizedChainUsingNotasInBlocks.GetBlockSn().Compare(fncSn) >= 0 {
		return nil
	}

	bc.freshestNotarizedChainUsingNotasInBlocks = bc.getBlock(fncSn)

	// Update finalized chain.
	candidateSn := bc.freshestNotarizedChainUsingNotasInBlocks.GetBlockSn()
	if candidateSn.S < bc.k+1 {
		return nil
	}
	candidateSn.S -= bc.k
	if bc.finalizedChain.GetBlockSn().Compare(candidateSn) < 0 {
		bc.finalizedChain = bc.getBlock(candidateSn)
		fc := bc.finalizedChain
		var sn BlockSn
		if bc.stopBlockNumber == fc.GetNumber() {
			sn = b.GetBlockSn()
		}
		e := FinalizedChainExtendedEvent{
			Sn: fc.GetBlockSn(),
			ReconfFinalizedByBlockSn: sn,
		}
		if fromOutside {
			bc.notifyEvent(e)
		} else {
			// Send the event along with the new block. See startWorker() for related info.
			bc.finalizedEvent = &e
		}
	}

	return nil
}

func (bc *BlockChainFake) StartCreatingNewBlocks(epoch Epoch) (chan BlockAndEvent, error) {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	if bc.isRunning {
		return nil, errors.Errorf("is still running")
	}

	parentSn := bc.getFreshestNotarizedChain().GetBlockSn()
	pb := bc.getBlock(parentSn)
	if pb == nil {
		return nil, errors.Errorf("Parent %s does not exist", parentSn)
	}
	nota := bc.getNotarization(parentSn)
	if nota == nil {
		if !parentSn.IsGenesis() {
			return nil, errors.Errorf("Parent %s is not notarized", parentSn)
		}
	}
	ch := make(chan BlockAndEvent, bc.k)
	bc.isRunning = true
	go bc.startWorker(pb, epoch, ch, bc.stopChan)
	return ch, nil
}

// Simulate the real implementation which creates a new block in a worker goroutine.
func (bc *BlockChainFake) startWorker(
	parent Block, epoch Epoch, ch chan BlockAndEvent, stopChan chan chan error) {
	// Doubly-pipelined Pala always begin with (epoch, 1).
	s := BlockSn{epoch, 1}
	running := true
	for running {
		var notas []Notarization
		canCreate := false
		// If this is the first block of the epoch, wait for until parent block from previous
		// epoch is fully notarized before creating new blocks.
		if s.S == 1 && !parent.GetBlockSn().IsGenesis() {
			bc.mutex.Lock()
			notas = bc.getNotarizations(parent, int(bc.k))
			canCreate = notas != nil
			if !canCreate {
				logger.Error("parent block from previous epoch is not fully notarized (parent=%s)",
					parent.GetBlockSn())
			}
			bc.mutex.Unlock()
		} else if s.S <= bc.k {
			// Allow up to k unnotarized blocks.
			canCreate = true
		} else {
			bc.mutex.Lock()
			// Allow up to k unnotarized blocks.
			ns := BlockSn{epoch, s.S - bc.k}
			n := bc.getNotarization(ns)
			if n != nil {
				notas = append(notas, n)
				canCreate = true
			}
			bc.mutex.Unlock()
		}

		if canCreate {
			if bc.delay > 0 {
				// Simulate the time of creating a new block.
				<-time.NewTimer(bc.delay).C
			}
			nb := NewBlockFake(s, parent.GetBlockSn(), parent.GetNumber()+1, notas, s.String())
			var e *FinalizedChainExtendedEvent
			bc.mutex.Lock()
			// Insert the new block before sending it to the channel.
			// We always adds the block before adding the block's notarization.
			if err := bc.insertBlock(nb, false); err != nil {
				utils.Bug("startWorker fails to insert the created block; err=%s", err)
			}
			bc.mutex.Unlock()
			parent = nb
			s.S++
			e = bc.finalizedEvent
			bc.finalizedEvent = nil
			// If there is a reconfiguration, ensure closing blockChan before sending the new data.
			if e != nil && !e.ReconfFinalizedByBlockSn.IsNil() {
				running = false
				bc.mutex.Lock()
				if err := bc.stopCreatingNewBlocks(false); err != nil {
					logger.Warn("failed to stop; err=%s", err)
				}
				bc.mutex.Unlock()
			}
			ch <- BlockAndEvent{nb, e}
			continue
		}

		select {
		case ch := <-stopChan:
			ch <- nil
			running = false
		case <-bc.notaChan:
			// Receive a notarization which may not be in blocks. Try creating a new block.
		}
	}
}

// getNotarizations returns up to k notarizations that share the same epoch as block b
// starting from and including block b going backwards.
// If one of the above k notarizations is not found, nil is returned.
// I.e. let b.seq = (e,s) then this will return notarizations for blocks (e,s')
// for all s' in [max(1, s-k), s-1] if they exist.
// Notarizations are returned in ascending order.
func (bc *BlockChainFake) getNotarizations(b Block, k int) []Notarization {
	bc.mutex.CheckIsLocked("")

	var notas []Notarization
	epoch := b.GetBlockSn().Epoch
	for i := 0; i < k && epoch == b.GetBlockSn().Epoch; i++ {
		n := bc.getNotarization(b.GetBlockSn())
		if n != nil {
			notas = append(notas, n)
			b = getParentBlock(bc, b)
		} else {
			return nil
		}
	}
	reverse(notas)
	return notas
}

func getParentBlock(bc *BlockChainFake, b Block) Block {
	return bc.getBlock(b.GetParentBlockSn())
}

// Stop the creation. However, there may be some blocks in the returned channel
// by StartCreatingNewBlocks().
func (bc *BlockChainFake) StopCreatingNewBlocks() error {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	return bc.stopCreatingNewBlocks(true)
}

func (bc *BlockChainFake) stopCreatingNewBlocks(fromOutside bool) error {
	bc.mutex.CheckIsLocked("")

	if !bc.isRunning {
		return errors.Errorf("the worker goroutine is not running")
	}
	bc.isRunning = false

	if fromOutside {
		// Wait the worker goroutine to end.
		ch := make(chan error)
		bc.mutex.Unlock()
		bc.stopChan <- ch
		bc.mutex.Lock()
		return <-ch
	}
	return nil
}

func (bc *BlockChainFake) IsCreatingBlock() bool {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	return bc.isRunning
}

func (bc *BlockChainFake) AddNotarization(n Notarization) error {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	return bc.addNotarization(n)
}

func (bc *BlockChainFake) addNotarization(n Notarization) error {
	bc.mutex.CheckIsLocked("")

	if oldNota := bc.getNotarization(n.GetBlockSn()); oldNota != nil {
		// Collect the late votes.
		if oldNota.GetNVote() < n.GetNVote() {
			bc.notasInMemory[n.GetBlockSn()] = n
		}
		return nil
	}

	// Allow adding the notarization only if the parent block and notarization exist.
	// This constraint simplifies the implementation of maintaining the freshest notarized chain.
	sn := n.GetBlockSn()
	fnc := bc.getBlock(sn)
	if fnc == nil {
		return errors.Errorf("add notarization %s but the corresponding block does not exist", sn)
	}
	parentSn := fnc.GetParentBlockSn()
	if !parentSn.IsGenesis() && bc.getNotarization(parentSn) == nil {
		return errors.Errorf("%s's parent notarization %s does not exist", sn, parentSn)
	}

	bc.notasInMemory[sn] = n
	if bc.isRunning {
		// The work goroutine is running. Notify it there is a new notarization.
		bc.notaChan <- n
	}

	if fnc.Compare(bc.freshestNotarizedChain) > 0 {
		bc.freshestNotarizedChain = fnc
		bc.notifyEvent(
			FreshestNotarizedChainExtendedEvent{fnc.GetBlockSn()})
	}
	return nil
}

func (bc *BlockChainFake) notifyEvent(e interface{}) {
	for _, ch := range bc.eventChans {
		select {
		case ch <- e:
		default:
		}
	}
}

func (bc *BlockChainFake) NewNotificationChannel() <-chan interface{} {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()
	ch := make(chan interface{}, 1024)
	bc.eventChans = append(bc.eventChans, ch)
	return ch
}

func (bc *BlockChainFake) RemoveNotificationChannel(target <-chan interface{}) {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()
	for i, ch := range bc.eventChans {
		if ch == target {
			bc.eventChans = append(bc.eventChans[:i], bc.eventChans[i+1:]...)
			break
		}
	}
}

func (bc *BlockChainFake) SetStopBlockNumber(stopBlockNumber uint32) {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	bc.stopBlockNumber = stopBlockNumber
}

// Reset wipes out all data.
func (bc *BlockChainFake) Reset() error {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	bc.blocks = make(map[BlockSn]Block)
	bc.freshestNotarizedChain = bc.genesis
	bc.finalizedChain = bc.genesis
	bc.notasInMemory = make(map[BlockSn]Notarization)
	bc.notasInBlocks = make(map[BlockSn]Notarization)
	bc.stopBlockNumber = 0

	return bc.insertBlock(bc.genesis, false)
}

//--------------------------------------------------------------------

func NewNotarizationFake(sn BlockSn, voterIds []string) Notarization {
	tmp := make([]string, len(voterIds))
	copy(tmp, voterIds)
	sort.Strings(tmp)
	return &NotarizationFake{sn, tmp}
}

func (n *NotarizationFake) GetBlockSn() BlockSn {
	return n.sn
}

func (n *NotarizationFake) GetDebugString() string {
	return fmt.Sprintf("nota{%s, %d}", n.sn, n.GetNVote())
}

func (n *NotarizationFake) Verify() bool {
	return true
}

func (n *NotarizationFake) ImplementsNotarization() {
}

func (n *NotarizationFake) GetNVote() uint16 {
	return uint16(len(n.voterIds))
}

func (n *NotarizationFake) GetBlockHash() Hash {
	return Hash(n.sn.ToBytes())
}

func (n *NotarizationFake) GetVoterIds() []string {
	return n.voterIds
}

func (n *NotarizationFake) GetType() Type {
	return TypeNotarization
}

func (n *NotarizationFake) GetBody() []byte {
	var out [][]byte
	out = append(out, n.sn.ToBytes())
	out = append(out, utils.Uint16ToBytes(n.GetNVote()))
	for _, v := range n.voterIds {
		out = append(out, utils.StringToBytes(v))
	}
	return utils.ConcatCopyPreAllocate(out)
}

//--------------------------------------------------------------------

func NewClockMsgNotaFake(epoch Epoch, voterIds []string) ClockMsgNota {
	tmp := make([]string, len(voterIds))
	copy(tmp, voterIds)
	sort.Strings(tmp)
	return &ClockMsgNotaFake{epoch, tmp}
}

func (n *ClockMsgNotaFake) GetBlockSn() BlockSn {
	return BlockSn{n.epoch, 1}
}

func (n *ClockMsgNotaFake) GetDebugString() string {
	return fmt.Sprintf("nota{%d, %d}", n.epoch, n.GetNVote())
}

func (n *ClockMsgNotaFake) Verify() bool {
	return true
}

func (n *ClockMsgNotaFake) ImplementsClockMsgNota() {
}

func (n *ClockMsgNotaFake) GetEpoch() Epoch {
	return n.epoch
}

func (n *ClockMsgNotaFake) GetNVote() uint16 {
	return uint16(len(n.voterIds))
}

func (n *ClockMsgNotaFake) GetVoterIds() []string {
	return n.voterIds
}

func (n *ClockMsgNotaFake) GetType() Type {
	return TypeClockMsgNota
}

func (n *ClockMsgNotaFake) GetBody() []byte {
	var out [][]byte
	out = append(out, utils.Uint32ToBytes(uint32(n.epoch)))
	out = append(out, utils.Uint16ToBytes(n.GetNVote()))
	for _, v := range n.voterIds {
		out = append(out, utils.StringToBytes(v))
	}
	return utils.ConcatCopyPreAllocate(out)
}

//--------------------------------------------------------------------

func (b *BlockFake) ImplementsBlock() {
}

func (b *BlockFake) GetBlockSn() BlockSn {
	return b.sn
}

func (b *BlockFake) GetDebugString() string {
	return fmt.Sprintf("block{%s, %d}", b.sn, len(b.notas))
}

func (b *BlockFake) GetParentBlockSn() BlockSn {
	return b.parentSn
}

func (b *BlockFake) GetHash() Hash {
	return Hash(b.sn.ToBytes())
}

func (b *BlockFake) GetNumber() uint32 {
	return b.nBlock
}

func (b *BlockFake) GetNotarizations() []Notarization {
	return b.notas
}

func (b *BlockFake) GetBodyString() string {
	return b.body
}

func (b *BlockFake) Compare(other Block) int {
	return b.GetBlockSn().Compare(other.GetBlockSn())
}

func (b *BlockFake) GetType() Type {
	return TypeBlock
}

func (b *BlockFake) GetBody() []byte {
	var out [][]byte
	// sn
	out = append(out, b.sn.ToBytes())
	// parent
	out = append(out, b.GetParentBlockSn().ToBytes())
	// nBlock
	out = append(out, utils.Uint32ToBytes(b.GetNumber()))
	// notas
	out = append(out, utils.Uint16ToBytes(uint16(len(b.notas))))
	for _, n := range b.notas {
		out = append(out, n.GetBody())
	}
	// body
	out = append(out, utils.Uint32ToBytes(uint32(len(b.body))))
	out = append(out, []byte(b.body))
	return utils.ConcatCopyPreAllocate(out)
}

//--------------------------------------------------------------------

// PrepareFakeChain adds blocks to the existing fake chain.
// Assume using the stability-favoring approach.
// k affects how to add the notarization in the block. For (e,s)
// * s=1       : contain the notarizations of the previous k blocks.
// * s in [2,k]: contain no notarization.
// * s>k       : contain the notarization of (e,s-k).
func PrepareFakeChain(
	req *require.Assertions, bc BlockChain, base BlockSn, epoch Epoch, k uint32,
	voters []string, newBlockBodies []string) {
	req.NotEqual(uint32(0), k)

	p := bc.GetBlock(base)
	req.NotNil(p)
	for i, s := range newBlockBodies {
		notas := make([]Notarization, 0)
		np := p
		if i == 0 {
			for j := uint32(0); j < k && np != nil && np.GetBlockSn().Epoch > 0; j++ {
				notas = append(notas,
					NewNotarizationFake(np.GetBlockSn(), voters))
				np = GetParentBlock(bc, np)
			}
			reverse(notas)
		} else {
			for j := uint32(0); j < k-1 && np != nil; j++ {
				np = GetParentBlock(bc, np)
			}
			if np.GetBlockSn().Epoch == epoch {
				notas = append(notas, NewNotarizationFake(np.GetBlockSn(), voters))
			}
		}
		nb := NewBlockFake(
			BlockSn{epoch, uint32(i + 1)}, p.GetBlockSn(), p.GetNumber()+1, notas, s)
		err := bc.InsertBlock(nb)
		req.NoError(err)
		p = nb
	}
}

func NewBlockFake(sn BlockSn, parentSn BlockSn, nBlock uint32, notas []Notarization, body string) Block {
	return &BlockFake{
		sn:       sn,
		parentSn: parentSn,
		nBlock:   nBlock,
		notas:    notas,
		body:     body,
	}
}

func DumpFakeChain(bc BlockChain, b Block, showNota bool) string {
	bs := make([]Block, 0)
	for {
		bs = append(bs, b)
		b = GetParentBlock(bc, b)
		if b == nil {
			break
		}
	}

	var sb strings.Builder
	_, _ = sb.WriteString(bs[len(bs)-1].GetBodyString())
	if showNota {
		ns := bs[len(bs)-1].GetNotarizations()
		_, _ = sb.WriteString("[" + notarizationsToString(bc, ns) + "]")
	}
	for i := len(bs) - 2; i >= 0; i-- {
		_, _ = sb.WriteString("->")
		_, _ = sb.WriteString(bs[i].GetBodyString())
		if showNota {
			ns := bs[i].GetNotarizations()
			_, _ = sb.WriteString("[" + notarizationsToString(bc, ns) + "]")
		}
	}
	return sb.String()
}

func notarizationsToString(bc BlockChain, notas []Notarization) string {
	var sb strings.Builder
	for i, n := range notas {
		if i > 0 {
			_, _ = sb.WriteString(",")
		}
		s := n.GetBlockSn()
		_, _ = sb.WriteString(bc.GetBlock(s).GetBodyString())
	}
	return sb.String()
}

//--------------------------------------------------------------------

func NewVerifierFake(
	myProposerIds, myVoterIds []string, proposers ElectionResult, voters ElectionResult,
) *VerifierFake {
	v := &VerifierFake{
		myProposerIds: myProposerIds,
		myVoterIds:    myVoterIds,
	}
	v.AddElectionResult(proposers, voters)
	return v
}

func (v *VerifierFake) AddElectionResult(proposers ElectionResult, voters ElectionResult) {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	v.proposerLists = append(v.proposerLists, &proposers)
	v.voterLists = append(v.voterLists, &voters)
}

func (v *VerifierFake) Propose(b Block) (Proposal, error) {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	proposerId := v.getProposerId(b.GetBlockSn().Epoch)
	if proposerId == "" {
		return nil, errors.Errorf("have no valid proposer id")
	}
	return &ProposalFake{proposerId, b}, nil
}

func (v *VerifierFake) VerifyProposal(p Proposal) error {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	proposerIds := v.findProposers(p.GetBlockSn().Epoch)
	for _, pid := range proposerIds {
		if pid == p.GetProposerId() {
			return nil
		}
	}
	return errors.Errorf("invalid proposer id=%s at %s", p.GetProposerId(), p.GetBlockSn())
}

func (v *VerifierFake) Vote(p Proposal) (Vote, error) {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	if voterId := v.getVoterId(p.GetBlockSn().Epoch); voterId == "" {
		return nil, errors.Errorf("not a voter at %s", p.GetBlockSn())
	} else {
		return &VoteFake{p.GetBlock().GetBlockSn(), voterId}, nil
	}
}

func (v *VerifierFake) getProposerId(epoch Epoch) string {
	v.mutex.CheckIsLocked("")

	for i := 0; i < len(v.proposerLists); i++ {
		for _, id := range v.myProposerIds {
			if v.proposerLists[i].Contain(id, epoch) {
				return id
			}
		}
	}
	return ""
}

func (v *VerifierFake) getVoterId(epoch Epoch) string {
	v.mutex.CheckIsLocked("")

	for i := 0; i < len(v.voterLists); i++ {
		for _, id := range v.myVoterIds {
			if v.voterLists[i].Contain(id, epoch) {
				return id
			}
		}
	}
	return ""
}

func (v *VerifierFake) VerifyVote(vote Vote) error {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	for _, id := range v.findVoters(vote.GetBlockSn().Epoch) {
		if id == vote.GetVoterId() {
			return nil
		}
	}
	return errors.Errorf("invalid voter id=%s at %s", vote.GetVoterId(), vote.GetBlockSn())
}

func (v *VerifierFake) Notarize(votes []Vote) (Notarization, error) {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	if len(votes) == 0 {
		return nil, errors.Errorf("not enough votes")
	}

	for i := 1; i < len(votes); i++ {
		if votes[i].GetBlockSn() != votes[0].GetBlockSn() {
			return nil, errors.Errorf("votes have different block sequence number")
		}
	}

	nVoter := float64(len(v.findVoters(votes[0].GetBlockSn().Epoch)))
	if float64(len(votes)) < math.Ceil(nVoter*2.0/3.0) {
		return nil, errors.Errorf("not enough votes")
	}
	s := votes[0].GetBlockSn()
	var voterIds []string
	for _, v := range votes {
		voterIds = append(voterIds, v.GetVoterId())
	}
	return NewNotarizationFake(s, voterIds), nil
}

func (v *VerifierFake) findProposers(epoch Epoch) []string {
	v.mutex.CheckIsLocked("")

	for i := 0; i < len(v.proposerLists); i++ {
		if v.proposerLists[i].Contain("", epoch) {
			return v.proposerLists[i].GetConsensusIds()
		}
	}
	return []string{}
}

func (v *VerifierFake) findVoters(epoch Epoch) []string {
	v.mutex.CheckIsLocked("")

	for i := 0; i < len(v.voterLists); i++ {
		if v.voterLists[i].Contain("", epoch) {
			return v.voterLists[i].GetConsensusIds()
		}
	}
	return []string{}
}

func (v *VerifierFake) VerifyNotarization(n Notarization) error {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	// TODO(thunder)
	return nil
}

func (v *VerifierFake) NewClockMsg(e Epoch) (ClockMsg, error) {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	if voterId := v.getVoterId(e - Epoch(1)); voterId == "" {
		return nil, errors.Errorf("not a voter at e=%d", e-Epoch(1))
	} else {
		return &ClockMsgFake{e, voterId}, nil
	}
}

func (v *VerifierFake) VerifyClockMsg(c ClockMsg) error {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	for _, id := range v.findVoters(c.GetEpoch()) {
		if id == c.GetVoterId() {
			return nil
		}
	}
	return errors.Errorf("invalid voter id=%s at epoch=%d", c.GetVoterId(), c.GetEpoch())
}

func (v *VerifierFake) NewClockMsgNota(clocks []ClockMsg) (ClockMsgNota, error) {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	if len(clocks) == 0 {
		return nil, errors.Errorf("not enough votes")
	}

	for i := 1; i < len(clocks); i++ {
		if clocks[i].GetEpoch() != clocks[0].GetEpoch() {
			return nil, errors.Errorf("clocks have different epoch")
		}
	}

	e := clocks[0].GetEpoch()
	nVoter := float64(len(v.findVoters(e)))
	if float64(len(clocks)) < math.Ceil(nVoter*2.0/3.0) {
		return nil, errors.Errorf("not enough votes")
	}
	var voterIds []string
	for _, v := range clocks {
		voterIds = append(voterIds, v.GetVoterId())
	}
	return NewClockMsgNotaFake(e, voterIds), nil
}

func (v *VerifierFake) VerifyClockMsgNota(cn ClockMsgNota) error {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	// TODO(thunder)
	return nil
}

func (v *VerifierFake) Sign(id string, bytes []byte) ([]byte, error) {
	return append(utils.StringToBytes(id), bytes...), nil
}

func (v *VerifierFake) VerifySignature(targetId string, signature []byte, expected []byte) error {
	if !v.doesIdExist(targetId) {
		return errors.Errorf("the requested id=%s does not exist", targetId)
	}

	id2, bytes, err := utils.BytesToString(signature)
	if err != nil {
		return err
	}
	if targetId != id2 {
		return errors.Errorf("wrong signature: id mismatched: %s != %s", targetId, id2)
	}
	if string(bytes) != string(expected) {
		return errors.Errorf("wrong signature: signature mismatched: %v != %v", expected, bytes)
	}
	return nil
}

func (v *VerifierFake) doesIdExist(targetId string) bool {
	for i := 0; i < len(v.proposerLists); i++ {
		for _, id := range v.proposerLists[i].GetConsensusIds() {
			if id == targetId {
				return true
			}
		}
	}

	for i := 0; i < len(v.voterLists); i++ {
		for _, id := range v.voterLists[i].GetConsensusIds() {
			if id == targetId {
				return true
			}
		}
	}

	return false
}

//--------------------------------------------------------------------

func NewProposalFake(id string, b Block) Proposal {
	return &ProposalFake{id, b}
}

func (p *ProposalFake) ImplementsProposal() {
}

func (p *ProposalFake) GetBlockSn() BlockSn {
	return p.block.GetBlockSn()
}

func (p *ProposalFake) GetDebugString() string {
	return fmt.Sprintf("proposal{%s}", p.GetBlockSn())
}

func (p *ProposalFake) GetBlock() Block {
	return p.block
}

func (p *ProposalFake) GetProposerId() string {
	return p.proposerId
}

func (p *ProposalFake) GetType() Type {
	return TypeProposal
}

func (p *ProposalFake) GetBody() []byte {
	bytes := utils.StringToBytes(p.proposerId)
	return append(bytes, p.block.GetBody()...)
}

//--------------------------------------------------------------------

func NewVoteFake(sn BlockSn, id string) Vote {
	return &VoteFake{sn, id}
}

func (v *VoteFake) ImplementsVote() {
}

func (v *VoteFake) GetBlockSn() BlockSn {
	return v.sn
}

func (v *VoteFake) GetDebugString() string {
	return fmt.Sprintf("vote{%s}", v.sn)
}

func (v *VoteFake) GetType() Type {
	return TypeVote
}

func (v *VoteFake) GetBody() []byte {
	return append(v.sn.ToBytes(), utils.StringToBytes(v.voterId)...)
}

func (v *VoteFake) GetVoterId() string {
	return v.voterId
}

func reverse(notas []Notarization) {
	for i, j := 0, len(notas)-1; i < j; i, j = i+1, j-1 {
		n := notas[i]
		notas[i] = notas[j]
		notas[j] = n
	}
}

//--------------------------------------------------------------------

func (c *ClockMsgFake) ImplementsClockMsg() {
}

// A helper function for logging.
func (c *ClockMsgFake) GetBlockSn() BlockSn {
	return BlockSn{c.epoch, 1}
}

func (c *ClockMsgFake) GetEpoch() Epoch {
	return c.epoch
}

func (c *ClockMsgFake) GetDebugString() string {
	return fmt.Sprintf("clock{%d}", c.epoch)
}

func (c *ClockMsgFake) GetType() Type {
	return TypeClockMsg
}

func (c *ClockMsgFake) GetBody() []byte {
	return append(utils.Uint32ToBytes(uint32(c.epoch)), utils.StringToBytes(c.voterId)...)
}

func (c *ClockMsgFake) GetVoterId() string {
	return c.voterId
}

//--------------------------------------------------------------------

// UnmarshalProposal unmarshal the output of ProposalFake.GetBody().
func (du *DataUnmarshallerFake) UnmarshalProposal(bytes []byte) (Proposal, []byte, error) {
	id, bytes, err := utils.BytesToString(bytes)
	if err != nil {
		return nil, nil, err
	}
	if block, bytes, err := du.UnmarshalBlock(bytes); err != nil {
		return nil, nil, err
	} else {
		return &ProposalFake{id, block}, bytes, nil
	}
}

// UnmarshalBlock unmarshal the output of BlockFake.GetBody().
func (du *DataUnmarshallerFake) UnmarshalBlock(bytes []byte) (Block, []byte, error) {
	sn, bytes, err := NewBlockSnFromBytes(bytes)
	if err != nil {
		return nil, nil, err
	}

	psn, bytes, err := NewBlockSnFromBytes(bytes)
	if err != nil {
		return nil, nil, err
	}

	nBlock, bytes, err := utils.BytesToUint32(bytes)
	if err != nil {
		return nil, nil, err
	}

	notas := make([]Notarization, 0)
	n, bytes, err := utils.BytesToUint16(bytes)
	if err != nil {
		return nil, nil, err
	}
	for i := 0; i < int(n); i++ {
		var n Notarization
		var err error
		if n, bytes, err = du.UnmarshalNotarization(bytes); err != nil {
			return nil, nil, err
		}
		notas = append(notas, n)
	}

	bn, bytes, err := utils.BytesToUint32(bytes)
	if err != nil {
		return nil, nil, err
	}
	body := string(bytes[:bn])
	bytes = bytes[bn:]
	return NewBlockFake(sn, psn, nBlock, notas, body), bytes, nil
}

// UnmarshalVote unmarshal the output of VoteFake.GetBody().
func (du *DataUnmarshallerFake) UnmarshalVote(bytes []byte) (Vote, []byte, error) {
	sn, bytes, err := NewBlockSnFromBytes(bytes)
	if err != nil {
		return nil, nil, err
	}
	voterId, bytes, err := utils.BytesToString(bytes)
	if err != nil {
		return nil, nil, err
	}
	return &VoteFake{sn, voterId}, bytes, nil
}

// UnmarshalNotarization unmarshal the output of NotarizationFake.GetBody().
func (du *DataUnmarshallerFake) UnmarshalNotarization(bytes []byte) (Notarization, []byte, error) {
	var voterIds []string
	sn, bytes, err := NewBlockSnFromBytes(bytes)
	if err != nil {
		return nil, nil, err
	}
	nVote, bytes, err := utils.BytesToUint16(bytes)
	if err != nil {
		return nil, nil, err
	}
	for i := 0; i < int(nVote); i++ {
		var v string
		var err error
		v, bytes, err = utils.BytesToString(bytes)
		if err != nil {
			return nil, nil, err
		}
		voterIds = append(voterIds, v)
	}
	return &NotarizationFake{sn, voterIds}, bytes, nil
}

// UnmarshalClockMsg unmarshal the output of ClockMsgFake.GetBody().
func (du *DataUnmarshallerFake) UnmarshalClockMsg(bytes []byte) (ClockMsg, []byte, error) {
	epoch, bytes, err := utils.BytesToUint32(bytes)
	if err != nil {
		return nil, nil, err
	}
	voterId, bytes, err := utils.BytesToString(bytes)
	if err != nil {
		return nil, nil, err
	}
	return &ClockMsgFake{Epoch(epoch), voterId}, bytes, nil
}

// UnmarshalClockMsgNota unmarshal the output of ClockMsgNotaFake.GetBody().
func (du *DataUnmarshallerFake) UnmarshalClockMsgNota(
	bytes []byte) (ClockMsgNota, []byte, error) {
	var voterIds []string
	epoch, bytes, err := utils.BytesToUint32(bytes)
	if err != nil {
		return nil, nil, err
	}
	nVote, bytes, err := utils.BytesToUint16(bytes)
	if err != nil {
		return nil, nil, err
	}
	for i := 0; i < int(nVote); i++ {
		var v string
		var err error
		v, bytes, err = utils.BytesToString(bytes)
		if err != nil {
			return nil, nil, err
		}
		voterIds = append(voterIds, v)
	}
	return &ClockMsgNotaFake{Epoch(epoch), voterIds}, bytes, nil
}

//--------------------------------------------------------------------

func NewElectionResult(consensusId []string, begin Epoch, end Epoch) ElectionResult {
	return ElectionResult{consensusId, begin, end}
}

func (t ElectionResult) Contain(targetId string, epoch Epoch) bool {
	if epoch < t.begin || epoch > t.end {
		return false
	}

	if targetId == "" {
		return true
	}

	for _, id := range t.consensusIds {
		if id == targetId {
			return true
		}
	}
	return false
}

func (t ElectionResult) IsNil() bool {
	return (t.begin == 0 && t.end == 0) || t.begin > t.end
}

func (t ElectionResult) GetConsensusIds() []string {
	return t.consensusIds
}

func (t ElectionResult) String() string {
	var b strings.Builder
	_, _ = b.WriteString("[(")
	_, _ = b.WriteString(strings.Join(t.consensusIds, ","))
	_, _ = b.WriteString("):")
	_, _ = b.WriteString(fmt.Sprintf("%d", t.begin))
	_, _ = b.WriteString("-")
	_, _ = b.WriteString(fmt.Sprintf("%d", t.end))
	_, _ = b.WriteString("]")
	return b.String()
}
