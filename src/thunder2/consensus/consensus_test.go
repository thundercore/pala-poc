package consensus

import (
	"testing"
	"thunder2/blockchain"
	"thunder2/network"
	"time"

	"github.com/stretchr/testify/require"
)

// TODO(thunder): test k = 2.

type nodeConfigForTest struct {
	t             *testing.T
	loggingId     string
	k             uint32
	myProposerIds []string
	myVoterIds    []string
	proposerList  blockchain.ElectionResult
	voterList     blockchain.ElectionResult
	blockDelay    time.Duration
	timer         Timer
}

var (
	hasRunTestCollectingLateVotes = false
)

func createNodeForTest(cfg nodeConfigForTest) (*Node, NodeConfig) {
	nc := NewNodeClientFake(cfg.loggingId)
	chain, err := blockchain.NewBlockChainFakeWithDelay(cfg.k, cfg.blockDelay)
	require.NoError(cfg.t, err)

	epoch := blockchain.Epoch(1)
	if cfg.timer == nil {
		cfg.timer = NewTimer(epoch)
	}

	nodeCfg := NodeConfig{
		LoggingId:  cfg.loggingId,
		K:          cfg.k,
		Chain:      chain,
		NodeClient: nc,
		Role:       NewRoleAssignerFake(cfg.myProposerIds, cfg.myVoterIds, "", cfg.proposerList, cfg.voterList),
		Verifier:   blockchain.NewVerifierFake(cfg.myProposerIds, cfg.myVoterIds, cfg.proposerList, cfg.voterList),
		Epoch:      epoch,
		Timer:      cfg.timer,
	}
	n := NewNode(nodeCfg)
	return &n, nodeCfg
}

// Demonstrate the concept of how to test the protocol step by step.
//
// You can think the testing code simulates one of the possible operation sequences
// of the Mediator.
func TestOneProposerAndOneVoter(t *testing.T) {
	req := require.New(t)

	// Prepare
	epoch := blockchain.Epoch(1)
	k := uint32(1)
	proposerList := blockchain.NewElectionResult([]string{"p1"}, 0, epoch)
	voterList := blockchain.NewElectionResult([]string{"v1"}, 0, epoch)

	proposer, cfg := createNodeForTest(nodeConfigForTest{
		t:             t,
		loggingId:     "proposer 1",
		k:             k,
		myProposerIds: []string{"p1"},
		proposerList:  proposerList,
		voterList:     voterList,
	})
	proposerMediator := cfg.NodeClient.(*NodeClientFake)
	proposerChain := cfg.Chain
	proposer.Start()
	defer proposer.Stop()

	voter, cfg := createNodeForTest(nodeConfigForTest{
		t:            t,
		loggingId:    "voter 1",
		k:            k,
		myVoterIds:   []string{"v1"},
		proposerList: proposerList,
		voterList:    voterList,
	})
	voterMediator := cfg.NodeClient.(*NodeClientFake)
	voterChain := cfg.Chain
	voter.Start()
	defer voter.Stop()

	//
	// Test
	//
	// Simulate how the proposer receives a block from the blockchain
	// and make a new proposal.
	ch, err := proposerChain.StartCreatingNewBlocks(epoch)
	req.NoError(err)
	b := (<-ch).Block

	// Add a block to the proposing node. Expect to see the node broadcasts a proposal.
	errChan := proposer.AddBlock(b, BlockCreatedBySelf)
	req.NoError(<-errChan)
	m := <-proposerMediator.MessageChan
	p, ok := m.(*blockchain.ProposalFake)
	req.True(ok)

	// Simulate how the proposer sends the proposal to the voter
	// and receives the vote from the voter.
	dummyMsg := network.Message{}
	errChan = voter.AddProposal(p, &dummyMsg, BlockCreatedByOther)
	req.NoError(<-errChan)
	m = <-voterMediator.MessageChan
	v, ok := m.(*blockchain.VoteFake)
	req.True(ok)

	errChan = proposer.AddVote(v)
	req.NoError(<-errChan)

	// Expect the proposer creates and broadcasts the notarization.
	m = <-proposerMediator.MessageChan
	nota, ok := m.(*blockchain.NotarizationFake)
	req.True(ok)

	errChan = voter.AddNotarization(nota)
	req.NoError(<-errChan)

	// Expect the freshest notarized chain is extended.
	bc := proposerChain
	actual := bc.GetFreshestNotarizedChain()
	req.Equal("0[]->(1,1)[]", blockchain.DumpFakeChain(bc, actual, true))

	bc = voterChain
	actual = bc.GetFreshestNotarizedChain()
	req.Equal("0[]->(1,1)[]", blockchain.DumpFakeChain(bc, actual, true))

	// Create another new proposal based on a new block.
	b = (<-ch).Block

	errChan = proposer.AddBlock(b, BlockCreatedBySelf)
	req.NoError(<-errChan)
	m = <-proposerMediator.MessageChan
	p, ok = m.(*blockchain.ProposalFake)
	req.True(ok)

	errChan = voter.AddProposal(p, &dummyMsg, BlockCreatedByOther)
	req.NoError(<-errChan)
	m = <-voterMediator.MessageChan
	v, ok = m.(*blockchain.VoteFake)
	req.True(ok)

	errChan = proposer.AddVote(v)
	req.NoError(<-errChan)

	err = proposerChain.StopCreatingNewBlocks()
	req.NoError(err)

	m = <-proposerMediator.MessageChan
	nota, ok = m.(*blockchain.NotarizationFake)
	req.True(ok)

	errChan = voter.AddNotarization(nota)
	req.NoError(<-errChan)

	// Expect the freshest notarized chain is extended again.
	bc = proposerChain
	actual = bc.GetFreshestNotarizedChain()
	req.Equal("0[]->(1,1)[]->(1,2)[(1,1)]", blockchain.DumpFakeChain(bc, actual, true))

	bc = voterChain
	actual = bc.GetFreshestNotarizedChain()
	req.Equal("0[]->(1,1)[]->(1,2)[(1,1)]", blockchain.DumpFakeChain(bc, actual, true))
}

func TestSignedBySelf(t *testing.T) {
	req := require.New(t)

	// Prepare
	k := uint32(1)
	epoch := blockchain.Epoch(1)
	proposerList := blockchain.NewElectionResult([]string{"p1"}, 0, epoch)
	voterList := blockchain.NewElectionResult([]string{"v1"}, 0, epoch)

	proposer, cfg := createNodeForTest(nodeConfigForTest{
		t:             t,
		loggingId:     "proposer 1",
		k:             k,
		myProposerIds: []string{"p1"},
		myVoterIds:    []string{"v1"},
		proposerList:  proposerList,
		voterList:     voterList,
	})
	proposerMediator := cfg.NodeClient.(*NodeClientFake)
	proposerChain := cfg.Chain
	proposer.Start()
	defer proposer.Stop()

	//
	// Test
	//
	// Create a new proposal based on a new block.
	ch, err := proposerChain.StartCreatingNewBlocks(epoch)
	req.NoError(err)
	b := (<-ch).Block

	err = proposerChain.StopCreatingNewBlocks()
	req.NoError(err)

	errChan := proposer.AddBlock(b, BlockCreatedBySelf)
	req.NoError(<-errChan)
	m := <-proposerMediator.MessageChan
	_, ok := m.(*blockchain.ProposalFake)
	req.True(ok)

	m = <-proposerMediator.MessageChan
	_, ok = m.(*blockchain.NotarizationFake)
	req.True(ok)

	actual := proposerChain.GetFreshestNotarizedChain()
	req.Equal("0[]->(1,1)[]", blockchain.DumpFakeChain(proposerChain, actual, true))
}

// TODO(thunder): test more cases:
// * proposer's freshest notarized chain is fresher (same epoch and different epoch)
// * voter's freshest notarized chain is fresher.
func TestVotingWithInconsistentView(t *testing.T) {
	req := require.New(t)

	// Prepare
	k := uint32(1)
	epoch := blockchain.Epoch(1)
	proposerList := blockchain.NewElectionResult([]string{"p1"}, 0, epoch+1)
	voterIds := []string{"v1", "v2", "v3"}
	voterList := blockchain.NewElectionResult(voterIds, 0, epoch+1)

	proposer, proposerCfg := createNodeForTest(nodeConfigForTest{
		t:             t,
		loggingId:     "proposer 1",
		k:             k,
		myProposerIds: []string{"p1"},
		proposerList:  proposerList,
		voterList:     voterList,
	})
	proposerMediator := proposerCfg.NodeClient.(*NodeClientFake)
	proposerChain := proposerCfg.Chain

	genesisSequence := blockchain.GetGenesisBlockSn()
	blockchain.PrepareFakeChain(
		req, proposerChain, genesisSequence, epoch, k,
		voterIds, []string{"1", "2", "3", "4", "5"})

	proposer.Start()
	defer proposer.Stop()

	voter, voterCfg := createNodeForTest(nodeConfigForTest{
		t:            t,
		loggingId:    "voter 1",
		k:            k,
		myVoterIds:   voterIds[:1],
		proposerList: proposerList,
		voterList:    voterList,
	})
	voterChain := voterCfg.Chain

	// Let the voter have a different freshest notarized chain.
	blockchain.PrepareFakeChain(
		req, voterChain, genesisSequence, epoch, k,
		voterIds, []string{"1", "2", "3"})

	voter.Start()
	defer voter.Stop()

	//
	// Test
	//
	// Create a new proposal based on a new block.
	epoch++
	proposer.SetEpoch(epoch)
	voter.SetEpoch(epoch)

	ps := blockchain.BlockSn{Epoch: 1, S: 5}
	proposerChain.AddNotarization(blockchain.NewNotarizationFake(ps, voterIds))
	ch, err := proposerChain.StartCreatingNewBlocks(epoch)
	req.NoError(err)

	b := (<-ch).Block
	errChan := proposer.AddBlock(b, BlockCreatedBySelf)
	req.NoError(<-errChan)
	m := <-proposerMediator.MessageChan
	p, ok := m.(*blockchain.ProposalFake)
	req.True(ok)

	dummyMsg := network.Message{}
	errChan = voter.AddProposal(p, &dummyMsg, BlockCreatedByOther)
	// Failed because the block is invalid in the voter's chain.
	req.Error(<-errChan)
}

func TestCollectingLateVotes(t *testing.T) {
	if hasRunTestCollectingLateVotes {
		return
	}
	hasRunTestCollectingLateVotes = true

	req := require.New(t)

	// Prepare
	epoch := blockchain.Epoch(1)
	k := uint32(2)
	voterIds := []string{"v1", "v2", "v3"}
	proposerList := blockchain.NewElectionResult([]string{"p1"}, 0, epoch)
	voterList := blockchain.NewElectionResult(voterIds, 0, epoch)

	proposer, cfg := createNodeForTest(nodeConfigForTest{
		t:             t,
		loggingId:     "p1",
		k:             k,
		myProposerIds: []string{"p1"},
		proposerList:  proposerList,
		voterList:     voterList,
		// Need a delay to collect late votes.
		blockDelay: time.Duration(100) * time.Millisecond,
	})
	proposerMediator := cfg.NodeClient.(*NodeClientFake)
	proposerChain := cfg.Chain
	proposer.Start()
	defer proposer.Stop()

	//
	// Test
	//
	// Simulate how the proposer receives a block from the blockchain
	// and make a new proposal.
	ch, err := proposerChain.StartCreatingNewBlocks(epoch)
	req.NoError(err)
	b := (<-ch).Block

	// Add a block to the proposing node. Expect to see the node broadcasts a proposal.
	errChan := proposer.AddBlock(b, BlockCreatedBySelf)
	req.NoError(<-errChan)
	m := <-proposerMediator.MessageChan
	p, ok := m.(*blockchain.ProposalFake)
	req.True(ok)

	// Simulate how the proposer receives votes from the voters.
	firstBlockSn := p.GetBlockSn()
	for _, id := range voterIds {
		v := blockchain.NewVoteFake(firstBlockSn, id)
		errChan = proposer.AddVote(v)
		req.NoError(<-errChan)
	}

	// Expect the proposer creates and broadcasts the notarization.
	m = <-proposerMediator.MessageChan
	nota, ok := m.(*blockchain.NotarizationFake)
	req.True(ok)
	// Once the proposer receives enough votes, it broadcasts the notarization immediately.
	req.Equal(uint16(2), nota.GetNVote())

	// Create the second proposal based on a new block.
	b = (<-ch).Block

	errChan = proposer.AddBlock(b, BlockCreatedBySelf)
	req.NoError(<-errChan)
	m = <-proposerMediator.MessageChan
	p, ok = m.(*blockchain.ProposalFake)
	req.True(ok)

	// Expect there is no notarization.
	b = p.GetBlock()
	req.NotNil(b)
	notas := b.GetNotarizations()
	req.Equal(0, len(notas))

	// Create the third proposal based on a new block.
	b = (<-ch).Block

	errChan = proposer.AddBlock(b, BlockCreatedBySelf)
	req.NoError(<-errChan)
	m = <-proposerMediator.MessageChan
	p, ok = m.(*blockchain.ProposalFake)
	req.True(ok)

	// Expect the notarization in the proposal has full votes.
	b = p.GetBlock()
	req.NotNil(b)
	notas = b.GetNotarizations()
	req.Equal(1, len(notas))
	req.Equal(firstBlockSn, notas[0].GetBlockSn())
	req.Equal(voterIds, notas[0].(*blockchain.NotarizationFake).GetVoterIds())
}

func TestInsertBlockWithoutParent(t *testing.T) {
	req := require.New(t)

	// Prepare
	k := uint32(1)
	proposerList := blockchain.NewElectionResult([]string{"p1"}, 0, blockchain.Epoch(1))
	voterList := blockchain.NewElectionResult([]string{"v1"}, 0, blockchain.Epoch(1))

	voter, cfg := createNodeForTest(nodeConfigForTest{
		t:            t,
		loggingId:    "voter 1",
		k:            k,
		myVoterIds:   []string{"v1"},
		proposerList: proposerList,
		voterList:    voterList,
	})
	voterMediator := cfg.NodeClient.(*NodeClientFake)
	voter.Start()
	defer voter.Stop()

	// Simulate how the proposer sends the proposal to the voter
	dummyMsg := network.Message{}
	sn := blockchain.BlockSn{Epoch: 1, S: 10}
	parentSn := blockchain.BlockSn{Epoch: 1, S: 9}
	b := blockchain.NewBlockFake(sn, parentSn, 11, nil, "data")
	p := blockchain.NewProposalFake("p1", b)
	errChan := voter.AddProposal(p, &dummyMsg, BlockCreatedByOther)

	// Expect the voter fails to vote the proposal and requests catching up.
	req.Error(<-errChan)
	catchUpSn := <-voterMediator.CatchUpChan
	req.Equal(parentSn, catchUpSn)
}

func TestAdvancingLocalEpoch(t *testing.T) {
	req := require.New(t)

	// Prepare
	epoch := blockchain.Epoch(1)
	k := uint32(1)
	proposerList := blockchain.NewElectionResult([]string{"p1", "p2"}, 0, blockchain.Epoch(10))
	voterList := blockchain.NewElectionResult([]string{"v1"}, 0, blockchain.Epoch(10))

	// Use a shorten timer to reduce the testing time.
	proposer2, cfg := createNodeForTest(nodeConfigForTest{
		t:             t,
		loggingId:     "proposer 1",
		k:             k,
		myProposerIds: []string{"p2"},
		proposerList:  proposerList,
		voterList:     voterList,
		timer:         NewTimer(epoch),
	})
	proposerMediator2 := cfg.NodeClient.(*NodeClientFake)
	proposerChain := cfg.Chain
	proposer2.Start()
	defer proposer2.Stop()

	timer := NewTimerFake(epoch)
	timer.(*TimerFake).AllowAdvancingEpochTo(epoch+blockchain.Epoch(1), 50*time.Millisecond)
	voter, cfg := createNodeForTest(nodeConfigForTest{
		t:            t,
		loggingId:    "voter 1",
		k:            k,
		myVoterIds:   []string{"v1"},
		proposerList: proposerList,
		voterList:    voterList,
		timer:        timer,
	})
	voterMediator := cfg.NodeClient.(*NodeClientFake)
	voterChain := cfg.Chain
	voter.Start()
	defer voter.Stop()

	//
	// Test
	//
	epoch++
	// Expect a timeout happens and the voter broadcasts a ClockMsg.
	m := <-voterMediator.MessageChan
	c, ok := m.(*blockchain.ClockMsgFake)
	req.True(ok, m)
	req.Equal(epoch, c.GetEpoch())

	errChan := proposer2.AddClockMsg(c)
	req.NoError(<-errChan)

	// Expect the proposer creates and broadcasts the clock message notarization.
	m = <-proposerMediator2.MessageChan
	cNota, ok := m.(*blockchain.ClockMsgNotaFake)
	req.True(ok, m)
	req.Equal(epoch, cNota.GetEpoch())

	// Expect the proposer updates clock message notarization.
	m = <-proposerMediator2.MessageChan
	cNota, ok = m.(*blockchain.ClockMsgNotaFake)
	req.True(ok, m)
	req.Equal(epoch, cNota.GetEpoch())
	proposer2.SetEpoch(epoch)

	// Pass the clock message notarization to the voter.
	errChan = voter.AddClockMsgNota(cNota)
	req.NoError(<-errChan)
	m = <-voterMediator.MessageChan
	cNota, ok = m.(*blockchain.ClockMsgNotaFake)
	req.True(ok, m)
	voter.SetEpoch(cNota.GetEpoch())

	// Simulate how the proposer receives a block from the blockchain
	// and make a new proposal.
	ch, err := proposerChain.StartCreatingNewBlocks(epoch)
	req.NoError(err)
	b := (<-ch).Block

	// Add a block to the proposing node. Expect to see the node broadcasts a proposal.
	errChan = proposer2.AddBlock(b, BlockCreatedBySelf)
	req.NoError(<-errChan)
	m = <-proposerMediator2.MessageChan
	p, ok := m.(*blockchain.ProposalFake)
	req.True(ok, m)

	// Simulate how the proposer sends the proposal to the voter
	// and receives the vote from the voter.
	dummyMsg := network.Message{}
	errChan = voter.AddProposal(p, &dummyMsg, BlockCreatedByOther)
	req.NoError(<-errChan)
	m = <-voterMediator.MessageChan
	v, ok := m.(*blockchain.VoteFake)
	req.True(ok, m)

	errChan = proposer2.AddVote(v)
	req.NoError(<-errChan)

	// Expect the proposer creates and broadcasts the notarization.
	m = <-proposerMediator2.MessageChan
	nota, ok := m.(*blockchain.NotarizationFake)
	req.True(ok, m)

	errChan = voter.AddNotarization(nota)
	req.NoError(<-errChan)

	// Expect the freshest notarized chain is extended.
	bc := proposerChain
	actual := bc.GetFreshestNotarizedChain()
	req.Equal("0[]->(2,1)[]", blockchain.DumpFakeChain(bc, actual, true))

	bc = voterChain
	actual = bc.GetFreshestNotarizedChain()
	req.Equal("0[]->(2,1)[]", blockchain.DumpFakeChain(bc, actual, true))
}
