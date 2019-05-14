// Use a different package to ensure we only test the public API.
package consensus_test

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"thunder2/blockchain"
	. "thunder2/consensus"
	"thunder2/network"
	"thunder2/testutils"
	"thunder2/utils"
	"time"

	"github.com/stretchr/testify/require"
)

// Expect the |mediators| notify FinalizedChainExtendedEvent with sn in range [|beginS|, |endS|]
// at the same |epoch|.
func verifyFinalizedChain(
	t *testing.T, id string, ch <-chan interface{},
	epoch blockchain.Epoch, beginS uint32, endS uint32, verifyProgress bool,
	chain blockchain.BlockChain) {
	last := blockchain.BlockSn{Epoch: epoch, S: endS}
	fcS := beginS
	for e := range ch {
		switch v := e.(type) {
		case FreshestNotarizedChainExtendedEvent:
			// Skip checking the event. Note that some BlockSn may be skipped.
			// For example, if k=2 and the node receives block(1,1), block(1,2) and nota(1,2),
			// then FreshestNotarizedChainExtendedEvent(1,1) is skipped.
			// The sequence does happen during the test because the node finishes the handshake
			// after nota(1,1) is broadcasted.
		case FinalizedChainExtendedEvent:
			if verifyProgress {
				expected := blockchain.BlockSn{Epoch: epoch, S: fcS}
				if expected.Epoch > v.Sn.Epoch {
					// Skip the old events from the last run.
					continue
				}
				var s string
				if expected != v.Sn && chain != nil {
					s = blockchain.DumpFakeChain(chain, chain.GetFreshestNotarizedChain(), false)
				}
				require.Equal(t, expected, v.Sn, "id=%s; chain=%s", id, s)
				fcS++
			}
			if v.Sn == last {
				return
			}
			if v.Sn.Compare(last) > 0 {
				utils.Bug("v.Sn %s > last %s", v.Sn, last)
			}
		}
	}
	require.FailNow(t, "%s is not received", last)
}

func dumpDebugState(ms []*Mediator) {
	fmt.Println("--- Debug State (begin) ---")
	for _, m := range ms {
		s := m.GetDebugState()
		fmt.Println(<-s)
	}
	fmt.Println("--- Debug State (end)   ---")
}

func registerDumpDebugStateHandler(ms []*Mediator) chan os.Signal {
	ch := make(chan os.Signal, 1)
	go func() {
		signal.Notify(ch, syscall.SIGUSR1)
		for {
			s0, ok := <-ch
			if !ok { // channel closed, stopping goroutine
				return
			}
			if _, ok := s0.(syscall.Signal); ok {
				dumpDebugState(ms)
			}
		}
	}()
	return ch
}

func stopSignalHandler(ch chan os.Signal) {
	signal.Stop(ch)
	close(ch)
}

func TestLivenessAndDisasterRecovery(t *testing.T) {
	k := uint32(2)
	epoch := blockchain.Epoch(1)

	// Prepare the proposer
	proposerList := blockchain.NewElectionResult([]string{"p1"}, 0, blockchain.Epoch(2))
	voterIds := []string{"v1", "v2", "v3"}
	voterList := blockchain.NewElectionResult(voterIds, 0, blockchain.Epoch(2))
	newProposer := func(epoch blockchain.Epoch) (*Mediator, blockchain.BlockChain) {
		em := NewEpochManagerFake()
		cNota := blockchain.NewClockMsgNotaFake(epoch, voterIds)
		em.UpdateByClockMsgNota(cNota)
		mediator, chain := testutils.NewMediatorForTest(testutils.MediatorTestConfig{
			LoggingId:     "p1",
			MyProposerIds: []string{"p1"},
			ProposerList:  proposerList,
			VoterList:     voterList,
			K:             k,
			EpochManager:  em,
		})
		return mediator, chain
	}
	proposer, _ := newProposer(epoch)
	proposerNotificationChan := proposer.NewNotificationChannel()

	// Prepare three voters
	voterEpochManagers := []EpochManager{}
	var voters []*Mediator
	var voterNotificationChans []<-chan interface{}
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("v%d", i+1)
		em := NewEpochManagerFake()
		cNota := blockchain.NewClockMsgNotaFake(epoch, voterIds)
		em.UpdateByClockMsgNota(cNota)
		voterEpochManagers = append(voterEpochManagers, em)
		v, _ := testutils.NewMediatorForTest(testutils.MediatorTestConfig{
			LoggingId:    id,
			MyVoterIds:   []string{id},
			ProposerList: proposerList,
			VoterList:    voterList,
			K:            k,
			EpochManager: em,
		})
		voterNotificationChans = append(voterNotificationChans, v.NewNotificationChannel())
		voters = append(voters, v)
	}

	// Register the debug helper.
	var mediators []*Mediator
	mediators = append(mediators, proposer)
	mediators = append(mediators, voters...)

	signalChan := registerDumpDebugStateHandler(mediators)
	defer stopSignalHandler(signalChan)

	t.Run("normal case", func(t *testing.T) {
		req := require.New(t)

		err := proposer.Start()
		req.NoError(err)

		proposerHost := proposer.GetHostForTest()
		for _, v := range voters {
			err := v.Start()
			req.NoError(err)
			network.FakeConnect(v.GetHostForTest(), proposerHost)
		}

		// Expect the proposer and voters to finalize block (1,1-30)
		verifyFinalizedChain(
			t, proposer.GetLoggingId(), proposerNotificationChan, 1, 1, 30, true, nil)
		for i := 0; i < len(voters); i++ {
			verifyFinalizedChain(
				t, voters[i].GetLoggingId(), voterNotificationChans[i], 1, 1, 30, true, nil)
		}

		// Stop proposers/voters.
		for _, m := range mediators {
			err = m.Stop()
			req.NoError(err)
			err = m.Wait()
			req.NoError(err)
		}
	})

	// Aka "disaster recover plan B"
	t.Run("wipe out proposer's data", func(t *testing.T) {
		req := require.New(t)

		// Manually update the epoch so that the proposer will propose (2,1)
		// after performing reconciliation. To test this case many times in a short time,
		// this necessary to reduce the waiting time to send clock messages.
		epoch++
		cNota := blockchain.NewClockMsgNotaFake(epoch, voterIds)
		for _, em := range voterEpochManagers {
			em.UpdateByClockMsgNota(cNota)
		}

		// Create a new proposer to simulate wiping out the proposer's data.
		proposer, proposerChain := newProposer(epoch)
		proposerNotificationChan := proposer.NewNotificationChannel()
		mediators[0] = proposer

		// Restart the consensus nodes..
		err := proposer.Start()
		req.NoError(err)

		proposerHost := proposer.GetHostForTest()
		for _, v := range voters {
			err := v.Start()
			req.NoError(err)
			network.FakeConnect(v.GetHostForTest(), proposerHost)
		}

		// Verify the proposer.
		//
		// Expect the old data are back.
		// Note that since (1,30) is finalized, (1,32) is notarized but (1,33-34) may
		// or may not be finalized. When doing the chain sync, nodes only accept notarized blocks,
		// so the proposer may not pull block (1,33) or (1,34). Given that (1,32) may be
		// the last notarized block, it's reliable to expect (1,28) is finalized.
		verifyFinalizedChain(
			t, proposer.GetLoggingId(), proposerNotificationChan, 1, 1, 28, true, nil)
		// Expect the liveness is back
		verifyFinalizedChain(
			t, proposer.GetLoggingId(), proposerNotificationChan, 2, 1, 30, true, nil)
		// Verify the parent block.
		b := proposerChain.GetBlock(blockchain.BlockSn{Epoch: 2, S: 1})
		req.NotNil(b)
		parentSn := b.GetParentBlockSn()
		req.Equal(blockchain.Epoch(1), parentSn.Epoch)
		req.True(parentSn.S >= uint32(30)+k, blockchain.DumpFakeChain(proposerChain, b, true))

		// Verify voters.
		for i := 0; i < len(voters); i++ {
			verifyFinalizedChain(
				t, voters[i].GetLoggingId(), voterNotificationChans[i], 2, 1, 30, true, nil)
		}

		// Stop proposers/voters.
		for _, m := range mediators {
			err = m.Stop()
			req.NoError(err)
			err = m.Wait()
			req.NoError(err)
		}
	})
}

// Aka the challenge-response authentication.
func TestRoleAuthentication(t *testing.T) {
	req := require.New(t)

	// Prepare one proposer, one bootnode and three voters:
	// * v1, v3 are invalid.
	// * v2 is valid.
	// * All voters and the bootnode connect to the proposer
	// * v3 also connects to the bootnode.
	// Expect
	// * v1 doesn't receive the proposed block.
	// * v2 received the proposed block (via the proposer).
	// * v3 received the proposed block (via the bootnode).
	k := uint32(2)

	proposerList := blockchain.NewElectionResult([]string{"p1"}, 0, blockchain.Epoch(1))
	voterList := blockchain.NewElectionResult([]string{"v2"}, 0, blockchain.Epoch(1))

	proposer, _ := testutils.NewMediatorForTest(testutils.MediatorTestConfig{
		LoggingId:     "p1",
		MyProposerIds: []string{"p1"},
		ProposerList:  proposerList,
		VoterList:     voterList,
		K:             k,
	})
	err := proposer.Start()
	req.NoError(err)
	proposerNotificationChan := proposer.NewNotificationChannel()

	bootnodeId := "b1"
	blockchain.SetBootnodeIdsForTest([]string{bootnodeId})
	bootnode, _ := testutils.NewMediatorForTest(testutils.MediatorTestConfig{
		LoggingId:    bootnodeId,
		MyBootnodeId: bootnodeId,
		ProposerList: proposerList,
		VoterList:    voterList,
		K:            k,
	})
	err = bootnode.Start()
	req.NoError(err)
	network.FakeConnect(bootnode.GetHostForTest(), proposer.GetHostForTest())
	bootnodeNotificationChan := bootnode.NewNotificationChannel()

	voters := []*Mediator{}
	voterChains := []blockchain.BlockChain{}
	var voterNotificationChans []<-chan interface{}
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("v%d", i+1)
		v, chain := testutils.NewMediatorForTest(testutils.MediatorTestConfig{
			LoggingId:    id,
			MyVoterIds:   []string{id},
			ProposerList: proposerList,
			VoterList:    voterList,
			K:            k,
		})
		voters = append(voters, v)
		voterChains = append(voterChains, chain)
		voterNotificationChans = append(voterNotificationChans, v.NewNotificationChannel())

		err := v.Start()
		req.NoError(err)

		network.FakeConnect(v.GetHostForTest(), proposer.GetHostForTest())
	}
	network.FakeConnect(voters[2].GetHostForTest(), bootnode.GetHostForTest())

	// Create some blocks and stop.
	// Use a larger endS to avoid v3 just catching up via the first reconciliation.
	endS := uint32(50)
	verifyFinalizedChain(
		t, proposer.GetLoggingId(), proposerNotificationChan, 1, 1, endS, true, nil)

	// Expect the bootnode receives proposed blocks.
	verifyFinalizedChain(
		t, bootnode.GetLoggingId(), bootnodeNotificationChan, 1, 1, endS, true, nil)
	// Expect v2 receives proposed blocks (via the proposer)
	verifyFinalizedChain(
		t, voters[1].GetLoggingId(), voterNotificationChans[1], 1, 1, endS, true, nil)
	// Expect v3 receives proposed blocks (via the bootnode)
	verifyFinalizedChain(
		t, voters[2].GetLoggingId(), voterNotificationChans[2], 1, 1, endS, true, nil)

	// Stop proposers/voters.
	var mediators []*Mediator
	mediators = append(mediators, proposer, bootnode)
	mediators = append(mediators, voters...)
	for _, m := range mediators {
		err = m.Stop()
		req.NoError(err)
		err = m.Wait()
		req.NoError(err)
	}

	// Expect v1 doesn't receive proposed blocks because v1 fails to authenticate
	// its role as a voter and v1 doesn't connect to the bootnode.
	sn := blockchain.BlockSn{Epoch: 1, S: 1}
	b := voterChains[0].GetBlock(sn)
	req.Nil(b, "v1 shouldn't get %s", sn)
}

func TestCatchUpAndVote(t *testing.T) {
	req := require.New(t)
	k := uint32(2)
	epoch := blockchain.Epoch(2)
	voterIds := []string{"v1"}
	em := NewEpochManagerFake()
	cNota := blockchain.NewClockMsgNotaFake(epoch, voterIds)
	em.UpdateByClockMsgNota(cNota)
	proposerList := blockchain.NewElectionResult([]string{"p1"}, 0, epoch)
	voterList := blockchain.NewElectionResult(voterIds, 0, epoch)
	proposer, proposerChain := testutils.NewMediatorForTest(testutils.MediatorTestConfig{
		LoggingId:     "p1",
		MyProposerIds: []string{"p1"},
		ProposerList:  proposerList,
		VoterList:     voterList,
		K:             k,
		EpochManager:  em,
	})
	proposerHost := proposer.GetHostForTest()
	proposerNotificationChan := proposer.NewNotificationChannel()

	em = NewEpochManagerFake()
	voter, _ := testutils.NewMediatorForTest(testutils.MediatorTestConfig{
		LoggingId:    "v1",
		MyVoterIds:   []string{"v1"},
		ProposerList: proposerList,
		VoterList:    voterList,
		K:            k,
		EpochManager: em,
	})
	voterNotificationChan := voter.NewNotificationChannel()

	// Let the proposer have longer freshest notarized chain,
	// so we can test the voter will catch up.
	blockchain.PrepareFakeChain(req, proposerChain, blockchain.GetGenesisBlockSn(),
		epoch-1, k, voterIds,
		[]string{"1", "2", "3", "4", "5", "6", "7", "8", "9"})

	// Simulate the voter connects to the proposer.
	network.FakeConnect(voter.GetHostForTest(), proposerHost)

	err := voter.Start()
	req.NoError(err)

	err = proposer.Start()
	req.NoError(err)

	// Verify
	verifyFinalizedChain(
		t, proposer.GetLoggingId(), proposerNotificationChan, 2, 1, 30, false, nil)
	verifyFinalizedChain(
		t, voter.GetLoggingId(), voterNotificationChan, 2, 1, 30, false, nil)

	// Stop proposers/voters.
	var mediators []*Mediator
	mediators = append(mediators, proposer, voter)
	for _, m := range mediators {
		err = m.Stop()
		req.NoError(err)
		err = m.Wait()
		req.NoError(err)
	}
}

// TODO(thunder): Refine ChainSyncer and related code to make this test reliable.
// Currently it may hang sometimes.
// Voter reconfiguration is what we call "committee switch" in Thunder 0.5.
func TestVoterReconfiguration(t *testing.T) {
	req := require.New(t)

	// Overview of the test.
	// * One proposer p1.
	// * The first generation of voters are (v1, v2)
	// * The second generation of voters are (v2, v3)
	// * Expect v3 is catching up with the bootnode before it becomes the voter.
	// * Expect v3 connects to p1 during the reconfiguration.
	// * Expect p1 drops the connection to v1 during the reconfiguration.
	// * Expect v2 continues in office.

	k := uint32(2)
	// Note that the genesis block is the 0th block.
	stopBlockNumber := uint32(10)
	voterIds := [][]string{
		[]string{"v1", "v2"},
		[]string{"v2", "v3"},
	}
	proposerList := blockchain.NewElectionResult([]string{"p1"}, 0, blockchain.Epoch(2))
	voterList := blockchain.NewElectionResult(voterIds[0], 0, blockchain.Epoch(1))
	voterList2 := blockchain.NewElectionResult(voterIds[1], 0, blockchain.Epoch(2))

	// Prepare the proposer.
	// Keep the proposer unchanged after the reconfiguration.
	r := NewReconfigurerFake(ReconfigurationConfigFake{
		LoggingId:     "p1",
		MyProposerIds: []string{"p1"},
		ProposerList:  proposerList,
		VoterList:     voterList2,
	})
	proposer, proposerChain := testutils.NewMediatorForTest(testutils.MediatorTestConfig{
		LoggingId:       "p1",
		MyProposerIds:   []string{"p1"},
		ProposerList:    proposerList,
		VoterList:       voterList,
		K:               k,
		StopBlockNumber: stopBlockNumber,
		Reconfigurer:    r,
	})

	proposerNotificationChan := proposer.NewNotificationChannel()
	err := proposer.Start()
	req.NoError(err)

	// Prepare the bootnode.
	bootnodeId := "b1"
	blockchain.SetBootnodeIdsForTest([]string{bootnodeId})
	r = NewReconfigurerFake(ReconfigurationConfigFake{
		LoggingId:    "b1",
		ProposerList: proposerList,
		VoterList:    voterList2,
	})
	bootnode, _ := testutils.NewMediatorForTest(testutils.MediatorTestConfig{
		LoggingId:       bootnodeId,
		MyBootnodeId:    bootnodeId,
		ProposerList:    proposerList,
		VoterList:       voterList,
		K:               k,
		StopBlockNumber: stopBlockNumber,
		Reconfigurer:    r,
	})
	err = bootnode.Start()
	req.NoError(err)

	// Prepare the voters.
	var voters []*Mediator
	var voterNotificationChans []<-chan interface{}
	var voterReconfiguers []Reconfigurer
	var voterChains []blockchain.BlockChain
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("v%d", i+1)
		var newMyVoterIds []string
		switch i {
		case 0:
			// Retire from a voter.
		case 1:
			// Continue in office.
			newMyVoterIds = []string{"v2"}
		case 2:
			// Become a voter.
			newMyVoterIds = []string{"v3"}

		}
		r := NewReconfigurerFake(ReconfigurationConfigFake{
			LoggingId:    id,
			MyVoterIds:   newMyVoterIds,
			ProposerList: proposerList,
			VoterList:    voterList2,
		})
		voterReconfiguers = append(voterReconfiguers, r)

		v, chain := testutils.NewMediatorForTest(testutils.MediatorTestConfig{
			LoggingId:       id,
			MyVoterIds:      []string{id},
			ProposerList:    proposerList,
			VoterList:       voterList,
			K:               k,
			StopBlockNumber: stopBlockNumber,
			Reconfigurer:    r,
		})
		voterChains = append(voterChains, chain)
		voterNotificationChans = append(voterNotificationChans, v.NewNotificationChannel())
		voters = append(voters, v)

		err := v.Start()
		req.NoError(err)
	}

	// Register the debug helper.
	var mediators []*Mediator
	mediators = append(mediators, proposer)
	mediators = append(mediators, voters...)
	mediators = append(mediators, bootnode)

	signalChan := registerDumpDebugStateHandler(mediators)
	defer stopSignalHandler(signalChan)

	// Setup network connections.
	proposerHost := proposer.GetHostForTest()
	voterReconfiguers[2].(*ReconfigurerFake).SetNetworkReconfiguration(
		func(bc blockchain.BlockChain, host *network.Host) error {
			network.FakeConnect(voters[2].GetHostForTest(), proposerHost)
			return nil
		})

	// v1, v2 connect to the proposer
	network.FakeConnect(voters[0].GetHostForTest(), proposerHost)
	network.FakeConnect(voters[1].GetHostForTest(), proposerHost)
	// All nodes connect to the bootnode
	bootnodeHost := bootnode.GetHostForTest()
	network.FakeConnect(proposerHost, bootnodeHost)
	for i := 0; i < len(voters); i++ {
		network.FakeConnect(voters[i].GetHostForTest(), bootnodeHost)
	}

	// Verify
	end := stopBlockNumber
	verifyFinalizedChain(
		t, proposer.GetLoggingId(), proposerNotificationChan, 1, 1, end, true, nil)
	verifyFinalizedChain(
		t, proposer.GetLoggingId(), proposerNotificationChan, 2, 1, 5, true, nil)
	b := proposerChain.GetBlock(blockchain.BlockSn{Epoch: 2, S: 1})
	req.NotNil(b)
	parentSn := b.GetParentBlockSn()
	req.Equal(blockchain.Epoch(1), parentSn.Epoch)
	// TODO(thunder): The current implementation is not 100% reliable. We need to ensure the
	// parent block is the stop block.
	// TODO(thunder): add the test case to demonstrate that we may have two different finalized
	// chains if we don't truncate blocks after the stop block. The reason is that we can only
	// ensure the stop block is in the finalized chain but we cannot ensure two sessions of
	// consensus nodes have the same finalized chain made by consensus nodes in this session.
	req.True(parentSn.S >= end+k, blockchain.DumpFakeChain(proposerChain, b, true))

	// Verify all voters for (1,s)
	for i := 0; i < len(voters); i++ {
		verifyFinalizedChain(
			t, voters[i].GetLoggingId(), voterNotificationChans[i], 1, 1, end, true, nil)
	}
	// Verify the new voters for (2,s)
	verifyFinalizedChain(
		t, voters[1].GetLoggingId(), voterNotificationChans[1], 2, 1, 5, true, nil)
	verifyFinalizedChain(
		t, voters[2].GetLoggingId(), voterNotificationChans[2], 2, 1, 5, true, nil)

	// Expect v1 doesn't receive notarization (2,3) because v1 does not connect to b1
	// and p1 drops the connection to v1 after (2,1) is finalized.
	sn := voterChains[0].GetFreshestNotarizedChain().GetBlockSn()
	req.True(blockchain.BlockSn{Epoch: 2, S: 1 + 2*k}.Compare(sn) > 0, sn)

	// Stop proposers/voters.
	for _, m := range mediators {
		err = m.Stop()
		req.NoError(err)
		err = m.Wait()
		req.NoError(err)
	}

	// Expect (v1, v2) are voters of block{1,1}, ..., block{1,14}
	epoch := blockchain.Epoch(1)
	for i := uint32(1); i <= end+k; i++ {
		sn := blockchain.BlockSn{Epoch: epoch, S: i}
		nota := proposerChain.GetNotarization(sn)
		req.NotNil(nota, "i=%d", i)
		voterIds := nota.(*blockchain.NotarizationFake).GetVoterIds()
		req.Equal(2, len(voterIds))
		req.Equal("v1", voterIds[0], "sn=%s, voterIds=%s", sn, voterIds)
		req.Equal("v2", voterIds[1], "sn=%s, voterIds=%s", sn, voterIds)
	}

	// Expect (v2, v3) are voters of block{2,1}, ..., block{2,10}
	epoch++
	for i := uint32(1); i <= uint32(5)+k; i++ {
		sn := blockchain.BlockSn{Epoch: epoch, S: i}
		nota := proposerChain.GetNotarization(sn)
		req.NotNil(nota, "i=%d", i)
		voterIds := nota.(*blockchain.NotarizationFake).GetVoterIds()
		req.Equal(2, len(voterIds))
		req.Equal("v2", voterIds[0], "sn=%s, voterIds=%s", sn, voterIds)
		req.Equal("v3", voterIds[1], "sn=%s, voterIds=%s", sn, voterIds)
	}
}

func TestProposerSwitch(t *testing.T) {
	k := uint32(2)

	// TODO(thunder): think how to make this test more reliable.
	t.Run("simple case", func(t *testing.T) {
		req := require.New(t)

		// Prepare one proposer and one voter. The proposer becomes the primary one at epoch 3.
		proposerList := blockchain.NewElectionResult(
			[]string{"p1", "p2", "p3"}, 0, blockchain.Epoch(10))
		voterList := blockchain.NewElectionResult([]string{"v1"}, 0, blockchain.Epoch(10))

		timer := NewTimerFake(1)
		timer.(*TimerFake).AllowAdvancingEpochTo(3, 50*time.Millisecond)
		proposer3, proposerChain3 := testutils.NewMediatorForTest(testutils.MediatorTestConfig{
			LoggingId:     "p3",
			MyProposerIds: []string{"p3"},
			ProposerList:  proposerList,
			VoterList:     voterList,
			K:             k,
		})
		err := proposer3.Start()
		req.NoError(err)
		proposerNotificationChan3 := proposer3.NewNotificationChannel()

		voter, _ := testutils.NewMediatorForTest(testutils.MediatorTestConfig{
			LoggingId:    "v1",
			MyVoterIds:   []string{"v1"},
			ProposerList: proposerList,
			VoterList:    voterList,
			K:            k,
			Timer:        timer,
		})
		voterNotificationChan := voter.NewNotificationChannel()
		err = voter.Start()
		req.NoError(err)

		network.FakeConnect(voter.GetHostForTest(), proposer3.GetHostForTest())

		// Register the debug helper.
		var mediators []*Mediator
		mediators = append(mediators, proposer3, voter)

		signalChan := registerDumpDebugStateHandler(mediators)
		defer stopSignalHandler(signalChan)

		// Expect the liveness starts at epoch=3.
		verifyFinalizedChain(
			t, proposer3.GetLoggingId(), proposerNotificationChan3, 3, 1, 10, true, nil)
		verifyFinalizedChain(
			t, voter.GetLoggingId(), voterNotificationChan, 3, 1, 10, true, nil)

		b := proposerChain3.GetBlock(blockchain.BlockSn{Epoch: 3, S: 1})
		req.NotNil(b)
		parentSn := b.GetParentBlockSn()
		req.Equal(blockchain.GetGenesisBlockSn(), parentSn)

		// Stop proposers/voters.
		for _, m := range mediators {
			err = m.Stop()
			req.NoError(err)
			err = m.Wait()
			req.NoError(err)
		}
	})

	// TODO(thunder): think how to make this test more reliable.
	t.Run("switch to another and switch back to the original one", func(t *testing.T) {
		req := require.New(t)

		//
		// Prepare two proposers and three voters.
		//
		k := uint32(2)
		proposerList := blockchain.NewElectionResult([]string{"p1", "p2"}, 0, blockchain.Epoch(10))
		voterList := blockchain.NewElectionResult([]string{"v1", "v2", "v3"}, 0, blockchain.Epoch(10))

		// Prepare two proposers
		var proposers []*Mediator
		var proposerChains []blockchain.BlockChain
		var proposerNotificationChans []<-chan interface{}
		for i := 0; i < 2; i++ {
			id := fmt.Sprintf("p%d", i+1)
			p, chain := testutils.NewMediatorForTest(testutils.MediatorTestConfig{
				LoggingId:     id,
				MyProposerIds: []string{id},
				ProposerList:  proposerList,
				VoterList:     voterList,
				K:             k,
			})

			err := p.Start()
			req.NoError(err)

			proposers = append(proposers, p)
			proposerChains = append(proposerChains, chain)
			proposerNotificationChans = append(
				proposerNotificationChans, p.NewNotificationChannel())
		}

		// Prepare three voters
		var voterTimers []Timer
		var voters []*Mediator
		for i := 0; i < 3; i++ {
			id := fmt.Sprintf("v%d", i+1)
			timer := NewTimerFake(1)
			voterTimers = append(voterTimers, timer)
			v, _ := testutils.NewMediatorForTest(testutils.MediatorTestConfig{
				LoggingId:    id,
				MyVoterIds:   []string{id},
				ProposerList: proposerList,
				VoterList:    voterList,
				K:            k,
				Timer:        timer,
			})
			voters = append(voters, v)

			err := v.Start()
			req.NoError(err)
		}

		// Register the debug helper.
		var mediators []*Mediator
		mediators = append(mediators, proposers...)
		mediators = append(mediators, voters...)

		signalChan := registerDumpDebugStateHandler(mediators)
		defer stopSignalHandler(signalChan)

		//
		// Prepare network connections.
		//
		// Simulate that p1 is offline after (1,10) is notarized.
		// Expect p2 takes over afterward.
		net := NewNetworkSimulator()
		net.AddRule(NetworkSimulatorRule{
			From: []string{"p1"},
			To:   nil,
			Type: blockchain.TypeNotarization,
			Sn:   blockchain.BlockSn{Epoch: 1, S: 10},
			Action: &network.FilterAction{
				PostCallback: func(from string, to string, typ uint8, blob []byte) network.PassedOrDropped {
					for _, timer := range voterTimers {
						timer.(*TimerFake).AllowAdvancingEpochTo(2, 50*time.Millisecond)
					}
					return network.Dropped
				},
			},
		})
		// Simulate that p1 is online after (2,1)
		net.AddRule(NetworkSimulatorRule{
			From: []string{"p2"},
			To:   []string{"v1"},
			Type: blockchain.TypeNotarization,
			Sn:   blockchain.BlockSn{Epoch: 2, S: 1},
			Action: &network.FilterAction{
				PostCallback: func(from string, to string, typ uint8, blob []byte) network.PassedOrDropped {
					net.Connect(proposers[0].GetHostForTest(), proposers[1].GetHostForTest())
					for _, v := range voters {
						net.Connect(v.GetHostForTest(), proposers[0].GetHostForTest())
					}
					return network.Passed
				},
			},
		})
		// Simulate that p2 is offline after (2,10) is notarized.
		// Expect p1 takes over afterward.
		net.AddRule(NetworkSimulatorRule{
			From: []string{"p2"},
			To:   nil,
			Type: blockchain.TypeNotarization,
			Sn:   blockchain.BlockSn{Epoch: 2, S: 10},
			Action: &network.FilterAction{
				PostCallback: func(from string, to string, typ uint8, blob []byte) network.PassedOrDropped {
					for _, timer := range voterTimers {
						timer.(*TimerFake).AllowAdvancingEpochTo(3, 50*time.Millisecond)
					}
					return network.Dropped
				},
			},
		})
		// Connect hosts.
		net.Connect(proposers[1].GetHostForTest(), proposers[0].GetHostForTest())
		for _, v := range voters {
			for _, p := range proposers {
				net.Connect(v.GetHostForTest(), p.GetHostForTest())
			}
		}

		// Test
		loggingId := proposers[0].GetLoggingId()
		ch := proposerNotificationChans[0]
		chain := proposerChains[0]
		verifyFinalizedChain(t, loggingId, ch, 1, 1, 10-2*k, true, chain)
		verifyFinalizedChain(t, loggingId, ch, 2, 1, 10-2*k, true, chain)
		verifyFinalizedChain(t, loggingId, ch, 3, 1, 10, true, chain)

		// Stop proposers/voters.
		for _, m := range mediators {
			err := m.Stop()
			req.NoError(err)
			err = m.Wait()
			req.NoError(err)
		}
	})
}

// TODO(thunder): test a voter is much behind and hard to catch up?
