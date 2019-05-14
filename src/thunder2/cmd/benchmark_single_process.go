package main

import (
	"flag"
	"fmt"
	"thunder2/blockchain"
	"thunder2/consensus"
	"thunder2/lgr"
	"thunder2/network"
	"thunder2/testutils"
	"thunder2/utils"
	"time"
)

// Prepare arguments.
var (
	times           int
	nVoter          int
	k               int
	runningTimeInS  int
	packetDelayInMS int
)

func run() {
	// Prepare nodes' data.
	proposerList := blockchain.NewElectionResult([]string{"p1"}, 0, blockchain.Epoch(10))
	var voterIds []string
	for i := 1; i <= nVoter; i++ {
		voterIds = append(voterIds, fmt.Sprintf("v%d", i))
	}
	voterList := blockchain.NewElectionResult(voterIds, 0, blockchain.Epoch(10))

	// Prepare one proposer
	var proposer *consensus.Mediator
	var proposerChain blockchain.BlockChain
	for i := 1; i <= 1; i++ {
		id := fmt.Sprintf("p%d", i)
		proposer, proposerChain = testutils.NewMediatorForTest(testutils.MediatorTestConfig{
			LoggingId:     id,
			MyProposerIds: []string{id},
			ProposerList:  proposerList,
			VoterList:     voterList,
			K:             uint32(k),
		})

		if err := proposer.Start(); err != nil {
			utils.Bug(err.Error())
		}
	}

	// Prepare voters
	var voters []*consensus.Mediator
	for i := 1; i <= nVoter; i++ {
		id := fmt.Sprintf("v%d", i)
		v, _ := testutils.NewMediatorForTest(testutils.MediatorTestConfig{
			LoggingId:    id,
			MyVoterIds:   []string{id},
			ProposerList: proposerList,
			VoterList:    voterList,
			K:            uint32(k),
		})
		voters = append(voters, v)

		if err := v.Start(); err != nil {
			utils.Bug(err.Error())
		}
	}

	// Prepare network delay.
	net := consensus.NewNetworkSimulator()
	_ = net.SetBaseDelay(network.Delay{
		Mean: time.Duration(packetDelayInMS) * time.Millisecond,
	})

	// Connect hosts.
	for _, v := range voters {
		_ = net.Connect(v.GetHostForTest(), proposer.GetHostForTest())
	}

	// Warm up.
	proposerNotificationChan := proposer.NewNotificationChannel()
WarmUpLoop:
	for e := range proposerNotificationChan {
		switch e.(type) {
		case consensus.FreshestNotarizedChainExtendedEvent:
		case consensus.FinalizedChainExtendedEvent:
			break WarmUpLoop
		}
	}
	proposer.RemoveNotificationChannel(proposerNotificationChan)
	// Wait a while to have stable results.
	<-time.NewTimer(time.Second).C

	// Wait the designated time.
	begin := proposerChain.GetFinalizedChain()
	<-time.NewTimer(time.Duration(runningTimeInS) * time.Second).C
	end := proposerChain.GetFinalizedChain()
	nBlock := end.GetNumber() - begin.GetNumber()
	fmt.Printf("%d\t%d\t%.1f\n", nBlock, runningTimeInS, float64(nBlock)/float64(runningTimeInS))

	// Stop proposers/voters.
	var mediators []*consensus.Mediator
	mediators = append(mediators, proposer)
	mediators = append(mediators, voters...)
	for _, m := range mediators {
		if err := m.Stop(); err != nil {
			utils.Bug(err.Error())
		}
		if err := m.Wait(); err != nil {
			utils.Bug(err.Error())
		}
	}
}

func main() {
	flag.IntVar(&times, "times", 1, "number of running times")
	flag.IntVar(&nVoter, "nVoter", 1, "number of voters")
	flag.IntVar(&k, "k", 1, "outstanding unnotarized proposal")
	flag.IntVar(&runningTimeInS, "runningTimeInS", 1, "running time in second")
	flag.IntVar(&packetDelayInMS, "packetDelayInMS", 0, "packet delay time in millisecond")
	flag.Parse()

	// Use a higher log level to avoid overheads.
	_ = lgr.SetLogLevel("/", lgr.LvlError)

	for i := 0; i < times; i++ {
		run()
	}
}
