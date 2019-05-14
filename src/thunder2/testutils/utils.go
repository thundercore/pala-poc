package testutils

import (
	"thunder2/blockchain"
	"thunder2/consensus"
	"thunder2/network"
	"thunder2/utils"
)

type MediatorTestConfig struct {
	LoggingId       string
	MyProposerIds   []string
	MyVoterIds      []string
	MyBootnodeId    string
	ProposerList    blockchain.ElectionResult
	VoterList       blockchain.ElectionResult
	K               uint32
	StopBlockNumber uint32
	Reconfigurer    consensus.Reconfigurer
	EpochManager    consensus.EpochManager
	Timer           consensus.Timer
}

// NOTE: we'll reuse this function in many places (e.g., a benchmark program),
// so do not access code related to testing.
func NewMediatorForTest(cfg MediatorTestConfig) (*consensus.Mediator, blockchain.BlockChain) {
	chain, err := blockchain.NewBlockChainFake(cfg.K)
	if err != nil {
		utils.Bug("cannot create the chain: err=%s", err)
	}
	if cfg.StopBlockNumber > 0 {
		bcf := chain.(*blockchain.BlockChainFake)
		bcf.SetStopBlockNumber(cfg.StopBlockNumber)
	}
	role := consensus.NewRoleAssignerFake(
		cfg.MyProposerIds, cfg.MyVoterIds, cfg.MyBootnodeId, cfg.ProposerList, cfg.VoterList)
	verifier := blockchain.NewVerifierFake(
		cfg.MyProposerIds, cfg.MyVoterIds, cfg.ProposerList, cfg.VoterList)
	if cfg.Reconfigurer == nil {
		cfg.Reconfigurer = consensus.NewReconfigurerFake(consensus.ReconfigurationConfigFake{})
	}
	if cfg.EpochManager == nil {
		cfg.EpochManager = consensus.NewEpochManagerFake()
	}
	if cfg.Timer == nil {
		cfg.Timer = consensus.NewTimer(cfg.EpochManager.GetEpoch())
	}
	mediatorCfg := consensus.MediatorConfig{
		LoggingId:        cfg.LoggingId,
		K:                cfg.K,
		BlockChain:       chain,
		DataUnmarshaller: &blockchain.DataUnmarshallerFake{},
		Reconfigurer:     cfg.Reconfigurer,
		EpochManager:     cfg.EpochManager,
		Role:             role,
		Verifier:         verifier,
		Selector:         network.ZeroSelector,
		Timer:            cfg.Timer,
	}
	return consensus.NewMediator(mediatorCfg), chain
}
