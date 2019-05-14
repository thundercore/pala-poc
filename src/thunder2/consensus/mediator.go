package consensus

import (
	"flag"
	"fmt"
	"math"
	"sort"
	"strings"
	"thunder2/blockchain"
	"thunder2/network"
	"thunder2/utils"
	"time"

	"github.com/petar/GoLLRB/llrb"
	"github.com/pkg/errors"
)

var delayOfMakingFirstProposal = 1000

type MediatorConfig struct {
	LoggingId        string
	K                uint32 // The outstanding window.
	BlockChain       blockchain.BlockChain
	Role             RoleAssigner
	Verifier         blockchain.Verifier
	DataUnmarshaller blockchain.DataUnmarshaller
	Reconfigurer     Reconfigurer
	EpochManager     EpochManager
	Selector         func() int
	Timer            Timer
}

// TODO(thunder): Mediator must try to connect disconnected consensus nodes periodically.
// Do this after the real network implementation is done.
// Note that when using the fake network, the testing code simulates connecting/disconnecting.
// Need to add a flag to skip code in real connection in the testing mode.

// Mediator uses Mediator Pattern. It is the center of all main objects.
// We can easily test Node using Mediator.
//
// About goroutine safety:
// * Mediator waits data from BlockChain and Host in its worker goroutine.
// * Node asks Mediator to forward data to Host in Node's worker goroutine.
// * Mediator can do a blocking wait for operations of BlockChain/Node/ChainSyncer if needed,
//   but the reversed way is disallowed to avoid any potential deadlock.
// * See the design document for more info
//   https://docs.google.com/presentation/d/1AY-GiujqkzRdfdleDSrj516d48-3w-z70w4DQiy_3HY/edit?usp=sharing
//
// Implements NodeClient and ChainSyncerClient
type Mediator struct {
	StartStopWaiter

	// Read-only / Assigned once
	loggingId string
	k         uint32
	host      *network.Host
	role      RoleAssigner

	//
	// Only used in worker goroutine
	//
	syncer              *ChainSyncer
	chain               blockchain.BlockChain
	selfChan            chan interface{}
	messageChan         chan *network.Message
	blockChan           chan blockchain.BlockAndEvent
	blockChainEventChan <-chan interface{}
	// Only use verifier for challenge-response authentication. Let node verify consensus data.
	verifier     blockchain.Verifier
	node         *Node
	unmarshaller blockchain.DataUnmarshaller
	reconfigurer Reconfigurer
	// About epoch management:
	// * Mediator owns EpochManager and is responsible to update and save the epoch using EpochManager.
	// * To make Node's decision be consistent in a work, update Node's epoch asynchronously.
	//   Do not let Node share EpochManager; otherwise, it's hard to prevent Node from using
	//   different epochs while processing the same work.
	epochManager                   EpochManager
	connections                    map[string]network.ConnectionHandle
	handshakeStates                map[network.ConnectionHandle]handshakeState
	reconciliationWithAllBeginTime time.Time
	// consensus nodes which we have the consistent view.
	syncedIds                map[string]bool
	unnotarizedProposals     map[blockchain.BlockSn]blockchain.Proposal
	reconfFinalizedByBlockSn blockchain.BlockSn
	// Used to catch up the reconfiguration block.
	// key: BlockSn, value: Proposal
	recentProposals *llrb.LLRB
	// For debug purpose.
	lastBroadcastedProposal blockchain.BlockSn

	//
	// Used in multiple goroutines
	//
	// eventChans is indirectly accessed by callers of Mediator.
	// Protect it by mutex is simpler.
	eventChansMutex utils.CheckedLock
	eventChans      []chan interface{}
}

// There are three ways to update the epoch:
// 1. Use ClockMsgNota. Usually this is the case.
// 2. Use Notarization. Expect this happens only when a node loses its ClockMsgNota
//    and uses Notarization as the proof to update the epoch.
// 3. Reconfigurer updates the epoch using the reconfiguration block as the proof.
//
// All methods must be goroutine safe.
type EpochManager interface {
	GetEpoch() blockchain.Epoch
	UpdateByClockMsgNota(cn blockchain.ClockMsgNota) error
	UpdateByNotarization(nota blockchain.Notarization) error
	GetClockMsgNota(epoch blockchain.Epoch) blockchain.ClockMsgNota
	GetNotarization(epoch blockchain.Epoch) blockchain.Notarization
}

type FreshestNotarizedChainExtendedEvent struct {
	Sn blockchain.BlockSn
}

type FinalizedChainExtendedEvent struct {
	Sn blockchain.BlockSn
}

// Reconfigurer can update the context of objects when the proposer/voter reconfiguration
// happens. The implementation of Reconfigurer should know the implementation of those
// corresponding classes.
type Reconfigurer interface {
	// UpdateVerifier gets the new data (e.g., proposing/voting keys) from |bc|
	// and updates the new data to |verifier|.
	UpdateVerifier(bc blockchain.BlockChain, verifier blockchain.Verifier) error
	// UpdateRoleAssigner gets the new data (e.g., proposing/voting keys) from |bc|
	// and updates the new data to |role|.
	UpdateRoleAssigner(bc blockchain.BlockChain, role RoleAssigner) error
	// UpdateHost gets the new data (e.g., proposers' network IPs and ports) from |bc|
	// and updates the new data to |host|. Note that the role of network may change.
	UpdateHost(bc blockchain.BlockChain, host *network.Host) error
	// UpdateEpochManager confirms the reconfiguration happens in |bc|
	// and updates the new epoch to |em|.
	UpdateEpochManager(bc blockchain.BlockChain, em EpochManager) error
}

// Must be exclusive from blockchain.Type
type Type uint8

const (
	TypeSentChallengeResponse           = Type(0x81)
	TypeRequestStatus                   = Type(0x82)
	TypeRespondStatus                   = Type(0x83)
	TypeRequestNotarizedBlock           = Type(0x84)
	TypeRespondNotarizedBlock           = Type(0x85)
	TypeRespondNotarizedBlockNotExisted = Type(0x86)
	TypeRequestProposal                 = Type(0x87)
	TypeRespondProposal                 = Type(0x88)
	TypeRespondProposalNotExisted       = Type(0x89)
	TypeRequestUnnotarizedProposals     = Type(0x8a)
	TypeRespondUnnotarizedProposals     = Type(0x8b)
	TypeRequestEpoch                    = Type(0x8c)
	TypeRespondEpochByClockMsgNota      = Type(0x8d)
	TypeRespondEpochByNotarization      = Type(0x8e)
	TypeRespondEpochNotExisted          = Type(0x8f)
	TypeRespondEpochInvalid             = Type(0x90)
)

// Status represents the node's main states. Nodes will exchange their
// status and request verifiable data when they are behind.
type Status struct {
	// The last block's sequence number of the freshest notarized chain.
	FncBlockSn blockchain.BlockSn
	Epoch      blockchain.Epoch
	// The block's sequence number which finalized the reconfiguration block.
	// This is assigned only during the reconfiguration.
	ReconfFinalizedByBlockSn blockchain.BlockSn
}

type DebugState struct {
	Identity             string
	Status               Status
	SyncerState          SyncerDebugState
	SyncedIds            []string
	ConnectedIds         []string
	ProposalInfo         string
	IsMakingBlock        bool
	LastBroadcastedBlock blockchain.BlockSn
}

// TODO(thunder): Check protocol version in the beginning.
//
// Handshake is done in Mediator.handshake(). Here is the flow:
//
// Client                                     |    Server
// -------------------------------------------+---------------------------------------
// event: (connection established)            | event: (connection established)
// action: send TypeSentChallengeResponse     | action: none
// state: N.A -> hsSentChallengeResponse      | state: N.A. -> hsWaitChallengeResponse
//                                            |
//                                            | event: receive TypeSentChallengeResponse
//                                            | action:
//                                            | 1. Verify the message.
//                                            | 2. Send TypeSentChallengeResponse
//                                            | state: hsWaitChallengeResponse -> hsDone
//                                            |
// event: receive TypeSentChallengeResponse   |
// action: verify the message                 |
// state: hsSentChallengeResponse -> hsDone   |
type handshakeState int

const (
	hsWaitChallengeResponse = handshakeState(1)
	hsSentChallengeResponse = handshakeState(2)
	hsDone                  = handshakeState(3)
)

// Types used with selfChan - Begin
type broadcastEvent struct {
	msg blockchain.Message
}

type replyEvent struct {
	source *network.Message
	msg    blockchain.Message
}

type catchUpStatusEvent struct {
	source *network.Message
	sn     blockchain.BlockSn
}

type startCreatingBlocksEvent struct {
}

type reconciliationWithAllEvent struct {
}

type requestEpochProofEvent struct {
	id    string
	epoch blockchain.Epoch
}

type requestDataEvent struct {
	typ uint8
	id  string
	sn  blockchain.BlockSn
}

type onCaughtUpEvent struct {
	id string
	s  Status
}

// Types used with selfChan - End

//--------------------------------------------------------------------

func init() {
	if flag.Lookup("test.v") != nil {
		// Speed up the test.
		delayOfMakingFirstProposal = 50
	}
}

func IsSyncMessage(v uint8) bool {
	return v&0x80 > 0
}

func (typ Type) String() string {
	switch typ {
	case TypeSentChallengeResponse:
		return "TypeSentChallengeResponse"
	case TypeRequestStatus:
		return "TypeRequestStatus"
	case TypeRespondStatus:
		return "TypeRespondStatus"
	case TypeRequestNotarizedBlock:
		return "TypeRequestNotarizedBlock"
	case TypeRespondNotarizedBlock:
		return "TypeRespondNotarizedBlock"
	case TypeRespondNotarizedBlockNotExisted:
		return "TypeRespondNotarizedBlockNotExisted"
	case TypeRequestProposal:
		return "TypeRequestProposal"
	case TypeRespondProposal:
		return "TypeRespondProposal"
	case TypeRespondProposalNotExisted:
		return "TypeRespondProposalNotExisted"
	case TypeRequestUnnotarizedProposals:
		return "TypeRequestUnnotarizedProposals"
	case TypeRespondUnnotarizedProposals:
		return "TypeRespondUnnotarizedProposals"
	case TypeRequestEpoch:
		return "TypeRequestEpoch"
	case TypeRespondEpochByClockMsgNota:
		return "TypeRespondEpochByClockMsgNota"
	case TypeRespondEpochByNotarization:
		return "TypeRespondEpochByNotarization"
	case TypeRespondEpochNotExisted:
		return "TypeRespondEpochNotExisted"
	case TypeRespondEpochInvalid:
		return "TypeRespondEpochInvalid"
	default:
		return "unknown"
	}
}

func hsStateToString(s handshakeState) string {
	switch s {
	case hsWaitChallengeResponse:
		return "hsWaitChallengeResponse"
	case hsSentChallengeResponse:
		return "hsSentChallengeResponse"
	case hsDone:
		return "hsDone"
	default:
		return "unknown"
	}
}

func NewMediator(cfg MediatorConfig) *Mediator {
	if len(cfg.LoggingId) == 0 || cfg.K == 0 || cfg.BlockChain == nil ||
		cfg.Role == nil || cfg.Verifier == nil || cfg.DataUnmarshaller == nil ||
		cfg.Reconfigurer == nil || cfg.Selector == nil || cfg.Timer == nil {
		logger.Error("NewMediator: must fill all fields in MediatorConfig")
		return nil
	}
	nr := network.RoleSpoke
	epoch := cfg.EpochManager.GetEpoch()
	if cfg.Role.IsProposer("", epoch) ||
		cfg.Role.IsBootnode("") {
		nr = network.RoleHub
	}
	ch := make(chan *network.Message, 1024)
	h := network.NewHost(cfg.LoggingId, nr, cfg.Selector, ch)
	m := Mediator{
		loggingId:            cfg.LoggingId,
		k:                    cfg.K,
		host:                 h,
		role:                 cfg.Role,
		chain:                cfg.BlockChain,
		messageChan:          ch,
		verifier:             cfg.Verifier,
		unmarshaller:         cfg.DataUnmarshaller,
		reconfigurer:         cfg.Reconfigurer,
		epochManager:         cfg.EpochManager,
		connections:          make(map[string]network.ConnectionHandle),
		handshakeStates:      make(map[network.ConnectionHandle]handshakeState),
		syncedIds:            make(map[string]bool),
		unnotarizedProposals: make(map[blockchain.BlockSn]blockchain.Proposal),
		recentProposals:      llrb.New(),
	}
	n := NewNode(NodeConfig{
		LoggingId:  cfg.LoggingId,
		K:          cfg.K,
		Chain:      cfg.BlockChain,
		NodeClient: &m,
		Role:       cfg.Role,
		Verifier:   cfg.Verifier,
		Epoch:      cfg.EpochManager.GetEpoch(),
		Timer:      cfg.Timer,
	})
	m.node = &n

	m.reset()
	return &m
}

//
// NodeClient - begin
//
// Called in Node's worker goroutine.
func (m *Mediator) Broadcast(msg blockchain.Message) {
	logger.Debug("[%s] Broadcast %s", m.loggingId, msg.GetDebugString())
	m.selfChan <- broadcastEvent{msg}
}

// Called in Node's worker goroutine.
func (m *Mediator) Reply(source *network.Message, msg blockchain.Message) {
	logger.Debug("[%s] Reply %s %s",
		m.loggingId, msg.GetDebugString(), source.GetSourceDebugInfo())
	m.selfChan <- replyEvent{source, msg}
}

// Called in Node's worker goroutine.
func (m *Mediator) CatchUp(source *network.Message, sn blockchain.BlockSn) {
	m.selfChan <- catchUpStatusEvent{source, sn}
}

// Called in Node's worker goroutine.
func (m *Mediator) UpdateEpoch(cNota blockchain.ClockMsgNota) {
	m.selfChan <- cNota
}

// NodeClient - end

//
// ChainSyncerClient - begin
//
// Called in the ChainSyncer's worker goroutine in the future.
func (m *Mediator) RequestEpochProof(id string, epoch blockchain.Epoch) {
	logger.Debug("[%s] send request for the proof of epoch=%d to id=%s", m.loggingId, epoch, id)
	m.selfChan <- requestEpochProofEvent{id, epoch}
}

// Called in the ChainSyncer's worker goroutine in the future.
func (m *Mediator) RequestNotarizedBlock(id string, sn blockchain.BlockSn) {
	logger.Debug("[%s] send request for notarized block %s to id=%s", m.loggingId, sn, id)
	m.selfChan <- requestDataEvent{uint8(TypeRequestNotarizedBlock), id, sn}
}

// Called in the ChainSyncer's worker goroutine in the future.
func (m *Mediator) RequestProposal(id string, sn blockchain.BlockSn) {
	logger.Debug("[%s] send request for proposal %s to id=%s", m.loggingId, sn, id)
	m.selfChan <- requestDataEvent{uint8(TypeRequestProposal), id, sn}
}

// Called in the ChainSyncer's worker goroutine in the future.
func (m *Mediator) OnCaughtUp(id string, s Status) {
	logger.Info("[%s] caught up to %s:%s", m.loggingId, id, s)
	m.selfChan <- onCaughtUpEvent{id, s}
}

// ChainSyncerClient - end

// Called in handleEventLoop goroutine.
func (m *Mediator) respondNotarizedBlockRequest(msg *network.Message) {
	sn, _, err := blockchain.NewBlockSnFromBytes(msg.GetBlob())
	if err != nil {
		logger.Debug("[%s] receive invalid request for block; err=%s", m.loggingId, err)
		return
	}

	var nm *network.Message
	if b := m.chain.GetBlock(sn); b == nil {
		logger.Debug("[%s] receive a request for block %s, but it does not exist", m.loggingId, sn)
		nm = network.NewMessage(uint8(TypeRespondNotarizedBlockNotExisted), 0, sn.ToBytes())
	} else if n := m.chain.GetNotarization(sn); n == nil {
		logger.Debug("[%s] receive a request for notarization %s, but it does not exist", m.loggingId, sn)
		nm = network.NewMessage(uint8(TypeRespondNotarizedBlockNotExisted), 0, sn.ToBytes())
	} else {
		var out [][]byte
		out = append(out, n.GetBody())
		out = append(out, b.GetBody())
		bytes := utils.ConcatCopyPreAllocate(out)
		nm = network.NewMessage(uint8(TypeRespondNotarizedBlock), 0, bytes)
	}
	if err := msg.Reply(nm); err != nil {
		logger.Warn("[%s] %s", m.loggingId, err)
	}
}

// Called in handleEventLoop goroutine for now.
func (m *Mediator) respondProposalRequest(msg *network.Message) {
	sn, _, err := blockchain.NewBlockSnFromBytes(msg.GetBlob())
	if err != nil {
		logger.Warn("[%s] receive invalid request for block; err=%s", m.loggingId, err)
		return
	}

	var nm *network.Message
	if p := LLRBItemToProposal(m.recentProposals.Get(ToItem(sn))); p != nil {
		logger.Debug("[%s] respond proposal %s", m.loggingId, sn)
		nm = network.NewMessage(uint8(TypeRespondProposal), 0, p.GetBody())
	} else {
		logger.Warn("[%s] receive a request for proposal %s, but it does not exist",
			m.loggingId, sn)
		nm = network.NewMessage(uint8(TypeRespondProposalNotExisted), 0, sn.ToBytes())
	}
	if err := msg.Reply(nm); err != nil {
		logger.Warn("[%s] %s", m.loggingId, err)
	}
}

// Called in handleEventLoop goroutine for now.
func (m *Mediator) requestUnnotarizedProposals() error {
	nm := network.NewMessage(uint8(TypeRequestUnnotarizedProposals), 0, nil)
	sn := m.chain.GetFreshestNotarizedChain().GetBlockSn()
	epoch := m.epochManager.GetEpoch()
	if epoch < sn.Epoch {
		return errors.Errorf("[%s] epoch %d is behind freshest notarized block %s",
			m.loggingId, epoch, sn)
	}

	for id, handle := range m.connections {
		if m.role.IsPrimaryProposer(id, epoch) {
			return m.host.Send(handle, nm)
		}
	}
	return errors.Errorf("[%s] haven't connected to the primary proposer at %d",
		m.loggingId, epoch)
}

// Called in handleEventLoop goroutine for now.
func (m *Mediator) respondUnnotarizedProposals(msg *network.Message) {
	var ps []blockchain.Message
	for _, p := range m.unnotarizedProposals {
		ps = append(ps, p)
	}
	sort.Sort(blockchain.ByBlockSn(ps))

	var out [][]byte
	out = append(out, utils.Uint16ToBytes(uint16(len(m.unnotarizedProposals))))
	for _, p := range ps {
		out = append(out, p.GetBody())
	}
	bytes := utils.ConcatCopyPreAllocate(out)

	nm := network.NewMessage(uint8(TypeRespondUnnotarizedProposals), 0, bytes)
	if err := msg.Reply(nm); err != nil {
		logger.Warn("[%s] %s", m.loggingId, err)
	}
}

// Called in handleEventLoop goroutine for now.
func (m *Mediator) respondEpoch(msg *network.Message) {
	tmp, _, err := utils.BytesToUint32(msg.GetBlob())
	if err != nil {
		logger.Warn("[%s] receive invalid request for epoch; err=%s", m.loggingId, err)
		return
	}
	epoch := blockchain.Epoch(tmp)

	var nm *network.Message
	localEpoch := m.epochManager.GetEpoch()
	// Instead of responding epoch, always respond the proof for localEpoch.
	// This approach has some advantages:
	// * The other end will get latest epoch.
	// * This node may not have the proof of epoch.
	if epoch > localEpoch {
		logger.Info("[%s] invalid request for epoch %d > %d", m.loggingId, epoch, localEpoch)
		nm = network.NewMessage(uint8(TypeRespondEpochInvalid), 0, utils.Uint32ToBytes(uint32(epoch)))
	} else if cNota := m.epochManager.GetClockMsgNota(localEpoch); cNota != nil {
		logger.Debug("[%s] respond epoch %d(%d) with clock message notarization",
			m.loggingId, epoch, localEpoch)
		nm = network.NewMessage(uint8(TypeRespondEpochByClockMsgNota), 0, cNota.GetBody())
	} else if nota := m.epochManager.GetNotarization(localEpoch); nota != nil {
		logger.Debug("[%s] respond epoch %d(%d) with notarization %s",
			m.loggingId, epoch, localEpoch, nota.GetBlockSn())
		nm = network.NewMessage(uint8(TypeRespondEpochByNotarization), 0, nota.GetBody())
	} else {
		logger.Error("[%s] there is no proof for epoch %d(%d)", m.loggingId, epoch, localEpoch)
		bytes := utils.Uint32ToBytes(uint32(epoch))
		bytes = append(bytes, utils.Uint32ToBytes(uint32(localEpoch))...)
		nm = network.NewMessage(uint8(TypeRespondEpochNotExisted), 0, bytes)
	}
	if err := msg.Reply(nm); err != nil {
		logger.Warn("[%s] %s", m.loggingId, err)
	}
}

func (m *Mediator) notifyEvent(e interface{}) {
	m.eventChansMutex.Lock()
	defer m.eventChansMutex.Unlock()
	for _, ch := range m.eventChans {
		select {
		case ch <- e:
		default:
		}
	}
}

func (m *Mediator) GetLoggingId() string {
	return m.loggingId
}

func (m *Mediator) Start() error {
	stoppedChan := make(chan interface{})
	action := func(stopChan chan interface{}) error {
		var err error
		status := m.getStatus()
		logger.Warn("[%s] init state: %s", m.loggingId, status)
		epoch := m.epochManager.GetEpoch()
		if err = m.node.Start(); err != nil {
			return err
		}
		m.node.SetEpoch(epoch)
		if err := m.syncer.SetEpoch(epoch); err != nil {
			logger.Warn("[%s] %s", m.loggingId, err)
		}
		if epoch > m.chain.GetFreshestNotarizedChain().GetBlockSn().Epoch &&
			m.role.IsPrimaryProposer("", epoch) {
			m.selfChan <- reconciliationWithAllEvent{}
		}
		go m.handleEventLoop(stopChan, stoppedChan)
		return nil
	}
	err := m.StartStopWaiter.Start(action, stoppedChan)
	if err == nil {
		logger.Info("[%s] started", m.loggingId)
	}
	return err
}

// Called in handleEventLoop goroutine or before the start.
func (m *Mediator) reset() {
	m.selfChan = make(chan interface{}, 1024)
	if m.blockChainEventChan != nil {
		m.chain.RemoveNotificationChannel(m.blockChainEventChan)
	}
	m.blockChainEventChan = m.chain.NewNotificationChannel()
	m.syncer = NewChainSyncer(m.loggingId, m)
	if err := m.syncer.SetEpoch(1); err != nil {
		logger.Warn("[%s] %s", m.loggingId, err)
	}
}

func (m *Mediator) handleEventLoop(
	stopChan chan interface{}, stoppedChan chan interface{},
) {
	for {
		select {
		case <-stopChan:
			m.stopEventLoop()
			close(stoppedChan)
			return
		case bae := <-m.blockChan:
			logger.Debug("[%s] handleEventLoop: block %s\n", m.loggingId, bae.Block.GetBlockSn())
			// Only used by the primary proposer.
			//
			// Ensure sending the block before the event,
			// so the order is consistent with the other roles.
			m.node.AddBlock(bae.Block, BlockCreatedBySelf)
			if bae.Event != nil {
				m.onReceivedFinalizedChainExtendedEvent(bae.Event)
			}
		case e := <-m.blockChainEventChan:
			m.handleBlockChainEvent(e)
		case msg := <-m.selfChan:
			m.handleSelfMessage(msg)
		case msg := <-m.messageChan:
			m.handleNetworkMessage(msg)
		}
	}
}

// Called in the handleEventLoop goroutine.
func (m *Mediator) stopEventLoop() {
	logger.Info("[%s] try stopping", m.loggingId)
	if err := m.syncer.Stop(); err != nil {
		logger.Warn("[%s] failed to stop ChainSyncer; err=%s", m.loggingId, err)
	}
	if err := m.node.StopAndWait(); err != nil {
		logger.Warn("[%s] failed to stop Node; err=%s", m.loggingId, err)
	}
	m.host.CloseAllConnections()
	// Wait the network closes all connections.
ForLoop:
	for {
		select {
		case <-m.messageChan:
		default:
			break ForLoop
		}
	}
	if n := m.host.GetNumHubs(); n > 0 {
		logger.Warn("[%s] there are %d hubs after closing all connections", m.loggingId, n)
	}
	if n := m.host.GetNumSpokes(); n > 0 {
		logger.Warn("[%s] there are %d spokes after closing all connections", m.loggingId, n)
	}
	if m.blockChan != nil {
		if err := m.chain.StopCreatingNewBlocks(); err != nil {
			logger.Warn("[%s] %s", m.loggingId, err)
		}
		m.blockChan = nil
	}

	m.reset()

	logger.Info("[%s] stopped", m.loggingId)
}

// Called in the handleEventLoop goroutine.
func (m *Mediator) handleBlockChainEvent(e interface{}) {
	switch v := e.(type) {
	case blockchain.FreshestNotarizedChainExtendedEvent:
		m.onReceivedFreshestNotarizedChainExtendedEvent(&v)
	case blockchain.FinalizedChainExtendedEvent:
		// Note that the primary proposer doesn't receive this event.
		// Instead, the event is received along with the new block from m.blockChan.
		m.onReceivedFinalizedChainExtendedEvent(&v)
	}
}

// Called in the handleEventLoop goroutine.
func (m *Mediator) onReceivedFreshestNotarizedChainExtendedEvent(
	e *blockchain.FreshestNotarizedChainExtendedEvent) {
	logger.Debug("[%s] onReceivedFreshestNotarizedChainExtendedEvent %s", m.loggingId, e.Sn)
	epoch := m.epochManager.GetEpoch()
	if e.Sn.Epoch > epoch {
		logger.Info("[%s] handleEventLoop: update epoch to %d because the freshest "+
			"notarization chain extended to %s", m.loggingId, e, e.Sn)
		if nota := m.chain.GetNotarization(e.Sn); nota == nil {
			utils.Bug("notarization %s does not exist after receiving "+
				"FreshestNotarizedChainExtendedEvent", e.Sn)
		} else if nota.GetBlockSn().Epoch > m.epochManager.GetEpoch() {
			oldEpoch := m.epochManager.GetEpoch()
			if err := m.epochManager.UpdateByNotarization(nota); err == nil {
				logger.Warn("[%s] the freshest notarized block has newer epoch (%s); "+
					"update epoch from %d to %d", m.loggingId, nota.GetBlockSn(),
					oldEpoch, m.epochManager.GetEpoch())
				m.onEpochUpdated()
			}
		}
	}
	if err := m.syncer.SetFreshestNotarizedChainBlockSn(e.Sn); err != nil {
		logger.Warn("[%s] %s", m.loggingId, err)
	}
	m.node.AddFreshestNotarizedChainExtendedEvent(*e)
	m.notifyEvent(FreshestNotarizedChainExtendedEvent{e.Sn})

	// Remove unnecessary proposals in recentProposals.
	cleanUpOldData(m.recentProposals, e.Sn)

	if m.role.IsBootnode("") {
		// Notify connected nodes, so proposer/voter candidates can catch up the updates.
		s := m.getStatus()
		nm := network.NewMessage(uint8(TypeRespondStatus), 0, MarshalStatus(s))
		if err := m.host.Broadcast(nm); err != nil {
			logger.Warn("[%s] %s", m.loggingId, err)
		}
	}
}

// Called in the handleEventLoop goroutine.
func (m *Mediator) onReceivedFinalizedChainExtendedEvent(
	e *blockchain.FinalizedChainExtendedEvent) {
	logger.Debug("[%s] onReceivedFinalizedChainExtendedEvent %s", m.loggingId, e.Sn)
	m.node.AddFinalizedChainExtendedEvent(*e)
	// TODO(thunder): add test and handle the case that the node crashes during the reconfiguration.
	if m.reconfFinalizedByBlockSn.IsNil() && !e.ReconfFinalizedByBlockSn.IsNil() {
		// TODO(thunder): ensure the test coverage and implementation is enough.
		logger.Info("[%s] handleEventLoop: reconfiguration happens at %s", m.loggingId, e.Sn)

		m.reconfFinalizedByBlockSn = e.ReconfFinalizedByBlockSn
		if err := m.syncer.SetReconfFinalizedByBlockSn(m.reconfFinalizedByBlockSn); err != nil {
			logger.Warn("[%s] %s", m.loggingId, err)
		}
		if m.role.IsBootnode("") {
			// Notify connected nodes, so proposer/voter candidates can catch up the updates.
			s := m.getStatus()
			nm := network.NewMessage(uint8(TypeRespondStatus), 0, MarshalStatus(s))
			if err := m.host.Broadcast(nm); err != nil {
				logger.Warn("[%s] %s", m.loggingId, err)
			}
		}

		if err := m.reconfigurer.UpdateVerifier(m.chain, m.verifier); err != nil {
			logger.Error("failed to update Verifier during reconfiguration %s: %s", e.Sn, err)
		}
		if err := m.reconfigurer.UpdateRoleAssigner(m.chain, m.role); err != nil {
			logger.Error("failed to update RoleAssigner during reconfiguration %s: %s", e.Sn, err)
		}
		if err := m.reconfigurer.UpdateHost(m.chain, m.host); err != nil {
			logger.Error("failed to update Host during reconfiguration %s: %s", e.Sn, err)
		}
		oldEpoch := m.epochManager.GetEpoch()
		if err := m.reconfigurer.UpdateEpochManager(m.chain, m.epochManager); err != nil {
			logger.Error("failed to update EpochManager during reconfiguration %s: %s", e.Sn, err)
		} else {
			newEpoch := m.epochManager.GetEpoch()
			if err := m.node.SetEpoch(newEpoch); err != nil {
				logger.Warn("[%s] %s", m.loggingId, err)
			}
			if err := m.syncer.SetEpoch(newEpoch); err != nil {
				logger.Warn("[%s] %s", m.loggingId, err)
			}
		}

		// NOTE: the management of the worker goroutine of blockchain:
		// * BlockChain stops the worker automatically before sending the finalized event.
		// * Mediator is responsible to start the worker when needed.
		if m.chain.IsCreatingBlock() {
			// Expect this won't happen.
			logger.Error("[%s] the chain is creating block after the reconfiguration happened", m.loggingId)
			if err := m.chain.StopCreatingNewBlocks(); err != nil {
				logger.Warn("[%s] %s", m.loggingId, err)
			}
		}
		m.blockChan = nil
		newEpoch := m.epochManager.GetEpoch()
		logger.Info("[%s] update epoch from %d to %d due to the reconfiguration",
			m.loggingId, oldEpoch, newEpoch)
		if m.role.IsPrimaryProposer("", newEpoch) {
			m.performReconciliationWithAll()
		}
	} else if !m.reconfFinalizedByBlockSn.IsNil() && e.Sn.S == 1 {
		// After the reconfiguration, the new proposers and voters have liveness now.
		// It's safe to reset the state and drop connections to old proposers and voters.
		m.reconfFinalizedByBlockSn = blockchain.BlockSn{}
		if err := m.syncer.SetReconfFinalizedByBlockSn(m.reconfFinalizedByBlockSn); err != nil {
			logger.Warn("[%s] %s", m.loggingId, err)
		}

		for id, handle := range m.connections {
			if !(m.role.IsProposer(id, e.Sn.Epoch) || m.role.IsVoter(id, e.Sn.Epoch) ||
				m.role.IsBootnode(id)) {
				logger.Info("[%s] drop a connection to an old proposer/voter (id=%s)",
					m.loggingId, id)
				m.closeConnection(handle)
			}
		}
	}
	m.notifyEvent(FinalizedChainExtendedEvent{e.Sn})
}

// Called in the handleEventLoop goroutine.
func (m *Mediator) handleSelfMessage(msg interface{}) {
	switch v := msg.(type) {
	case broadcastEvent:
		m.broadcast(v.msg)
	case replyEvent:
		nm := network.NewMessage(uint8(v.msg.GetType()), 0, v.msg.GetBody())
		if err := v.source.Reply(nm); err != nil {
			logger.Warn("[%s] cannot reply %s (sn=%s) to %s",
				m.loggingId, v.msg.GetType(), v.msg.GetBlockSn(), v.source.GetSourceDebugInfo())
		}
	case catchUpStatusEvent:
		id := m.getConsensusId(v.source.GetConnectionHandle())
		status := m.getStatus()
		if status.Epoch < v.sn.Epoch {
			logger.Warn("[%s] invalid request: current epoch=%d, request to catch up %s",
				m.loggingId, status.Epoch, v.sn)
			break
		}
		if status.FncBlockSn.Compare(v.sn) >= 0 {
			break
		}
		status.FncBlockSn = v.sn
		logger.Info("[%s] catch up %s:%s", m.loggingId, id, status)
		m.catchUpStatus(id, status, CatchUpPolicyIfNotInProgress)
	case chan DebugState:
		v <- m.getDebugState()
	case blockchain.ClockMsgNota:
		oldEpoch := m.epochManager.GetEpoch()
		if oldEpoch > v.GetEpoch() {
			break
		}
		if err := m.epochManager.UpdateByClockMsgNota(v); err != nil {
			logger.Error("[%s] cannot update epoch to %d with a verified clock message notarization",
				m.loggingId, v.GetEpoch())
			break
		}
		newEpoch := m.epochManager.GetEpoch()
		if newEpoch == oldEpoch {
			break
		}
		logger.Info("[%s] update epoch from %d to %d due to clock message notarization",
			m.loggingId, oldEpoch, newEpoch)
		m.onEpochUpdated()
	case reconciliationWithAllEvent:
		m.performReconciliationWithAll()
	case requestEpochProofEvent:
		if handle, ok := m.connections[v.id]; ok {
			nm := network.NewMessage(uint8(TypeRequestEpoch), 0, utils.Uint32ToBytes(uint32(v.epoch)))
			if err := m.host.Send(handle, nm); err != nil {
				logger.Warn("[%s] %s", err)
			}
		} else {
			logger.Error("[%s] cannot find connection handle for id=%s", m.loggingId, v.id)
		}
	case requestDataEvent:
		if handle, ok := m.connections[v.id]; ok {
			nm := network.NewMessage(v.typ, 0, v.sn.ToBytes())
			if err := m.host.Send(handle, nm); err != nil {
				logger.Warn("[%s] %s", err)
			}
		} else {
			// TODO(thunder): respond that the request is invalid and ask ChainSyncer to retry.
			logger.Error("[%s] cannot find connection handle for id=%s", m.loggingId, v.id)
		}
	case onCaughtUpEvent:
		m.syncedIds[v.id] = true
		epoch := m.epochManager.GetEpoch()
		// NOTE: If the bootnode doesn't have the last unnotarized proposals to finalize
		// the reconfiguration block, the new voters in the next generation won't get enough blocks
		// to finalize the reconfiguration block. Force the bootnode to fetch unnotarized proposals
		// to avoid such issue.
		// TODO(thunder): Can bootnode not request unnotarized proposals?
		if m.role.IsVoter("", v.s.FncBlockSn.Epoch) ||
			m.role.IsVoter("", epoch) ||
			m.role.IsBootnode("") {
			// TODO(thunder): avoid unnecessary requests.
			if err := m.requestUnnotarizedProposals(); err != nil {
				logger.Warn("[%s] %s", m.loggingId, err)
			}
		}
		if !m.role.IsPrimaryProposer("", epoch) || m.blockChan != nil {
			break
		}

		if m.reconciliationWithAllBeginTime.IsZero() {
			// We've scheduled a startCreatingBlocksEvent.
			break
		}

		// See if the primary proposer is ready to propose.
		fncSn := m.chain.GetFreshestNotarizedChain().GetBlockSn()
		if fncSn.Epoch >= epoch {
			logger.Error(
				"[%s] failed to create a new block: epoch %d is not greater "+
					"than the freshest notarized chain's epoch (%s)", m.loggingId, epoch, fncSn)
			break
		}

		count := 0
		for id := range m.syncedIds {
			if m.role.IsVoter(id, epoch) {
				count++
			}
		}
		nVoter := m.role.GetNumVoters(epoch)
		if nVoter <= 0 {
			logger.Error("[%s] there is no voter at %d", m.loggingId, epoch)
			break
			// TODO(thunder): get the ratio from Verifier.
		} else if float64(count) < math.Ceil(float64(nVoter)*2.0/3.0) {
			logger.Info("[%s] postpone starting creating new blocks; caught up %d/%d voters",
				m.loggingId, count, nVoter)
			break
		}

		now := time.Now()
		targetTime := m.reconciliationWithAllBeginTime.Add(
			time.Duration(delayOfMakingFirstProposal) * time.Millisecond)
		m.reconciliationWithAllBeginTime = time.Time{}
		if now.After(targetTime) {
			m.selfChan <- startCreatingBlocksEvent{}
		} else {
			go func(ch chan interface{}) {
				// Delay the reconciliation a while, so the freshest notarized chain may extend.
				// Note that the proposal may fail without the delay. Here is an example:
				// 1. The last block of the freshest notarized chain is (1,12).
				// 2. Ask the chain to create new block, so (2,1) extends from (1,12).
				// 3. Before sending the proposal, the proposer at epoch=1 received enough votes to
				//    make notarization (1,13) and broadcast it.
				// 4. Broadcast the proposal (2,1)
				// Then the voters will reject to vote because the last block of their freshest notarized
				// chains is (1,13).
				//
				// The above scenario is found by testing TestVoterReconfiguration 50 times.
				// It's more easily to reproduce when running the test without printing lots of logs.
				//
				// Delay a while to collect more info potentially.
				<-time.NewTimer(targetTime.Sub(now)).C
				ch <- startCreatingBlocksEvent{}
			}(m.selfChan)
		}
	case startCreatingBlocksEvent:
		if m.blockChan != nil {
			break
		}
		epoch := m.epochManager.GetEpoch()
		var err error
		if m.blockChan, err = m.chain.StartCreatingNewBlocks(epoch); err != nil {
			logger.Error("[%s] cannot start creating new blocks; err=%s", m.loggingId, err)
		} else {
			fncSn := m.chain.GetFreshestNotarizedChain().GetBlockSn()
			logger.Info("[%s] start creating new blocks from %s (epoch=%d)",
				m.loggingId, fncSn, epoch)
		}
	default:
		logger.Warn("[%s] received unknown self message %v", m.loggingId, msg)
	}
}

// Called in the handleEventLoop goroutine.
func (m *Mediator) broadcast(msg blockchain.Message) {
	if msg.GetType() == blockchain.TypeProposal {
		p := msg.(blockchain.Proposal)
		item := &Item{p.GetBlockSn(), p}
		if m.recentProposals.Get(item) == nil {
			m.lastBroadcastedProposal = p.GetBlockSn()
			m.recentProposals.ReplaceOrInsert(item)
			m.unnotarizedProposals[p.GetBlockSn()] = p
			for _, n := range p.GetBlock().GetNotarizations() {
				delete(m.unnotarizedProposals, n.GetBlockSn())
			}
		}
	}

	nm := network.NewMessage(uint8(msg.GetType()), 0, msg.GetBody())
	sn := msg.GetBlockSn()
	if msg.GetType() == blockchain.TypeClockMsg {
		// When the voter creates a clock message, it means the primary proposer is offline.
		// Send the clock message to all proposers and hope one of the proposer is online.
		for id, handle := range m.connections {
			if m.handshakeStates[handle] == hsDone && m.role.IsProposer(id, sn.Epoch) {
				if err := m.host.Send(handle, nm); err != nil {
					logger.Warn("[%s] cannot send %s (sn=%s) to %s",
						m.loggingId, msg.GetType(), sn, id)
				}
			}
		}
		return
	}

	if err := m.host.Broadcast(nm); err != nil {
		logger.Warn("[%s] cannot broadcast %s (sn=%s)", m.loggingId, msg.GetType(), sn)
	}
}

// Called in the handleEventLoop goroutine.
func (m *Mediator) onEpochUpdated() {
	newEpoch := m.epochManager.GetEpoch()
	if m.blockChan != nil {
		logger.Info("[%s] stop creating new block because epoch is updated to %d",
			m.loggingId, newEpoch)
		if err := m.chain.StopCreatingNewBlocks(); err != nil {
			logger.Warn("[%s] %s", m.loggingId, err)
		}
		m.blockChan = nil
	}
	m.node.SetEpoch(newEpoch)
	if err := m.syncer.SetEpoch(newEpoch); err != nil {
		logger.Warn("[%s] %s", m.loggingId, err)
	}
	m.reconciliationWithAllBeginTime = time.Time{}
	if m.role.IsPrimaryProposer("", newEpoch) {
		m.performReconciliationWithAll()
	}
}

// Called in the handleEventLoop goroutine.
// This function is only called on the primary proposer.
func (m *Mediator) performReconciliationWithAll() {
	logger.Info("[%s] performReconciliationWithAll (epoch=%d)",
		m.loggingId, m.epochManager.GetEpoch())
	m.reconciliationWithAllBeginTime = time.Now()
	m.syncedIds = make(map[string]bool)
	nm := network.NewMessage(uint8(TypeRequestStatus), 0, nil)
	// The broadcast triggers a series of events:
	// 1. Receive TypeRespondStatus from each connected node
	//    and call catchUpStatus() to start syncing.
	// 2. After having caught up the target node,
	//    call OnCaughtUp() to see if we can start creating new blocks.
	// 3. Make proposals from the new blocks and broadcast them.
	if err := m.host.Broadcast(nm); err != nil {
		// Maybe no node is connected. It's okay. We'll get the status after
		// connections are established.
		logger.Warn("[%s] failed to broadcast the status request; err=%s", m.loggingId, err)
	}
}

// Called in the handleEventLoop goroutine.
func (m *Mediator) handleNetworkMessage(msg *network.Message) {
	handle := msg.GetConnectionHandle()
	if msg.GetAttribute()&network.AttrClosed > 0 {
		logger.Info("[%s] the other end close the connection (id=%s)",
			m.loggingId, m.getConsensusId(handle))
		m.closeConnection(handle)
		return
	}
	if m.handshakeStates[handle] != hsDone {
		m.handshake(msg)
		return
	}

	if IsSyncMessage(msg.GetType()) {
		m.handleMediatorMessage(msg)
		return
	}

	typ := blockchain.Type(msg.GetType())
	logger.Debug("[%s] handleEventLoop receives type=%s %s\n",
		m.loggingId, typ, msg.GetSourceDebugInfo())
	switch typ {
	case blockchain.TypeBlock:
		// We should only take notarized blocks or proposals; otherwise, attackers can
		// send lots of invalid blocks to cause a deny of service.
		logger.Warn("[%s] handleEventLoop receives a block directly. Skip it", m.loggingId)
	case blockchain.TypeProposal:
		if p, _, err := m.unmarshaller.UnmarshalProposal(msg.GetBlob()); err != nil {
			logger.Warn("[%s] handleEventLoop receives invalid proposal; err=%s", err)
		} else {
			// TODO(thunder): confirm that the bootnode will never miss the proposals to
			// finalize the reconfiguration block. Think about the race conditions between
			// performing reconciliation and sending/receiving the last proposals.
			// TODO(thunder): after ChainSyncer is more mature, check whether we should
			// add warning logs when receiving duplicated proposals.
			m.recentProposals.ReplaceOrInsert(&Item{p.GetBlockSn(), p})
			m.node.AddProposal(p, msg, BlockCreatedByOther)
		}
	case blockchain.TypeVote:
		if v, _, err := m.unmarshaller.UnmarshalVote(msg.GetBlob()); err != nil {
			logger.Warn("[%s] handleEventLoop receives invalid vote; err=%s", err)
		} else {
			m.node.AddVote(v)
		}
	case blockchain.TypeNotarization:
		if n, _, err := m.unmarshaller.UnmarshalNotarization(msg.GetBlob()); err != nil {
			logger.Warn("[%s] handleEventLoop receives invalid notarization; err=%s", err)
		} else {
			m.node.AddNotarization(n)
		}
	case blockchain.TypeClockMsg:
		if c, _, err := m.unmarshaller.UnmarshalClockMsg(msg.GetBlob()); err != nil {
			logger.Warn("[%s] handleEventLoop receives invalid clock message; err=%s", err)
		} else {
			m.node.AddClockMsg(c)
		}
	case blockchain.TypeClockMsgNota:
		if cNota, _, err := m.unmarshaller.UnmarshalClockMsgNota(msg.GetBlob()); err != nil {
			logger.Warn("[%s] handleEventLoop receives invalid clock message notarization; err=%s", err)
		} else {
			m.node.AddClockMsgNota(cNota)
		}
	}
}

// Called in the handleEventLoop goroutine.
func (m *Mediator) handleMediatorMessage(msg *network.Message) {
	typ := Type(msg.GetType())
	logger.Debug("[%s] handleEventLoop receives type=%s %s",
		m.loggingId, typ, msg.GetSourceDebugInfo())
	switch typ {
	case TypeRequestStatus:
		if err := m.respondStatus(msg); err != nil {
			logger.Warn("[%s] %s", m.loggingId, err)
		}
	case TypeRespondStatus:
		if status, err := UnmarshalStatus(msg.GetBlob()); err != nil {
			logger.Error("[%s] receive invalid status; err=%s", m.loggingId, err)
		} else {
			// See if we need to catch up.
			id := m.getConsensusId(msg.GetConnectionHandle())
			// TODO(thunder): clean up unnecessary log related to the bootnode.
			logger.Info("[%s] receive status %s:%s (current=%s)",
				m.loggingId, id, status, m.getStatus())
			m.catchUpStatus(id, status, CatchUpPolicyMust)
		}
	case TypeRequestNotarizedBlock:
		m.respondNotarizedBlockRequest(msg)
	case TypeRespondNotarizedBlock:
		nota, bytes, err := m.unmarshaller.UnmarshalNotarization(msg.GetBlob())
		if err != nil {
			logger.Warn("[%s] receive invalid notarization in notarized block; err=%s",
				m.loggingId, err)
			break
		}
		block, _, err := m.unmarshaller.UnmarshalBlock(bytes)
		if err != nil {
			logger.Warn("[%s] receive invalid block in notarized block; err=%s",
				m.loggingId, err)
			break
		}
		m.node.AddNotarizedBlock(nota, block)
	case TypeRespondNotarizedBlockNotExisted:
		if sn, _, err := blockchain.NewBlockSnFromBytes(msg.GetBlob()); err != nil {
			logger.Warn("[%s] receive invalid response for notarized block not existed; err=%s",
				m.loggingId, err)
		} else if err := m.syncer.SetBlockNotExisted(sn); err != nil {
			logger.Warn("[%s] %s", m.loggingId, err)
		}
	case TypeRequestProposal:
		m.respondProposalRequest(msg)
	case TypeRespondProposal:
		if p, _, err := m.unmarshaller.UnmarshalProposal(msg.GetBlob()); err != nil {
			logger.Warn("[%s] receive invalid proposal; err=%s", m.loggingId, err)
		} else {
			if err := m.syncer.SetReceivedProposalBlockSn(p.GetBlockSn()); err != nil {
				logger.Warn("[%s] %s", m.loggingId, err)
			}
			m.node.AddProposal(p, msg, BlockCreatedByOther)
		}
	case TypeRespondProposalNotExisted:
		// TODO(thunder)
	case TypeRequestUnnotarizedProposals:
		m.respondUnnotarizedProposals(msg)
	case TypeRespondUnnotarizedProposals:
		n, bytes, err := utils.BytesToUint16(msg.GetBlob())
		if err != nil {
			logger.Warn("[%s] receive invalid unnotarized proposals; err=%s", m.loggingId, err)
			break
		}
		// TODO(thunder): if n is 0, should voters try the second time to avoid missing proposals
		// due to the race conditions?
		logger.Info("[%s] receive unnotarized proposals %d", m.loggingId, n)
		var p blockchain.Proposal
		for i := 0; i < int(n); i++ {
			var err error
			if p, bytes, err = m.unmarshaller.UnmarshalProposal(bytes); err != nil {
				logger.Warn("[%s] receive invalid unnotarized proposals; err=%s", m.loggingId, err)
				break
			} else {
				m.node.AddProposal(p, msg, BlockCreatedByOther)
			}
		}
	case TypeRequestEpoch:
		m.respondEpoch(msg)
	case TypeRespondEpochByClockMsgNota:
		if clockMsgNota, _, err := m.unmarshaller.UnmarshalClockMsgNota(msg.GetBlob()); err != nil {
			logger.Warn("[%s] invalid clock message nota; err=%s", m.loggingId, err)
		} else {
			m.node.AddClockMsgNota(clockMsgNota)
		}
	case TypeRespondEpochByNotarization:
		if nota, _, err := m.unmarshaller.UnmarshalNotarization(msg.GetBlob()); err != nil {
			logger.Warn("[%s] invalid notarization; err=%s", m.loggingId, err)
		} else {
			m.node.AddNotarization(nota)
		}
	case TypeRespondEpochNotExisted:
		// TODO(thunder)
	case TypeRespondEpochInvalid:
		// TODO(thunder)
	}
}

func (m *Mediator) NewNotificationChannel() <-chan interface{} {
	m.eventChansMutex.Lock()
	defer m.eventChansMutex.Unlock()
	ch := make(chan interface{}, 1024)
	m.eventChans = append(m.eventChans, ch)
	return ch
}

func (m *Mediator) RemoveNotificationChannel(target <-chan interface{}) {
	m.eventChansMutex.Lock()
	defer m.eventChansMutex.Unlock()
	for i, ch := range m.eventChans {
		if ch == target {
			m.eventChans = append(m.eventChans[:i], m.eventChans[i+1:]...)
			break
		}
	}
}

func (m *Mediator) GetHostForTest() *network.Host {
	return m.host
}

// Called in the handleEventLoop goroutine.
func (m *Mediator) requestStatus(source *network.Message) error {
	nm := network.NewMessage(uint8(TypeRequestStatus), 0, nil)
	return source.Reply(nm)
}

// Called in the handleEventLoop goroutine.
func (m *Mediator) respondStatus(source *network.Message) error {
	s := m.getStatus()
	nm := network.NewMessage(uint8(TypeRespondStatus), 0, MarshalStatus(s))
	return source.Reply(nm)
}

func (m *Mediator) GetDebugState() <-chan DebugState {
	ch := make(chan DebugState, 1)
	m.selfChan <- ch
	return ch
}

// Called in the handleEventLoop goroutine.
func (m *Mediator) getStatus() Status {
	fsh := m.chain.GetFreshestNotarizedChain()
	return Status{
		FncBlockSn: fsh.GetBlockSn(),
		Epoch:      m.epochManager.GetEpoch(),
		ReconfFinalizedByBlockSn: m.reconfFinalizedByBlockSn,
	}
}

// Called in the handleEventLoop goroutine.
func (m *Mediator) getDebugState() DebugState {
	var sb strings.Builder
	_, _ = sb.WriteString("[")
	first := true
	m.recentProposals.AscendGreaterOrEqual(m.recentProposals.Min(), func(i llrb.Item) bool {
		p := LLRBItemToProposal(i)
		b := p.GetBlock()
		if !first {
			_, _ = sb.WriteString(" ")
		}
		_, _ = sb.WriteString(fmt.Sprintf("%s<-%s", b.GetParentBlockSn(), b.GetBlockSn()))
		first = false
		return true
	})
	_, _ = sb.WriteString("]")

	ch := m.syncer.GetDebugState()
	ss := <-ch

	var syncedIds []string
	for id := range m.syncedIds {
		syncedIds = append(syncedIds, id)
	}
	sort.Strings(syncedIds)

	var connectedIds []string
	for id := range m.connections {
		connectedIds = append(connectedIds, id)
	}
	sort.Strings(connectedIds)

	return DebugState{
		Identity:             m.loggingId,
		Status:               m.getStatus(),
		SyncerState:          ss,
		SyncedIds:            syncedIds,
		ConnectedIds:         connectedIds,
		ProposalInfo:         sb.String(),
		IsMakingBlock:        m.blockChan != nil,
		LastBroadcastedBlock: m.lastBroadcastedProposal,
	}
}

func (m *Mediator) getConsensusId(handle network.ConnectionHandle) string {
	for id, h := range m.connections {
		if h == handle {
			return id
		}
	}
	return ""
}

// Called in handleEventLoop goroutine.
func (m *Mediator) catchUpStatus(id string, targetStatus Status, policy CatchUpPolicy) {
	if _, ok := m.connections[id]; !ok {
		// The connection to id is closed.
		logger.Info("[%s] ignore catching up %s because the connection is closed", m.loggingId, id)
		return
	}
	current := m.getStatus()
	if !current.isBehind(targetStatus) {
		logger.Info("[%s] already catching up %s (%s > %s)", m.loggingId, id, current, targetStatus)
		m.OnCaughtUp(id, targetStatus)
		return
	}
	delete(m.syncedIds, id)
	if err := m.syncer.SetFreshestNotarizedChainBlockSn(current.FncBlockSn); err != nil {
		logger.Warn("[%s] %s", m.loggingId, err)
	}
	if err := m.syncer.CatchUp(id, targetStatus, policy); err != nil {
		logger.Warn("[%s] %s", m.loggingId, err)
	}
}

// Called in the handleEventLoop goroutine.
func (m *Mediator) handshake(msg *network.Message) {
	handle := msg.GetConnectionHandle()
	if msg.GetAttribute()&network.AttrOpen > 0 {
		id := string(msg.GetBlob())
		logger.Info("[%s] new connection established; start handshake (id=%s)", m.loggingId, id)
		m.host.SetEnabledBroadcast(handle, false)
		if len(id) == 0 {
			// The server accepted a new connection.
			// Wait for the client to start the challenge-response process.
			m.handshakeStates[handle] = hsWaitChallengeResponse
		} else {
			// The client connected to the server. Send the challenge's response
			if m.sendChallengeResponse(id, msg) {
				m.handshakeStates[handle] = hsSentChallengeResponse
			}
		}
		return
	}

	hsState := m.handshakeStates[handle]
	switch hsState {
	case hsSentChallengeResponse, hsWaitChallengeResponse:
		id, signature, err := utils.BytesToString(msg.GetBlob())
		if err != nil {
			logger.Warn("[%s] handshake: invalid challenge's response; state=%s, err=%s",
				m.loggingId, hsStateToString(hsState), err)
			m.closeConnection(handle)
			return
		}
		key := m.host.GetTLSPublicKey(msg.GetConnectionHandle())
		if err := m.verifier.VerifySignature(id, signature, key); err != nil {
			if m.role.IsBootnode(m.role.GetBootnodeId()) {
				// A bootnode accepts any connection to help proposer/voter candidates catch up,
				// so ignore the error.
				// TODO(thunder): prioritize connections to avoid a DoS.
				logger.Info("[%s] handshake: bootnode ignores the failed verification for the "+
					"challenge's response (id=%s); state=%s, err=%s",
					m.loggingId, id, hsStateToString(hsState), err)
			} else {
				logger.Warn("[%s] handshake: failed to verify the challenge's response (id=%s); "+
					"state=%s, err=%s", m.loggingId, id, hsStateToString(hsState), err)
				m.closeConnection(handle)
				return
			}
		} else {
			logger.Info("[%s] handshake: successfully verify the challenge's response from id=%s (state=%s)",
				m.loggingId, id, hsStateToString(hsState))
		}

		if m.handshakeStates[handle] == hsWaitChallengeResponse {
			if !m.sendChallengeResponse(id, msg) {
				return
			}
		}
		m.handshakeStates[handle] = hsDone
		m.host.SetEnabledBroadcast(handle, true)
		if err := m.requestStatus(msg); err != nil {
			logger.Warn("[%s] handshake: failed to request status (id=%s); state=%s, err=%s",
				m.loggingId, id, hsStateToString(hsState), err)
			m.closeConnection(handle)
		}
	case hsDone:
		// Nothing
	}
}

// Called in the handleEventLoop goroutine.
func (m *Mediator) sendChallengeResponse(id string, msg *network.Message) bool {
	logger.Info("[%s] handshake: send challenge's response to %s", m.loggingId, id)
	handle := msg.GetConnectionHandle()
	// Use TLS's public key as the challenge to prevent man-in-the-middle attack.
	key := m.host.GetTLSPublicKey(handle)
	// A node can be a proposer and a voter at the same time. Use the proposer id in this case.
	// If the node has caught up and becomes an active consensus node, use the corresponding id.
	sn := m.chain.GetFreshestNotarizedChain().GetBlockSn()
	consensusId := m.role.GetProposerId(sn.Epoch)
	if consensusId == "" {
		consensusId = m.role.GetVoterId(sn.Epoch)
	}
	if consensusId == "" {
		consensusId = m.role.GetBootnodeId()
	}
	// If the node is not an active consensus node, try some id.
	if consensusId == "" {
		consensusId = m.role.GetDefaultProposerId()
	}
	if consensusId == "" {
		consensusId = m.role.GetDefaultVoterId()
	}

	if signature, err := m.verifier.Sign(consensusId, key); err != nil {
		logger.Warn("[%s] handshake: failed to create the challenge's response; err=%s", m.loggingId, err)
		m.closeConnection(handle)
		return false
	} else {
		bytes := append(utils.StringToBytes(consensusId), signature...)
		nm := network.NewMessage(uint8(TypeSentChallengeResponse), 0, bytes)
		if err := msg.Reply(nm); err != nil {
			logger.Warn("[%s] handshake: failed to reply the challenge's response; err=%s",
				m.loggingId, err)
		}
		m.connections[id] = handle
		return true
	}
}

func (m *Mediator) getIdByConnectionHandle(handle network.ConnectionHandle) string {
	for id, h := range m.connections {
		if h == handle {
			return id
		}
	}
	return ""
}

// Called in the handleEventLoop goroutine.
func (m *Mediator) closeConnection(handle network.ConnectionHandle) {
	m.host.CloseConnection(handle)
	delete(m.handshakeStates, handle)
	id := m.getIdByConnectionHandle(handle)
	logger.Info("[%s] close connection to %s", m.loggingId, id)
	if err := m.syncer.CancelCatchingUp(id); err != nil {
		logger.Warn("[%s] failed to cancel catching up with %s; err=%s", m.loggingId, id, err)
	}
	delete(m.syncedIds, id)
	delete(m.connections, id)
}

//--------------------------------------------------------------------

func MarshalStatus(s Status) []byte {
	var out [][]byte
	out = append(out, s.FncBlockSn.ToBytes())
	out = append(out, utils.Uint32ToBytes(uint32(s.Epoch)))
	out = append(out, s.ReconfFinalizedByBlockSn.ToBytes())
	return utils.ConcatCopyPreAllocate(out)
}

func UnmarshalStatus(bytes []byte) (Status, error) {
	s := Status{}
	var err error
	s.FncBlockSn, bytes, err = blockchain.NewBlockSnFromBytes(bytes)
	if err != nil {
		return Status{}, err
	}
	var tmp uint32
	if tmp, bytes, err = utils.BytesToUint32(bytes); err != nil {
		return Status{}, err
	} else {
		s.Epoch = blockchain.Epoch(tmp)
	}
	if s.ReconfFinalizedByBlockSn, _, err = blockchain.NewBlockSnFromBytes(bytes); err != nil {
		return Status{}, err
	}
	return s, nil
}

func (s Status) isBehind(other Status) bool {
	if s.Epoch < other.Epoch {
		return true
	}
	if s.FncBlockSn.Compare(other.FncBlockSn) < 0 {
		return true
	}
	return s.ReconfFinalizedByBlockSn.Compare(other.ReconfFinalizedByBlockSn) < 0
}

func (s Status) String() string {
	return fmt.Sprintf("[%d %s %s]",
		s.Epoch, s.FncBlockSn, s.ReconfFinalizedByBlockSn)
}
