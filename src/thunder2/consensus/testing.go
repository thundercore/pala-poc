// Put the fake implementations used by the production code for the integration test.
package consensus

import (
	"strings"
	"sync"
	"thunder2/blockchain"
	"thunder2/network"
	"thunder2/utils"
	"time"
)

const (
	// A large value never timeout.
	forever = 10 * 365 * 24 * time.Hour
)

// NodeClientFake implements NodeClient and relays Broadcast and Reply calls to MessageChan
type NodeClientFake struct {
	id          string
	MessageChan chan blockchain.Message
	CatchUpChan chan blockchain.BlockSn
}

type RoleAssignerFake struct {
	mutex         utils.CheckedLock
	myProposerIds []string
	myVoterIds    []string
	myBootnodeId  string
	proposerLists []*blockchain.ElectionResult
	voterLists    []*blockchain.ElectionResult
}

type ReconfigurerFake struct {
	loggingId       string
	myProposerIds   []string
	myVoterIds      []string
	proposerList    blockchain.ElectionResult
	voterList       blockchain.ElectionResult
	networkCallback NetworkCallback
}

type ReconfigurationConfigFake struct {
	LoggingId     string
	MyProposerIds []string
	MyVoterIds    []string
	ProposerList  blockchain.ElectionResult
	VoterList     blockchain.ElectionResult
}

type NetworkCallback func(bc blockchain.BlockChain, host *network.Host) error

type EpochManagerFake struct {
	mutex                    sync.Mutex
	epoch                    blockchain.Epoch
	clockMsgNota             blockchain.ClockMsgNota
	nota                     blockchain.Notarization
	updatedByReconfiguration bool
}

type TimerFake struct {
	mutex        utils.CheckedLock
	timer        *time.Timer
	duration     time.Duration
	ch           chan time.Time
	currentEpoch blockchain.Epoch
	targetEpoch  blockchain.Epoch
}

//--------------------------------------------------------------------

func NewNodeClientFake(id string) NodeClient {
	return &NodeClientFake{
		id:          id,
		MessageChan: make(chan blockchain.Message, 1024),
		CatchUpChan: make(chan blockchain.BlockSn, 1024),
	}
}

func (m *NodeClientFake) Broadcast(msg blockchain.Message) {
	logger.Debug("[%s] Broadcast: %T", m.id, msg)
	m.MessageChan <- msg
}

func (m *NodeClientFake) Reply(source *network.Message, msg blockchain.Message) {
	logger.Debug("[%s] Reply: %T (%s)", m.id, msg, source.GetSourceDebugInfo())
	m.MessageChan <- msg
}

func (m *NodeClientFake) CatchUp(source *network.Message, sn blockchain.BlockSn) {
	logger.Debug("[%s] CatchUp: %T %s", m.id, source, sn)
	m.CatchUpChan <- sn
}

func (m *NodeClientFake) UpdateEpoch(cNota blockchain.ClockMsgNota) {
	logger.Debug("[%s] UpdateEpoch: %T %d", m.id, cNota.GetEpoch())
	m.MessageChan <- cNota
}

//--------------------------------------------------------------------

func NewRoleAssignerFake(
	myProposerIds []string, myVoterIds []string, myBootnodeId string,
	proposers blockchain.ElectionResult, voters blockchain.ElectionResult) *RoleAssignerFake {
	r := &RoleAssignerFake{
		myProposerIds: myProposerIds,
		myVoterIds:    myVoterIds,
		myBootnodeId:  myBootnodeId,
	}
	r.AddElectionResult(proposers, voters)
	return r
}

func (r *RoleAssignerFake) IsProposer(id string, epoch blockchain.Epoch) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if len(id) == 0 {
		return r.getProposerId(epoch) != ""
	}
	for i := 0; i < len(r.proposerLists); i++ {
		if r.proposerLists[i].Contain(id, epoch) {
			return true
		}
	}
	return false
}

func (r *RoleAssignerFake) IsPrimaryProposer(id string, epoch blockchain.Epoch) bool {
	if !r.IsProposer(id, epoch) {
		return false
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()

	if id == "" {
		id = r.getProposerId(epoch)
	}
	primary := r.getPrimaryProposerId(epoch)
	return primary != "" && id == primary
}

// A simple round-robin schedule.
// Let n be the number of proposers and index = "(epoch-1) % n".
// proposers[index] is the primary proposer.
func (r *RoleAssignerFake) getPrimaryProposerId(epoch blockchain.Epoch) string {
	r.mutex.CheckIsLocked("")

	if epoch == 0 {
		return ""
	}

	for i := 0; i < len(r.proposerLists); i++ {
		if r.proposerLists[i].Contain("", epoch) {
			proposerIds := r.proposerLists[i].GetConsensusIds()
			return proposerIds[(int(epoch)-1)%len(proposerIds)]
		}
	}
	return ""
}

func (r *RoleAssignerFake) IsVoter(id string, epoch blockchain.Epoch) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if len(id) == 0 {
		return r.getVoterId(epoch) != ""
	}
	for i := 0; i < len(r.voterLists); i++ {
		if r.voterLists[i].Contain(id, epoch) {
			return true
		}
	}
	return false
}

func (r *RoleAssignerFake) IsBootnode(id string) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if id == "" {
		id = r.myBootnodeId
	}
	return id != "" && id == r.myBootnodeId
}

func (r *RoleAssignerFake) GetProposerId(epoch blockchain.Epoch) string {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	return r.getProposerId(epoch)
}

func (r *RoleAssignerFake) getProposerId(epoch blockchain.Epoch) string {
	r.mutex.CheckIsLocked("")

	for _, id := range r.myProposerIds {
		for i := 0; i < len(r.proposerLists); i++ {
			if r.proposerLists[i].Contain(id, epoch) {
				return id
			}
		}
	}
	return ""
}

func (r *RoleAssignerFake) GetVoterId(epoch blockchain.Epoch) string {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	return r.getVoterId(epoch)
}

func (r *RoleAssignerFake) getVoterId(epoch blockchain.Epoch) string {
	r.mutex.CheckIsLocked("")

	for _, id := range r.myVoterIds {
		for i := 0; i < len(r.voterLists); i++ {
			if r.voterLists[i].Contain(id, epoch) {
				return id
			}
		}
	}
	return ""
}

func (r *RoleAssignerFake) GetBootnodeId() string {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	return r.myBootnodeId
}

func (r *RoleAssignerFake) GetDefaultProposerId() string {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if len(r.myProposerIds) > 0 {
		return r.myProposerIds[0]
	}
	return ""
}

func (r *RoleAssignerFake) GetDefaultVoterId() string {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if len(r.myVoterIds) > 0 {
		return r.myVoterIds[0]
	}
	return ""
}

func (r *RoleAssignerFake) GetNumVoters(epoch blockchain.Epoch) int {
	for i := 0; i < len(r.voterLists); i++ {
		if r.voterLists[i].Contain("", epoch) {
			return len(r.voterLists[i].GetConsensusIds())
		}
	}
	return -1
}

func (r *RoleAssignerFake) AddElectionResult(
	proposerList blockchain.ElectionResult, voterList blockchain.ElectionResult) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.proposerLists = append(r.proposerLists, &proposerList)
	r.voterLists = append(r.voterLists, &voterList)
}

func (r *RoleAssignerFake) String() string {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	var b strings.Builder
	_, _ = b.WriteString("{ (")
	_, _ = b.WriteString(strings.Join(r.myProposerIds, ","))
	_, _ = b.WriteString("),(")
	_, _ = b.WriteString(strings.Join(r.myVoterIds, ","))
	_, _ = b.WriteString("),(")
	_, _ = b.WriteString(r.myBootnodeId)
	_, _ = b.WriteString("); proposerLists:")
	for i, ps := range r.proposerLists {
		if i > 0 {
			_, _ = b.WriteString(",")
		}
		_, _ = b.WriteString(ps.String())
	}
	_, _ = b.WriteString("; voterLists")
	for i, vs := range r.voterLists {
		if i > 0 {
			_, _ = b.WriteString(",")
		}
		_, _ = b.WriteString(vs.String())
	}
	_, _ = b.WriteString(") }")

	return b.String()
}

//--------------------------------------------------------------------

func NewReconfigurerFake(config ReconfigurationConfigFake) Reconfigurer {
	return &ReconfigurerFake{
		loggingId:     config.LoggingId,
		myProposerIds: config.MyProposerIds,
		myVoterIds:    config.MyVoterIds,
		proposerList:  config.ProposerList,
		voterList:     config.VoterList,
		networkCallback: func(bc blockchain.BlockChain, host *network.Host) error {
			return nil
		},
	}
}

func (r *ReconfigurerFake) UpdateVerifier(
	bc blockchain.BlockChain, verifier blockchain.Verifier) error {
	vf := verifier.(*blockchain.VerifierFake)
	vf.AddElectionResult(r.proposerList, r.voterList)
	return nil
}

func (r *ReconfigurerFake) UpdateRoleAssigner(
	bc blockchain.BlockChain, role RoleAssigner) error {
	role.(*RoleAssignerFake).AddElectionResult(r.proposerList, r.voterList)
	return nil
}

func (r *ReconfigurerFake) UpdateHost(
	bc blockchain.BlockChain, host *network.Host) error {
	return r.networkCallback(bc, host)
}

func (r *ReconfigurerFake) UpdateEpochManager(bc blockchain.BlockChain, em EpochManager) error {
	b := bc.GetFreshestNotarizedChain()
	return em.(*EpochManagerFake).SetEpochDueToReconfiguration(
		b.GetBlockSn().Epoch + blockchain.Epoch(1))
}

func (r *ReconfigurerFake) SetNetworkReconfiguration(callback NetworkCallback) {
	r.networkCallback = callback
}

//--------------------------------------------------------------------

func NewEpochManagerFake() EpochManager {
	return &EpochManagerFake{epoch: 1}
}

func (e *EpochManagerFake) GetEpoch() blockchain.Epoch {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	return e.epoch
}

func (e *EpochManagerFake) UpdateByClockMsgNota(cn blockchain.ClockMsgNota) error {
	epoch := cn.GetEpoch()
	if epoch > e.epoch {
		e.epoch = epoch
		e.clockMsgNota = cn
		e.updatedByReconfiguration = false
	}
	return nil
}

func (e *EpochManagerFake) UpdateByNotarization(nota blockchain.Notarization) error {
	epoch := nota.GetBlockSn().Epoch
	if epoch > e.epoch {
		e.epoch = epoch
		e.nota = nota
		e.updatedByReconfiguration = false
	}
	return nil
}

func (e *EpochManagerFake) GetClockMsgNota(epoch blockchain.Epoch) blockchain.ClockMsgNota {
	if e.clockMsgNota != nil && e.clockMsgNota.GetEpoch() == epoch {
		return e.clockMsgNota
	}
	return nil
}

func (e *EpochManagerFake) GetNotarization(epoch blockchain.Epoch) blockchain.Notarization {
	if e.nota != nil && e.nota.GetBlockSn().Epoch == epoch {
		return e.nota
	}
	return nil
}

func (e *EpochManagerFake) SetEpochDueToReconfiguration(epoch blockchain.Epoch) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	e.epoch = epoch
	e.updatedByReconfiguration = true
	return nil
}

func (e *EpochManagerFake) SetEpoch(epoch blockchain.Epoch) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	e.epoch = epoch
	return nil
}

//--------------------------------------------------------------------

func NewTimerFake(epoch blockchain.Epoch) Timer {
	t := &TimerFake{
		currentEpoch: epoch,
		ch:           make(chan time.Time, 1),
		timer:        time.NewTimer(forever),
	}
	go func() {
		for {
			now := <-t.timer.C
			t.ch <- now
		}
	}()
	t.Reset(epoch)
	return t
}

func (t *TimerFake) GetChannel() <-chan time.Time {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	return t.ch
}

func (t *TimerFake) Reset(epoch blockchain.Epoch) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.reset(epoch)
}

func (t *TimerFake) reset(epoch blockchain.Epoch) {
	t.mutex.CheckIsLocked("")

	t.currentEpoch = epoch
	t.timer.Stop()
	if t.currentEpoch >= t.targetEpoch {
		t.timer.Reset(forever)
	} else {
		t.timer.Reset(t.duration)
	}
}

func (t *TimerFake) AllowAdvancingEpochTo(epoch blockchain.Epoch, d time.Duration) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.targetEpoch = epoch
	t.duration = d

	t.reset(t.currentEpoch)
}
