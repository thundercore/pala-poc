// Put the fake implementations used by the production code for the integration test.
package consensus

import (
	"strings"
	"thunder2/blockchain"
	"thunder2/network"
	"thunder2/utils"
)

// NodeClientFake implements NodeClient and relays Broadcast and Reply calls to MessageChan
type NodeClientFake struct {
	id          string
	MessageChan chan blockchain.Message
}

type RoleAssignerFake struct {
	mutex         utils.CheckedLock
	myProposerIds []string
	myVoterIds    []string
	myBootnodeId  string
	proposerLists []*blockchain.ElectionResult
	voterLists    []*blockchain.ElectionResult
}

type NetworkCallback func(bc blockchain.BlockChain, host *network.Host) error

//--------------------------------------------------------------------

func NewNodeClientFake(id string) NodeClient {
	return &NodeClientFake{
		id:          id,
		MessageChan: make(chan blockchain.Message, 1024),
	}
}

func (m *NodeClientFake) Broadcast(msg blockchain.Message) {
	logger.Debug("[%s] Broadcast: %T", m.id, msg)
	m.MessageChan <- msg
}

func (m *NodeClientFake) Reply(source *network.Message, msg blockchain.Message) {
	logger.Debug("[%s] Reply: %T", m.id, msg)
	m.MessageChan <- msg
}

func (m *NodeClientFake) CatchUp(source *network.Message, sn blockchain.BlockSn) {
	logger.Debug("[%s] CatchUp: %T %s", m.id, source, sn)
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
	for i := 0; i < len(r.proposerLists); i++ {
		if r.proposerLists[i].Contain(id, epoch) {
			// Let the first one always be the primary proposer for now.
			return id == r.proposerLists[i].GetConsensusIds()[0]
		}
	}
	return false
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
