package consensus

import (
	"sync"
	"thunder2/blockchain"
	"thunder2/network"
	"thunder2/utils"
)

// NetworkSimulator can simulate delaying messages or breaking connections.
type NetworkSimulator struct {
	haveCalledConnect bool
	rules             []*NetworkSimulatorRule
	baseDelay         network.Delay
	stopChan          chan interface{}
	wg                sync.WaitGroup
}

// First matched.
type NetworkSimulatorRule struct {
	// nil means "match all"
	From []string
	// nil means "match all"
	To []string
	// TypeNil means "match all"
	Type blockchain.Type
	// BlockSn{0,0} means "match all"
	Sn     blockchain.BlockSn
	Action *network.FilterAction
}

//--------------------------------------------------------------------

func NewNetworkSimulator() *NetworkSimulator {
	return &NetworkSimulator{
		stopChan: make(chan interface{}),
	}
}

func (n *NetworkSimulator) SetBaseDelay(delay network.Delay) error {
	n.baseDelay = delay
	return nil
}

// TODO(thunder): return error if the connection is already established.
func (n *NetworkSimulator) Connect(fromHost *network.Host, toHost *network.Host) error {
	return n.ConnectWithDelay(fromHost, toHost, network.Delay{})

}

// TODO(thunder): return error if the connection is already established.
func (n *NetworkSimulator) ConnectWithDelay(
	fromHost *network.Host, toHost *network.Host, delay network.Delay) error {
	n.haveCalledConnect = true
	delay = n.baseDelay.Add(delay)
	network.FakeConnectWithFilter(fromHost, toHost, &n.wg, n.stopChan, delay,
		func(from string, to string, typ uint8, blob []byte) *network.FilterAction {
			if IsSyncMessage(typ) {
				return nil
			}

			unmarshaller := &blockchain.DataUnmarshallerFake{}
			// Apply the first matched rule.
			for _, r := range n.rules {
				if len(r.From) > 0 && !contains(r.From, from) {
					continue
				}
				if len(r.To) > 0 && !contains(r.To, to) {
					continue
				}
				typ := blockchain.Type(typ)
				if r.Type != blockchain.TypeNil && r.Type != typ {
					continue
				}
				switch typ {
				case blockchain.TypeBlock:
					value, _, err := unmarshaller.UnmarshalBlock(blob)
					if err != nil {
						utils.Bug("unmarshal err=%s", err)
					}
					if !r.Sn.IsNil() && r.Sn != value.GetBlockSn() {
						continue
					}
				case blockchain.TypeProposal:
					value, _, err := unmarshaller.UnmarshalProposal(blob)
					if err != nil {
						utils.Bug("unmarshal err=%s", err)
					}
					if !r.Sn.IsNil() && r.Sn != value.GetBlockSn() {
						continue
					}
				case blockchain.TypeVote:
					value, _, err := unmarshaller.UnmarshalVote(blob)
					if err != nil {
						utils.Bug("unmarshal err=%s", err)
					}
					if !r.Sn.IsNil() && r.Sn != value.GetBlockSn() {
						continue
					}
				case blockchain.TypeNotarization:
					value, _, err := unmarshaller.UnmarshalNotarization(blob)
					if err != nil {
						utils.Bug("unmarshal err=%s", err)
					}
					if !r.Sn.IsNil() && r.Sn != value.GetBlockSn() {
						continue
					}
				case blockchain.TypeClockMsg:
					value, _, err := unmarshaller.UnmarshalClockMsg(blob)
					if err != nil {
						utils.Bug("unmarshal err=%s", err)
					}
					if !r.Sn.IsNil() && r.Sn != value.GetBlockSn() {
						continue
					}
				case blockchain.TypeClockMsgNota:
					value, _, err := unmarshaller.UnmarshalClockMsgNota(blob)
					if err != nil {
						utils.Bug("unmarshal err=%s", err)
					}
					if !r.Sn.IsNil() && r.Sn != value.GetBlockSn() {
						continue
					}
				default:
					utils.Bug("unexpected type %s", typ)
				}

				return r.Action
			}
			return nil
		})
	return nil
}

func contains(set []string, target string) bool {
	if len(set) == 0 {
		return false
	}
	for _, s := range set {
		if s == target {
			return true
		}
	}
	return false
}

// AddRule must be called before Connect.
func (n *NetworkSimulator) AddRule(rule NetworkSimulatorRule) {
	if n.haveCalledConnect {
		utils.Bug("must call AddRule() before any call of Connect()")
	}
	n.rules = append(n.rules, &rule)
}

func (n *NetworkSimulator) Stop() {
	close(n.stopChan)
	n.wg.Wait()
}
