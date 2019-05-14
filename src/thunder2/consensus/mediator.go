package consensus

import (
	"fmt"
	"thunder2/blockchain"
	"thunder2/utils"
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
	}
	s.Epoch = blockchain.Epoch(tmp)
	if s.ReconfFinalizedByBlockSn, bytes, err = blockchain.NewBlockSnFromBytes(bytes); err != nil {
		return Status{}, err
	}
	return s, nil
}

func (s Status) String() string {
	return fmt.Sprintf("[%d %s %s]",
		s.Epoch, s.FncBlockSn, s.ReconfFinalizedByBlockSn)
}
