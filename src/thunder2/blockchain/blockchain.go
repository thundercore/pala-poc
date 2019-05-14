package blockchain

import (
	"encoding/binary"
	"fmt"
	"thunder2/lgr"
	"thunder2/utils"

	"github.com/petar/GoLLRB/llrb"
	"github.com/pkg/errors"
)

var logger = lgr.NewLgr("/blockchain")

type Config struct {
}

// Estimate a rough lower bound: (2**32-1) / (86400*365) == 136.19 (years)
// uint32 is large enough.
// TODO(thunder): add Session and refine the reconfiguration.
type BlockSn struct {
	Epoch Epoch
	S     uint32
}

type Epoch uint32

type BlockSnGetter interface {
	GetBlockSn() BlockSn
}

type Hash []byte

// Requirement:
// * All operations are goroutine-safe.
// * Follow the rule to store notarizations on chain. For block (e,s):
//   * s=1       : contain the notarizations of the previous k blocks.
//   * s in [2,k]: contain no notarization.
//   * s>k       : contain the notarization of (e,s-k).
// * The implementation must receive an argument K (outstanding unnotarized proposals)
//   which affect GetFinalizedChain() and StartCreatingNewBlocks().
// * InsertBlock()/AddNotarization() must guarantee the blocks/notarizations are added in order.
//   This constraint simplifies the implementation of managing freshest notarized chain.
type BlockChain interface {
	// ContainsBlock returns true if the block(s) exists.
	// Expect this is more efficient compared to using GetBlock() == nil.
	ContainsBlock(s BlockSn) bool
	// GetBlock returns nil if there is no such value.
	GetBlock(s BlockSn) Block
	GetGenesisBlock() Block
	// GetNotarization returns nil if there is no such value.
	GetNotarization(s BlockSn) Notarization
	// GetFreshestNotarizedChain() returns the last block of the freshest notarized chain
	// decided by the all received notarizations.
	GetFreshestNotarizedChain() Block
	// GetFinalizedChain() returns the last block of the finalized chain.
	// Unlike the freshest notarized chain, we use notarizations in blocks to decide the finalized
	// chain. In other words, notarizations received from AddNotarization() are not used.
	// This constraint makes the finality stronger in practice.
	// When the freshest notarized chain contains 2K consecutive normal blocks,
	// the finalize chain is the one without the last 2k blocks. Note that it is possible
	// that the finalized chain doesn't grow while the freshest notarized chain keeps growing.
	// NOTE: To simplify the implementation, the notarizations in a timeout block doesn't finalize
	// any new block in current design. We can remove this constraint later.
	GetFinalizedChain() Block

	// InsertBlock returns error if:
	// * |b| already exists.
	// * |b|'s parent doesn't exist in the chain.
	// * notarizations in |b| doesn't follow the rules. See rules above BlockChain.
	// * Any other invalid block format based on the implementation definition.
	// The error may support IsTemporary().
	// If the error is temporary, the caller should catch up previous blocks and then insert
	// the block again. During the call, the finalized chain may be updated.
	InsertBlock(b Block) error

	// StartCreatingNewBlocks returns a channel with buffer size = |K|.
	// The first new block's BlockSn is {|epoch|,1} and its parent is the freshest notarized block.
	// BlockAndEvent.Event is set if the new block extends the finalized chain.
	// This fits the stability-favoring approach in PaLa.
	StartCreatingNewBlocks(epoch Epoch) (chan BlockAndEvent, error)
	// StopCreatingNewBlocks stops the creation. However, there may be some blocks in the channel returned
	// by StartCreatingNewBlocks(). The call returns after the worker goroutine ends.
	// It's safe to call this without calling StartCreatingNewBlocks().
	StopCreatingNewBlocks() error
	// IsCreatingBlock() returns true if the worker goroutine keeps trying to create new blocks.
	IsCreatingBlock() bool
	// AddNotarization may update the freshest notarized chain and/or continue creating a new block
	// if StartCreatingNewBlocks() is called before. Return error if:
	// * the corresponding block does not exist.
	// * the parent block's notarization does not exist.
	// Note that the notarization can be stored anywhere (even in memory) and may be lost
	// after we recreate the BlockChain object.
	AddNotarization(n Notarization) error

	// NewNotificationChannel creates a new channel used to notify events such as
	// FreshestNotarizedChainExtendedEvent and FinalizedChainExtendedEvent.
	NewNotificationChannel() <-chan interface{}
	// RemoveNotificationChannel removes the returned channel from NewNotificationChannel.
	RemoveNotificationChannel(target <-chan interface{})

	// TODO(thunder): we may need to add methods to get proposer/voter info list.
	// A proposer info contains the public proposing key and network IP and port.
	// A voter info contains the public voting key. We'll need a way to get proposers'
	// network addresses.
}

type BlockAndEvent struct {
	Block Block
	Event *FinalizedChainExtendedEvent
}

type Type uint8

const (
	TypeNil          = Type(0)
	TypeBlock        = Type(1)
	TypeProposal     = Type(2)
	TypeVote         = Type(3)
	TypeNotarization = Type(4)
	TypeClockMsg     = Type(5)
	TypeClockMsgNota = Type(6)
)

// Message is a marshal/unmarshal helper.
type Message interface {
	GetType() Type
	GetBody() []byte
	GetBlockSn() BlockSn
	GetDebugString() string
}

type ByBlockSn []Message

// NOTE: the function ImplementsX() is used to ensure no interface includes another
// interface's methods. For example, assume A and B are interfaces and A includes
// all methods of B. The object implemented A can be converted to B. Adding ImplementsX()
// to ensure these data-type interfaces are exclusive. Otherwise, we may get unexpected
// result after doing a type cast. ImplementsX() does nothing and shouldn't be called.
type Block interface {
	Message

	ImplementsBlock()

	GetParentBlockSn() BlockSn
	GetHash() Hash
	// GetNumber() returns the number (height) of this block.
	GetNumber() uint32
	GetNotarizations() []Notarization
	Compare(other Block) int

	// GetBodyString() returns a string to represent the block.
	// This is used for logging/testing/debugging.
	GetBodyString() string
}

type Proposal interface {
	Message

	ImplementsProposal()

	GetBlock() Block
	GetProposerId() string
}

type Vote interface {
	Message

	ImplementsVote()

	GetVoterId() string
}

type Notarization interface {
	Message

	ImplementsNotarization()

	GetNVote() uint16
	GetBlockHash() Hash
}

type ClockMsg interface {
	Message

	ImplementsClockMsg()

	GetEpoch() Epoch
	GetVoterId() string
}

type ClockMsgNota interface {
	Message

	ImplementsClockMsgNota()

	GetEpoch() Epoch
	GetNVote() uint16
}

type DataUnmarshaller interface {
	// UnmarshalBlock receives Block.GetBody() and returns Block and the rest of the bytes.
	UnmarshalBlock([]byte) (Block, []byte, error)
	// UnmarshalProposal receives Proposal.GetBody() and returns Proposal and the rest of the bytes.
	UnmarshalProposal([]byte) (Proposal, []byte, error)
	// UnmarshalVote receives Vote.GetBody() and returns Vote and the rest of the bytes.
	UnmarshalVote([]byte) (Vote, []byte, error)
	// UnmarshalNotarization receives Notarization.GetBody() and returns Notarization and
	// the rest of the bytes.
	UnmarshalNotarization([]byte) (Notarization, []byte, error)
	// UnmarshalClockMsg receives ClockMsg.GetBody() and returns ClockMsg and the rest of the bytes.
	UnmarshalClockMsg([]byte) (ClockMsg, []byte, error)
	// UnmarshalClockMsgNota receives ClockMsgNota.GetBody()
	// and returns ClockMsgNota and the rest of the bytes.
	UnmarshalClockMsgNota([]byte) (ClockMsgNota, []byte, error)
}

type Verifier interface {
	Propose(b Block) (Proposal, error)
	// VerifyProposal verifies |p| is signed by the eligible proposer
	// and |p|'s block should contain valid notarizations of ancestor blocks.
	// See the rule above BlockChain for details.
	VerifyProposal(p Proposal) error
	Vote(p Proposal) (Vote, error)
	VerifyVote(v Vote) error
	Notarize(votes []Vote) (Notarization, error)
	VerifyNotarization(n Notarization) error
	NewClockMsg(e Epoch) (ClockMsg, error)
	VerifyClockMsg(c ClockMsg) error
	NewClockMsgNota(clocks []ClockMsg) (ClockMsgNota, error)
	VerifyClockMsgNota(cn ClockMsgNota) error

	// Used for verifying the other end's role (challenge-response)
	Sign(id string, bytes []byte) ([]byte, error)
	VerifySignature(id string, signature []byte, expected []byte) error
}

type FreshestNotarizedChainExtendedEvent struct {
	Sn BlockSn
}

type FinalizedChainExtendedEvent struct {
	Sn BlockSn
	// This field is set when there is a reconfiguration.
	ReconfFinalizedByBlockSn BlockSn
}

//--------------------------------------------------------------------

func (typ Type) String() string {
	switch typ {
	case TypeBlock:
		return "block"
	case TypeProposal:
		return "proposal"
	case TypeVote:
		return "vote"
	case TypeNotarization:
		return "notarization"
	case TypeClockMsg:
		return "clock"
	case TypeClockMsgNota:
		return "clock-nota"
	default:
		return "unknown"
	}
}

func GetGenesisBlockSn() BlockSn {
	return BlockSn{0, 1}
}

// New returns a BlockChain whose genesis block is always created.
//
// TODO(thunder): support a real BlockChain
func New(cfg Config) (BlockChain, error) {
	return NewBlockChainFake(1)
}

func GetParentBlock(bc BlockChain, b Block) Block {
	return bc.GetBlock(b.GetParentBlockSn())
}

//--------------------------------------------------------------------

// NewBlockSnFromBytes unmarshal the output of BlockSn.ToBytes().
// Return the result and the rest of the bytes.
func NewBlockSnFromBytes(bytes []byte) (BlockSn, []byte, error) {
	var sn BlockSn
	if len(bytes) < 8 {
		msg := fmt.Sprintf("Invalid input: the length (%d) is less than 8", len(bytes))
		logger.Warn(msg)
		return sn, bytes, errors.Errorf(msg)
	}

	var err error
	var tmp uint32
	tmp, bytes, err = utils.BytesToUint32(bytes)
	if err != nil {
		return BlockSn{}, nil, err
	}
	sn.Epoch = Epoch(tmp)
	sn.S, bytes, err = utils.BytesToUint32(bytes)
	if err != nil {
		return BlockSn{}, nil, err
	}
	return sn, bytes, nil
}

func (s BlockSn) String() string {
	return fmt.Sprintf("(%d,%d)", s.Epoch, s.S)
}

func (s BlockSn) IsGenesis() bool {
	return s.Epoch == 0 && s.S == 1
}

func (s BlockSn) IsNil() bool {
	return s.Epoch == 0 && s.S == 0
}

func (s BlockSn) Compare(s2 BlockSn) int {
	if s.Epoch != s2.Epoch {
		if s.Epoch < s2.Epoch {
			return -1
		} else {
			return 1
		}
	}
	if s.S != s2.S {
		if s.S < s2.S {
			return -1
		} else {
			return 1
		}
	}
	return 0
}

func (s BlockSn) GetBlockSn() BlockSn {
	return s
}

func (s BlockSn) Less(s2 llrb.Item) bool {
	return s.Compare(s2.(BlockSnGetter).GetBlockSn()) < 0
}

func (s BlockSn) ToBytes() []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint32(bytes, uint32(s.Epoch))
	binary.LittleEndian.PutUint32(bytes[4:], s.S)
	return bytes
}

func (h Hash) Equal(other Hash) bool {
	if len(h) != len(other) {
		return false
	}

	for i := 0; i < len(h); i++ {
		if h[i] != other[i] {
			return false
		}
	}
	return true
}

//--------------------------------------------------------------------

func (s ByBlockSn) Len() int {
	return len(s)
}

func (s ByBlockSn) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s ByBlockSn) Less(i, j int) bool {
	return s[i].GetBlockSn().Compare(s[j].GetBlockSn()) < 0
}
