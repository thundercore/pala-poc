package blockchain

import (
	"testing"
	"time"

	"github.com/petar/GoLLRB/llrb"
	"github.com/stretchr/testify/require"
)

func TestLongestChain(t *testing.T) {
	k := uint32(1)
	t.Run("case 1", func(t *testing.T) {
		req := require.New(t)

		bc, err := NewBlockChainFake(1)
		req.NoError(err)

		// 0->1->2->3->4
		//          |
		//          +->5->6
		genesis := bc.GetGenesisBlock()
		PrepareFakeChain(req, bc, genesis.GetBlockSn(), 1, k,
			[]string{"v1"},
			[]string{"1", "2", "3", "4"})
		PrepareFakeChain(req, bc, BlockSn{1, 3}, 2, k,
			[]string{"v1"},
			[]string{"5", "6"})

		actual := bc.(*BlockChainFake).GetLongestChain()

		req.NotNil(actual)
		req.Equal("0->1->2->3->5->6", DumpFakeChain(bc, actual, false))
	})

	t.Run("case 2", func(t *testing.T) {
		req := require.New(t)

		bc, err := New(Config{})
		req.NoError(err)

		// 0->1->2->3->4
		//    |     |
		//    |     +->A->B
		//    |
		//    +->5->6->7->8->9
		genesis := bc.GetGenesisBlock()
		PrepareFakeChain(req, bc, genesis.GetBlockSn(), 1, k,
			[]string{"v1"},
			[]string{"1", "2", "3", "4"})
		PrepareFakeChain(req, bc, BlockSn{1, 1}, 2, k,
			[]string{"v1"},
			[]string{"5", "6", "7", "8", "9"})
		PrepareFakeChain(req, bc, BlockSn{1, 3}, 3, k,
			[]string{"v1"},
			[]string{"A", "B"})

		actual := bc.(*BlockChainFake).GetLongestChain()

		req.NotNil(actual)
		req.Equal("0->1->5->6->7->8->9", DumpFakeChain(bc, actual, false))
	})
}

func TestFreshestNotarizedChain(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		req := require.New(t)

		bc, err := New(Config{})
		req.NoError(err)

		// 0->1->2->3->4
		//    |
		//    +->5->6
		k := uint32(1)
		genesis := bc.GetGenesisBlock()
		PrepareFakeChain(req, bc, genesis.GetBlockSn(), 1, k,
			[]string{"v1"},
			[]string{"1", "2", "3", "4"})
		PrepareFakeChain(req, bc, BlockSn{1, 1}, 2, k,
			[]string{"v1"},
			[]string{"5", "6"})

		actual := bc.GetFreshestNotarizedChain()

		req.NotNil(actual)
		req.Equal("0[]->1[]->5[1]", DumpFakeChain(bc, actual, true))
		req.Equal(bc.(*BlockChainFake).ComputeFreshestNotarizedChain(), actual)
	})

	t.Run("freshest is not longest", func(t *testing.T) {
		req := require.New(t)

		k := uint32(2)
		bc, err := NewBlockChainFake(k)
		req.NoError(err)

		// 0->1->2->3->4
		//    |  |
		//    |  +->A->B->C
		//    |
		//    +->5->6->7->8->9
		genesis := bc.GetGenesisBlock()
		PrepareFakeChain(req, bc, genesis.GetBlockSn(), 1, k,
			[]string{"v1"},
			[]string{"1", "2", "3", "4"})
		PrepareFakeChain(req, bc, BlockSn{1, 1}, 2, k,
			[]string{"v1"},
			[]string{"5", "6", "7", "8", "9"})
		PrepareFakeChain(req, bc, BlockSn{1, 2}, 3, k,
			[]string{"v1"},
			[]string{"A", "B", "C"})

		actual := bc.GetFreshestNotarizedChain()

		req.NotNil(actual)
		req.Equal("0[]->1[]->2[]->A[1,2]", DumpFakeChain(bc, actual, true))
		req.Equal(bc.(*BlockChainFake).ComputeFreshestNotarizedChain(), actual)
	})

	t.Run("newest is not notarized", func(t *testing.T) {
		req := require.New(t)

		k := uint32(2)
		bc, err := NewBlockChainFake(k)
		req.NoError(err)

		// 0->1->2->3->4
		//    |  |
		//    |  +->A->B
		//    |
		//    +->5->6->7->8->9
		genesis := bc.GetGenesisBlock()
		PrepareFakeChain(req, bc, genesis.GetBlockSn(), 1, k,
			[]string{"v1"},
			[]string{"1", "2", "3", "4"})
		PrepareFakeChain(req, bc, BlockSn{1, 1}, 2, k,
			[]string{"v1"},
			[]string{"5", "6", "7", "8", "9"})
		PrepareFakeChain(req, bc, BlockSn{1, 2}, 3, k,
			[]string{"v1"},
			[]string{"A", "B"})

		actual := bc.GetFreshestNotarizedChain()

		req.NotNil(actual)
		req.Equal("0[]->1[]->5[1]->6[]->7[5]", DumpFakeChain(bc, actual, true))
		req.Equal(bc.(*BlockChainFake).ComputeFreshestNotarizedChain(), actual)
	})
}

func TestFinalizedChain(t *testing.T) {
	k := uint32(2)

	t.Run("no new finalized block", func(t *testing.T) {
		req := require.New(t)

		bc, err := NewBlockChainFake(k)
		req.NoError(err)

		// 0->1->2->3->4
		genesis := bc.GetGenesisBlock()
		PrepareFakeChain(req, bc, genesis.GetBlockSn(), 1, k,
			[]string{"v1"},
			[]string{"1", "2", "3", "4"})

		actual := bc.GetFinalizedChain()

		req.NotNil(actual)
		req.Equal("0", DumpFakeChain(bc, actual, false))
		req.Equal(bc.(*BlockChainFake).ComputeFinalizedChain(), actual)
	})

	t.Run("minimal finalized requirement", func(t *testing.T) {
		req := require.New(t)

		bc, err := NewBlockChainFake(k)
		req.NoError(err)

		// 0->1->2->3->4->5
		genesis := bc.GetGenesisBlock()
		PrepareFakeChain(req, bc, genesis.GetBlockSn(), 1, k,
			[]string{"v1"},
			[]string{"1", "2", "3", "4", "5"})

		actual := bc.GetFinalizedChain()

		req.NotNil(actual)
		req.Equal("0->1", DumpFakeChain(bc, actual, false))
		req.Equal(bc.(*BlockChainFake).ComputeFinalizedChain(), actual)
	})

	t.Run("longer finalized chain", func(t *testing.T) {
		req := require.New(t)

		bc, err := NewBlockChainFake(k)
		req.NoError(err)

		// 0->1->2->3->4->5
		//       |
		//       +->6->7->8->9->A->B->C
		genesis := bc.GetGenesisBlock()
		PrepareFakeChain(req, bc, genesis.GetBlockSn(), 1, k,
			[]string{"v1"},
			[]string{"1", "2", "3", "4", "5"})
		PrepareFakeChain(req, bc, BlockSn{1, 2}, 2, k,
			[]string{"v1"},
			[]string{"6", "7", "8", "9", "A", "B", "C"})

		actual := bc.GetFinalizedChain()

		req.NotNil(actual)
		req.Equal("0->1->2->6->7->8", DumpFakeChain(bc, actual, false))
		req.Equal(bc.(*BlockChainFake).ComputeFinalizedChain(), actual)
	})
}

func TestStartCreatingNewBlock(t *testing.T) {
	req := require.New(t)
	unnotarizedWindow := uint32(2)
	bc, err := NewBlockChainFake(unnotarizedWindow)
	req.NoError(err)
	epoch := Epoch(1)

	// Create the blocks.
	var b Block
	ch, err := bc.StartCreatingNewBlocks(epoch)
	req.NoError(err)
	for i := uint32(0); i < unnotarizedWindow; i++ {
		b = (<-ch).Block
		req.Equal(BlockSn{epoch, i + 1}, b.GetBlockSn())
	}
	// Expect the blockchain blocks because there is no notarization.
	select {
	case <-ch:
		t.FailNow()
	case <-time.After(100 * time.Millisecond):
	}

	// Add notarization, so BlockChain can create new blocks.
	voterIds := []string{"v1"}
	err = bc.AddNotarization(&NotarizationFake{BlockSn{epoch, 1}, voterIds})
	req.NoError(err)
	b = (<-ch).Block
	req.Equal(BlockSn{epoch, 3}, b.GetBlockSn())
	notas := b.GetNotarizations()
	req.Equal(1, len(notas))
	// Expect storing the "parent-k"'s notarization.
	req.Equal(BlockSn{epoch, 1}, notas[0].GetBlockSn())

	err = bc.StopCreatingNewBlocks()
	req.NoError(err)

	// Expect created blocks are already inserted.
	for i := 1; i <= 3; i++ {
		b = bc.GetBlock(BlockSn{epoch, uint32(i)})
		req.NotNil(b)
		req.Equal(BlockSn{epoch, uint32(i)}, b.GetBlockSn())
	}

	// Create the blocks at the next epoch.
	err = bc.AddNotarization(&NotarizationFake{BlockSn{epoch, 2}, voterIds})
	req.NoError(err)
	err = bc.AddNotarization(&NotarizationFake{BlockSn{epoch, 3}, voterIds})
	req.NoError(err)

	epoch++
	fnc := bc.GetFreshestNotarizedChain()
	ch, err = bc.StartCreatingNewBlocks(epoch)
	req.NoError(err)
	b = (<-ch).Block
	req.NotNil(b)
	req.Equal(BlockSn{epoch, 1}, b.GetBlockSn())
	req.Equal(fnc.GetBlockSn(), b.GetParentBlockSn())

	// Expect the first block contains previous blocks' notarizations.
	notas = b.GetNotarizations()
	req.Equal(2, len(notas))
	req.Equal(BlockSn{epoch - 1, 2}, notas[0].GetBlockSn())
	req.Equal(BlockSn{epoch - 1, 3}, notas[1].GetBlockSn())
}

func TestFakeDataMarshalAndUnMarshal(t *testing.T) {
	req := require.New(t)
	bc, err := NewBlockChainFake(1)
	req.NoError(err)

	genesis := bc.GetGenesisBlock()
	PrepareFakeChain(req, bc, genesis.GetBlockSn(), 1, 1,
		[]string{"v1"}, []string{"1", "2"})

	sn := BlockSn{1, 2}
	b := bc.GetBlock(sn)
	p := ProposalFake{"p1", b}
	v := VoteFake{sn, "v1"}
	n := NewNotarizationFake(sn, []string{"v1"})
	c := ClockMsgFake{5, "v5"}
	cn := NewClockMsgNotaFake(5, []string{"v5"})

	du := &DataUnmarshallerFake{}

	asn, _, err := NewBlockSnFromBytes(sn.ToBytes())
	req.NoError(err)
	req.Equal(sn, asn)

	ab, _, err := du.UnmarshalBlock(b.GetBody())
	req.NoError(err)
	req.NotNil(ab)
	req.Equal(b, ab)

	ap, _, err := du.UnmarshalProposal(p.GetBody())
	req.NoError(err)
	req.NotNil(ap)
	var ep Proposal = &p
	req.Equal(ep, ap)

	av, _, err := du.UnmarshalVote(v.GetBody())
	req.NoError(err)
	req.NotNil(av)
	var ev Vote = &v
	req.Equal(ev, av)

	an, _, err := du.UnmarshalNotarization(n.GetBody())
	req.NoError(err)
	req.NotNil(an)
	req.Equal(n, an)

	ac, _, err := du.UnmarshalClockMsg(c.GetBody())
	req.NoError(err)
	req.NotNil(ac)
	var ec ClockMsg = &c
	req.Equal(ec, ac)

	acn, _, err := du.UnmarshalClockMsgNota(cn.GetBody())
	req.NoError(err)
	req.NotNil(acn)
	req.Equal(cn, acn)
}

type testItem struct {
	sn    BlockSn
	value string
}

func (i *testItem) Less(other llrb.Item) bool {
	return i.sn.Compare(other.(BlockSnGetter).GetBlockSn()) < 0
}

func (i *testItem) GetBlockSn() BlockSn {
	return i.sn
}

func TestUsingLLRB(t *testing.T) {
	data := []testItem{
		testItem{BlockSn{3, 1}, "3-1"},
		testItem{BlockSn{5, 1}, "5-1"},
		testItem{BlockSn{1, 1}, "1-1"},
		testItem{BlockSn{4, 1}, "4-1"},
		testItem{BlockSn{2, 1}, "2-1"},
	}
	expected := []testItem{
		testItem{BlockSn{1, 1}, "1-1"},
		testItem{BlockSn{2, 1}, "2-1"},
		testItem{BlockSn{3, 1}, "3-1"},
		testItem{BlockSn{4, 1}, "4-1"},
		testItem{BlockSn{5, 1}, "5-1"},
	}

	t.Run("iterator", func(t *testing.T) {
		req := require.New(t)

		tree := llrb.New()
		for i := range data {
			tree.ReplaceOrInsert(&data[i])
		}

		req.Equal(len(data), tree.Len())

		i := 0
		tree.AscendGreaterOrEqual(tree.Min(), func(actual llrb.Item) bool {
			req.Equal(expected[i], *actual.(*testItem))
			i++
			return true
		})
	})

	t.Run("get", func(t *testing.T) {
		req := require.New(t)

		tree := llrb.New()
		for i := range data {
			tree.ReplaceOrInsert(&data[i])
		}

		req.Equal(len(data), tree.Len())

		// Use the same type as the key with an empty value.
		for i := 0; i < len(data); i++ {
			key := testItem{expected[i].sn, ""}
			req.Equal(expected[i], *tree.Get(&key).(*testItem))
		}

		// Use BlockSnGetter.
		for i := 0; i < len(data); i++ {
			req.Equal(expected[i], *tree.Get(expected[i].sn).(*testItem))
		}
	})

	t.Run("delete min", func(t *testing.T) {
		req := require.New(t)

		tree := llrb.New()
		for i := range data {
			tree.ReplaceOrInsert(&data[i])
		}

		req.Equal(len(data), tree.Len())

		for i := 0; i < len(data); i++ {
			min := tree.Min()
			req.Equal(expected[i], *min.(*testItem))
			tmp := tree.DeleteMin()
			req.Equal(min, tmp)
		}
	})
}
