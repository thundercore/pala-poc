package consensus

import (
	"testing"
	"thunder2/blockchain"
	"thunder2/network"
	"time"

	"github.com/stretchr/testify/require"
)

var hasTestedDelay = false

func TestNetworkSimulator(t *testing.T) {
	t.Run("drop connection before sending", func(t *testing.T) {
		req := require.New(t)
		voterSink := make(chan *network.Message, 1024)
		proposerSink := make(chan *network.Message, 1024)
		voter := network.NewHost("v1", network.RoleSpoke, network.ZeroSelector, voterSink)
		proposer := network.NewHost("p1", network.RoleHub, network.ZeroSelector, proposerSink)
		net := NewNetworkSimulator()
		sn := blockchain.BlockSn{Epoch: 1, S: 1}
		net.AddRule(NetworkSimulatorRule{
			From: []string{"v1"},
			To:   []string{"p1"},
			Type: blockchain.TypeVote,
			Sn:   sn,
			Action: &network.FilterAction{
				PreCallback: network.ConnectionDropper,
			},
		})
		err := net.Connect(voter, proposer)
		req.NoError(err)

		// Verify the connection is established.
		select {
		case msg := <-proposerSink:
			req.True(msg.GetAttribute()&network.AttrOpen > 0)
		default:
			req.FailNow("no msg")
		}

		// Expect the first Send() succeeds.
		vote := blockchain.NewVoteFake(sn, "v1")
		msg := network.NewMessage(uint8(vote.GetType()), 0, vote.GetBody())
		err = voter.SendToHub(msg)
		req.NoError(err)

		// Expect the connection is closed after Send().
		select {
		case msg := <-proposerSink:
			req.True(msg.GetAttribute()&network.AttrClosed > 0)
		case <-time.NewTimer(time.Second).C:
			req.FailNow("no msg")
		}

		net.Stop()
	})

	t.Run("drop connection after sending", func(t *testing.T) {
		req := require.New(t)
		voterSink := make(chan *network.Message, 1024)
		proposerSink := make(chan *network.Message, 1024)
		voter := network.NewHost("v1", network.RoleSpoke, network.ZeroSelector, voterSink)
		proposer := network.NewHost("p1", network.RoleHub, network.ZeroSelector, proposerSink)
		net := NewNetworkSimulator()
		sn := blockchain.BlockSn{Epoch: 1, S: 1}
		net.AddRule(NetworkSimulatorRule{
			From: []string{"v1"},
			To:   []string{"p1"},
			Type: blockchain.TypeVote,
			Sn:   sn,
			Action: &network.FilterAction{
				PostCallback: network.ConnectionDropper,
			},
		})
		err := net.Connect(voter, proposer)
		req.NoError(err)

		// Verify the connection is established.
		select {
		case msg := <-proposerSink:
			req.True(msg.GetAttribute()&network.AttrOpen > 0)
		default:
			req.FailNow("no msg")
		}

		// Expect the first Send() succeeds.
		vote := blockchain.NewVoteFake(sn, "v1")
		msg := network.NewMessage(uint8(vote.GetType()), 0, vote.GetBody())
		err = voter.SendToHub(msg)
		req.NoError(err)

		// Verify the proposer received the vote.
		select {
		case msg := <-proposerSink:
			req.Equal(blockchain.TypeVote, blockchain.Type(msg.GetType()))
		case <-time.NewTimer(time.Second).C:
			req.FailNow("no msg")
		}

		// Expect the second Send() fails because it's closed by NetworkSimulator.
		err = voter.SendToHub(msg)
		req.Error(err)

		select {
		case msg := <-proposerSink:
			req.True(msg.GetAttribute()&network.AttrClosed > 0)
		case <-time.NewTimer(time.Second).C:
			req.FailNow("no msg")
		}

		net.Stop()
	})

	t.Run("pre and post callbacks", func(t *testing.T) {
		req := require.New(t)
		voterSink := make(chan *network.Message, 1024)
		proposerSink := make(chan *network.Message, 1024)
		voter := network.NewHost("v1", network.RoleSpoke, network.ZeroSelector, voterSink)
		proposer := network.NewHost("p1", network.RoleHub, network.ZeroSelector, proposerSink)
		net := NewNetworkSimulator()
		sn := blockchain.BlockSn{Epoch: 1, S: 1}

		preDoCalled := false
		postDoCalled := false
		net.AddRule(NetworkSimulatorRule{
			From: []string{"v1"},
			To:   []string{"p1"},
			Type: blockchain.TypeVote,
			Sn:   sn,
			Action: &network.FilterAction{
				PreCallback: func(from string, to string, typ uint8, blob []byte) network.PassedOrDropped {
					req.Equal(blockchain.TypeVote, blockchain.Type(typ))
					req.False(preDoCalled)
					req.False(postDoCalled)
					preDoCalled = true
					return network.Passed
				},
				PostCallback: func(from string, to string, typ uint8, blob []byte) network.PassedOrDropped {
					req.Equal(blockchain.TypeVote, blockchain.Type(typ))
					req.True(preDoCalled)
					req.False(postDoCalled)
					postDoCalled = true
					return network.Passed
				},
			},
		})
		err := net.Connect(voter, proposer)
		req.NoError(err)

		// Verify the connection is established.
		select {
		case msg := <-proposerSink:
			req.True(msg.GetAttribute()&network.AttrOpen > 0)
		default:
			req.FailNow("no msg")
		}

		// Expect the first Send() succeeds.
		vote := blockchain.NewVoteFake(sn, "v1")
		msg := network.NewMessage(uint8(vote.GetType()), 0, vote.GetBody())
		err = voter.SendToHub(msg)
		req.NoError(err)

		// Verify the proposer received the vote.
		select {
		case msg := <-proposerSink:
			req.Equal(blockchain.TypeVote, blockchain.Type(msg.GetType()))
		case <-time.NewTimer(time.Second).C:
			req.FailNow("no msg")
		}

		// Verify both callbacks are called.
		net.Stop()

		req.True(preDoCalled)
		req.True(postDoCalled)
	})

	t.Run("delay", func(t *testing.T) {
		if hasTestedDelay {
			return
		}
		hasTestedDelay = true

		req := require.New(t)
		voterSink := make(chan *network.Message, 1024)
		proposerSink := make(chan *network.Message, 1024)
		voter := network.NewHost("v1", network.RoleSpoke, network.ZeroSelector, voterSink)
		proposer := network.NewHost("p1", network.RoleHub, network.ZeroSelector, proposerSink)
		net := NewNetworkSimulator()
		smallDelay := time.Duration(20 * time.Millisecond)
		delay := time.Duration(200 * time.Millisecond)
		net.SetBaseDelay(network.Delay{
			Mean: delay,
		})
		sn := blockchain.BlockSn{Epoch: 1, S: 1}
		err := net.Connect(voter, proposer)
		req.NoError(err)

		// Verify the connection is established.
		select {
		case msg := <-proposerSink:
			req.True(msg.GetAttribute()&network.AttrOpen > 0)
		default:
			req.FailNow("no msg")
		}

		// Expect the first Send() succeeds.
		nVote := 10
		for i := 0; i < nVote; i++ {
			vote := blockchain.NewVoteFake(sn, "v1")
			msg := network.NewMessage(uint8(vote.GetType()), 0, vote.GetBody())
			err = voter.SendToHub(msg)
			req.NoError(err)
		}

		// Expect the message is delayed.
		select {
		case <-proposerSink:
			req.FailNow("no delay")
		case <-time.NewTimer(smallDelay).C:
		}

		// Verify the proposer received the delayed votes.
		ch := make(chan bool)
		go func() {
			i := 0
			for msg := range proposerSink {
				req.Equal(blockchain.TypeVote, blockchain.Type(msg.GetType()))
				i++
				if i == nVote {
					break
				}
			}
			req.Equal(nVote, i)
			ch <- true
		}()
		// If the implementation is correct, the votes should arrive in a batch.
		// A wrong implementation may stack the delays.
		select {
		case <-time.NewTimer(delay + smallDelay).C:
			req.FailNow("The delay is too long")
		case <-ch:
		}

		net.Stop()
	})
}
