// Use a different package to ensure we only test the public API.
package network_test

import (
	"fmt"
	"testing"
	. "thunder2/network"
	"time"

	"github.com/stretchr/testify/require"
)

// TODO(thunder): test add new connection after broadcast.
func TestBroadcast(t *testing.T) {
	//
	// Prepare
	//
	req := require.New(t)

	// Prepare hubs
	var hubSpokePairs []ConnectionFakePair
	var hubSinks []chan *Message
	var hubs []*Host
	for i := 0; i < 3; i++ {
		ch := make(chan *Message, 1024)
		hubSinks = append(hubSinks, ch)
		id := fmt.Sprintf("h%d", i)
		h := NewHost(id, RoleHub, ZeroSelector, ch)
		hubs = append(hubs, h)

		// Prepare connections between hubs
		for j := 0; j < i; j++ {
			FakeConnect(hubs[j], hubs[i])
			m := <-hubSinks[j]
			req.Equal(AttrOpen, m.GetAttribute())
			hubs[j].SetEnabledBroadcast(m.GetConnectionHandle(), true)

			m = <-hubSinks[i]
			req.Equal(AttrOpen, m.GetAttribute())
			hubs[i].SetEnabledBroadcast(m.GetConnectionHandle(), true)
		}
	}

	// Prepare spokes
	var spokeSinks []chan *Message
	var spokes []*Host
	for i := 0; i < 4; i++ {
		ch := make(chan *Message, 1024)
		spokeSinks = append(spokeSinks, ch)
		id := fmt.Sprintf("s%d", i)
		h := NewHost(id, RoleSpoke, ZeroSelector, ch)

		// Prepare connections between hubs and spokes.
		for j := 0; j < len(hubs); j++ {
			pair := FakeConnect(h, hubs[j])
			m := <-hubSinks[j]
			req.Equal(AttrOpen, m.GetAttribute())
			hubs[j].SetEnabledBroadcast(m.GetConnectionHandle(), true)

			m = <-ch
			req.Equal(AttrOpen, m.GetAttribute())
			h.SetEnabledBroadcast(m.GetConnectionHandle(), true)

			hubSpokePairs = append(hubSpokePairs, pair)
		}

		spokes = append(spokes, h)
	}

	for i := 0; i < len(hubs); i++ {
		req.Equal(len(hubs)-1, hubs[i].GetNumHubs())
		req.Equal(len(spokes), hubs[i].GetNumSpokes())
	}

	for i := 0; i < len(spokes); i++ {
		req.Equal(len(hubs), spokes[i].GetNumHubs())
		req.Equal(0, spokes[i].GetNumSpokes())
	}

	// Test
	t.Run("direct broadcast", func(t *testing.T) {
		req := require.New(t)
		senderIndex := 1
		inputs := []string{"hello", "world"}
		for i := 0; i < len(inputs); i++ {
			m := NewMessage(1, 0, []byte(inputs[i]))
			err := hubs[senderIndex].Broadcast(m)
			req.NoError(err)
		}

		// Verify hubs.
		for i := 0; i < len(hubSinks); i++ {
			if i == senderIndex {
				continue
			}
			for j := 0; j < len(inputs); j++ {
				m := <-hubSinks[i]
				req.Equal(inputs[j], string(m.GetBlob()))
			}
		}

		// Verify spokes.
		for i := 0; i < len(spokeSinks); i++ {
			for j := 0; j < len(inputs); j++ {
				m := <-spokeSinks[i]
				req.Equal(inputs[j], string(m.GetBlob()))
			}
		}
	})

	t.Run("direct send", func(t *testing.T) {
		req := require.New(t)
		inputs := []string{"send hello", "send world"}
		senderIndex := 1
		for i := 0; i < len(inputs); i++ {
			m := NewMessage(1, 0, []byte(inputs[i]))
			err := spokes[senderIndex].SendToHub(m)
			req.NoError(err)
		}

		// Verify hub.
		for i := 0; i < len(inputs); i++ {
			// Expect hubSinks[0] receives the message because we use the ZeroSelector.
			m := <-hubSinks[0]
			req.Equal(inputs[i], string(m.GetBlob()))
		}
		for i := 0; i < len(hubSinks); i++ {
			select {
			case <-hubSinks[i]:
				req.FailNow("received data from a hub")
			case <-time.After(10 * time.Millisecond):
				// Expect no data. Pass.
			}
		}

		// Verify spokes.
		for i := 0; i < len(spokeSinks); i++ {
			select {
			case <-spokeSinks[i]:
				req.FailNow("received data from a spoke")
			case <-time.After(10 * time.Millisecond):
				// Expect no data. Pass.
			}
		}
	})

	t.Run("relay broadcast", func(t *testing.T) {
		req := require.New(t)
		senderIndex := 1
		inputs := []string{"send hello", "send world"}
		// Test
		for i := 0; i < len(inputs); i++ {
			m := NewMessage(1, 0, []byte(inputs[i]))
			err := spokes[senderIndex].Broadcast(m)
			req.NoError(err)
		}

		// Verify hubs.
		for i := 0; i < len(hubSinks); i++ {
			for j := 0; j < len(inputs); j++ {
				m := <-hubSinks[i]
				req.Equal(inputs[j], string(m.GetBlob()))
			}
		}

		// Verify spokes.
		for i := 0; i < len(spokeSinks); i++ {
			if i == senderIndex {
				select {
				case m := <-spokeSinks[i]:
					msg := fmt.Sprintf("%s %s\n",
						string(m.GetBlob()), m.GetSourceDebugInfo())
					req.FailNow(msg)
				case <-time.After(10 * time.Millisecond):
					// Expect no data. Pass.
				}
				continue
			}
			for j := 0; j < len(inputs); j++ {
				m := <-spokeSinks[i]
				req.Equal(inputs[j], string(m.GetBlob()))
			}
		}
	})

	t.Run("close", func(t *testing.T) {
		req := require.New(t)

		for _, pair := range hubSpokePairs {
			pair.Close()
		}

		// Wait for the end of the read goroutines.
		for i := 0; i < len(hubSinks); i++ {
			for j := 0; j < len(spokes); j++ {
				m := <-hubSinks[i]
				req.Equal(AttrClosed, m.GetAttribute())
			}
		}
		for i := 0; i < len(spokeSinks); i++ {
			for j := 0; j < len(hubs); j++ {
				m := <-spokeSinks[i]
				req.Equal(AttrClosed, m.GetAttribute())
			}
		}

		for i := 0; i < len(hubs); i++ {
			req.Equal(len(hubs)-1, hubs[i].GetNumHubs(), "hub %d", i)
			req.Equal(0, hubs[i].GetNumSpokes(), "hub %d", i)
		}

		for i := 0; i < len(spokes); i++ {
			req.Equal(0, spokes[i].GetNumHubs(), "spoke %d", i)
			req.Equal(0, spokes[i].GetNumSpokes(), "spoke %d", i)
		}
	})
}

func TestBroadcastWithoutAnyHost(t *testing.T) {
	req := require.New(t)

	// Test spoke
	ch := make(chan *Message, 1024)
	h := NewHost("s1", RoleSpoke, ZeroSelector, ch)
	m := NewMessage(1, 0, []byte("hello"))
	err := h.Broadcast(m)
	req.Error(err)

	// Test hub
	ch = make(chan *Message, 1024)
	h = NewHost("s1", RoleHub, ZeroSelector, ch)
	err = h.Broadcast(m)
	req.Error(err)
}

// TODO(thunder): test SetEnabledBroadcast()
// TODO(thunder): test checking protocol version
