package network

import (
	"math/rand"
	"thunder2/lgr"
	"thunder2/utils"
	"time"

	"github.com/pkg/errors"
)

var (
	logger = lgr.NewLgr("/network")
)

type Config struct {
}

type Role int

const (
	RoleHub   = Role(1)
	RoleSpoke = Role(2)
)

// TODO(thunder): support TLS
// All operations of Host are go-routine safe.
// * All public methods hold mutex by themselves.
// * Private methods assume the caller holds the mutex.
type Host struct {
	// Member fields below are set once
	id       string
	selector func() int
	sink     chan<- *Message

	// Protect member fields below
	mutex       utils.CheckedLock
	role        Role
	hubs        []connection
	spokes      []connection
	connections map[ConnectionHandle]connection
	nextHandle  ConnectionHandle
}

type ConnectionHandle uint32

type Message struct {
	typ       uint8
	attribute uint16
	source    connection
	handle    ConnectionHandle
	// NOTE: Assume the size is not large (say, <10M),
	// Preparing a large array is inefficient.
	// Use some other data structure when needed.
	blob []byte
}

// Use one packet to represent one message.
// If we want to support prioritized packets, use multiple packets to
// represent one message and let high priority packets be able to preempt.
type packet struct {
	size      uint32
	typ       uint8
	attribute uint16
	blob      []byte
}

// All operations must be goroutine-safe.
type connection interface {
	read() (*packet, error)
	write(p *packet) error
	close() error
	getRole() Role
	SetEnabledForBroadcast(enabled bool)
	IsEnabledForBroadcast() bool
	getTLSPublicKey() []byte
	getDebugInfo() string
}

const (
	// A new connection is established.
	AttrOpen = uint16(1 << 0)
	// A connection is closed.
	AttrClosed = uint16(1 << 1)
	// It's a relay message.
	AttrRelay = uint16(1 << 2)
)

//------------------------------------------------------------------------------

func NewRandomSelector() func() int {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	return r.Int
}

func NewHost(id string, role Role, hubSelector func() int, sink chan<- *Message) *Host {
	return &Host{
		id:          id,
		role:        role,
		selector:    hubSelector,
		sink:        sink,
		connections: make(map[ConnectionHandle]connection),
		nextHandle:  1,
	}
}

// TODO(thunder): listen to a port and create new connections (call addConnection())
// The new connection's role is RoleHub if the target is a hub; otherwise, it's RoleSpoke.
func (h *Host) Accept() {
}

// TODO(thunder): connect to a host and create a new connection (call addConnection()).
// The new connection's role is RoleHub if the target is a hub; otherwise, it's RoleSpoke.
func (h *Host) Connect() {
}

func (h *Host) GetNumHubs() int {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	return len(h.hubs)
}

func (h *Host) GetNumSpokes() int {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	return len(h.spokes)
}

func (h *Host) GetRole() Role {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	return h.role
}

func (h *Host) SetRole(role Role) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.role = role
}

func (h *Host) Broadcast(m *Message) error {
	p := newPacket(m)

	if h.GetRole() == RoleSpoke {
		h.mutex.Lock()
		defer h.mutex.Unlock()

		p.attribute |= AttrRelay
		hub := h.selectHub()
		if hub == nil {
			return errors.Errorf("No hub exists")
		}

		if !hub.IsEnabledForBroadcast() {
			return errors.Errorf("Hub is not enabled")
		}

		return hub.write(p)
	}

	// NOTE: p will be shared by multiple goroutines if the connections
	// are implemented by channels. However, we will not modify the content of p
	// normally, so there is no data race.
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if len(h.hubs) == 0 && len(h.spokes) == 0 {
		return errors.Errorf("No connected host")
	}

	for _, c := range h.hubs {
		// A hub should not send a relay message to another hub.
		// Add the check as a defense.
		if c == m.source {
			logger.Warn("%s; Receive a relay message from another hub",
				c.getDebugInfo())
			continue
		}

		if !c.IsEnabledForBroadcast() {
			continue
		}

		err := c.write(p)
		if err != nil {
			logger.Warn("%s: %s", c.getDebugInfo(), err)
		}
	}
	for _, c := range h.spokes {
		if c == m.source {
			continue
		}

		if !c.IsEnabledForBroadcast() {
			continue
		}

		err := c.write(p)
		if err != nil {
			logger.Warn("%s: %s", c.getDebugInfo(), err)
		}
	}
	return nil
}

func (h *Host) SetEnabledBroadcast(handle ConnectionHandle, enabled bool) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if c, ok := h.connections[handle]; ok {
		c.SetEnabledForBroadcast(enabled)
	}
}

func (h *Host) Send(handle ConnectionHandle, m *Message) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if c, ok := h.connections[handle]; !ok {
		return errors.Errorf("failed to send message (type=%d); handle %s not exist",
			m.GetType(), handle)
	} else {
		p := newPacket(m)
		return c.write(p)
	}
}

// SendToHub sends the message to one of the hub.
func (h *Host) SendToHub(m *Message) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	p := newPacket(m)
	hub := h.selectHub()
	if hub == nil {
		return errors.Errorf("no hub exists")
	}
	return hub.write(p)
}

func (h *Host) selectHub() connection {
	h.mutex.CheckIsLocked("")

	if len(h.hubs) == 0 {
		return nil
	}
	return h.hubs[h.selector()%len(h.hubs)]
}

func (h *Host) addConnection(id string, c connection) {
	h.mutex.CheckIsLocked("")

	if c.getRole() == RoleHub {
		h.hubs = append(h.hubs, c)
	} else {
		h.spokes = append(h.spokes, c)
	}

	h.connections[h.nextHandle] = c
	m := Message{
		attribute: AttrOpen,
		source:    c,
		handle:    h.nextHandle,
		blob:      []byte(id),
	}
	h.sink <- &m
	go h.receive(c, h.nextHandle)

	h.nextHandle++
}

func (h *Host) receive(c connection, handle ConnectionHandle) {
	for {
		p, err := c.read()
		if err != nil {
			logger.Info("%s %s: read fails; err=%s", h.id, c.getDebugInfo(), err)

			h.mutex.Lock()
			defer h.mutex.Unlock()
			h.removeConnection(c)
			return
		}

		needBroadcast := h.GetRole() == RoleHub && (p.attribute&AttrRelay) > 0
		if needBroadcast {
			p.attribute &= ^AttrRelay
		}
		m, err := newMessageByPacket(p, c, handle)
		if err != nil {
			logger.Warn("%s: %s", c.getDebugInfo(), err)

			h.mutex.Lock()
			defer h.mutex.Unlock()
			h.removeConnection(c)
			return
		}

		if needBroadcast {
			if err := h.Broadcast(m); err != nil {
				logger.Warn("[%s] failed to broadcast: %s (%s)", h.id, err, c.getDebugInfo())
			}
		}

		h.sink <- m
	}
}

func (h *Host) removeConnection(cc connection) {
	h.mutex.CheckIsLocked("")

	defer func() {
		var handle ConnectionHandle
		for h, c := range h.connections {
			if c == cc {
				handle = h
				break
			}
		}
		delete(h.connections, handle)

		// Explicitly notify the client that the read goroutine ends.
		m := Message{
			attribute: AttrClosed,
			source:    cc,
			handle:    handle,
			blob:      []byte{},
		}
		h.sink <- &m
	}()

	if err := cc.close(); err != nil {
		logger.Warn("[%s] failed to close: %s (%s)", h.id, err, cc.getDebugInfo())
	}

	for i, c := range h.hubs {
		if c == cc {
			h.hubs = append(h.hubs[:i], h.hubs[i+1:]...)
			return
		}
	}
	for i, c := range h.spokes {
		if c == cc {
			h.spokes = append(h.spokes[:i], h.spokes[i+1:]...)
			return
		}
	}
}

func (h *Host) GetTLSPublicKey(handle ConnectionHandle) []byte {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if c, ok := h.connections[handle]; ok {
		return c.getTLSPublicKey()
	}
	return nil
}

func (h *Host) CloseAllConnections() {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	for _, c := range h.connections {
		h.removeConnection(c)
	}
}

func (h *Host) CloseConnection(handle ConnectionHandle) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if c, ok := h.connections[handle]; ok {
		h.removeConnection(c)
	}
}

//--------------------------------------------------------------------

// For now, one packet represents one message.
// When we want to support the session layer (resume connection silently)
// or prioritized messages, we'll need to support the one-to-many mapping
// between the message and packets.
func newMessageByPacket(p *packet, c connection, h ConnectionHandle) (*Message, error) {
	m := Message{
		typ:       p.typ,
		attribute: p.attribute,
		source:    c,
		handle:    h,
		blob:      p.blob,
	}
	return &m, nil
}

// For now, one packet represents one message.
// When we want to support the session layer (be able to resume connection silently)
// or prioritized messages, we'll need to support the one-to-many mapping
// between the message and packets.
func newPacket(m *Message) *packet {
	return &packet{
		size:      uint32(len(m.GetBlob())),
		typ:       m.GetType(),
		attribute: m.GetAttribute(),
		blob:      m.GetBlob(),
	}
}

//--------------------------------------------------------------------

func NewMessage(typ uint8, attribute uint16, blob []byte) *Message {
	return &Message{
		typ:       typ,
		attribute: attribute,
		source:    nil, // The receiver will set this field.
		blob:      blob,
	}
}

func (m *Message) GetType() uint8 {
	return m.typ
}

func (m *Message) GetAttribute() uint16 {
	return m.attribute
}

func (m *Message) IsRelay() bool {
	return (m.attribute & AttrRelay) > 0
}

func (m *Message) GetConnectionHandle() ConnectionHandle {
	return m.handle
}

func (m *Message) GetBlob() []byte {
	return m.blob
}

func (m *Message) Reply(msg *Message) error {
	if msg.source != nil {
		return errors.Errorf("Reply is called with source == nil")
	}
	return m.source.write(newPacket(msg))
}

func (m *Message) GetSourceDebugInfo() string {
	if m.source == nil {
		return "nil"
	}
	return m.source.getDebugInfo()
}
