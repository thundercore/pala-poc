// Put the fake implementations used by the production code for the integration test.
package network

import (
	"fmt"
	"sort"
	"strings"
	"thunder2/utils"

	"github.com/pkg/errors"
)

// All operations of connectionFake are goroutine-safe.
type connectionFake struct {
	debugInfo string
	role      Role
	reader    <-chan *packet

	peer *connectionFake

	// Protect member fields below
	mutex               utils.CheckedLock
	isClosed            bool
	writer              chan<- *packet
	enabledForBroadcast bool
}

type ConnectionFakePair struct {
	connections []connection
}

//--------------------------------------------------------------------

// FakeConnect simulates that a node connects to another node.
// After the connection is done, each node will have a new connection object.
func FakeConnect(src *Host, dst *Host) ConnectionFakePair {
	srcToDst := make(chan *packet, 1024)
	dstToSrc := make(chan *packet, 1024)

	dstConn := connectionFake{
		debugInfo:           fmt.Sprintf("(%s<-%s)", dst.id, src.id),
		role:                src.GetRole(),
		enabledForBroadcast: false, // Disable by default.
		reader:              srcToDst,
		writer:              dstToSrc,
	}
	srcConn := connectionFake{
		debugInfo:           fmt.Sprintf("(%s<-%s)", src.id, dst.id),
		role:                dst.GetRole(),
		enabledForBroadcast: false, // Disable by default.
		reader:              dstToSrc,
		writer:              srcToDst,
	}
	dstConn.peer = &srcConn
	srcConn.peer = &dstConn

	// The end who starts the connection knows the other end's id.
	// The upper layer can start the follow up process to do bidirectional
	// challenge-response with the id.
	src.mutex.Lock()
	defer src.mutex.Unlock()
	src.addConnection(dst.id, &srcConn)

	// The end who accepts a new connection doesn't know the other end
	// when the connection is established.
	dst.mutex.Lock()
	defer dst.mutex.Unlock()
	dst.addConnection("", &dstConn)

	return ConnectionFakePair{[]connection{&dstConn, &srcConn}}
}

func (p ConnectionFakePair) Close() {
	// The other end will be closed after close one end.
	if err := p.connections[0].close(); err != nil {
		logger.Warn("failed to close: %s", err)
	}
}

func (c *connectionFake) read() (*packet, error) {
	if p, ok := <-c.reader; ok {
		return p, nil
	} else {
		return nil, errors.Errorf("closed")
	}
}

func (c *connectionFake) write(p *packet) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.isClosed {
		return errors.Errorf("write to closed connection (%s)", c.debugInfo)
	}
	c.writer <- p
	return nil
}

func (c *connectionFake) close() error {
	c.mutex.Lock()
	if c.isClosed {
		c.mutex.Unlock()
		return nil
	}
	c.isClosed = true
	close(c.writer)
	c.mutex.Unlock()
	return c.peer.close()
}

func (c *connectionFake) getRole() Role {
	return c.role
}
func (c *connectionFake) SetEnabledForBroadcast(enabled bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.enabledForBroadcast = enabled
}

func (c *connectionFake) IsEnabledForBroadcast() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.enabledForBroadcast
}

func (c *connectionFake) getTLSPublicKey() []byte {
	ss := []string{"TLS", c.getDebugInfo(), c.peer.getDebugInfo()}
	sort.Strings(ss)
	return []byte(strings.Join(ss, ":"))
}

func (c *connectionFake) getDebugInfo() string {
	return c.debugInfo
}

func ZeroSelector() int {
	return 0
}
