// Put the fake implementations used by the production code for the integration test.
package network

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"thunder2/utils"
	"time"

	"github.com/petar/GoLLRB/llrb"
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

type Filter func(from string, to string, typ uint8, blob []byte) *FilterAction

type FilterAction struct {
	// Called before the packet arrives. Return Dropped to drop the connection.
	PreCallback Callback
	// Called after the packet arrives. Return Dropped to drop the connection.
	PostCallback Callback
}

type Callback func(from string, to string, typ uint8, blob []byte) PassedOrDropped

type PassedOrDropped int

// Add more parameter when needed.
type Delay struct {
	Mean time.Duration
}

type item struct {
	time    time.Time
	packets []*packet
}

var (
	Passed  = PassedOrDropped(0)
	Dropped = PassedOrDropped(1)
)

var ConnectionDropper = func(from string, to string, typ uint8, blob []byte,
) PassedOrDropped {
	return Dropped
}

//--------------------------------------------------------------------

// FakeConnect simulates that a node connects to another node.
// After the connection is done, each node will have a new connection object.
func FakeConnect(src *Host, dst *Host) ConnectionFakePair {
	return FakeConnectWithFilter(src, dst, nil, nil, Delay{}, nil)
}

// FakeConnectWithFilter uses |filter| to sniff all packages and act based on the response
// of |filter|. If |filter| is set, |wg| and |stopChan| must be set as well.
// |wg| and |stopChan| allows the client of the fake connection to know
// whether the sniffer goroutine ends.
func FakeConnectWithFilter(
	src *Host, dst *Host, wg *sync.WaitGroup, stopChan chan interface{},
	delay Delay, filter Filter,
) ConnectionFakePair {
	if filter != nil && (wg == nil || stopChan == nil) {
		utils.Bug("invalid arguments")
	}

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

	if !delay.IsNil() || filter != nil {
		addManInTheMiddle(src.id, dst.id, delay, filter, &srcConn, wg, stopChan)
		addManInTheMiddle(dst.id, src.id, delay, filter, &dstConn, wg, stopChan)
	}

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

func addManInTheMiddle(
	srcId string, dstId string, delay Delay, filter Filter, conn *connectionFake,
	wg *sync.WaitGroup, stopChan chan interface{},
) {
	middle := make(chan *packet, 1024)
	writer := conn.writer
	conn.writer = middle
	wg.Add(1)
	go func() {
		defer func() {
			wg.Done()
		}()

		packets := llrb.New()
		timer := time.NewTimer(time.Millisecond)
	Loop:
		for {
			select {
			case <-stopChan:
				return
			case p, ok := <-middle:
				if !ok {
					// closed.
					break Loop
				}
				if p == nil {
					utils.Bug("receive nil")
					writer <- p
					continue
				}

				targetTime := time.Now().Add(delay.Mean)
				// Use millisecond as the base unit.
				targetTime = targetTime.Round(time.Millisecond)
				key := &item{targetTime, nil}
				var ps *item
				if tmp := packets.Get(key); tmp != nil {
					ps = tmp.(*item)
				} else {
					ps = &item{time: targetTime}
					packets.ReplaceOrInsert(ps)
				}
				ps.packets = append(ps.packets, p)

				setupTimer(timer, packets)
			case now := <-timer.C:
				next, closed := processPackets(now, filter, packets, srcId, dstId, writer, conn)
				if closed {
					return
				}
				if next.IsZero() {
					continue
				}
				delay := next.Sub(now)
				timer.Reset(delay)
			}
		}

		// Process the rest of packets.
		for packets.Len() > 0 {
			setupTimer(timer, packets)
			now := <-timer.C
			next, closed := processPackets(now, filter, packets, srcId, dstId, writer, conn)
			if closed {
				return
			}
			if next.IsZero() {
				continue
			}
			delay := next.Sub(now)
			timer.Reset(delay)
		}
	}()
}

func setupTimer(timer *time.Timer, packets *llrb.LLRB) {
	now := time.Now().Round(time.Millisecond)
	next := packets.Min().(*item).time
	delay := time.Duration(0)
	if next.After(now) {
		delay = next.Sub(now)
	}
	timer.Reset(delay)
}

// process |packets| whose arrival time <= |now|.
// The first return value is the closet packets to process next time.
// If there is no packet, return 0.
// The second return value indicates whether the connection is already closed.
func processPackets(
	now time.Time, filter Filter, packets *llrb.LLRB,
	srcId string, dstId string, writer chan<- *packet, conn *connectionFake,
) (time.Time, bool) {
	// Use millisecond as the base unit.
	now = now.Round(time.Millisecond)
	for packets.Len() > 0 {
		min := packets.Min()
		ps := min.(*item)
		if ps.time.After(now) {
			return ps.time, false
		}

		for _, p := range ps.packets {
			action := filter(srcId, dstId, p.typ, p.blob)
			if action == nil {
				writer <- p
				continue
			}

			if action.PreCallback != nil &&
				action.PreCallback(srcId, dstId, p.typ, p.blob) == Dropped {
				if err := conn.close(); err != nil {
					logger.Warn("failed to close: %s", err)
				}
				close(writer)
				return time.Time{}, true
			}

			writer <- p

			if action.PostCallback != nil &&
				action.PostCallback(srcId, dstId, p.typ, p.blob) == Dropped {
				if err := conn.close(); err != nil {
					logger.Warn("failed to close: %s", err)
				}
				close(writer)
				return time.Time{}, true
			}
		}

		packets.DeleteMin()
	}
	return time.Time{}, false
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

//--------------------------------------------------------------------

func (d Delay) IsNil() bool {
	return d.Mean == 0
}

func (d Delay) String() string {
	return fmt.Sprintf("mean:%s", d.Mean)
}

func (d Delay) Add(other Delay) Delay {
	return Delay{
		Mean: d.Mean + other.Mean,
	}
}

func (i item) Less(i2 llrb.Item) bool {
	return i.time.Before(i2.(*item).time)
}
