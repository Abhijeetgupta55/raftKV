// Package nemesis is the fault-injection harness (M6): TCP interposition
// proxies for network faults, process control (kill -9, suspend/resume)
// for node faults, a concurrent workload generator, and a timestamped
// operation-history recorder in the shape a linearizability checker
// consumes. Design rationale (proxy over iptables, connection-level vs
// packet-level asymmetry): DESIGN.md "The nemesis (M6)".
package nemesis

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// Proxy forwards one directed edge of the cluster graph: every connection
// node FROM initiates to node TO passes through here. Dropping the edge
// severs those connections and refuses new ones; delay adds latency to
// every copied chunk. Zero-value semantics: forwarding, no delay.
type Proxy struct {
	From, To uint64
	listener net.Listener
	target   string

	mu      sync.Mutex
	dropped bool
	delay   time.Duration
	conns   map[net.Conn]struct{}
	closed  bool
}

func newProxy(from, to uint64, target string) (*Proxy, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &Proxy{From: from, To: to, listener: lis, target: target, conns: map[net.Conn]struct{}{}}
	go p.acceptLoop()
	return p, nil
}

// Addr is what node FROM must dial to reach node TO.
func (p *Proxy) Addr() string { return p.listener.Addr().String() }

func (p *Proxy) acceptLoop() {
	for {
		c, err := p.listener.Accept()
		if err != nil {
			return // listener closed
		}
		p.mu.Lock()
		if p.dropped || p.closed {
			p.mu.Unlock()
			c.Close() // edge is cut: refuse immediately
			continue
		}
		p.conns[c] = struct{}{}
		p.mu.Unlock()
		go p.forward(c)
	}
}

func (p *Proxy) forward(client net.Conn) {
	defer p.untrack(client)
	server, err := net.DialTimeout("tcp", p.target, 2*time.Second)
	if err != nil {
		client.Close()
		return
	}
	p.mu.Lock()
	if p.dropped || p.closed {
		p.mu.Unlock()
		client.Close()
		server.Close()
		return
	}
	p.conns[server] = struct{}{}
	p.mu.Unlock()

	done := make(chan struct{}, 2)
	pipe := func(dst, src net.Conn) {
		buf := make([]byte, 32*1024)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				if d := p.currentDelay(); d > 0 {
					time.Sleep(d)
				}
				if _, werr := dst.Write(buf[:n]); werr != nil {
					break
				}
			}
			if err != nil {
				break
			}
		}
		dst.Close()
		src.Close()
		done <- struct{}{}
	}
	go pipe(server, client)
	pipe(client, server)
	<-done
	p.untrack(server)
}

func (p *Proxy) currentDelay() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.delay
}

func (p *Proxy) untrack(c net.Conn) {
	p.mu.Lock()
	delete(p.conns, c)
	p.mu.Unlock()
}

// setDropped cuts (or restores) this edge. Cutting closes every live
// connection so in-flight RPCs fail now, not at some future timeout.
func (p *Proxy) setDropped(v bool) {
	p.mu.Lock()
	p.dropped = v
	var toClose []net.Conn
	if v {
		for c := range p.conns {
			toClose = append(toClose, c)
		}
		p.conns = map[net.Conn]struct{}{}
	}
	p.mu.Unlock()
	for _, c := range toClose {
		c.Close()
	}
}

func (p *Proxy) setDelay(d time.Duration) {
	p.mu.Lock()
	p.delay = d
	p.mu.Unlock()
}

func (p *Proxy) close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.listener.Close()
	p.setDropped(true)
}

// Net owns every directed edge between cluster nodes. The harness holds
// it in-process, so control is direct method calls — no admin socket
// needed (the interposition is still real TCP between real OS processes).
type Net struct {
	mu      sync.Mutex
	proxies map[[2]uint64]*Proxy // [from, to] -> edge
}

// NewNet builds a full mesh of directed-edge proxies over the given real
// node addresses. PeerAddr(i, j) then yields node i's dial address for j.
func NewNet(realAddrs map[uint64]string) (*Net, error) {
	n := &Net{proxies: map[[2]uint64]*Proxy{}}
	for from := range realAddrs {
		for to, target := range realAddrs {
			if from == to {
				continue
			}
			p, err := newProxy(from, to, target)
			if err != nil {
				n.Close()
				return nil, err
			}
			n.proxies[[2]uint64{from, to}] = p
		}
	}
	return n, nil
}

// PeerAddr is the proxied address node `from` must use to reach `to`.
func (n *Net) PeerAddr(from, to uint64) string {
	return n.proxies[[2]uint64{from, to}].Addr()
}

// ProxyOwner maps a proxied address back to the real node id it fronts —
// used by clients to resolve leader hints (which carry the rejecting
// node's proxied view of the leader) to a directly reachable node.
func (n *Net) ProxyOwner(addr string) (uint64, bool) {
	for key, p := range n.proxies {
		if p.Addr() == addr {
			return key[1], true
		}
	}
	return 0, false
}

// Blackhole cuts the single directed edge from -> to. (TCP-level: node
// `from` can no longer INITIATE to `to`; responses on connections `to`
// initiated still flow. See DESIGN.md for why that is the honest fault a
// userspace proxy can inject.)
func (n *Net) Blackhole(from, to uint64) { n.edge(from, to).setDropped(true) }

// HealEdge restores one directed edge.
func (n *Net) HealEdge(from, to uint64) { n.edge(from, to).setDropped(false) }

// Partition splits the cluster into groups: every edge crossing a group
// boundary is cut, both directions; edges within a group are restored.
func (n *Net) Partition(groups ...[]uint64) {
	side := map[uint64]int{}
	for i, g := range groups {
		for _, id := range g {
			side[id] = i
		}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	for key, p := range n.proxies {
		p.setDropped(side[key[0]] != side[key[1]])
	}
}

// Heal restores every edge.
func (n *Net) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, p := range n.proxies {
		p.setDropped(false)
		p.setDelay(0)
	}
}

// DelayAll injects latency on every edge (jitter comes from the OS
// scheduler on top of the base).
func (n *Net) DelayAll(d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, p := range n.proxies {
		p.setDelay(d)
	}
}

func (n *Net) edge(from, to uint64) *Proxy {
	n.mu.Lock()
	defer n.mu.Unlock()
	p, ok := n.proxies[[2]uint64{from, to}]
	if !ok {
		panic(fmt.Sprintf("nemesis: no edge %d->%d", from, to))
	}
	return p
}

func (n *Net) Close() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, p := range n.proxies {
		p.close()
	}
}
