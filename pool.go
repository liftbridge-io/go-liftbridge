package liftbridge

import (
	"sync"
	"time"

	"google.golang.org/grpc"
)

type connFactory func() (*grpc.ClientConn, error)

type connPool struct {
	mu            sync.Mutex
	conns         []*grpc.ClientConn
	maxConns      int
	keepAliveTime time.Duration
	timers        map[*grpc.ClientConn]*time.Timer
}

func newConnPool(maxConns int, keepAliveTime time.Duration) *connPool {
	return &connPool{
		maxConns:      maxConns,
		keepAliveTime: keepAliveTime,
		conns:         make([]*grpc.ClientConn, 0, maxConns),
		timers:        make(map[*grpc.ClientConn]*time.Timer),
	}
}

func (p *connPool) get(factory connFactory) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var c *grpc.ClientConn
	var e error
	if len(p.conns) > 0 {
		c, p.conns = p.conns[0], p.conns[1:]
		// Cancel the timer if there is one for this connection.
		timer, ok := p.timers[c]
		if ok {
			timer.Stop()
			delete(p.timers, c)
		}
	} else {
		c, e = factory()
	}
	return c, e
}

func (p *connPool) put(conn *grpc.ClientConn) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.maxConns == 0 || len(p.conns) < p.maxConns {
		p.conns = append(p.conns, conn)
		if p.keepAliveTime > 0 {
			// Start timer to close conn if it's unused for keepAliveTime.
			timer := time.AfterFunc(p.keepAliveTime, p.connExpired(conn))
			p.timers[conn] = timer
		}
	} else {
		return conn.Close()
	}
	return nil
}

func (p *connPool) connExpired(conn *grpc.ClientConn) func() {
	return func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		for i, c := range p.conns {
			if c == conn {
				c.Close()
				p.conns = remove(p.conns, i)
				break
			}
		}
		delete(p.timers, conn)
	}
}

func (p *connPool) close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		if err := c.Close(); err != nil {
			return err
		}
	}
	p.conns = make([]*grpc.ClientConn, 0)
	for c, timer := range p.timers {
		timer.Stop()
		delete(p.timers, c)
	}
	return nil
}

func remove(conns []*grpc.ClientConn, i int) []*grpc.ClientConn {
	conns[len(conns)-1], conns[i] = conns[i], conns[len(conns)-1]
	return conns[:len(conns)-1]
}
