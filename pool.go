package liftbridge

import (
	"google.golang.org/grpc"
	"sync"
)

type connFactory func() (*grpc.ClientConn, error)

type connPool struct {
	mu       sync.Mutex
	conns    []*grpc.ClientConn
	maxConns int
}

func newConnPool(maxConns int) *connPool {
	return &connPool{maxConns: maxConns, conns: make([]*grpc.ClientConn, 0, maxConns)}
}

func (p *connPool) get(factory connFactory) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var c *grpc.ClientConn
	var e error
	if len(p.conns) > 0 {
		c, p.conns = p.conns[0], p.conns[1:]
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
	} else {
		return conn.Close()
	}
	return nil
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
	return nil
}
