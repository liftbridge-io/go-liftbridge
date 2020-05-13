package liftbridge

import (
	"sync"
	"time"

	proto "github.com/liftbridge-io/liftbridge-api/go"
	"google.golang.org/grpc"
)

// connFactory creates a Liftbridge conn.
type connFactory func() (*conn, error)

// conn is a gRPC client and its associated connection for communicating with a
// Liftbridge server.
type conn struct {
	proto.APIClient
	clientConn *grpc.ClientConn
}

func newConn(grpcConn *grpc.ClientConn) *conn {
	return &conn{
		APIClient:  proto.NewAPIClient(grpcConn),
		clientConn: grpcConn,
	}
}

func (c *conn) Close() error {
	return c.clientConn.Close()
}

// connPool maintains a pool of Liftbridge conns. It limits the number of
// connections based on maxConns and closes unused connections based on
// keepAliveTime.
type connPool struct {
	mu            sync.Mutex
	conns         []*conn
	maxConns      int
	keepAliveTime time.Duration
	timers        map[*conn]*time.Timer
}

// newConnPool creates a new connPool with the given maxConns and keepAliveTime
// settings. The maxConn setting caps the number of connections created. The
// keepAliveTime setting determines how long to wait before closing unused
// connections.
func newConnPool(maxConns int, keepAliveTime time.Duration) *connPool {
	return &connPool{
		maxConns:      maxConns,
		keepAliveTime: keepAliveTime,
		conns:         make([]*conn, 0, maxConns),
		timers:        make(map[*conn]*time.Timer),
	}
}

// get returns a Liftbridge conn either from the pool, if any, or by using the
// providing connFactory.
func (p *connPool) get(factory connFactory) (*conn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var c *conn
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

// put returns the given Liftbridge conn to the pool if there is capacity or
// closes it if there is not.
func (p *connPool) put(conn *conn) error {
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

// connExpired is called when the keepAliveTime timer has fired for the given
// connection. This will close and remove the connection from the pool.
func (p *connPool) connExpired(conn *conn) func() {
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

// close cleans up the connPool by closing all active connections and stopping
// all timers.
func (p *connPool) close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		if err := c.Close(); err != nil {
			return err
		}
	}
	p.conns = make([]*conn, 0)
	for c, timer := range p.timers {
		timer.Stop()
		delete(p.timers, c)
	}
	return nil
}

// remove returns the slice with the given index removed.
func remove(conns []*conn, i int) []*conn {
	conns[len(conns)-1], conns[i] = conns[i], conns[len(conns)-1]
	return conns[:len(conns)-1]
}
