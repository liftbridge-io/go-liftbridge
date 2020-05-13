package liftbridge

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestConnPoolMaxConns(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	p := newConnPool(2, 5*time.Second)
	conns := []*conn{}
	invoked := 0
	factory := func() (*conn, error) {
		invoked++
		grpcConn, err := grpc.Dial(fmt.Sprintf("localhost:%d", port), grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		c := newConn(grpcConn)
		conns = append(conns, c)
		return c, nil
	}

	require.Equal(t, 0, len(p.conns))

	c1, err := p.get(factory)
	require.NoError(t, err)
	if conns[0] != c1 {
		t.Fatal("Incorrect conn returned by get")
	}

	c2, err := p.get(factory)
	require.NoError(t, err)
	if conns[1] != c2 {
		t.Fatal("Incorrect conn returned by get")
	}

	c3, err := p.get(factory)
	require.NoError(t, err)
	if conns[2] != c3 {
		t.Fatal("Incorrect conn returned by get")
	}

	require.NoError(t, p.put(c1))
	require.Equal(t, 1, len(p.conns))

	require.NoError(t, p.put(c2))
	require.Equal(t, 2, len(p.conns))

	require.NoError(t, p.put(c3))
	require.Equal(t, 2, len(p.conns))

	require.Equal(t, 3, invoked)

	require.NoError(t, p.close())
	require.Equal(t, 0, len(p.conns))
}

func TestConnPoolReuse(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	p := newConnPool(2, 400*time.Millisecond)
	conns := []*conn{}
	invoked := 0
	factory := func() (*conn, error) {
		invoked++
		grpcConn, err := grpc.Dial(fmt.Sprintf("localhost:%d", port), grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		c := newConn(grpcConn)
		conns = append(conns, c)
		return c, nil
	}

	require.Equal(t, 0, len(p.conns))

	c1, err := p.get(factory)
	require.NoError(t, err)
	if conns[0] != c1 {
		t.Fatal("Incorrect conn returned by get")
	}

	require.NoError(t, p.put(c1))
	require.Equal(t, 1, len(p.conns))

	c2, err := p.get(factory)
	require.NoError(t, err)
	if c2 != c1 {
		t.Fatal("Incorrect conn returned by get")
	}
	require.Equal(t, 0, len(p.conns))

	require.NoError(t, p.put(c1))
	p.mu.Lock()
	require.Equal(t, 1, len(p.conns))
	require.Equal(t, 1, len(p.timers))
	p.mu.Unlock()

	// Wait for conn to expire.
	time.Sleep(500 * time.Millisecond)
	p.mu.Lock()
	require.Equal(t, 0, len(p.conns))
	require.Equal(t, 0, len(p.timers))
	p.mu.Unlock()

	require.Equal(t, 1, invoked)

	require.NoError(t, p.close())
}
