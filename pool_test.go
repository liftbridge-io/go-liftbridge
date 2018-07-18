package liftbridge

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/liftbridge-io/liftbridge/server"
	natsdTest "github.com/nats-io/gnatsd/test"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

var storagePath string

func init() {
	tmpDir, err := ioutil.TempDir("", "liftbridge_test_")
	if err != nil {
		panic(fmt.Errorf("Error creating temp dir: %v", err))
	}
	if err := os.Remove(tmpDir); err != nil {
		panic(fmt.Errorf("Error removing temp dir: %v", err))
	}
	storagePath = tmpDir

	// Disable gRPC's logging.
	logger := grpclog.NewLoggerV2(ioutil.Discard, ioutil.Discard, ioutil.Discard)
	grpclog.SetLoggerV2(logger)
}

func cleanupStorage(t *testing.T) {
	err := os.RemoveAll(storagePath)
	require.NoError(t, err)
}

func getTestConfig(id string, bootstrap bool, port int) *server.Config {
	config := server.NewDefaultConfig()
	config.Clustering.RaftBootstrap = bootstrap
	config.DataDir = filepath.Join(storagePath, id)
	config.Clustering.RaftSnapshots = 1
	config.Clustering.RaftLogging = true
	config.LogLevel = uint32(log.DebugLevel)
	config.NATS.Servers = []string{"nats://localhost:4222"}
	config.NoLog = true
	config.Port = port
	return config
}

func runServerWithConfig(t *testing.T, config *server.Config) *server.Server {
	server, err := server.RunServerWithConfig(config)
	require.NoError(t, err)
	return server
}

func TestConnPoolMaxConns(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	config := getTestConfig("a", true, 5050)
	s := runServerWithConfig(t, config)
	defer s.Stop()
	p := newConnPool(2, 5*time.Second)
	conns := []*grpc.ClientConn{}
	invoked := 0
	factory := func() (*grpc.ClientConn, error) {
		invoked++
		c, err := grpc.Dial("localhost:5050", grpc.WithInsecure())
		if err == nil {
			conns = append(conns, c)
		}
		return c, err
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
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	config := getTestConfig("a", true, 5050)
	s := runServerWithConfig(t, config)
	defer s.Stop()
	p := newConnPool(2, 400*time.Millisecond)
	conns := []*grpc.ClientConn{}
	invoked := 0
	factory := func() (*grpc.ClientConn, error) {
		invoked++
		c, err := grpc.Dial("localhost:5050", grpc.WithInsecure())
		if err == nil {
			conns = append(conns, c)
		}
		return c, err
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
	require.Equal(t, 1, len(p.conns))
	require.Equal(t, 1, len(p.timers))

	// Wait for conn to expire.
	time.Sleep(500 * time.Millisecond)
	require.Equal(t, 0, len(p.conns))
	require.Equal(t, 0, len(p.timers))

	require.Equal(t, 1, invoked)

	require.NoError(t, p.close())
}
