package liftbridge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	proto "github.com/liftbridge-io/liftbridge-api/go"
	"github.com/serialx/hashring"
	"google.golang.org/grpc"
)

type ackReceivedFunc func(*proto.PublishResponse)

type brokerStatus struct {
	ConnectionCount int
	// In Milisecond
	LastKnownLatency float64
}

// brokers represents a collection of connections to brokers.
type brokers struct {
	mu          sync.RWMutex
	brokers     []*broker
	brokersMap  map[string]*broker
	ring        *hashring.HashRing
	opts        []grpc.DialOption
	ackReceived ackReceivedFunc
}

func newBrokers(ctx context.Context, addrs []string, opts []grpc.DialOption, ackReceived ackReceivedFunc) (*brokers, error) {
	brokersSlice := make([]*broker, 0, len(addrs))
	brokersMap := make(map[string]*broker, len(addrs))

	// Ensure all connections are closed if an error occurs.
	connectedBrokers := make([]*broker, 0, len(addrs))
	defer func() {
		for _, broker := range connectedBrokers {
			broker.Close()
		}
	}()

	for _, addr := range addrs {
		broker, err := newBroker(ctx, addr, opts, ackReceived)
		if err != nil {
			return nil, err
		}
		brokersSlice = append(brokersSlice, broker)
		connectedBrokers = append(connectedBrokers, broker)
		brokersMap[addr] = broker
	}

	// All brokers are connected, disable deferred cleanup.
	connectedBrokers = nil

	return &brokers{
		brokers:     brokersSlice,
		brokersMap:  brokersMap,
		ring:        hashring.New(addrs),
		opts:        opts,
		ackReceived: ackReceived,
	}, nil
}

// Close closes all connections to the brokers.
func (b *brokers) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, c := range b.brokers {
		c.Close()
	}
}

// Update updates the connections to the brokers by closing any connection to a
// broker that has been removed and opening connections to new brokers.
func (b *brokers) Update(ctx context.Context, addrs []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Store the new brokers in a map so we can close removed brokers.
	newBrokersSlice := make([]*broker, 0)
	newBrokersMap := make(map[string]*broker)
	for _, addr := range addrs {
		existingBroker, found := b.brokersMap[addr]
		if found {
			newBrokersMap[addr] = existingBroker
			newBrokersSlice = append(newBrokersSlice, existingBroker)
		} else {
			broker, err := newBroker(ctx, addr, b.opts, b.ackReceived)
			if err != nil {
				return err
			}
			newBrokersMap[addr] = broker
			newBrokersSlice = append(newBrokersSlice, broker)
		}
	}

	for addr, broker := range b.brokersMap {
		_, found := newBrokersMap[addr]
		if !found {
			// Close any removed broker.
			broker.Close()
		}
	}

	b.brokers = newBrokersSlice
	b.brokersMap = newBrokersMap
	b.ring = hashring.New(addrs)

	return nil
}

// FromAddr returns an API client to a broker using its address.
func (b *brokers) FromAddr(addr string) (proto.APIClient, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	broker, found := b.brokersMap[addr]
	if !found {
		return nil, fmt.Errorf("no broker found: %v", addr)
	}

	return broker.client, nil
}

func (b *brokers) ChooseBroker() (proto.APIClient, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.brokers) == 0 {
		return nil, errors.New("no borkers")
	}

	broker := b.brokers[rand.Intn(len(b.brokers))]
	return broker.client, nil
}

// PublicationStream returns a publication stream based on a stream name and a
// partition.
func (b *brokers) PublicationStream(stream string, partition int32) (proto.API_PublishAsyncClient, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := brokerHashringKey(stream, partition)
	addr, ok := b.ring.GetNode(key)
	if !ok {
		return nil, errors.New("no brokers")
	}

	broker, ok := b.brokersMap[addr]
	if !ok {
		return nil, fmt.Errorf("broker not found: %v", addr)
	}

	return broker.stream, nil
}

func brokerHashringKey(stream string, partition int32) string {
	return fmt.Sprintf("%s:%d", stream, partition)
}

// broker represents a connection to a broker.
type broker struct {
	conn   *grpc.ClientConn
	client proto.APIClient
	stream proto.API_PublishAsyncClient
	wg     sync.WaitGroup
	status *brokerStatus
}

func newBroker(ctx context.Context, addr string, opts []grpc.DialOption, ackReceived ackReceivedFunc) (*broker, error) {
	conn, err := dialBroker(ctx, addr, opts)
	if err != nil {
		return nil, err
	}

	b := &broker{
		conn:   conn,
		client: proto.NewAPIClient(conn),
		status: &brokerStatus{ConnectionCount: 0, LastKnownLatency: 0},
	}

	if b.stream, err = b.client.PublishAsync(ctx); err != nil {
		conn.Close()
		return nil, err
	}

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.dispatchAcks(ackReceived)
	}()

	if err := b.updateStatus(ctx); err != nil {
		return nil, err
	}

	return b, nil
}

func (b *broker) updateStatus(ctx context.Context) error {
	// Measure instant server response time
	start := time.Now()

	_, err := b.client.FetchMetadata(ctx, &proto.FetchMetadataRequest{})

	elapsed := time.Now().Sub(start)

	if err != nil {
		return err
	}

	// TODO: parse connection count from metadata
	b.status.LastKnownLatency = float64(elapsed.Milliseconds())

	return nil

}

func (b *broker) Close() {
	b.conn.Close()
	b.wg.Wait()
}

func (b *broker) dispatchAcks(ackReceived ackReceivedFunc) {
	for {
		resp, err := b.stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			// TODO: reconnect?
			return
		}
		ackReceived(resp)
	}
}

func dialBroker(ctx context.Context, addr string, opts []grpc.DialOption) (*grpc.ClientConn, error) {
	// Perform a blocking dial if a context with a deadline has been provided.
	_, hasDeadline := ctx.Deadline()
	if hasDeadline {
		opts = append(opts, grpc.WithBlock())
	}
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
