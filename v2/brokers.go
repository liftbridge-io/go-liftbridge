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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ackReceivedFunc func(*proto.PublishResponse)

type brokerStatus struct {
	PartitionCount   int32
	LastKnownLatency time.Duration
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

	return broker.grpcClient.Client(), nil
}

func (b *brokers) ChooseBroker(selectionCriteria SelectionCriteria) (proto.APIClient, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.brokers) == 0 {
		return nil, errors.New("no brokers")
	}
	// Initiate a random broker
	broker := b.brokers[rand.Intn(len(b.brokers))]

	// Choose brokers based on criteria
	switch selectionCriteria {
	case Latency:
		minLatency := -1

		// Find server with lowest latency
		for i := 0; i < len(b.brokers); i++ {
			status := b.brokers[i].Status()
			if i == 0 {
				minLatency = int(status.LastKnownLatency)
				broker = b.brokers[i]
				continue
			}
			if int(status.LastKnownLatency) < minLatency {
				minLatency = int(status.LastKnownLatency)
				broker = b.brokers[i]
			}

		}
		return broker.grpcClient.Client(), nil
	case Workload:
		// Find server with lowest work load
		minPartitionCount := -1

		for i := 0; i < len(b.brokers); i++ {
			status := b.brokers[i].Status()
			if i == 0 {
				minPartitionCount = int(status.PartitionCount)
				broker = b.brokers[i]
				continue
			}
			if int(b.brokers[i].status.PartitionCount) < minPartitionCount {
				minPartitionCount = int(status.PartitionCount)
				broker = b.brokers[i]
			}

		}
		return broker.grpcClient.Client(), nil
	case Random:
		// Return the current broker (randomly chosen)
		return broker.grpcClient.Client(), nil
	default:
		// Return the current broker (randomly chosen)
		return broker.grpcClient.Client(), nil
	}

}

// grpcClient wraps a gRPC APIClient and API_PublishAsyncClient.
type grpcClient struct {
	addr        string
	dialOpts    []grpc.DialOption
	conn        *grpc.ClientConn
	client      proto.APIClient
	asyncClient proto.API_PublishAsyncClient
	mu          sync.RWMutex
	closed      bool
}

func newGrpcClient(ctx context.Context, addr string, opts []grpc.DialOption) (*grpcClient, error) {
	g := &grpcClient{addr: addr, dialOpts: opts}
	if err := g.redial(ctx); err != nil {
		return nil, err
	}
	return g, nil
}

func (g *grpcClient) redial(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return errors.New("client was closed")
	}
	oldConn := g.conn
	conn, err := dialBroker(ctx, g.addr, g.dialOpts)
	if err != nil {
		return err
	}
	newClient := proto.NewAPIClient(conn)
	newAsyncClient, err := newClient.PublishAsync(ctx)
	if err != nil {
		conn.Close()
		return err
	}
	g.conn = conn
	g.client = newClient
	g.asyncClient = newAsyncClient
	if oldConn != nil {
		oldConn.Close()
	}
	return nil
}

func (g *grpcClient) close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return
	}
	g.conn.Close()
	g.closed = true
}

func (g *grpcClient) Client() proto.APIClient {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.client
}

func (g *grpcClient) AsyncClient() proto.API_PublishAsyncClient {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.asyncClient
}

// GetGrpcClient returns a grpcClient based on a stream name and a partition.
func (b *brokers) GetGrpcClient(stream string, partition int32) (*grpcClient, error) {
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

	return broker.grpcClient, nil
}

func brokerHashringKey(stream string, partition int32) string {
	return fmt.Sprintf("%s:%d", stream, partition)
}

// broker represents a connection to a broker.
type broker struct {
	grpcClient *grpcClient
	wg         sync.WaitGroup
	status     *brokerStatus
	closed     chan struct{}
	mu         sync.RWMutex
}

func newBroker(ctx context.Context, addr string, opts []grpc.DialOption, ackReceived ackReceivedFunc) (*broker, error) {
	client, err := newGrpcClient(ctx, addr, opts)
	if err != nil {
		return nil, err
	}

	b := &broker{
		grpcClient: client,
		status:     &brokerStatus{PartitionCount: 0, LastKnownLatency: 0},
		closed:     make(chan struct{}),
	}

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.dispatchAcks(ackReceived)
	}()

	if err := b.updateStatus(ctx, addr); err != nil {
		return nil, err
	}

	return b, nil
}

func (b *broker) updateStatus(ctx context.Context, addr string) error {
	// Measure instant server response time
	start := time.Now()

	resp, err := b.grpcClient.Client().FetchMetadata(ctx, &proto.FetchMetadataRequest{})

	elapsed := time.Since(start)

	if err != nil {
		return err
	}

	// Parse broker status
	updatedStatus := &brokerStatus{LastKnownLatency: elapsed}

	// Count total number of partitions for this broker
	for _, broker := range resp.Brokers {
		brokerInfo := &BrokerInfo{
			id:             broker.Id,
			host:           broker.Host,
			port:           broker.Port,
			leaderCount:    broker.LeaderCount,
			partitionCount: broker.PartitionCount,
		}
		if brokerInfo.Addr() == addr {
			updatedStatus.PartitionCount = brokerInfo.LeaderCount() + brokerInfo.PartitionCount()
			break
		}

	}

	b.mu.Lock()
	b.status = updatedStatus
	b.mu.Unlock()

	return nil
}

func (b *broker) Status() *brokerStatus {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.status
}

func (b *broker) Close() {
	select {
	case <-b.closed:
		return
	default:
	}
	b.grpcClient.close()
	close(b.closed)
	b.wg.Wait()
}

func (b *broker) dispatchAcks(ackReceived ackReceivedFunc) {
	stream := b.grpcClient.AsyncClient()
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			// Check if the broker connection has been closed.
			select {
			case <-b.closed:
				return
			default:
			}
			if status.Code(err) == codes.Unavailable {
				// Attempt to reconnect.
				if err := b.reconnect(); err == nil {
					stream = b.grpcClient.AsyncClient()
					continue
				}
			}
			return
		}
		ackReceived(resp)
	}
}

func (b *broker) reconnect() error {
	b.mu.RLock()
	var (
		err error
		ctx = context.Background()
	)
	b.mu.RUnlock()
	for i := 0; i < 5; i++ {
		if er := b.grpcClient.redial(ctx); er != nil {
			err = er
			sleepContext(ctx, 50*time.Millisecond)
			continue
		}
		return nil
	}
	return err
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
