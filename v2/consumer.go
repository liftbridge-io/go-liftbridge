package liftbridge

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nats-io/nuid"

	proto "github.com/liftbridge-io/liftbridge-api/go"
)

func defaultFetchAssignmentsInterval(timeout time.Duration) time.Duration {
	return time.Duration(0.7 * float64(timeout))
}

// ConsumerOptions are used to configure new consumers.
type ConsumerOptions struct {
	// ConsumerID uniquely identifies a logical consumer. This ID should not be
	// reused across consumers. If a ConsumerID is not supplied, a random one
	// will be generated.
	ConsumerID string

	// AutoCheckpoint determines if the consumer's position should be
	// automatically checkpointed.
	AutoCheckpoint bool

	// FetchAssignmentsInterval is a function which returns the frequency to
	// fetch partition assignments from the consumer group coordinator. This
	// also acts as a health check to keep the consumer active in the group.
	// Increasing this too much may cause the group coordinator to think the
	// consumer has failed and remove it from the group. The function argument
	// is the timeout duration configured on the server. If not set, this will
	// default to 0.7 * timeout.
	FetchAssignmentsInterval func(timeout time.Duration) time.Duration
}

// ConsumerOption is a function on the ConsumerOptions for a consumer. These
// are used to configure particular consumer options.
type ConsumerOption func(*ConsumerOptions) error

// AutoCheckpoint determines if the consumer's position should be automatically
// checkpointed.
func AutoCheckpoint(enabled bool) ConsumerOption {
	return func(o *ConsumerOptions) error {
		o.AutoCheckpoint = enabled
		return nil
	}
}

// ConsumerID uniquely identifies a logical consumer. This ID should not be
// reused across consumers. If a ConsumerID is not supplied, a random one
// will be generated.
func ConsumerID(id string) ConsumerOption {
	return func(o *ConsumerOptions) error {
		if id == "" {
			return errors.New("invalid consumer id")
		}
		o.ConsumerID = id
		return nil
	}
}

type streamSubscriptions map[int32]context.CancelFunc

// Consumer is a member of a consumer group. Consumer groups provide an API to
// better facilitate the consumption of Liftbridge streams. This encompasses
// several different but related goals:
// 1) Provide a mechanism for clients to track their position in a stream
// automatically, i.e. "durable" consumers.
// 2) Provide a mechanism for distributed, fault-tolerant stream consumption.
// 3) Provide a mechanism for coordinating and balancing stream consumption by
// managing partition assignments for consumers.
// 4) Provide a mechanism for consuming multiple streams in aggregate.
type Consumer struct {
	opts          *ConsumerOptions
	groupID       string
	client        *client
	closed        chan struct{}
	mu            sync.RWMutex
	subscriptions map[string]streamSubscriptions
	ctx           context.Context
	cancelCtx     context.CancelFunc
}

func (c *client) newConsumer(groupID string, options []ConsumerOption) (*Consumer, error) {
	if groupID == "" {
		return nil, errors.New("invalid consumer group id")
	}
	opts := &ConsumerOptions{FetchAssignmentsInterval: defaultFetchAssignmentsInterval}
	for _, opt := range options {
		if err := opt(opts); err != nil {
			return nil, err
		}
	}
	if opts.ConsumerID == "" {
		opts.ConsumerID = nuid.Next()
	}

	cons := &Consumer{
		opts:          opts,
		groupID:       groupID,
		client:        c,
		closed:        make(chan struct{}),
		subscriptions: make(map[string]streamSubscriptions),
	}
	return cons, nil
}

// Subscribe begins consuming from assigned partitions. If no partitions are
// assigned to this consumer, this will wait for partitions to be assigned.
func (c *Consumer) Subscribe(ctx context.Context, streams []string, handler Handler) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ctx != nil {
		return errors.New("subscribe has already been called - cancel previous Context before calling Subscribe again")
	}

	joinReq := &proto.JoinConsumerGroupRequest{
		GroupId:    c.groupID,
		ConsumerId: c.opts.ConsumerID,
		Streams:    streams,
	}
	var resp *proto.JoinConsumerGroupResponse
	err := c.client.doResilientRPC(ctx, func(client proto.APIClient) error {
		r, err := client.JoinConsumerGroup(ctx, joinReq)
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	if err != nil {
		return err
	}

	interval := c.opts.FetchAssignmentsInterval(time.Duration(resp.ConsumersTimeout))

	var client proto.APIClient
	for i := 0; i < 5; i++ {
		client, err = c.client.getAPIClientForBroker(resp.Coordinator)
		if err != nil {
			sleepContext(ctx, 50*time.Millisecond)
			c.client.updateMetadata(ctx)
			continue
		}
		c.ctx, c.cancelCtx = context.WithCancel(ctx)
		go c.consumerLoop(c.ctx, client, interval, resp.CoordinatorEpoch, handler)
		return nil
	}
	return err
}

// Close the consumer and remove them from the consumer group.
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	select {
	case <-c.closed:
		return nil
	default:
	}

	if c.ctx != nil {
		c.cancelCtx()
		c.ctx = nil
	}

	// TODO make RPC to leave group

	close(c.closed)
	c.subscriptions = make(map[string]streamSubscriptions)
	return nil
}

func (c *Consumer) consumerLoop(ctx context.Context, client proto.APIClient,
	interval time.Duration, coordinatorEpoch uint64, handler Handler) {

	for {
		req := &proto.FetchConsumerGroupAssignmentsRequest{
			GroupId:          c.groupID,
			ConsumerId:       c.opts.ConsumerID,
			CoordinatorEpoch: coordinatorEpoch,
		}
		var resp *proto.FetchConsumerGroupAssignmentsResponse
		err := c.client.doResilientRPC(ctx, func(client proto.APIClient) error {
			r, err := client.FetchConsumerGroupAssignments(ctx, req)
			if err != nil {
				return err
			}
			resp = r
			return nil
		})
		if err != nil {
			// TODO: need to handle this somehow
			// extend FetchMetadata to include info on consumer groups
		}

		c.reconcileSubscriptions(ctx, resp.Assignments, handler)

		select {
		case <-c.closed:
			return
		case <-time.After(interval):
		}
	}
}

func (c *Consumer) reconcileSubscriptions(
	ctx context.Context, assignments []*proto.PartitionAssignment, handler Handler) {

	assignmentsMap := make(map[string]map[int32]struct{}, len(assignments))
	for _, assignment := range assignments {
		m := make(map[int32]struct{}, len(assignment.Partitions))
		for _, partition := range assignment.Partitions {
			m[partition] = struct{}{}
		}
		assignmentsMap[assignment.Stream] = m
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Cancel all subscriptions that we no longer have assignments for.
	for stream, subscriptions := range c.subscriptions {
		for partition, cancel := range subscriptions {
			streamAssignments, ok := assignmentsMap[stream]
			if !ok {
				cancel()
				delete(subscriptions, partition)
			}
			if _, ok := streamAssignments[partition]; !ok {
				cancel()
				delete(subscriptions, partition)
			}
			if len(subscriptions) == 0 {
				delete(c.subscriptions, stream)
			}
		}
	}

	// Ensure we have subscriptions for partitions we do have assignments for.
	for stream, assignments := range assignmentsMap {
		subscriptions, ok := c.subscriptions[stream]
		if !ok {
			subscriptions = make(streamSubscriptions)
			c.subscriptions[stream] = subscriptions
		}
		for partition := range assignments {
			// Check if subscription already exists for partition.
			if _, ok := subscriptions[partition]; ok {
				continue
			}
			// Otherwise set up a new subscription.
			cancel, err := c.subscribeToPartition(ctx, stream, partition, handler)
			if err != nil {
				// TODO: should we wrap this error?
				go handler(nil, err)
				continue
			}
			subscriptions[partition] = cancel
		}
	}
}

func (c *Consumer) subscribeToPartition(ctx context.Context, stream string, partition int32, handler Handler) (
	context.CancelFunc, error) {

	cursor, err := c.client.FetchCursor(ctx, c.groupID, stream, partition)
	if err != nil {
		return nil, err
	}
	if cursor == -1 {
		// Cursor doesn't exist. Use default start behavior.
		// TODO
	}
	subCtx, cancel := context.WithCancel(ctx)
	if err := c.client.Subscribe(subCtx, stream, handler, Partition(partition), StartAtOffset(cursor)); err != nil {
		cancel()
		return nil, err
	}

	return cancel, nil
}
