// Package liftbridge implements a client for the Liftbridge messaging system.
// Liftbridge provides lightweight, fault-tolerant message streams by
// implementing a durable stream augmentation NATS. In particular, it offers a
// publish-subscribe log API that is highly available and horizontally
// scalable.
//
// This package provides APIs for creating and consuming Liftbridge streams and
// some utility APIs for using Liftbridge in combination with NATS.
package liftbridge

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	proto "github.com/liftbridge-io/liftbridge-api/go"
)

// MaxReplicationFactor can be used to tell the server to set the replication
// factor equal to the current number of servers in the cluster when creating a
// stream.
const MaxReplicationFactor int32 = -1

// StartPosition controls where to begin consuming in a stream.
type StartPosition int32

func (s StartPosition) toProto() proto.StartPosition {
	return proto.StartPosition(s)
}

const (
	defaultMaxConnsPerBroker   = 2
	defaultKeepAliveTime       = 30 * time.Second
	defaultResubscribeWaitTime = 30 * time.Second
)

var (
	// ErrStreamExists is returned by CreateStream if the specified stream
	// already exists in the Liftbridge cluster.
	ErrStreamExists = errors.New("stream already exists")

	// ErrNoSuchStream is returned by DeleteStream if the specified stream does
	// not exist in the Liftbridge cluster.
	ErrNoSuchStream = errors.New("stream does not exist")

	// ErrNoSuchPartition is returned by Subscribe if the specified stream
	// partition does not exist in the Liftbridge cluster.
	ErrNoSuchPartition = errors.New("stream partition does not exist")

	// ErrStreamDeleted is sent to subscribers when the stream they are
	// subscribed to has been deleted.
	ErrStreamDeleted = errors.New("stream has been deleted")

	// ErrPartitionPaused is sent to subscribers when the stream partition they
	// are subscribed to has been paused.
	ErrPartitionPaused = errors.New("stream partition has been paused")
)

// Handler is the callback invoked by Subscribe when a message is received on
// the specified stream. If err is not nil, the subscription will be terminated
// and no more messages will be received.
type Handler func(msg *Message, err error)

// StreamOptions are used to configure new streams.
type StreamOptions struct {
	// Group is the name of a load-balance group. When there are multiple
	// streams in the same group, messages will be balanced among them.
	Group string

	// ReplicationFactor controls the number of servers to replicate a stream
	// to. E.g. a value of 1 would mean only 1 server would have the data, and
	// a value of 3 would be 3 servers would have it. If this is not set, it
	// defaults to 1. A value of -1 will signal to the server to set the
	// replication factor equal to the current number of servers in the
	// cluster.
	ReplicationFactor int32

	// Partitions determines how many partitions to create for a stream. If 0,
	// this will behave as a stream with a single partition. If this is not
	// set, it defaults to 1.
	Partitions int32
}

// StreamOption is a function on the StreamOptions for a stream. These are used
// to configure particular stream options.
type StreamOption func(*StreamOptions) error

// Group is a StreamOption to set the load-balance group for a stream. When
// there are multiple streams in the same group, messages will be balanced
// among them.
func Group(group string) StreamOption {
	return func(o *StreamOptions) error {
		o.Group = group
		return nil
	}
}

// ReplicationFactor is a StreamOption to set the replication factor for a
// stream. The replication factor controls the number of servers to replicate a
// stream to. E.g. a value of 1 would mean only 1 server would have the data,
// and a value of 3 would be 3 servers would have it. If this is not set, it
// defaults to 1. A value of -1 will signal to the server to set the
// replication factor equal to the current number of servers in the cluster.
func ReplicationFactor(replicationFactor int32) StreamOption {
	return func(o *StreamOptions) error {
		o.ReplicationFactor = replicationFactor
		return nil
	}
}

// MaxReplication is a StreamOption to set the stream replication factor equal
// to the current number of servers in the cluster.
func MaxReplication() StreamOption {
	return func(o *StreamOptions) error {
		o.ReplicationFactor = MaxReplicationFactor
		return nil
	}
}

// Partitions is a StreamOption to set the number of partitions for a stream.
// Partitions are ordered, replicated, and durably stored on disk and serve as
// the unit of storage and parallelism for a stream. A partitioned stream for
// NATS subject "foo.bar" with three partitions internally maps to the NATS
// subjects "foo.bar", "foo.bar.1", and "foo.bar.2". A single partition would
// map to "foo.bar" to match behavior of an "un-partitioned" stream. If this is
// not set, it defaults to 1.
func Partitions(partitions int32) StreamOption {
	return func(o *StreamOptions) error {
		if partitions < 0 {
			return fmt.Errorf("invalid number of partitions: %d", partitions)
		}
		o.Partitions = partitions
		return nil
	}
}

// Client is the main API used to communicate with a Liftbridge cluster. Call
// Connect to get a Client instance.
type Client interface {
	// Close the client connection.
	Close() error

	// CreateStream creates a new stream attached to a NATS subject. Subject is
	// the NATS subject the stream is attached to, and name is the stream
	// identifier, unique per subject. It returns ErrStreamExists if a stream
	// with the given subject and name already exists.
	CreateStream(ctx context.Context, subject, name string, opts ...StreamOption) error

	// DeleteStream deletes a stream and all of its partitions. Name is the
	// stream identifier, globally unique.
	DeleteStream(ctx context.Context, name string) error

	// PauseStream pauses a stream and some or all of its partitions. Name is
	// the stream identifier, globally unique. It returns an ErrNoSuchPartition
	// if the given stream or partition does not exist. By default, this will
	// pause all partitions. A partition is resumed when it is published to via
	// the Liftbridge Publish API or ResumeAll is enabled and another partition
	// in the stream is published to.
	PauseStream(ctx context.Context, name string, opts ...PauseOption) error

	// Subscribe creates an ephemeral subscription for the given stream. It
	// begins receiving messages starting at the configured position and waits
	// for new messages when it reaches the end of the stream. The default
	// start position is the end of the stream. It returns an
	// ErrNoSuchPartition if the given stream or partition does not exist. Use
	// a cancelable Context to close a subscription.
	Subscribe(ctx context.Context, stream string, handler Handler, opts ...SubscriptionOption) error

	// Publish publishes a new message to the Liftbridge stream. The partition
	// that gets published to is determined by the provided partition or
	// Partitioner passed through MessageOptions, if any. If a partition or
	// Partitioner is not provided, this defaults to the base partition. This
	// partition determines the underlying NATS subject that gets published to.
	// To publish directly to a specific NATS subject, use the low-level
	// PublishToSubject API.
	//
	// If the AckPolicy is not NONE and a deadline is provided, this will
	// synchronously block until the ack is received. If the ack is not
	// received in time, a DeadlineExceeded status code is returned. If an
	// AckPolicy and deadline are configured, this returns the Ack on success,
	// otherwise it returns nil.
	Publish(ctx context.Context, stream string, value []byte, opts ...MessageOption) (*Ack, error)

	// PublishToSubject publishes a new message to the NATS subject. Note that
	// because this publishes directly to a subject, there may be multiple (or
	// no) streams that receive the message. As a result, MessageOptions
	// related to partitioning will be ignored. To publish at the
	// stream/partition level, use the high-level Publish API.
	//
	// If the AckPolicy is not NONE and a deadline is provided, this will
	// synchronously block until the first ack is received. If an ack is not
	// received in time, a DeadlineExceeded status code is returned. If an
	// AckPolicy and deadline are configured, this returns the first Ack on
	// success, otherwise it returns nil.
	PublishToSubject(ctx context.Context, subject string, value []byte, opts ...MessageOption) (*Ack, error)

	// FetchMetadata returns cluster metadata including broker and stream
	// information.
	FetchMetadata(ctx context.Context) (*Metadata, error)
}

// client implements the Client interface. It maintains a pool of connections
// for each broker in the cluster, limiting the number of connections and
// closing them when they go unused for a prolonged period of time.
type client struct {
	mu        sync.RWMutex
	apiClient proto.APIClient
	conn      *grpc.ClientConn
	metadata  *metadataCache
	pools     map[string]*connPool
	opts      ClientOptions
	dialOpts  []grpc.DialOption
	closed    bool
}

// ClientOptions are used to control the Client configuration.
type ClientOptions struct {
	// Brokers it the set of hosts the client will use when attempting to
	// connect.
	Brokers []string

	// MaxConnsPerBroker is the maximum number of connections to pool for a
	// given broker in the cluster. The default is 2.
	MaxConnsPerBroker int

	// KeepAliveTime is the amount of time a pooled connection can be idle
	// before it is closed and removed from the pool. The default is 30
	// seconds.
	KeepAliveTime time.Duration

	// TLSCert is the TLS certificate file to use. The client does not use a
	// TLS connection if this is not set.
	TLSCert string

	// TLSConfig is the TLS configuration to use. The client does not use a
	// TLS connection if this is not set. Overrides TLSCert if set.
	TLSConfig *tls.Config

	// ResubscribeWaitTime is the amount of time to attempt to re-establish a
	// stream subscription after being disconnected. For example, if the server
	// serving a subscription dies and the stream is replicated, the client
	// will attempt to re-establish the subscription once the stream leader has
	// failed over. This failover can take several moments, so this option
	// gives the client time to retry. The default is 30 seconds.
	ResubscribeWaitTime time.Duration
}

// Connect will attempt to connect to a Liftbridge server with multiple
// options.
func (o ClientOptions) Connect() (Client, error) {
	if len(o.Brokers) == 0 {
		return nil, errors.New("no addresses provided")
	}
	var (
		conn *grpc.ClientConn
		err  error
		opts = []grpc.DialOption{}
	)

	if o.TLSConfig != nil {
		// Setup TLS configuration if it is provided.
		creds := credentials.NewTLS(o.TLSConfig)
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else if o.TLSCert != "" {
		// Setup TLS credentials if cert is provided.
		creds, err := credentials.NewClientTLSFromFile(o.TLSCert, "")
		if err != nil {
			return nil, fmt.Errorf("could not load tls cert: %s", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		// Otherwise use an insecure connection.
		opts = append(opts, grpc.WithInsecure())
	}

	perm := rand.Perm(len(o.Brokers))
	for _, i := range perm {
		addr := o.Brokers[i]
		conn, err = grpc.Dial(addr, opts...)
		if err == nil {
			break
		}
	}
	if conn == nil {
		return nil, err
	}

	c := &client{
		conn:      conn,
		apiClient: proto.NewAPIClient(conn),
		pools:     make(map[string]*connPool),
		opts:      o,
		dialOpts:  opts,
	}
	c.metadata = newMetadataCache(o.Brokers, c.doResilientRPC)
	if _, err := c.metadata.update(context.Background()); err != nil {
		return nil, err
	}
	return c, nil
}

// DefaultClientOptions returns the default configuration options for the
// client.
func DefaultClientOptions() ClientOptions {
	return ClientOptions{
		MaxConnsPerBroker:   defaultMaxConnsPerBroker,
		KeepAliveTime:       defaultKeepAliveTime,
		ResubscribeWaitTime: defaultResubscribeWaitTime,
	}
}

// ClientOption is a function on the ClientOptions for a connection. These are
// used to configure particular client options.
type ClientOption func(*ClientOptions) error

// MaxConnsPerBroker is a ClientOption to set the maximum number of connections
// to pool for a given broker in the cluster. The default is 2.
func MaxConnsPerBroker(max int) ClientOption {
	return func(o *ClientOptions) error {
		o.MaxConnsPerBroker = max
		return nil
	}
}

// KeepAliveTime is a ClientOption to set the amount of time a pooled
// connection can be idle before it is closed and removed from the pool. The
// default is 30 seconds.
func KeepAliveTime(keepAlive time.Duration) ClientOption {
	return func(o *ClientOptions) error {
		o.KeepAliveTime = keepAlive
		return nil
	}
}

// TLSCert is a ClientOption to set the TLS certificate for the client.
func TLSCert(cert string) ClientOption {
	return func(o *ClientOptions) error {
		o.TLSCert = cert
		return nil
	}
}

// TLSConfig is a ClientOption to set the TLS configuration for the client.
// Overrides TLSCert.
func TLSConfig(config *tls.Config) ClientOption {
	return func(o *ClientOptions) error {
		o.TLSConfig = config
		return nil
	}
}

// ResubscribeWaitTime is a ClientOption to set the amount of time to attempt
// to re-establish a stream subscription after being disconnected. For example,
// if the server serving a subscription dies and the stream is replicated, the
// client will attempt to re-establish the subscription once the stream leader
// has failed over. This failover can take several moments, so this option
// gives the client time to retry. The default is 30 seconds.
func ResubscribeWaitTime(wait time.Duration) ClientOption {
	return func(o *ClientOptions) error {
		o.ResubscribeWaitTime = wait
		return nil
	}
}

// Connect creates a Client connection for the given Liftbridge cluster.
// Multiple addresses can be provided. Connect will use whichever it connects
// successfully to first in random order. The Client will use the pool of
// addresses for failover purposes. Note that only one seed address needs to be
// provided as the Client will discover the other brokers when fetching
// metadata for the cluster.
func Connect(addrs []string, options ...ClientOption) (Client, error) {
	opts := DefaultClientOptions()
	opts.Brokers = addrs
	for _, opt := range options {
		if err := opt(&opts); err != nil {
			return nil, err
		}
	}
	return opts.Connect()
}

// Close the client connection.
func (c *client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	for _, pool := range c.pools {
		if err := pool.close(); err != nil {
			return err
		}
	}
	if err := c.conn.Close(); err != nil {
		return err
	}
	c.closed = true
	return nil
}

// CreateStream creates a new stream attached to a NATS subject. Subject is the
// NATS subject the stream is attached to, and name is the stream identifier,
// unique per subject. It returns ErrStreamExists if a stream with the given
// subject and name already exists.
func (c *client) CreateStream(ctx context.Context, subject, name string, options ...StreamOption) error {
	opts := &StreamOptions{}
	for _, opt := range options {
		if err := opt(opts); err != nil {
			return err
		}
	}

	req := &proto.CreateStreamRequest{
		Subject:           subject,
		Name:              name,
		ReplicationFactor: opts.ReplicationFactor,
		Group:             opts.Group,
		Partitions:        opts.Partitions,
	}
	err := c.doResilientRPC(func(client proto.APIClient) error {
		_, err := client.CreateStream(ctx, req)
		return err
	})
	if status.Code(err) == codes.AlreadyExists {
		return ErrStreamExists
	}
	return err
}

// DeleteStream deletes a stream and all of its partitions. Name is the stream
// identifier, globally unique.
func (c *client) DeleteStream(ctx context.Context, name string) error {
	req := &proto.DeleteStreamRequest{Name: name}
	err := c.doResilientRPC(func(client proto.APIClient) error {
		_, err := client.DeleteStream(ctx, req)
		return err
	})
	if status.Code(err) == codes.NotFound {
		return ErrNoSuchStream
	}
	return err
}

// PauseOptions are used to setup stream pausing.
type PauseOptions struct {
	// Partitions sets the list of partitions to pause or all of them if
	// nil/empty.
	Partitions []int32

	// ResumeAll will resume all partitions in the stream if one of them is
	// published to instead of resuming only that partition.
	ResumeAll bool
}

// PauseOption is a function on the PauseOptions for a pause call. These are
// used to configure particular pausing options.
type PauseOption func(*PauseOptions) error

// PausePartitions sets the list of partition to pause or all of them if
// nil/empty.
func PausePartitions(partitions ...int32) PauseOption {
	return func(o *PauseOptions) error {
		o.Partitions = partitions
		return nil
	}
}

// ResumeAll will resume all partitions in the stream if one of them is
// published to instead of resuming only that partition.
func ResumeAll() PauseOption {
	return func(o *PauseOptions) error {
		o.ResumeAll = true
		return nil
	}
}

// PauseStream pauses a stream and some or all of its partitions. Name is the
// stream identifier, globally unique. It returns an ErrNoSuchPartition if the
// given stream or partition does not exist. By default, this will pause all
// partitions. A partition is resumed when it is published to via the
// Liftbridge Publish API or ResumeAll is enabled and another partition in the
// stream is published to.
func (c *client) PauseStream(ctx context.Context, name string, options ...PauseOption) error {
	opts := &PauseOptions{}
	for _, opt := range options {
		if err := opt(opts); err != nil {
			return err
		}
	}

	req := &proto.PauseStreamRequest{
		Name:       name,
		Partitions: opts.Partitions,
		ResumeAll:  opts.ResumeAll,
	}
	err := c.doResilientRPC(func(client proto.APIClient) error {
		_, err := client.PauseStream(ctx, req)
		return err
	})
	if status.Code(err) == codes.NotFound {
		return ErrNoSuchPartition
	}
	return err
}

// SubscriptionOptions are used to control a subscription's behavior.
type SubscriptionOptions struct {
	// StartPosition controls where to begin consuming from in the stream.
	StartPosition StartPosition

	// StartOffset sets the stream offset to begin consuming from.
	StartOffset int64

	// StartTimestamp sets the stream start position to the given timestamp.
	StartTimestamp time.Time

	// Partition sets the stream partition to consume.
	Partition int32

	// ReadISRReplica sets client's ability to subscribe from a random ISR
	ReadISRReplica bool
}

// SubscriptionOption is a function on the SubscriptionOptions for a
// subscription. These are used to configure particular subscription options.
type SubscriptionOption func(*SubscriptionOptions) error

// StartAtOffset sets the desired start offset to begin consuming from in the
// stream.
func StartAtOffset(offset int64) SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.StartPosition = StartPosition(proto.StartPosition_OFFSET)
		o.StartOffset = offset
		return nil
	}
}

// StartAtTime sets the desired timestamp to begin consuming from in the
// stream.
func StartAtTime(start time.Time) SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.StartPosition = StartPosition(proto.StartPosition_TIMESTAMP)
		o.StartTimestamp = start
		return nil
	}
}

// StartAtTimeDelta sets the desired timestamp to begin consuming from in the
// stream using a time delta in the past.
func StartAtTimeDelta(ago time.Duration) SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.StartPosition = StartPosition(proto.StartPosition_TIMESTAMP)
		o.StartTimestamp = time.Now().Add(-ago)
		return nil
	}
}

// StartAtLatestReceived sets the subscription start position to the last
// message received in the stream.
func StartAtLatestReceived() SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.StartPosition = StartPosition(proto.StartPosition_LATEST)
		return nil
	}
}

// StartAtEarliestReceived sets the subscription start position to the earliest
// message received in the stream.
func StartAtEarliestReceived() SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.StartPosition = StartPosition(proto.StartPosition_EARLIEST)
		return nil
	}
}

// ReadISRReplica sets read replica option. If true, the client will request
// subscription from an random ISR replica instead of subscribing explicitly
// to partition's leader. As a random ISR replica is given, it may well be the
// partition's leader itself.
func ReadISRReplica() SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.ReadISRReplica = true
		return nil
	}
}

// Partition specifies the stream partition to consume. If not set, this
// defaults to 0.
func Partition(partition int32) SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		if partition < 0 {
			return fmt.Errorf("invalid partition: %d", partition)
		}
		o.Partition = partition
		return nil
	}
}

// Subscribe creates an ephemeral subscription for the given stream. It begins
// receiving messages starting at the configured position and waits for new
// messages when it reaches the end of the stream. The default start position
// is the end of the stream. It returns an ErrNoSuchPartition if the given
// stream or partition does not exist. Use a cancelable Context to close a
// subscription.
func (c *client) Subscribe(ctx context.Context, streamName string, handler Handler,
	options ...SubscriptionOption) (err error) {

	opts := &SubscriptionOptions{}
	for _, opt := range options {
		if err := opt(opts); err != nil {
			return err
		}
	}

	stream, releaseConn, err := c.subscribe(ctx, streamName, opts)
	if err != nil {
		return err
	}

	go c.dispatchStream(ctx, streamName, stream, releaseConn, handler)
	return nil
}

// Publish publishes a new message to the Liftbridge stream. The partition that
// gets published to is determined by the provided partition or Partitioner
// passed through MessageOptions, if any. If a partition or Partitioner is not
// provided, this defaults to the base partition. This partition determines the
// underlying NATS subject that gets published to.  To publish directly to a
// spedcific NATS subject, use the low-level PublishToSubject API.
//
// If the AckPolicy is not NONE and a deadline is provided, this will
// synchronously block until the ack is received. If the ack is not received in
// time, a DeadlineExceeded status code is returned. If an AckPolicy and
// deadline are configured, this returns the Ack on success, otherwise it
// returns nil.
func (c *client) Publish(ctx context.Context, stream string, value []byte,
	options ...MessageOption) (*Ack, error) {

	opts := &MessageOptions{Headers: make(map[string][]byte)}
	for _, opt := range options {
		opt(opts)
	}

	// Determine which partition to publish to.
	partition, err := c.partition(ctx, stream, opts.Key, value, opts)
	if err != nil {
		return nil, err
	}

	req := &proto.PublishRequest{
		Stream:        stream,
		Partition:     partition,
		Key:           opts.Key,
		Value:         value,
		AckInbox:      opts.AckInbox,
		CorrelationId: opts.CorrelationID,
		AckPolicy:     opts.AckPolicy.toProto(),
	}

	var ack *proto.Ack
	err = c.doResilientRPC(func(client proto.APIClient) error {
		resp, err := client.Publish(ctx, req)
		if err == nil {
			ack = resp.Ack
		}
		return err
	})
	return ackFromProto(ack), err
}

// PublishToSubject publishes a new message to the NATS subject. Note that
// because this publishes directly to a subject, there may be multiple (or no)
// streams that receive the message. As a result, MessageOptions related to
// partitioning will be ignored. To publish at the stream/partition level, use
// the high-level Publish API.
//
// If the AckPolicy is not NONE and a deadline is provided, this will
// synchronously block until the first ack is received. If an ack is not
// received in time, a DeadlineExceeded status code is returned. If an
// AckPolicy and deadline are configured, this returns the first Ack on
// success, otherwise it returns nil.
func (c *client) PublishToSubject(ctx context.Context, subject string, value []byte,
	options ...MessageOption) (*Ack, error) {

	opts := &MessageOptions{Headers: make(map[string][]byte)}
	for _, opt := range options {
		opt(opts)
	}

	req := &proto.PublishToSubjectRequest{
		Subject:       subject,
		Key:           opts.Key,
		Value:         value,
		AckInbox:      opts.AckInbox,
		CorrelationId: opts.CorrelationID,
		AckPolicy:     opts.AckPolicy.toProto(),
	}

	var ack *proto.Ack
	err := c.doResilientRPC(func(client proto.APIClient) error {
		resp, err := client.PublishToSubject(ctx, req)
		if err == nil {
			ack = resp.Ack
		}
		return err
	})
	return ackFromProto(ack), err
}

// FetchMetadata returns cluster metadata including broker and stream
// information.
func (c *client) FetchMetadata(ctx context.Context) (*Metadata, error) {
	return c.metadata.update(ctx)
}

// partition determines the partition ID to publish the message to. If a
// partition was explicitly provided, it will be returned. If a Partitioner was
// provided, it will be used to compute the partition. Otherwise, 0 will be
// returned.
func (c *client) partition(ctx context.Context, stream string, key, value []byte,
	opts *MessageOptions) (int32, error) {

	var partition int32
	// If a partition is explicitly provided, use it.
	if opts.Partition != nil {
		partition = *opts.Partition
	} else if opts.Partitioner != nil {
		// Make sure we have metadata for the stream and, if not, update it.
		metadata, err := c.waitForStreamMetadata(ctx, stream)
		if err != nil {
			return 0, err
		}
		partition = opts.Partitioner.Partition(stream, key, value, metadata)
	}
	return partition, nil
}

func (c *client) waitForStreamMetadata(ctx context.Context, stream string) (*Metadata, error) {
	for i := 0; i < 5; i++ {
		metadata := c.metadata.get()
		if metadata.hasStreamMetadata(stream) {
			return metadata, nil
		}
		time.Sleep(50 * time.Millisecond)
		c.metadata.update(ctx)
	}
	return nil, fmt.Errorf("no metadata for stream %s", stream)
}

func (c *client) subscribe(ctx context.Context, stream string,
	opts *SubscriptionOptions) (proto.API_SubscribeClient, func(), error) {
	var (
		pool *connPool
		addr string
		conn *grpc.ClientConn
		st   proto.API_SubscribeClient
		err  error
	)
	for i := 0; i < 5; i++ {
		pool, addr, err = c.getPoolAndAddr(stream, opts.Partition, opts.ReadISRReplica)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			c.metadata.update(ctx)
			continue
		}
		conn, err = pool.get(c.connFactory(addr))
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			c.metadata.update(ctx)
			continue
		}
		var (
			client = proto.NewAPIClient(conn)
			req    = &proto.SubscribeRequest{
				Stream:         stream,
				StartPosition:  opts.StartPosition.toProto(),
				StartOffset:    opts.StartOffset,
				StartTimestamp: opts.StartTimestamp.UnixNano(),
				Partition:      opts.Partition,
				ReadISRReplica: opts.ReadISRReplica,
			}
		)
		st, err = client.Subscribe(ctx, req)
		if err != nil {
			if status.Code(err) == codes.Unavailable {
				time.Sleep(50 * time.Millisecond)
				c.metadata.update(ctx)
				continue
			}
			return nil, nil, err
		}

		// The server will either send an empty message, indicating the
		// subscription was successfully created, or an error.
		_, err = st.Recv()
		if status.Code(err) == codes.FailedPrecondition {
			// This indicates the server was not the stream leader. Refresh
			// metadata and retry after waiting a bit.
			time.Sleep(time.Duration(10+i*50) * time.Millisecond)
			c.metadata.update(ctx)
			continue
		}
		if err != nil {
			if status.Code(err) == codes.NotFound {
				err = ErrNoSuchPartition
			}
			return nil, nil, err
		}
		return st, func() { pool.put(conn) }, nil
	}
	return nil, nil, err
}

func (c *client) dispatchStream(ctx context.Context, streamName string,
	stream proto.API_SubscribeClient, releaseConn func(), handler Handler) {

	defer releaseConn()
	var (
		lastOffset  int64
		lastError   error
		resubscribe bool
		closed      bool
	)
LOOP:
	for {
		var (
			msg, err = stream.Recv()
			code     = status.Code(err)
		)
		if msg != nil {
			lastOffset = msg.Offset
		}
		if err != nil {
			lastError = err
		}
		if err == nil || (err != nil && code != codes.Canceled) {
			switch code {
			case codes.Unavailable:
				// This indicates the server went away or the connection was
				// closed. Attempt to resubscribe to the stream leader starting
				// at the last received offset unless the connection has been
				// closed.
				c.mu.RLock()
				closed = c.closed
				c.mu.RUnlock()
				if !closed {
					resubscribe = true
				}
				break LOOP
			case codes.NotFound:
				// Indicates the stream was deleted.
				err = ErrStreamDeleted
			case codes.FailedPrecondition:
				// Indicates the partition was paused.
				err = ErrPartitionPaused
			}
			handler(messageFromProto(msg), err)
		}
		if err != nil {
			break
		}
	}

	// Attempt to resubscribe to the stream leader starting at the last
	// received offset. Do this in a loop with a backoff since it may take
	// some time for the leader to failover.
	if resubscribe {
		deadline := time.Now().Add(c.opts.ResubscribeWaitTime)
		for time.Now().Before(deadline) && !closed {
			err := c.Subscribe(ctx, streamName, handler, StartAtOffset(lastOffset+1))
			if err == nil {
				return
			}
			time.Sleep(time.Second + (time.Duration(rand.Intn(500)) * time.Millisecond))
			c.mu.RLock()
			closed = c.closed
			c.mu.RUnlock()
		}
		handler(nil, lastError)
	}
}

// connFactory returns a pool connFactory for the given address. The
// connFactory dials the address to create a gRPC ClientConn.
func (c *client) connFactory(addr string) connFactory {
	return func() (*grpc.ClientConn, error) {
		return grpc.Dial(addr, c.dialOpts...)
	}
}

// getPoolAndAddr returns the connPool and broker address for the given
// partition.
func (c *client) getPoolAndAddr(stream string, partition int32, readISRReplica bool) (*connPool, string, error) {
	addr, err := c.metadata.getAddr(stream, partition, readISRReplica)
	if err != nil {
		return nil, "", err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	pool, ok := c.pools[addr]
	if !ok {
		pool = newConnPool(c.opts.MaxConnsPerBroker, c.opts.KeepAliveTime)
		c.pools[addr] = pool
	}
	return pool, addr, nil
}

// doResilientRPC executes the given RPC and performs retries if it fails due
// to the broker being unavailable, cycling through the known broker list.
func (c *client) doResilientRPC(rpc func(client proto.APIClient) error) (err error) {
	c.mu.RLock()
	client := c.apiClient
	c.mu.RUnlock()

	for i := 0; i < 10; i++ {
		err = rpc(client)
		if status.Code(err) == codes.Unavailable {
			conn, err := c.dialBroker()
			if err != nil {
				return err
			}
			client = proto.NewAPIClient(conn)
			c.mu.Lock()
			c.apiClient = client
			c.conn.Close()
			c.conn = conn
			c.mu.Unlock()
		} else {
			break
		}
	}
	return
}

// dialBroker dials each broker in the cluster, in random order, returning a
// gRPC ClientConn to the first one that is successful.
func (c *client) dialBroker() (*grpc.ClientConn, error) {
	var (
		conn  *grpc.ClientConn
		err   error
		addrs = c.metadata.getAddrs()
		perm  = rand.Perm(len(addrs))
	)
	for _, i := range perm {
		conn, err = grpc.Dial(addrs[i], c.dialOpts...)
		if err == nil {
			break
		}
	}
	if conn == nil {
		return nil, err
	}
	return conn, nil
}
