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
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/nats-io/nuid"
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
	defaultAckWaitTime         = 5 * time.Second
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

	// ErrAckTimeout indicates a publish ack was not received in time.
	ErrAckTimeout = errors.New("publish ack timeout")
)

// Handler is the callback invoked by Subscribe when a message is received on
// the specified stream. If err is not nil, the subscription will be terminated
// and no more messages will be received.
type Handler func(msg *Message, err error)

// AckHandler is used to handle the results of asynchronous publishes to a
// stream. If the AckPolicy on the published message is not NONE, the handler
// will receive the ack once it's received from the cluster or an error if the
// message was not received successfully.
type AckHandler func(ack *Ack, err error)

// ackContext tracks state for an in-flight message expecting an ack.
type ackContext struct {
	handler AckHandler
	timer   *time.Timer
}

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

	// The maximum size a stream's log can grow to, in bytes, before we will
	// discard old log segments to free up space. A value of 0 indicates no
	// limit. If this is not set, it uses the server default value.
	RetentionMaxBytes *int64

	// The maximum size a stream's log can grow to, in number of messages,
	// before we will discard old log segments to free up space. A value of 0
	// indicates no limit. If this is not set, it uses the server default
	// value.
	RetentionMaxMessages *int64

	// The TTL for stream log segment files, after which they are deleted. A
	// value of 0 indicates no TTL. If this is not set, it uses the server
	// default value.
	RetentionMaxAge *time.Duration

	// The frequency to check if a new stream log segment file should be rolled
	// and whether any segments are eligible for deletion based on the
	// retention policy or compaction if enabled. If this is not set, it uses
	// the server default value.
	CleanerInterval *time.Duration

	// The maximum size of a single stream log segment file in bytes. Retention
	// is always done a file at a time, so a larger segment size means fewer
	// files but less granular control over retention. If this is not set, it
	// uses the server default value.
	SegmentMaxBytes *int64

	// The maximum time before a new stream log segment is rolled out. A value
	// of 0 means new segments will only be rolled when segment.max.bytes is
	// reached. Retention is always done a file at a time, so a larger value
	// means fewer files but less granular control over retention. If this is
	// not set, it uses the server default value.
	SegmentMaxAge *time.Duration

	// The maximum number of concurrent goroutines to use for compaction on a
	// stream log (only applicable if compact.enabled is true). If this is not
	// set, it uses the server default value.
	CompactMaxGoroutines *int32

	// CompactEnabled controls the activation of stream log compaction. If this
	// is not set, it uses the server default value.
	CompactEnabled *bool
}

func (s *StreamOptions) newRequest(subject, name string) *proto.CreateStreamRequest {
	req := &proto.CreateStreamRequest{
		Subject: subject,
		Name:    name,
	}
	req.ReplicationFactor = s.ReplicationFactor
	req.Group = s.Group
	req.Partitions = s.Partitions
	if s.RetentionMaxAge != nil {
		req.RetentionMaxAge = &proto.NullableInt64{Value: s.RetentionMaxAge.Milliseconds()}
	}
	if s.RetentionMaxBytes != nil {
		req.RetentionMaxBytes = &proto.NullableInt64{Value: *s.RetentionMaxBytes}
	}
	if s.RetentionMaxMessages != nil {
		req.RetentionMaxMessages = &proto.NullableInt64{Value: *s.RetentionMaxMessages}
	}
	if s.CleanerInterval != nil {
		req.CleanerInterval = &proto.NullableInt64{Value: s.CleanerInterval.Milliseconds()}
	}
	if s.SegmentMaxBytes != nil {
		req.SegmentMaxBytes = &proto.NullableInt64{Value: *s.SegmentMaxBytes}
	}
	if s.SegmentMaxAge != nil {
		req.SegmentMaxAge = &proto.NullableInt64{Value: s.SegmentMaxAge.Milliseconds()}
	}
	if s.CompactMaxGoroutines != nil {
		req.CompactMaxGoroutines = &proto.NullableInt32{Value: *s.CompactMaxGoroutines}
	}
	if s.CompactEnabled != nil {
		req.CompactEnabled = &proto.NullableBool{Value: *s.CompactEnabled}
	}
	return req
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

// RetentionMaxBytes sets the value of the retention.max.bytes configuration
// for the stream. This controls the maximum size a stream's log can grow to,
// in bytes, before we will discard old log segments to free up space. A value
// of 0 indicates no limit. If this is not set, it uses the server default
// value.
func RetentionMaxBytes(val int64) StreamOption {
	return func(o *StreamOptions) error {
		o.RetentionMaxBytes = &val
		return nil
	}
}

// RetentionMaxMessages sets the value of the retention.max.messages
// configuration for the stream. This controls the maximum size a stream's log
// can grow to, in number of messages, before we will discard old log segments
// to free up space. A value of 0 indicates no limit. If this is not set, it
// uses the server default value.
func RetentionMaxMessages(val int64) StreamOption {
	return func(o *StreamOptions) error {
		o.RetentionMaxMessages = &val
		return nil
	}
}

// RetentionMaxAge sets the value of the retention.max.age configuration for
// the stream. This controls the TTL for stream log segment files, after which
// they are deleted. A value of 0 indicates no TTL. If this is not set, it uses
// the server default value.
func RetentionMaxAge(val time.Duration) StreamOption {
	return func(o *StreamOptions) error {
		o.RetentionMaxAge = &val
		return nil
	}
}

// CleanerInterval sets the value of the cleaner.interval configuration for the
// stream. This controls the frequency to check if a new stream log segment
// file should be rolled and whether any segments are eligible for deletion
// based on the retention policy or compaction if enabled. If this is not set,
// it uses the server default value.
func CleanerInterval(val time.Duration) StreamOption {
	return func(o *StreamOptions) error {
		o.CleanerInterval = &val
		return nil
	}
}

// SegmentMaxBytes sets the value of the segment.max.bytes configuration for
// the stream. This controls the maximum size of a single stream log segment
// file in bytes. Retention is always done a file at a time, so a larger
// segment size means fewer files but less granular control over retention. If
// this is not set, it uses the server default value.
func SegmentMaxBytes(val int64) StreamOption {
	return func(o *StreamOptions) error {
		o.SegmentMaxBytes = &val
		return nil
	}
}

// SegmentMaxAge sets the value of the segment.max.age configuration for the
// stream. Thia controls the maximum time before a new stream log segment is
// rolled out. A value of 0 means new segments will only be rolled when
// segment.max.bytes is reached. Retention is always done a file at a time, so
// a larger value means fewer files but less granular control over retention.
// If this is not set, it uses the server default value.
func SegmentMaxAge(val time.Duration) StreamOption {
	return func(o *StreamOptions) error {
		o.SegmentMaxAge = &val
		return nil
	}
}

// CompactMaxGoroutines sets the value of the compact.max.goroutines
// configuration for the stream. This controls the maximum number of concurrent
// goroutines to use for compaction on a stream log (only applicable if
// compact.enabled is true). If this is not set, it uses the server default
// value.
func CompactMaxGoroutines(val int32) StreamOption {
	return func(o *StreamOptions) error {
		o.CompactMaxGoroutines = &val
		return nil
	}
}

// CompactEnabled sets the value of the compact.enabled configuration for the
// stream. This controls the activation of stream log compaction. If this is
// not set, it uses the server default value.
func CompactEnabled(val bool) StreamOption {
	return func(o *StreamOptions) error {
		o.CompactEnabled = &val
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
	// If the AckPolicy is not NONE, this will synchronously block until the
	// ack is received. If the ack is not received in time, ErrAckTimeout is
	// returned. If AckPolicy is NONE, this returns nil on success.
	Publish(ctx context.Context, stream string, value []byte, opts ...MessageOption) (*Ack, error)

	// PublishAsync publishes a new message to the Liftbridge stream and
	// asynchronously processes the ack or error for the message.
	PublishAsync(ctx context.Context, stream string, value []byte, ackHandler AckHandler, opts ...MessageOption) error

	// PublishToSubject publishes a new message to the NATS subject. Note that
	// because this publishes directly to a subject, there may be multiple (or
	// no) streams that receive the message. As a result, MessageOptions
	// related to partitioning will be ignored. To publish at the
	// stream/partition level, use the high-level Publish API.
	//
	// If the AckPolicy is not NONE and a deadline is provided, this will
	// synchronously block until the first ack is received. If an ack is not
	// received in time, ErrAckTimeout is returned. If an AckPolicy and
	// deadline are configured, this returns the first Ack on success,
	// otherwise it returns nil.
	PublishToSubject(ctx context.Context, subject string, value []byte, opts ...MessageOption) (*Ack, error)

	// FetchMetadata returns cluster metadata including broker and stream
	// information.
	FetchMetadata(ctx context.Context) (*Metadata, error)
}

// client implements the Client interface. It maintains a pool of connections
// for each broker in the cluster, limiting the number of connections and
// closing them when they go unused for a prolonged period of time.
type client struct {
	mu          sync.RWMutex
	conn        *conn
	asyncConn   *conn
	asyncStream proto.API_PublishAsyncClient
	ackContexts map[string]*ackContext
	metadata    *metadataCache
	pools       map[string]*connPool
	opts        ClientOptions
	dialOpts    []grpc.DialOption
	closed      chan struct{}
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

	// AckWaitTime is the default amount of time to wait for an ack to be
	// received for a published message before ErrAckTimeout is returned. This
	// can be overridden on individual requests by setting a timeout on the
	// Context. This defaults to 5 seconds if not set.
	AckWaitTime time.Duration
}

// Connect will attempt to connect to a Liftbridge server with multiple
// options.
func (o ClientOptions) Connect() (Client, error) {
	if len(o.Brokers) == 0 {
		return nil, errors.New("no addresses provided")
	}

	opts := []grpc.DialOption{}
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

	conn, err := dialBroker(o.Brokers, opts)
	if err != nil {
		return nil, err
	}

	asyncConn, err := dialBroker(o.Brokers, opts)
	if err != nil {
		conn.Close()
		return nil, err
	}

	asyncStream, err := asyncConn.PublishAsync(context.Background())
	if err != nil {
		conn.Close()
		asyncConn.Close()
		return nil, err
	}

	c := &client{
		conn:        conn,
		asyncConn:   asyncConn,
		asyncStream: asyncStream,
		pools:       make(map[string]*connPool),
		opts:        o,
		dialOpts:    opts,
		ackContexts: make(map[string]*ackContext),
		closed:      make(chan struct{}),
	}
	c.metadata = newMetadataCache(o.Brokers, c.doResilientRPC)
	if _, err := c.metadata.update(context.Background()); err != nil {
		return nil, err
	}
	go c.dispatchAcks()
	return c, nil
}

// DefaultClientOptions returns the default configuration options for the
// client.
func DefaultClientOptions() ClientOptions {
	return ClientOptions{
		MaxConnsPerBroker:   defaultMaxConnsPerBroker,
		KeepAliveTime:       defaultKeepAliveTime,
		ResubscribeWaitTime: defaultResubscribeWaitTime,
		AckWaitTime:         defaultAckWaitTime,
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

// AckWaitTime is a ClientOption to set the default amount of time to wait for
// an ack to be received for a published message before ErrAckTimeout is
// returned. This can be overridden on individual requests by setting a timeout
// on the Context. This defaults to 5 seconds if not set.
func AckWaitTime(wait time.Duration) ClientOption {
	return func(o *ClientOptions) error {
		o.AckWaitTime = wait
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
	select {
	case <-c.closed:
		return nil
	default:
	}
	for _, pool := range c.pools {
		if err := pool.close(); err != nil {
			return err
		}
	}
	if err := c.conn.Close(); err != nil {
		return err
	}
	close(c.closed)
	return nil
}

func (c *client) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
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

	req := opts.newRequest(subject, name)
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

	// ReadISRReplica sets client's ability to subscribe from a random ISR.
	ReadISRReplica bool

	// Resume controls if a paused partition can be resumed before
	// subscription.
	Resume bool
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

// Resume controls if a paused partition can be resumed before subscription. If
// true, subscribing to a paused partition will resume it before subscribing to
// it instead of failing.
func Resume() SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.Resume = true
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
// If the AckPolicy is not NONE, this will synchronously block until the ack is
// received. If the ack is not received in time, ErrAckTimeout is returned. If
// AckPolicy is NONE, this returns nil on success.
func (c *client) Publish(ctx context.Context, stream string, value []byte,
	options ...MessageOption) (*Ack, error) {

	opts := &MessageOptions{Headers: make(map[string][]byte)}
	for _, opt := range options {
		opt(opts)
	}

	if opts.AckPolicy == AckPolicy(proto.AckPolicy_NONE) {
		// Fire and forget.
		err := c.publishAsync(ctx, stream, value, nil, opts)
		return nil, err
	}

	// Publish and wait for ack.
	var (
		ackCh   = make(chan *Ack, 1)
		errorCh = make(chan error, 1)
	)
	err := c.publishAsync(ctx, stream, value, func(ack *Ack, err error) {
		if err != nil {
			errorCh <- err
			return
		}
		ackCh <- ack
	}, opts)
	if err != nil {
		return nil, err
	}

	select {
	case ack := <-ackCh:
		return ack, nil
	case err := <-errorCh:
		return nil, err
	}
}

// PublishAsync publishes a new message to the Liftbridge stream and
// asynchronously processes the ack or error for the message.
func (c *client) PublishAsync(ctx context.Context, stream string, value []byte,
	ackHandler AckHandler, options ...MessageOption) error {

	opts := &MessageOptions{Headers: make(map[string][]byte)}
	for _, opt := range options {
		opt(opts)
	}
	return c.publishAsync(ctx, stream, value, ackHandler, opts)
}

func (c *client) publishAsync(ctx context.Context, stream string, value []byte,
	ackHandler AckHandler, opts *MessageOptions) error {

	if opts.CorrelationID == "" {
		opts.CorrelationID = nuid.Next()
	}

	req, err := c.newPublishRequest(ctx, stream, value, opts)
	if err != nil {
		return err
	}

	c.mu.Lock()
	asyncStream := c.asyncStream
	if ackHandler != nil {
		// Setup ack timeout.
		var (
			timeout      = c.opts.AckWaitTime
			deadline, ok = ctx.Deadline()
		)
		if ok {
			timeout = time.Until(deadline)
		}
		ack := &ackContext{
			handler: ackHandler,
			timer: time.AfterFunc(timeout, func() {
				ackCtx := c.removeAckContext(req.CorrelationId)
				// Ack was processed before timeout finished.
				if ackCtx == nil {
					return
				}
				if ackCtx.handler != nil {
					ackCtx.handler(nil, ErrAckTimeout)
				}
			}),
		}
		c.ackContexts[req.CorrelationId] = ack
	}
	c.mu.Unlock()

	if err := asyncStream.Send(req); err != nil {
		c.removeAckContext(req.CorrelationId)
		return err
	}

	return nil
}

// PublishToSubject publishes a new message to the NATS subject. Note that
// because this publishes directly to a subject, there may be multiple (or no)
// streams that receive the message. As a result, MessageOptions related to
// partitioning will be ignored. To publish at the stream/partition level, use
// the high-level Publish API.
//
// If the AckPolicy is not NONE and a deadline is provided, this will
// synchronously block until the first ack is received. If an ack is not
// received in time, ErrAckTimeout is returned. If an AckPolicy and deadline
// are configured, this returns the first Ack on success, otherwise it returns
// nil.
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

	// Setup ack timeout.
	var (
		cancel func()
		_, ok  = ctx.Deadline()
	)
	if !ok {
		ctx, cancel = context.WithTimeout(ctx, c.opts.AckWaitTime)
		defer cancel()
	}

	var ack *proto.Ack
	err := c.doResilientRPC(func(client proto.APIClient) error {
		resp, err := client.PublishToSubject(ctx, req)
		if err == nil {
			ack = resp.Ack
		}
		return err
	})
	if status.Code(err) == codes.DeadlineExceeded {
		err = ErrAckTimeout
	}
	return ackFromProto(ack), err
}

// FetchMetadata returns cluster metadata including broker and stream
// information.
func (c *client) FetchMetadata(ctx context.Context) (*Metadata, error) {
	return c.metadata.update(ctx)
}

func (c *client) removeAckContext(cid string) *ackContext {
	var timer *time.Timer
	c.mu.Lock()
	ctx := c.ackContexts[cid]
	if ctx != nil {
		timer = ctx.timer
		delete(c.ackContexts, cid)
	}
	c.mu.Unlock()
	// Cancel ack timeout if any.
	if timer != nil {
		timer.Stop()
	}
	return ctx
}

func (c *client) dispatchAcks() {
	c.mu.RLock()
	asyncStream := c.asyncStream
	c.mu.RUnlock()
	for {
		resp, err := asyncStream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			stream, ok := c.newAsyncStream()
			if !ok {
				return
			}
			asyncStream = stream
			c.mu.Lock()
			c.asyncStream = stream
			c.mu.Unlock()
			continue
		}

		ctx := c.removeAckContext(resp.Ack.CorrelationId)
		if ctx != nil && ctx.handler != nil {
			ctx.handler(ackFromProto(resp.Ack), nil)
		}
	}
}

func (c *client) newAsyncStream() (stream proto.API_PublishAsyncClient, ok bool) {
	for {
		err := c.doResilientRPC(func(client proto.APIClient) error {
			resp, err := client.PublishAsync(context.Background())
			if err != nil {
				return err
			}
			stream = resp
			return nil
		})
		if err == nil {
			return stream, true
		}
		if c.isClosed() {
			return nil, false
		}
		time.Sleep(50 * time.Millisecond)
	}
}
func (c *client) newPublishRequest(ctx context.Context, stream string, value []byte,
	opts *MessageOptions) (*proto.PublishRequest, error) {

	// Determine which partition to publish to.
	partition, err := c.partition(ctx, stream, opts.Key, value, opts)
	if err != nil {
		return nil, err
	}

	return &proto.PublishRequest{
		Stream:        stream,
		Partition:     partition,
		Key:           opts.Key,
		Value:         value,
		AckInbox:      opts.AckInbox,
		CorrelationId: opts.CorrelationID,
		AckPolicy:     opts.AckPolicy.toProto(),
	}, nil
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
		conn *conn
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
			req = &proto.SubscribeRequest{
				Stream:         stream,
				StartPosition:  opts.StartPosition.toProto(),
				StartOffset:    opts.StartOffset,
				StartTimestamp: opts.StartTimestamp.UnixNano(),
				Partition:      opts.Partition,
				ReadISRReplica: opts.ReadISRReplica,
				Resume:         opts.Resume,
			}
		)
		st, err = conn.Subscribe(ctx, req)
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
				closed = c.isClosed()
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
			closed = c.isClosed()
			c.mu.RUnlock()
		}
		handler(nil, lastError)
	}
}

// connFactory returns a pool connFactory for the given address. The
// connFactory dials the address to create a Liftbridge conn.
func (c *client) connFactory(addr string) connFactory {
	return func() (*conn, error) {
		grpcConn, err := grpc.Dial(addr, c.dialOpts...)
		if err != nil {
			return nil, err
		}
		return newConn(grpcConn), nil
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
	conn := c.conn
	c.mu.RUnlock()

	for i := 0; i < 10; i++ {
		err = rpc(conn)
		if status.Code(err) == codes.Unavailable {
			conn, err := c.dialBroker()
			if err != nil {
				return err
			}
			c.mu.Lock()
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
// Liftbridge conn for the first one that is successful.
func (c *client) dialBroker() (*conn, error) {
	return dialBroker(c.metadata.getAddrs(), c.dialOpts)
}

// dialBroker dials each broker in the list of addresses, in random order,
// returning a Liftbridge conn for the first one that is successful.
func dialBroker(addrs []string, opts []grpc.DialOption) (*conn, error) {
	var (
		grpcConn *grpc.ClientConn
		err      error
		perm     = rand.Perm(len(addrs))
	)
	for _, i := range perm {
		grpcConn, err = grpc.Dial(addrs[i], opts...)
		if err == nil {
			break
		}
	}
	if grpcConn == nil {
		return nil, err
	}
	return newConn(grpcConn), nil
}
