package liftbridge

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	proto "github.com/liftbridge-io/liftbridge-api/go"
)

// StreamInfo contains information for a Liftbridge stream.
type StreamInfo struct {
	subject      string
	name         string
	partitions   map[int32]*PartitionInfo
	creationTime time.Time
}

// Subject returns the stream's subject.
func (s *StreamInfo) Subject() string {
	return s.subject
}

// Name returns the stream's name.
func (s *StreamInfo) Name() string {
	return s.name
}

// GetPartition returns the partition info for the given partition id or nil if
// no such partition exists.
func (s *StreamInfo) GetPartition(id int32) *PartitionInfo {
	return s.partitions[id]
}

// Partitions returns a map containing partition IDs and partitions for the
// stream.
func (s *StreamInfo) Partitions() map[int32]*PartitionInfo {
	return s.partitions
}

// CreationTime returns the time when the stream has been created.
func (s *StreamInfo) CreationTime() time.Time {
	return s.creationTime
}

// PartitionEventTimestamps contains the first and latest times when a partition
// event has occurred.
type PartitionEventTimestamps struct {
	firstTime  time.Time
	latestTime time.Time
}

// FirstTime returns the time when the first event occurred.
func (e PartitionEventTimestamps) FirstTime() time.Time {
	return e.firstTime
}

// LatestTime returns the time when the latest event occurred.
func (e PartitionEventTimestamps) LatestTime() time.Time {
	return e.latestTime
}

// PartitionInfo contains information for a Liftbridge stream partition.
type PartitionInfo struct {
	id                         int32
	leader                     *BrokerInfo
	replicas                   []*BrokerInfo
	isr                        []*BrokerInfo
	highWatermark              int64
	newestOffset               int64
	paused                     bool
	readonly                   bool
	messagesReceivedTimestamps PartitionEventTimestamps
	pauseTimestamps            PartitionEventTimestamps
	readonlyTimestamps         PartitionEventTimestamps
}

// ID of the partition.
func (p *PartitionInfo) ID() int32 {
	return p.id
}

// Replicas returns the list of brokers replicating the partition.
func (p *PartitionInfo) Replicas() []*BrokerInfo {
	return p.replicas
}

// ISR returns the list of replicas currently in the in-sync replica set.
func (p *PartitionInfo) ISR() []*BrokerInfo {
	return p.isr
}

// Leader returns the broker acting as leader for this partition or nil if
// there is no leader.
func (p *PartitionInfo) Leader() *BrokerInfo {
	return p.leader
}

// HighWatermark returns highwatermark of the partition leader
func (p *PartitionInfo) HighWatermark() int64 {
	return p.highWatermark
}

// NewestOffset returns newestoffset of the partition leader
func (p *PartitionInfo) NewestOffset() int64 {
	return p.newestOffset
}

// Paused returns true if this partition is paused.
func (p *PartitionInfo) Paused() bool {
	return p.paused
}

// Readonly returns true if this partition is read-only.
func (p *PartitionInfo) Readonly() bool {
	return p.readonly
}

// MessagesReceivedTimestamps returns the first and latest times a message was
// received on this partition.
func (p *PartitionInfo) MessagesReceivedTimestamps() PartitionEventTimestamps {
	return p.messagesReceivedTimestamps
}

// PauseTimestamps returns the first and latest time this partition was paused
// or resumed.
func (p *PartitionInfo) PauseTimestamps() PartitionEventTimestamps {
	return p.pauseTimestamps
}

// ReadonlyTimestamps returns the first and latest time this partition had its
// read-only status changed.
func (p *PartitionInfo) ReadonlyTimestamps() PartitionEventTimestamps {
	return p.readonlyTimestamps
}

// BrokerInfo contains information for a Liftbridge cluster node.
type BrokerInfo struct {
	id             string
	host           string
	port           int32
	leaderCount    int32
	partitionCount int32
}

// ID of the broker.
func (b *BrokerInfo) ID() string {
	return b.id
}

// Host of the broker server.
func (b *BrokerInfo) Host() string {
	return b.host
}

// Port of the broker server.
func (b *BrokerInfo) Port() int32 {
	return b.port
}

// Addr returns <host>:<port> for the broker server.
func (b *BrokerInfo) Addr() string {
	return fmt.Sprintf("%s:%d", b.host, b.port)
}

// Number of partition leaders exists on this broker.
func (b *BrokerInfo) LeaderCount() int32 {
	return b.leaderCount
}

// Total number of partitions on this broker.
func (b *BrokerInfo) PartitionCount() int32 {
	return b.partitionCount
}

// ConsumerGroupInfo contains information for a consumer group.
type ConsumerGroupInfo struct {
	id          string
	coordinator string
	epoch       uint64
}

// ID of the consumer group.
func (c *ConsumerGroupInfo) ID() string {
	return c.id
}

// Coordinator of the consumer group.
func (c *ConsumerGroupInfo) Coordinator() string {
	return c.coordinator
}

// Epoch of the consumer group.
func (c *ConsumerGroupInfo) Epoch() uint64 {
	return c.epoch
}

// Metadata contains an immutable snapshot of information for a cluster and
// subset of streams.
type Metadata struct {
	lastUpdated time.Time
	brokers     map[string]*BrokerInfo
	addrs       map[string]struct{}
	streams     map[string]*StreamInfo
	groups      map[string]*ConsumerGroupInfo
}

func newMetadata(brokers map[string]*BrokerInfo, streams map[string]*StreamInfo,
	groups map[string]*ConsumerGroupInfo) *Metadata {

	addrs := make(map[string]struct{}, len(brokers))
	for _, broker := range brokers {
		addrs[broker.Addr()] = struct{}{}
	}
	return &Metadata{
		lastUpdated: time.Now(),
		brokers:     brokers,
		addrs:       addrs,
		streams:     streams,
		groups:      groups,
	}
}

// LastUpdated returns the time when this metadata was last updated from the
// server.
func (m *Metadata) LastUpdated() time.Time {
	return m.lastUpdated
}

// Brokers returns a list of the cluster nodes.
func (m *Metadata) Brokers() []*BrokerInfo {
	brokers := make([]*BrokerInfo, 0, len(m.brokers))
	for _, broker := range m.brokers {
		brokers = append(brokers, broker)
	}
	return brokers
}

// Broker returns the broker for the given id or nil if it doesn't exist.
func (m *Metadata) Broker(id string) *BrokerInfo {
	return m.brokers[id]
}

// Addrs returns the list of known broker addresses.
func (m *Metadata) Addrs() []string {
	addrs := make([]string, 0, len(m.addrs))
	for addr := range m.addrs {
		addrs = append(addrs, addr)
	}
	return addrs
}

// GetStream returns the given stream or nil if unknown.
func (m *Metadata) GetStream(name string) *StreamInfo {
	return m.streams[name]
}

// Streams returns the list of known streams.
func (m *Metadata) Streams() []*StreamInfo {
	streams := make([]*StreamInfo, 0, len(m.streams))
	for _, stream := range m.streams {
		streams = append(streams, stream)
	}
	return streams
}

// PartitionCountForStream returns the number of partitions for the given
// stream.
func (m *Metadata) PartitionCountForStream(stream string) int32 {
	info := m.GetStream(stream)
	if info == nil {
		return 0
	}
	return int32(len(info.partitions))
}

// GetConsumerGroup returns the consumer group for the given id or nil if it
// doesn't exist.
func (m *Metadata) GetConsumerGroup(id string) *ConsumerGroupInfo {
	return m.groups[id]
}

// hasStreamMetadata indicates if the Metadata has info for the given stream.
func (m *Metadata) hasStreamMetadata(stream string) bool {
	return m.GetStream(stream) != nil
}

type metadataCache struct {
	mu             sync.RWMutex
	metadata       *Metadata
	bootstrapAddrs []string
	doRPC          func(context.Context, func(proto.APIClient) error) error
}

func newMetadataCache(addrs []string, doRPC func(context.Context, func(proto.APIClient) error) error) *metadataCache {
	return &metadataCache{
		metadata:       &Metadata{},
		bootstrapAddrs: addrs,
		doRPC:          doRPC,
	}
}

// update fetches the latest cluster metadata, including stream and broker
// information.
func (m *metadataCache) update(ctx context.Context, streamNames, groupNames []string,
	replaceCache bool) (*Metadata, error) {

	var resp *proto.FetchMetadataResponse
	if err := m.doRPC(ctx, func(client proto.APIClient) (err error) {
		resp, err = client.FetchMetadata(ctx, &proto.FetchMetadataRequest{
			Streams: streamNames,
			Groups:  groupNames,
		})
		return err
	}); err != nil {
		return nil, err
	}

	brokers := make(map[string]*BrokerInfo, len(resp.Brokers))
	for _, broker := range resp.Brokers {
		brokers[broker.Id] = &BrokerInfo{
			id:             broker.Id,
			host:           broker.Host,
			port:           broker.Port,
			leaderCount:    broker.LeaderCount,
			partitionCount: broker.PartitionCount,
		}
	}

	streams := make(map[string]*StreamInfo)
	for _, streamMetadata := range resp.StreamMetadata {
		stream := &StreamInfo{
			subject:      streamMetadata.Subject,
			name:         streamMetadata.Name,
			partitions:   make(map[int32]*PartitionInfo, len(streamMetadata.Partitions)),
			creationTime: time.Unix(0, streamMetadata.CreationTimestamp),
		}
		for _, partition := range streamMetadata.Partitions {
			replicas := make([]*BrokerInfo, 0, len(partition.Replicas))
			for _, replica := range partition.Replicas {
				replicas = append(replicas, brokers[replica])
			}
			isr := make([]*BrokerInfo, 0, len(partition.Isr))
			for _, replica := range partition.Isr {
				isr = append(isr, brokers[replica])
			}
			stream.partitions[partition.Id] = &PartitionInfo{
				id:                         partition.GetId(),
				leader:                     brokers[partition.Leader],
				replicas:                   replicas,
				isr:                        isr,
				paused:                     partition.Paused,
				readonly:                   partition.Readonly,
				highWatermark:              partition.HighWatermark,
				newestOffset:               partition.NewestOffset,
				messagesReceivedTimestamps: protoToEventTimestamps(partition.GetMessagesReceivedTimestamps()),
				pauseTimestamps:            protoToEventTimestamps(partition.GetPauseTimestamps()),
				readonlyTimestamps:         protoToEventTimestamps(partition.ReadonlyTimestamps),
			}
		}
		streams[stream.name] = stream
	}

	groups := make(map[string]*ConsumerGroupInfo, len(resp.GroupMetadata))
	for _, group := range resp.GroupMetadata {
		groups[group.GroupId] = &ConsumerGroupInfo{
			id:          group.GroupId,
			coordinator: group.Coordinator,
			epoch:       group.Epoch,
		}
	}

	metadata := newMetadata(brokers, streams, groups)

	if replaceCache {
		m.mu.Lock()
		m.metadata = metadata
		m.mu.Unlock()
	}

	return metadata, nil
}

// getAddrs returns a list of all broker addresses.
func (m *metadataCache) getAddrs() []string {
	m.mu.RLock()
	var (
		metadata       = m.metadata
		bootstrapAddrs = m.bootstrapAddrs
	)
	m.mu.RUnlock()
	addrs := metadata.Addrs()
	for _, addr := range bootstrapAddrs {
		addrs = append(addrs, addr)
	}
	return addrs
}

// getAddr returns the broker address for the given stream partition.
func (m *metadataCache) getAddrForPartition(stream string, partitionID int32, readISRReplica bool) (string, error) {
	m.mu.RLock()
	metadata := m.metadata
	m.mu.RUnlock()
	st := metadata.GetStream(stream)
	if st == nil {
		return "", errors.New("no known stream")
	}
	partition := st.GetPartition(partitionID)
	if partition == nil {
		return "", errors.New("no known partition")
	}
	// Request to subscribe to a random ISR
	if readISRReplica {
		replicasISR := partition.ISR()
		randomReplica := replicasISR[rand.Intn(len(replicasISR))]
		return randomReplica.Addr(), nil
	}
	if partition.Leader() == nil {
		return "", errors.New("no known leader for partition")
	}
	return partition.Leader().Addr(), nil
}

// getAddrForBroker returns the address for the given broker id.
func (m *metadataCache) getAddrForBroker(id string) (string, error) {
	m.mu.RLock()
	metadata := m.metadata
	m.mu.RUnlock()
	broker := metadata.Broker(id)
	if broker == nil {
		return "", fmt.Errorf("no known broker for id %s", id)
	}
	return broker.Addr(), nil
}

// get returns the current Metadata.
func (m *metadataCache) get() *Metadata {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.metadata
}
