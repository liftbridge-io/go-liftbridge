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
	subject    string
	name       string
	partitions map[int32]*PartitionInfo
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

// PartitionInfo contains information for a Liftbridge stream partition.
type PartitionInfo struct {
	id       int32
	leader   *BrokerInfo
	replicas []*BrokerInfo
	isr      []*BrokerInfo
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

// BrokerInfo contains information for a Liftbridge cluster node.
type BrokerInfo struct {
	id   string
	host string
	port int32
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

// Metadata contains an immutable snapshot of information for a cluster and
// subset of streams.
type Metadata struct {
	lastUpdated time.Time
	brokers     map[string]*BrokerInfo
	addrs       map[string]struct{}
	streams     map[string]*StreamInfo
}

func newMetadata(brokers map[string]*BrokerInfo, streams map[string]*StreamInfo) *Metadata {
	addrs := make(map[string]struct{}, len(brokers))
	for _, broker := range brokers {
		addrs[broker.Addr()] = struct{}{}
	}
	return &Metadata{
		lastUpdated: time.Now(),
		brokers:     brokers,
		addrs:       addrs,
		streams:     streams,
	}
}

// LastUpdated returns the time when this metadata was last updated from the
// server.
func (m *Metadata) LastUpdated() time.Time {
	return m.lastUpdated
}

// Brokers returns a list of the cluster nodes.
func (m *Metadata) Brokers() []*BrokerInfo {
	var (
		brokers = make([]*BrokerInfo, len(m.brokers))
		i       = 0
	)
	for _, broker := range m.brokers {
		brokers[i] = broker
		i++
	}
	return brokers
}

// Addrs returns the list of known broker addresses.
func (m *Metadata) Addrs() []string {
	var (
		addrs = make([]string, len(m.addrs))
		i     = 0
	)
	for addr := range m.addrs {
		addrs[i] = addr
		i++
	}
	return addrs
}

// GetStream returns the given stream or nil if unknown.
func (m *Metadata) GetStream(name string) *StreamInfo {
	return m.streams[name]
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

// hasStreamMetadata indicates if the Metadata has info for the given stream.
func (m *Metadata) hasStreamMetadata(stream string) bool {
	return m.GetStream(stream) != nil
}

type metadataCache struct {
	mu             sync.RWMutex
	metadata       *Metadata
	bootstrapAddrs []string
	doRPC          func(func(proto.APIClient) error) error
}

func newMetadataCache(addrs []string, doRPC func(func(proto.APIClient) error) error) *metadataCache {
	return &metadataCache{
		metadata:       &Metadata{},
		bootstrapAddrs: addrs,
		doRPC:          doRPC,
	}
}

// update fetches the latest cluster metadata, including stream and broker
// information.
func (m *metadataCache) update(ctx context.Context) (*Metadata, error) {
	var resp *proto.FetchMetadataResponse
	if err := m.doRPC(func(client proto.APIClient) (err error) {
		resp, err = client.FetchMetadata(ctx, &proto.FetchMetadataRequest{})
		return err
	}); err != nil {
		return nil, err
	}

	brokers := make(map[string]*BrokerInfo, len(resp.Brokers))
	for _, broker := range resp.Brokers {
		brokers[broker.Id] = &BrokerInfo{
			id:   broker.Id,
			host: broker.Host,
			port: broker.Port,
		}
	}

	streams := make(map[string]*StreamInfo)
	for _, streamMetadata := range resp.Metadata {
		stream := &StreamInfo{
			subject:    streamMetadata.Subject,
			name:       streamMetadata.Name,
			partitions: make(map[int32]*PartitionInfo, len(streamMetadata.Partitions)),
		}
		for _, partition := range streamMetadata.Partitions {
			replicas := make([]*BrokerInfo, len(partition.Replicas))
			for i, replica := range partition.Replicas {
				replicas[i] = brokers[replica]
			}
			isr := make([]*BrokerInfo, len(partition.Isr))
			for i, replica := range partition.Isr {
				isr[i] = brokers[replica]
			}
			stream.partitions[partition.Id] = &PartitionInfo{
				id:       partition.Id,
				leader:   brokers[partition.Leader],
				replicas: replicas,
				isr:      isr,
			}
		}
		streams[stream.name] = stream
	}

	metadata := newMetadata(brokers, streams)

	m.mu.Lock()
	m.metadata = metadata
	m.mu.Unlock()

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
func (m *metadataCache) getAddr(stream string, partitionID int32, readISRReplica bool) (string, error) {
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

// get returns the current Metadata.
func (m *metadataCache) get() *Metadata {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.metadata
}
