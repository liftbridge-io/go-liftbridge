package liftbridge

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/liftbridge-io/go-liftbridge/liftbridge-grpc"
)

// streamIndex maps a subject and stream name to a stream.
type streamIndex map[string]map[string]*StreamInfo

// addStream adds the given stream to the index.
func (s streamIndex) addStream(stream *StreamInfo) {
	streams, ok := s[stream.subject]
	if !ok {
		streams = make(map[string]*StreamInfo)
		s[stream.subject] = streams
	}
	streams[stream.name] = stream
}

// getStream returns the given stream or nil if it does not exist.
func (s streamIndex) getStream(subject, name string) *StreamInfo {
	streams, ok := s[subject]
	if !ok {
		return nil
	}
	return streams[name]
}

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
	streams     streamIndex
}

func newMetadata(brokers map[string]*BrokerInfo, streams streamIndex) *Metadata {
	return &Metadata{
		lastUpdated: time.Now(),
		brokers:     brokers,
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

// GetStream returns the given stream or nil if unknown.
func (m *Metadata) GetStream(subject, name string) *StreamInfo {
	return m.streams.getStream(subject, name)
}

// GetStreams returns a map containing stream names and streams for all streams
// with the given subject. This does not match on wildcard subjects, e.g.
// "foo.*".
func (m *Metadata) GetStreams(subject string) map[string]*StreamInfo {
	streams := m.streams[subject]
	if streams == nil {
		streams = make(map[string]*StreamInfo)
	}
	return streams
}

// PartitionCountsForSubject returns a map containing stream names and the
// number of partitions for the stream. This does not match on wildcard
// subjects, e.g. "foo.*".
func (m *Metadata) PartitionCountsForSubject(subject string) map[string]int32 {
	var (
		streams = m.GetStreams(subject)
		counts  = make(map[string]int32, len(streams))
	)
	for name, stream := range streams {
		counts[name] = int32(len(stream.Partitions()))
	}
	return counts
}

// hasSubjectMetadata indicates if the Metadata has info for at least one
// stream with the given subject literal.
func (m *Metadata) hasSubjectMetadata(subject string) bool {
	streams := m.GetStreams(subject)
	return len(streams) > 0
}

type metadataCache struct {
	mu       sync.RWMutex
	metadata *Metadata
	doRPC    func(func(proto.APIClient) error) error
}

func newMetadataCache(addrs []string, doRPC func(func(proto.APIClient) error) error) *metadataCache {
	addrMap := make(map[string]struct{}, len(addrs))
	for _, addr := range addrs {
		addrMap[addr] = struct{}{}
	}
	return &metadataCache{
		doRPC: doRPC,
	}
}

// update fetches the latest cluster metadata, including stream and broker
// information.
func (m *metadataCache) update() (*Metadata, error) {
	var resp *proto.FetchMetadataResponse
	if err := m.doRPC(func(client proto.APIClient) (err error) {
		resp, err = client.FetchMetadata(context.Background(), &proto.FetchMetadataRequest{})
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

	streamIndex := make(streamIndex)
	for _, streamMetadata := range resp.Metadata {
		stream := &StreamInfo{
			subject:    streamMetadata.Stream.Subject,
			name:       streamMetadata.Stream.Name,
			partitions: make(map[int32]*PartitionInfo, len(streamMetadata.Partitions)),
		}
		for _, partition := range streamMetadata.Partitions {
			replicas := make([]*BrokerInfo, len(partition.Replicas))
			for i, replica := range partition.Replicas {
				replicas[i] = brokers[replica]
			}
			isr := make([]*BrokerInfo, len(partition.Isr))
			for i, replica := range partition.Replicas {
				isr[i] = brokers[replica]
			}
			stream.partitions[partition.Id] = &PartitionInfo{
				id:       partition.Id,
				leader:   brokers[partition.Leader],
				replicas: replicas,
				isr:      isr,
			}
		}
		streamIndex.addStream(stream)
	}

	metadata := newMetadata(brokers, streamIndex)

	m.mu.Lock()
	m.metadata = metadata
	m.mu.Unlock()

	return metadata, nil
}

// getAddrs returns a list of all broker addresses.
func (m *metadataCache) getAddrs() []string {
	m.mu.RLock()
	metadata := m.metadata
	m.mu.RUnlock()
	addrs := make([]string, len(metadata.Brokers()))
	for i, broker := range metadata.Brokers() {
		addrs[i] = broker.Addr()
	}
	return addrs
}

// getAddr returns the broker address for the given stream partition.
func (m *metadataCache) getAddr(subject, name string, partitionID int32) (string, error) {
	m.mu.RLock()
	metadata := m.metadata
	m.mu.RUnlock()
	stream := metadata.GetStream(subject, name)
	if stream == nil {
		return "", errors.New("no known stream")
	}
	partition := stream.GetPartition(partitionID)
	if partition == nil {
		return "", errors.New("no known partition")
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
