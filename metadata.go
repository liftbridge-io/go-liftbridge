package liftbridge

import (
	"errors"
	"fmt"
	"sync"

	"golang.org/x/net/context"

	"github.com/liftbridge-io/go-liftbridge/liftbridge-grpc"
)

type stream struct {
	partitionAddrs map[int32]string
}

// streamIndex maps a subject to a map of stream names to streams.
type streamIndex map[string]map[string]*stream

func (s streamIndex) getStream(subject, name string) *stream {
	streams, ok := s[subject]
	if !ok {
		return nil
	}
	return streams[name]
}

type metadataCache struct {
	addrs       map[string]struct{}
	brokerAddrs map[string]string
	streams     streamIndex
	mu          sync.RWMutex
	doRPC       func(func(proto.APIClient) error) error
}

func newMetadataCache(addrs []string, doRPC func(func(proto.APIClient) error) error) *metadataCache {
	addrMap := make(map[string]struct{}, len(addrs))
	for _, addr := range addrs {
		addrMap[addr] = struct{}{}
	}
	return &metadataCache{
		addrs:       addrMap,
		brokerAddrs: make(map[string]string),
		streams:     make(streamIndex),
		doRPC:       doRPC,
	}
}

// update fetches the latest cluster metadata, including stream and broker
// information. This maintains a map from broker ID to address and a map from
// partition to broker address.
func (m *metadataCache) update() error {
	var resp *proto.FetchMetadataResponse
	if err := m.doRPC(func(client proto.APIClient) (err error) {
		resp, err = client.FetchMetadata(context.Background(), &proto.FetchMetadataRequest{})
		return err
	}); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	brokerAddrs := make(map[string]string)
	for _, broker := range resp.Brokers {
		addr := fmt.Sprintf("%s:%d", broker.Host, broker.Port)
		brokerAddrs[broker.Id] = addr
		m.addrs[addr] = struct{}{}
	}
	m.brokerAddrs = brokerAddrs

	streams := make(streamIndex)
	for _, streamMetadata := range resp.Metadata {
		st := &stream{
			partitionAddrs: make(map[int32]string, len(streamMetadata.Partitions)),
		}
		for _, partition := range streamMetadata.Partitions {
			st.partitionAddrs[partition.Id] = m.brokerAddrs[partition.Leader]
		}
		subjectStreams, ok := streams[streamMetadata.Stream.Subject]
		if !ok {
			subjectStreams = make(map[string]*stream)
			streams[streamMetadata.Stream.Subject] = subjectStreams
		}
		subjectStreams[streamMetadata.Stream.Name] = st
	}
	m.streams = streams
	return nil
}

func (m *metadataCache) getAddr(subject, name string, partition int32) (string, error) {
	m.mu.RLock()
	stream := m.streams.getStream(subject, name)
	m.mu.RUnlock()
	if stream == nil {
		return "", errors.New("no known stream")
	}
	addr, ok := stream.partitionAddrs[partition]
	if !ok {
		return "", errors.New("no known broker for partition")
	}
	return addr, nil
}

func (m *metadataCache) getPartitions(subject, name string) (int32, error) {
	m.mu.RLock()
	stream := m.streams.getStream(subject, name)
	m.mu.RUnlock()
	if stream == nil {
		return 0, errors.New("no known stream")
	}
	return int32(stream.partitionAddrs), nil
}

func (m *metadataCache) getAddrs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	addrs := make([]string, len(m.addrs))
	i := 0
	for addr := range m.addrs {
		addrs[i] = addr
		i++
	}
	return addrs
}
