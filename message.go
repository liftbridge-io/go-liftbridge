package liftbridge

import (
	"bytes"
	"hash/crc32"
	"sync"

	"github.com/liftbridge-io/liftbridge-api/go"
)

var (
	envelopeCookie        = []byte("LIFT")
	envelopeCookieLen     = len(envelopeCookie)
	partitionByKey        = new(keyPartitioner)
	partitionByRoundRobin = newRoundRobinPartitioner()
	hasher                = crc32.ChecksumIEEE
)

// Partitioner is used to map a message to a stream partition.
type Partitioner interface {
	// Partition computes the partition number for a given message.
	Partition(msg *proto.Message, metadata *Metadata) int32
}

// keyPartitioner is an implementation of Partitioner which partitions messages
// based on a hash of the key.
type keyPartitioner struct{}

// Partition computes the partition number for a given message by hashing the
// key and modding by the number of partitions for the first stream found with
// the subject of the message. This does not work with streams containing
// wildcards in their subjects, e.g. "foo.*", since this matches on the subject
// literal of the published message. This also has undefined behavior if there
// are multiple streams for the given subject.
func (k *keyPartitioner) Partition(msg *proto.Message, metadata *Metadata) int32 {
	key := msg.Key
	if key == nil {
		key = []byte("")
	}

	partitions := getPartitionCount(msg.Subject, metadata)
	if partitions == 0 {
		return 0
	}

	return int32(hasher(key)) % partitions
}

type subjectCounter struct {
	sync.Mutex
	count int32
}

// roundRobinPartitioner is an implementation of Partitioner which partitions
// messages in a round-robin fashion.
type roundRobinPartitioner struct {
	sync.Mutex
	subjectCounterMap map[string]*subjectCounter
}

func newRoundRobinPartitioner() Partitioner {
	return &roundRobinPartitioner{
		subjectCounterMap: make(map[string]*subjectCounter),
	}
}

// Partition computes the partition number for a given message in a round-robin
// fashion by atomically incrementing a counter for the message subject and
// modding by the number of partitions for the first stream found with the
// subject. This does not work with streams containing wildcards in their
// subjects, e.g. "foo.*", since this matches on the subject literal of the
// published message. This also has undefined behavior if there are multiple
// streams for the given subject.
func (r *roundRobinPartitioner) Partition(msg *proto.Message, metadata *Metadata) int32 {
	partitions := getPartitionCount(msg.Subject, metadata)
	if partitions == 0 {
		return 0
	}
	r.Lock()
	counter, ok := r.subjectCounterMap[msg.Subject]
	if !ok {
		counter = new(subjectCounter)
		r.subjectCounterMap[msg.Subject] = counter
	}
	r.Unlock()
	counter.Lock()
	count := counter.count
	counter.count++
	counter.Unlock()
	return count % partitions
}

func getPartitionCount(subject string, metadata *Metadata) int32 {
	counts := metadata.PartitionCountsForSubject(subject)

	// Get the first matching stream's count.
	for _, count := range counts {
		return count
	}

	return 0
}

// MessageOptions are used to configure optional settings for a Message.
type MessageOptions struct {
	// Key to set on the Message. If Liftbridge has stream compaction enabled,
	// the stream will retain only the last value for each key.
	Key []byte

	// AckInbox sets the NATS subject Liftbridge should publish the Message ack
	// to. If it's not set, Liftbridge will not send an ack.
	AckInbox string

	// CorrelationID sets the identifier used to correlate an ack with the
	// published Message. If it's not set, the ack will not have a correlation
	// id.
	CorrelationID string

	// AckPolicy controls the behavior of Message acks sent by the server. By
	// default, Liftbridge will send an ack when the stream leader has written
	// the Message to its write-ahead log.
	AckPolicy proto.AckPolicy

	// Headers are key-value pairs to set on the Message.
	Headers map[string][]byte

	// Partitioner specifies the strategy for mapping a Message to a stream
	// partition.
	Partitioner Partitioner

	// Partition specifies the stream partition to publish the Message to. If
	// this is set, any Partitioner will not be used. This is a pointer to
	// allow distinguishing between unset and 0.
	Partition *int32
}

// MessageOption is a function on the MessageOptions for a Message. These are
// used to configure particular optional Message fields.
type MessageOption func(*MessageOptions)

// Key is a MessageOption to set the key on a Message. If Liftbridge has stream
// compaction enabled, the stream will retain only the last value for each key.
func Key(key []byte) MessageOption {
	return func(o *MessageOptions) {
		o.Key = key
	}
}

// AckInbox is a MessageOption to set the NATS subject Liftbridge should
// publish the Message ack to. If it's not set, Liftbridge will not send an
// ack.
func AckInbox(ackInbox string) MessageOption {
	return func(o *MessageOptions) {
		o.AckInbox = ackInbox
	}
}

// CorrelationID is a MessageOption to set the identifier used to correlate an
// ack with the published Message. If it's not set, the ack will not have a
// correlation id.
func CorrelationID(correlationID string) MessageOption {
	return func(o *MessageOptions) {
		o.CorrelationID = correlationID
	}
}

// AckPolicyLeader is a MessageOption that sets the AckPolicy of the Message to
// LEADER. This means the Message ack will be sent when the stream leader has
// written it to its write-ahead log.
func AckPolicyLeader() MessageOption {
	return func(o *MessageOptions) {
		o.AckPolicy = proto.AckPolicy_LEADER
	}
}

// AckPolicyAll is a MessageOption that sets the AckPolicy of the Message to
// ALL. This means the Message ack will be sent when the message has been
// written to all replicas.
func AckPolicyAll() MessageOption {
	return func(o *MessageOptions) {
		o.AckPolicy = proto.AckPolicy_ALL
	}
}

// AckPolicyNone is a MessageOption that sets the AckPolicy of the Message to
// NONE. This means no ack will be sent.
func AckPolicyNone() MessageOption {
	return func(o *MessageOptions) {
		o.AckPolicy = proto.AckPolicy_NONE
	}
}

// Header is a MessageOption that adds a single header to the Message. This may
// overwrite previously set headers.
func Header(name string, value []byte) MessageOption {
	return func(o *MessageOptions) {
		o.Headers[name] = value
	}
}

// Headers is a MessageOption that adds a set of headers to the Message. This
// may overwrite previously set headers.
func Headers(headers map[string][]byte) MessageOption {
	return func(o *MessageOptions) {
		for name, value := range headers {
			o.Headers[name] = value
		}
	}
}

// ToPartition is a MessageOption that specifies the stream partition to
// publish the Message to. If this is set, any Partitioner will not be used.
func ToPartition(partition int32) MessageOption {
	return func(o *MessageOptions) {
		o.Partition = &partition
	}
}

// PartitionBy is a MessageOption that specifies a Partitioner used to map
// Messages to stream partitions.
func PartitionBy(partitioner Partitioner) MessageOption {
	return func(o *MessageOptions) {
		o.Partitioner = partitioner
	}
}

// PartitionByKey is a MessageOption that maps Messages to stream partitions
// based on a hash of the Message key. This computes the partition number for a
// given message by hashing the key and modding by the number of partitions for
// the first stream found with the subject of the published message. This does
// not work with streams containing wildcards in their subjects, e.g. "foo.*",
// since this matches on the subject literal of the published message. This
// also has undefined behavior if there are multiple streams for the given
// subject.
func PartitionByKey() MessageOption {
	return PartitionBy(partitionByKey)
}

// PartitionByRoundRobin is a MessageOption that maps Messages to stream
// partitions in a round-robin fashion. This computes the partition number for
// a given message by atomically incrementing a counter for the message subject
// and modding by the number of partitions for the first stream found with the
// subject. This does not work with streams containing wildcards in their
// subjects, e.g. "foo.*", since this matches on the subject literal of the
// published message. This also has undefined behavior if there are multiple
// streams for the given subject.
func PartitionByRoundRobin() MessageOption {
	return PartitionBy(partitionByRoundRobin)
}

// NewMessage returns a serialized message for the given payload and options.
func NewMessage(value []byte, options ...MessageOption) []byte {
	opts := &MessageOptions{Headers: make(map[string][]byte)}
	for _, opt := range options {
		opt(opts)
	}

	msg, err := (&proto.Message{
		Key:           opts.Key,
		Value:         value,
		AckInbox:      opts.AckInbox,
		CorrelationId: opts.CorrelationID,
		AckPolicy:     opts.AckPolicy,
	}).Marshal()
	if err != nil {
		panic(err)
	}

	buf := make([]byte, envelopeCookieLen+len(msg))
	copy(buf[0:], envelopeCookie)
	copy(buf[envelopeCookieLen:], msg)
	return buf
}

// UnmarshalAck deserializes an Ack from the given byte slice. It returns an
// error if the given data is not actually an Ack.
func UnmarshalAck(data []byte) (*proto.Ack, error) {
	var (
		ack = &proto.Ack{}
		err = ack.Unmarshal(data)
	)
	return ack, err
}

// UnmarshalMessage deserializes a message from the given byte slice.  It
// returns a bool indicating if the given data was actually a Message or not.
func UnmarshalMessage(data []byte) (*proto.Message, bool) {
	if len(data) <= envelopeCookieLen {
		return nil, false
	}
	if !bytes.Equal(data[:envelopeCookieLen], envelopeCookie) {
		return nil, false
	}
	var (
		msg = &proto.Message{}
		err = msg.Unmarshal(data[envelopeCookieLen:])
	)
	if err != nil {
		return nil, false
	}
	return msg, true
}
