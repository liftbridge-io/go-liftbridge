package liftbridge

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	pb "github.com/golang/protobuf/proto"
	"github.com/liftbridge-io/liftbridge-api/go"
)

var (
	envelopeMagicNumber    = []byte{0xB9, 0x0E, 0x43, 0xB4}
	envelopeMagicNumberLen = len(envelopeMagicNumber)
	partitionByKey         = new(keyPartitioner)
	partitionByRoundRobin  = newRoundRobinPartitioner()
	hasher                 = crc32.ChecksumIEEE
	crc32cTable            = crc32.MakeTable(crc32.Castagnoli)
)

const (
	envelopeProtoV0      = 0x00
	envelopeMinHeaderLen = 8
)

type msgType byte

const (
	msgTypePublish msgType = iota
	msgTypeAck
)

// AckPolicy controls the behavior of message acknowledgements.
type AckPolicy int32

func (a AckPolicy) toProto() proto.AckPolicy {
	return proto.AckPolicy(a)
}

// Message received from a Liftbridge stream.
type Message struct {
	offset       int64
	key          []byte
	value        []byte
	partition    int32
	timestamp    time.Time
	stream       string
	subject      string
	replySubject string
	headers      map[string][]byte
}

func messageFromProto(wireMsg *proto.Message) *Message {
	if wireMsg == nil {
		return nil
	}
	msg := &Message{
		offset:       wireMsg.GetOffset(),
		key:          wireMsg.GetKey(),
		value:        wireMsg.GetValue(),
		partition:    wireMsg.GetPartition(),
		timestamp:    time.Unix(0, wireMsg.GetTimestamp()),
		stream:       wireMsg.GetStream(),
		subject:      wireMsg.GetSubject(),
		replySubject: wireMsg.GetReplySubject(),
		headers:      wireMsg.GetHeaders(),
	}
	return msg
}

// Offset is a monotonic message sequence in the stream partition.
func (m *Message) Offset() int64 {
	return m.offset
}

// Key is an optional label set on a Message, useful for partitioning and
// stream compaction.
func (m *Message) Key() []byte {
	return m.key
}

// Value is the Message payload.
func (m *Message) Value() []byte {
	return m.value
}

// Timestamp is the time the Message was received by the server.
func (m *Message) Timestamp() time.Time {
	return m.timestamp
}

// Subject is the NATS subject the Message was received on.
func (m *Message) Subject() string {
	return m.subject
}

// ReplySubject is the NATS reply subject on the Message, if any.
func (m *Message) ReplySubject() string {
	return m.replySubject
}

// Headers is a set of key-value pairs.
func (m *Message) Headers() map[string][]byte {
	headers := make(map[string][]byte, len(m.headers))
	for key, value := range m.headers {
		headers[key] = value
	}
	return headers
}

// Stream the Message was received on.
func (m *Message) Stream() string {
	return m.stream
}

// Partition the Message was received on.
func (m *Message) Partition() int32 {
	return m.partition
}

// Ack represents an acknowledgement that a message was committed to a stream
// partition.
type Ack struct {
	stream           string
	partitionSubject string
	messageSubject   string
	offset           int64
	ackInbox         string
	correlationID    string
	ackPolicy        AckPolicy
}

func ackFromProto(wireAck *proto.Ack) *Ack {
	if wireAck == nil {
		return nil
	}
	ack := &Ack{
		stream:           wireAck.GetStream(),
		partitionSubject: wireAck.GetPartitionSubject(),
		messageSubject:   wireAck.GetMsgSubject(),
		offset:           wireAck.GetOffset(),
		ackInbox:         wireAck.GetAckInbox(),
		correlationID:    wireAck.GetCorrelationId(),
		ackPolicy:        AckPolicy(wireAck.GetAckPolicy()),
	}
	return ack
}

// Stream the Message was received on.
func (a *Ack) Stream() string {
	return a.stream
}

// PartitionSubject is the NATS subject the partition is attached to.
func (a *Ack) PartitionSubject() string {
	return a.partitionSubject
}

// MessageSubject is the NATS subject the message was received on.
func (a *Ack) MessageSubject() string {
	return a.messageSubject
}

// Offset is the partition offset the message was committed to.
func (a *Ack) Offset() int64 {
	return a.offset
}

// AckInbox is the NATS subject the ack was published to.
func (a *Ack) AckInbox() string {
	return a.ackInbox
}

// CorrelationID is the user-supplied value from the message.
func (a *Ack) CorrelationID() string {
	return a.correlationID
}

// AckPolicy sent on the message.
func (a *Ack) AckPolicy() AckPolicy {
	return a.ackPolicy
}

// Partitioner is used to map a message to a stream partition.
type Partitioner interface {
	// Partition computes the partition number for a given message.
	Partition(stream string, key, value []byte, metadata *Metadata) int32
}

// keyPartitioner is an implementation of Partitioner which partitions messages
// based on a hash of the key.
type keyPartitioner struct{}

// Partition computes the partition number for a given message by hashing the
// key and modding by the number of stream partitions.
func (k *keyPartitioner) Partition(stream string, key, value []byte, metadata *Metadata) int32 {
	if key == nil {
		key = []byte("")
	}

	partitions := metadata.PartitionCountForStream(stream)
	if partitions == 0 {
		return 0
	}

	return int32(hasher(key) % uint32(partitions))
}

type streamCounter struct {
	sync.Mutex
	count int32
}

// roundRobinPartitioner is an implementation of Partitioner which partitions
// messages in a round-robin fashion.
type roundRobinPartitioner struct {
	sync.Mutex
	streamCounterMap map[string]*streamCounter
}

func newRoundRobinPartitioner() Partitioner {
	return &roundRobinPartitioner{
		streamCounterMap: make(map[string]*streamCounter),
	}
}

// Partition computes the partition number for a given message in a round-robin
// fashion by atomically incrementing a counter for the message stream and
// modding by the number of stream partitions.
func (r *roundRobinPartitioner) Partition(stream string, key, value []byte, metadata *Metadata) int32 {
	partitions := metadata.PartitionCountForStream(stream)
	if partitions == 0 {
		return 0
	}
	r.Lock()
	counter, ok := r.streamCounterMap[stream]
	if !ok {
		counter = new(streamCounter)
		r.streamCounterMap[stream] = counter
	}
	r.Unlock()
	counter.Lock()
	count := counter.count
	counter.count++
	counter.Unlock()
	return count % partitions
}

// MessageOptions are used to configure optional settings for a Message.
type MessageOptions struct {
	// Key to set on the Message. If Liftbridge has stream compaction enabled,
	// the stream will retain only the last value for each key.
	Key []byte

	// AckInbox sets the NATS subject Liftbridge should publish the Message ack
	// to. If this is not set, the server will generate a random inbox.
	AckInbox string

	// CorrelationID sets the identifier used to correlate an ack with the
	// published Message. If this is not set, the ack will not have a
	// correlation id.
	CorrelationID string

	// AckPolicy controls the behavior of Message acks sent by the server. By
	// default, Liftbridge will send an ack when the stream leader has written
	// the Message to its write-ahead log.
	AckPolicy AckPolicy

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
// publish the Message ack to. If this is not set, the server will generate a
// random inbox.
func AckInbox(ackInbox string) MessageOption {
	return func(o *MessageOptions) {
		o.AckInbox = ackInbox
	}
}

// CorrelationID is a MessageOption to set the identifier used to correlate an
// ack with the published Message. If this is not set, the ack will not have a
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
		o.AckPolicy = AckPolicy(proto.AckPolicy_LEADER)
	}
}

// AckPolicyAll is a MessageOption that sets the AckPolicy of the Message to
// ALL. This means the Message ack will be sent when the message has been
// written to all replicas.
func AckPolicyAll() MessageOption {
	return func(o *MessageOptions) {
		o.AckPolicy = AckPolicy(proto.AckPolicy_ALL)
	}
}

// AckPolicyNone is a MessageOption that sets the AckPolicy of the Message to
// NONE. This means no ack will be sent.
func AckPolicyNone() MessageOption {
	return func(o *MessageOptions) {
		o.AckPolicy = AckPolicy(proto.AckPolicy_NONE)
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
	// TODO: Implement option for CRC32.
	for _, opt := range options {
		opt(opts)
	}

	msg, err := marshalEnvelope(&proto.Message{
		Key:           opts.Key,
		Value:         value,
		AckInbox:      opts.AckInbox,
		CorrelationId: opts.CorrelationID,
		AckPolicy:     opts.AckPolicy.toProto(),
		Headers:       opts.Headers,
	}, msgTypePublish)
	if err != nil {
		panic(err)
	}
	return msg
}

// UnmarshalAck deserializes an Ack from the given byte slice. It returns an
// error if the given data is not actually an Ack.
func UnmarshalAck(data []byte) (*Ack, error) {
	ack := new(proto.Ack)
	if err := unmarshalEnvelope(data, ack, msgTypeAck); err != nil {
		return nil, err
	}
	return ackFromProto(ack), nil
}

// UnmarshalMessage deserializes a message from the given byte slice. It
// returns an error if the given data is not actually a Message.
func UnmarshalMessage(data []byte) (*Message, error) {
	msg := new(proto.Message)
	if err := unmarshalEnvelope(data, msg, msgTypePublish); err != nil {
		return nil, err
	}
	return messageFromProto(msg), nil
}

func unmarshalEnvelope(data []byte, msg pb.Message, expectedType msgType) error {
	if len(data) <= envelopeMinHeaderLen {
		return errors.New("data missing envelope header")
	}
	if !bytes.Equal(data[:envelopeMagicNumberLen], envelopeMagicNumber) {
		return errors.New("unexpected envelope magic number")
	}
	if data[4] != envelopeProtoV0 {
		return fmt.Errorf("unknown envelope protocol: %v", data[4])
	}

	var (
		headerLen  = int(data[5])
		flags      = data[6]
		actualType = msgType(data[7])
		payload    = data[headerLen:]
	)

	if actualType != expectedType {
		return fmt.Errorf("MsgType mismatch: expected %v, got %v", expectedType, actualType)
	}

	// Check CRC.
	if hasBit(flags, 0) {
		// Make sure there is a CRC present.
		if headerLen != envelopeMinHeaderLen+4 {
			return errors.New("incorrect envelope header size")
		}
		crc := binary.BigEndian.Uint32(data[envelopeMinHeaderLen:headerLen])
		if c := crc32.Checksum(payload, crc32cTable); c != crc {
			return fmt.Errorf("crc mismatch: expected %d, got %d", crc, c)
		}
	}

	return pb.Unmarshal(payload, msg)
}

func marshalEnvelope(data pb.Message, msgType msgType) ([]byte, error) {
	msg, err := pb.Marshal(data)
	if err != nil {
		return nil, err
	}

	var (
		buf       = make([]byte, envelopeMagicNumberLen+4+len(msg))
		pos       = 0
		headerLen = envelopeMinHeaderLen
	)
	copy(buf[pos:], envelopeMagicNumber)
	pos += envelopeMagicNumberLen
	buf[pos] = envelopeProtoV0 // Version
	pos++
	buf[pos] = byte(headerLen) // HeaderLen
	pos++
	buf[pos] = 0x00 // Flags
	pos++
	buf[pos] = byte(msgType) // MsgType
	pos++
	if pos != headerLen {
		panic(fmt.Sprintf("Payload position (%d) does not match expected HeaderLen (%d)",
			pos, headerLen))
	}
	copy(buf[pos:], msg)
	return buf, nil
}

func setBit(n byte, pos uint8) byte {
	n |= (1 << pos)
	return n
}

func clearBit(n byte, pos uint8) byte {
	mask := byte(^(1 << pos))
	n &= mask
	return n
}

func hasBit(n byte, pos uint8) bool {
	val := n & (1 << pos)
	return (val > 0)
}
