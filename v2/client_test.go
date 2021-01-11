package liftbridge

import (
	"context"
	"fmt"
	"testing"
	"time"

	proto "github.com/liftbridge-io/liftbridge-api/go"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestUnmarshalAck(t *testing.T) {
	ack := &proto.Ack{
		Stream:     "bar",
		MsgSubject: "foo",
		Offset:     1,
		AckInbox:   "acks",
	}
	data := marshalAck(t, ack)
	actual, err := UnmarshalAck(data)
	require.NoError(t, err)
	require.Equal(t, ack.Stream, actual.Stream())
	require.Equal(t, ack.MsgSubject, actual.MessageSubject())
	require.Equal(t, ack.Offset, actual.Offset())
	require.Equal(t, ack.AckInbox, actual.AckInbox())
}

func TestUnmarshalAckError(t *testing.T) {
	_, err := UnmarshalAck([]byte("blah"))
	require.Error(t, err)
}

func TestNewMessageUnmarshal(t *testing.T) {
	var (
		key     = []byte("foo")
		value   = []byte("bar")
		headers = map[string][]byte{"foo": []byte("bar"), "baz": []byte("qux")}
	)
	msg := NewMessage(value,
		Key(key),
		Headers(headers),
	)
	actual, err := UnmarshalMessage(msg)
	require.Nil(t, err)
	require.Equal(t, key, actual.Key())
	require.Equal(t, value, actual.Value())
	require.Equal(t, headers, actual.Headers())
}

func TestUnmarshalMessageError(t *testing.T) {
	_, err := UnmarshalMessage(nil)
	require.Error(t, err)

	_, err = UnmarshalMessage([]byte("blahh"))
	require.Error(t, err)

	buf := make([]byte, 8)
	copy(buf, envelopeMagicNumber)
	copy(buf[envelopeMagicNumberLen:], []byte("blah"))
	_, err = UnmarshalMessage(buf)
	require.Error(t, err)

	// CRC flag set with no CRC present.
	msg := NewMessage([]byte("hello"))
	msg[6] = setBit(msg[6], 0)
	_, err = UnmarshalMessage(msg)
	require.Error(t, err)

	// CRC flag set with invalid CRC.
	msg = NewMessage([]byte("hello"))
	msg[6] = setBit(msg[6], 0)
	buf = make([]byte, len(msg)+4)
	copy(buf, msg[:8])
	buf[8] = byte(32)
	copy(buf[12:], msg[8:])
	buf[5] = byte(12)
	_, err = UnmarshalMessage(buf)
	require.Error(t, err)

	// Unknown protocol version.
	msg = NewMessage([]byte("hello"))
	msg[4] = 0x01
	_, err = UnmarshalMessage(msg)
	require.Error(t, err)

	// Mismatched MsgType.
	msg = marshalAck(t, new(proto.Ack))
	_, err = UnmarshalMessage(msg)
	require.Error(t, err)
}

func TestConnectNoAddrs(t *testing.T) {
	_, err := Connect(nil)
	require.Error(t, err)
}

func TestCreateStream(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	server.SetupMockCreateStreamError(status.Error(codes.AlreadyExists, "stream already exists"))

	err = client.CreateStream(context.Background(), "foo", "bar", ReplicationFactor(2))
	require.Equal(t, ErrStreamExists, err)
	req := server.GetCreateStreamRequests()[0]
	require.Equal(t, "bar", req.Name)
	require.Equal(t, "foo", req.Subject)
	require.Equal(t, "", req.Group)
	require.Equal(t, int32(2), req.ReplicationFactor)
	require.Equal(t, int32(0), req.Partitions)

	server.SetupMockCreateStreamError(nil)
	server.SetupMockCreateStreamResponse(new(proto.CreateStreamResponse))

	require.NoError(t, client.CreateStream(context.Background(), "foo", "bar",
		Group("group"), MaxReplication(), Partitions(3)))
	req = server.GetCreateStreamRequests()[1]
	require.Equal(t, "bar", req.Name)
	require.Equal(t, "foo", req.Subject)
	require.Equal(t, "group", req.Group)
	require.Equal(t, int32(-1), req.ReplicationFactor)
	require.Equal(t, int32(3), req.Partitions)

	err = client.CreateStream(context.Background(), "foo", "bar", Partitions(-1))
	require.Error(t, err)
}

func TestDeleteStream(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	server.SetupMockDeleteStreamError(status.Error(codes.NotFound, "stream not found"))

	err = client.DeleteStream(context.Background(), "foo")
	require.Equal(t, ErrNoSuchStream, err)
	require.Equal(t, "foo", server.GetDeleteStreamRequests()[0].Name)

	server.SetupMockDeleteStreamError(nil)
	server.SetupMockDeleteStreamResponse(new(proto.DeleteStreamResponse))

	require.NoError(t, client.DeleteStream(context.Background(), "foo"))
	require.Equal(t, "foo", server.GetDeleteStreamRequests()[1].Name)
}

func TestPauseStream(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	server.SetupMockPauseStreamError(status.Error(codes.NotFound, "stream not found"))

	err = client.PauseStream(context.Background(), "foo")
	require.Equal(t, ErrNoSuchPartition, err)
	req := server.GetPauseStreamRequests()[0]
	require.Equal(t, "foo", req.Name)
	require.Equal(t, []int32(nil), req.Partitions)
	require.False(t, req.ResumeAll)

	server.SetupMockPauseStreamError(nil)
	server.SetupMockPausetreamResponse(new(proto.PauseStreamResponse))

	require.NoError(t, client.PauseStream(context.Background(), "foo",
		PausePartitions(0, 1), ResumeAll()))
	req = server.GetPauseStreamRequests()[1]
	require.Equal(t, "foo", req.Name)
	require.Equal(t, []int32{0, 1}, req.Partitions)
	require.True(t, req.ResumeAll)
}

func TestSetStreamReadonly(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	server.SetupMockSetStreamReadonlyError(status.Error(codes.NotFound, "stream not found"))

	err = client.SetStreamReadonly(context.Background(), "foo")
	require.Equal(t, ErrNoSuchPartition, err)
	req := server.GetSetStreamReadonlyRequests()[0]
	require.Equal(t, "foo", req.Name)
	require.Equal(t, []int32(nil), req.Partitions)
	require.True(t, req.Readonly)

	server.SetupMockSetStreamReadonlyError(nil)
	server.SetupMockSetStreamReadonlyResponse(new(proto.SetStreamReadonlyResponse))

	require.NoError(t, client.SetStreamReadonly(context.Background(), "foo",
		ReadonlyPartitions(0, 1), Readonly(false)))
	req = server.GetSetStreamReadonlyRequests()[1]
	require.Equal(t, "foo", req.Name)
	require.Equal(t, []int32{0, 1}, req.Partitions)
	require.False(t, req.Readonly)
}

func TestSubscribe(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	metadataResp := &proto.FetchMetadataResponse{
		Brokers: []*proto.Broker{{
			Id:   "a",
			Host: "localhost",
			Port: int32(port),
		}},
		Metadata: []*proto.StreamMetadata{{
			Name:    "foo",
			Subject: "foo",
			Partitions: map[int32]*proto.PartitionMetadata{
				0: {
					Id:       0,
					Leader:   "a",
					Replicas: []string{"a"},
					Isr:      []string{"a"},
				},
			},
		}},
	}
	server.SetupMockFetchMetadataResponse(metadataResp)
	timestamp := time.Now().UnixNano()
	messages := []*proto.Message{
		{
			Offset:        0,
			Key:           []byte("key"),
			Value:         []byte("value"),
			Timestamp:     timestamp,
			Stream:        "foo",
			Partition:     0,
			Subject:       "foo",
			Headers:       map[string][]byte{"foo": []byte("bar")},
			AckInbox:      "ack",
			CorrelationId: "123",
			AckPolicy:     proto.AckPolicy_ALL,
		},
	}
	server.SetupMockSubscribeMessages(messages)

	ch := make(chan *Message)
	err = client.Subscribe(context.Background(), "foo", func(msg *Message, err error) {
		require.NoError(t, err)
		ch <- msg
	})
	require.NoError(t, err)

	select {
	case msg := <-ch:
		require.Equal(t, int64(0), msg.Offset())
		require.Equal(t, []byte("key"), msg.Key())
		require.Equal(t, []byte("value"), msg.Value())
		require.Equal(t, time.Unix(0, timestamp), msg.Timestamp())
		require.Equal(t, "foo", msg.Subject())
		require.Equal(t, map[string][]byte{"foo": []byte("bar")}, msg.Headers())
	case <-time.After(2 * time.Second):
		t.Fatal("Did not receive expected message")
	}

	req := server.GetSubscribeRequests()[0]
	require.Equal(t, "foo", req.Stream)
	require.Equal(t, int32(0), req.Partition)
	require.Equal(t, proto.StartPosition_NEW_ONLY, req.StartPosition)
	require.False(t, req.ReadISRReplica)
}

func TestSubscribeNoKnownPartition(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	metadataResp := &proto.FetchMetadataResponse{
		Brokers: []*proto.Broker{{
			Id:   "a",
			Host: "localhost",
			Port: int32(port),
		}},
		Metadata: []*proto.StreamMetadata{{
			Name:    "foo",
			Subject: "foo",
			Partitions: map[int32]*proto.PartitionMetadata{
				0: {
					Id:       0,
					Leader:   "a",
					Replicas: []string{"a"},
					Isr:      []string{"a"},
				},
			},
		}},
	}
	server.SetupMockFetchMetadataResponse(metadataResp)

	err = client.Subscribe(context.Background(), "foo", func(msg *Message, err error) {}, Partition(1))
	require.Error(t, err)
	require.Equal(t, "no known partition", err.Error())

	require.Len(t, server.GetSubscribeRequests(), 0)
}

func TestSubscribeNoPartition(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	metadataResp := &proto.FetchMetadataResponse{
		Brokers: []*proto.Broker{{
			Id:   "a",
			Host: "localhost",
			Port: int32(port),
		}},
		Metadata: []*proto.StreamMetadata{{
			Name:    "foo",
			Subject: "foo",
			Partitions: map[int32]*proto.PartitionMetadata{
				0: {
					Id:       0,
					Leader:   "a",
					Replicas: []string{"a"},
					Isr:      []string{"a"},
				},
			},
		}},
	}
	server.SetupMockFetchMetadataResponse(metadataResp)
	server.SetupMockSubscribeError(status.Error(codes.NotFound, "No such partition"))

	err = client.Subscribe(context.Background(), "foo", func(msg *Message, err error) {},
		StartAtOffset(3), ReadISRReplica())
	require.Equal(t, ErrNoSuchPartition, err)

	req := server.GetSubscribeRequests()[0]
	require.Equal(t, "foo", req.Stream)
	require.Equal(t, int32(0), req.Partition)
	require.Equal(t, proto.StartPosition_OFFSET, req.StartPosition)
	require.Equal(t, int64(3), req.StartOffset)
	require.True(t, req.ReadISRReplica)
}

func TestSubscribeNoKnownStream(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	metadataResp := &proto.FetchMetadataResponse{
		Brokers: []*proto.Broker{{
			Id:   "a",
			Host: "localhost",
			Port: int32(port),
		}},
		Metadata: []*proto.StreamMetadata{},
	}
	server.SetupMockFetchMetadataResponse(metadataResp)

	err = client.Subscribe(context.Background(), "foo", func(msg *Message, err error) {})
	require.Error(t, err)
	require.Equal(t, "no known stream", err.Error())

	require.Len(t, server.GetSubscribeRequests(), 0)
}

func TestSubscribeNoLeader(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	metadataResp := &proto.FetchMetadataResponse{
		Brokers: []*proto.Broker{{
			Id:   "a",
			Host: "localhost",
			Port: int32(port),
		}},
		Metadata: []*proto.StreamMetadata{{
			Name:    "foo",
			Subject: "foo",
			Partitions: map[int32]*proto.PartitionMetadata{
				0: {
					Id:       0,
					Replicas: []string{"a"},
				},
			},
		}},
	}
	server.SetupMockFetchMetadataResponse(metadataResp)

	err = client.Subscribe(context.Background(), "foo", func(msg *Message, err error) {})
	require.Error(t, err)
	require.Equal(t, "no known leader for partition", err.Error())

	require.Len(t, server.GetSubscribeRequests(), 0)
}

func TestSubscribeNotLeaderRetry(t *testing.T) {
	server := newMockServer()
	server.SetAutoClearError()

	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	metadataResp := &proto.FetchMetadataResponse{
		Brokers: []*proto.Broker{{
			Id:   "a",
			Host: "localhost",
			Port: int32(port),
		}},
		Metadata: []*proto.StreamMetadata{{
			Name:    "foo",
			Subject: "foo",
			Partitions: map[int32]*proto.PartitionMetadata{
				0: {
					Id:       0,
					Leader:   "a",
					Replicas: []string{"a"},
					Isr:      []string{"a"},
				},
			},
		}},
	}
	server.SetupMockFetchMetadataResponse(metadataResp)
	timestamp := time.Now().UnixNano()
	messages := []*proto.Message{
		{
			Offset:        0,
			Key:           []byte("key"),
			Value:         []byte("value"),
			Timestamp:     timestamp,
			Stream:        "foo",
			Partition:     0,
			Subject:       "foo",
			Headers:       map[string][]byte{"foo": []byte("bar")},
			AckInbox:      "ack",
			CorrelationId: "123",
			AckPolicy:     proto.AckPolicy_ALL,
		},
	}

	server.SetupMockSubscribeError(status.Error(codes.FailedPrecondition, "not leader"))
	server.SetupMockSubscribeMessages(messages)

	ch := make(chan *Message)
	err = client.Subscribe(context.Background(), "foo", func(msg *Message, err error) {
		require.NoError(t, err)
		ch <- msg
	}, StartAtEarliestReceived())
	require.NoError(t, err)

	select {
	case msg := <-ch:
		require.Equal(t, int64(0), msg.Offset())
		require.Equal(t, []byte("key"), msg.Key())
		require.Equal(t, []byte("value"), msg.Value())
		require.Equal(t, time.Unix(0, timestamp), msg.Timestamp())
		require.Equal(t, "foo", msg.Subject())
		require.Equal(t, map[string][]byte{"foo": []byte("bar")}, msg.Headers())
	case <-time.After(2 * time.Second):
		t.Fatal("Did not receive expected message")
	}

	req := server.GetSubscribeRequests()[0]
	require.Equal(t, "foo", req.Stream)
	require.Equal(t, int32(0), req.Partition)
	require.Equal(t, proto.StartPosition_EARLIEST, req.StartPosition)
	require.False(t, req.ReadISRReplica)
}

func TestSubscribeResubscribe(t *testing.T) {
	server := newMockServer()

	server.SetAutoClearError()

	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	metadataResp := &proto.FetchMetadataResponse{
		Brokers: []*proto.Broker{{
			Id:   "a",
			Host: "localhost",
			Port: int32(port),
		}},
		Metadata: []*proto.StreamMetadata{{
			Name:    "foo",
			Subject: "foo",
			Partitions: map[int32]*proto.PartitionMetadata{
				0: {
					Id:       0,
					Leader:   "a",
					Replicas: []string{"a"},
					Isr:      []string{"a"},
				},
			},
		}},
	}
	server.SetupMockFetchMetadataResponse(metadataResp)
	timestamp := time.Now().UnixNano()
	messages := []*proto.Message{
		{
			Offset:        0,
			Key:           []byte("key"),
			Value:         []byte("value"),
			Timestamp:     timestamp,
			Stream:        "foo",
			Partition:     0,
			Subject:       "foo",
			Headers:       map[string][]byte{"foo": []byte("bar")},
			AckInbox:      "ack",
			CorrelationId: "123",
			AckPolicy:     proto.AckPolicy_ALL,
		},
	}

	server.SetupMockSubscribeAsyncError(status.Error(codes.Unavailable, "temporarily unavailable"))
	server.SetupMockSubscribeMessages(messages)

	ch := make(chan *Message)
	err = client.Subscribe(context.Background(), "foo", func(msg *Message, err error) {
		require.NoError(t, err)
		ch <- msg
	}, StartAtTimeDelta(time.Minute))
	require.NoError(t, err)

	select {
	case msg := <-ch:
		require.Equal(t, int64(0), msg.Offset())
		require.Equal(t, []byte("key"), msg.Key())
		require.Equal(t, []byte("value"), msg.Value())
		require.Equal(t, time.Unix(0, timestamp), msg.Timestamp())
		require.Equal(t, "foo", msg.Subject())
		require.Equal(t, map[string][]byte{"foo": []byte("bar")}, msg.Headers())
	case <-time.After(2 * time.Second):
		t.Fatal("Did not receive expected message")
	}

	req := server.GetSubscribeRequests()[0]
	require.Equal(t, "foo", req.Stream)
	require.Equal(t, int32(0), req.Partition)
	require.Equal(t, proto.StartPosition_TIMESTAMP, req.StartPosition)
	require.False(t, req.ReadISRReplica)
}

func TestSubscribeStreamDeleted(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	metadataResp := &proto.FetchMetadataResponse{
		Brokers: []*proto.Broker{{
			Id:   "a",
			Host: "localhost",
			Port: int32(port),
		}},
		Metadata: []*proto.StreamMetadata{{
			Name:    "foo",
			Subject: "foo",
			Partitions: map[int32]*proto.PartitionMetadata{
				0: {
					Id:       0,
					Leader:   "a",
					Replicas: []string{"a"},
					Isr:      []string{"a"},
				},
			},
		}},
	}
	server.SetupMockFetchMetadataResponse(metadataResp)

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	server.SetupMockSubscribeAsyncError(status.Error(codes.NotFound, "stream deleted"))

	timestamp := time.Now()
	ch := make(chan error)
	err = client.Subscribe(context.Background(), "foo", func(msg *Message, err error) {
		ch <- err
	}, StartAtTime(timestamp))
	require.NoError(t, err)

	select {
	case err := <-ch:
		require.Equal(t, ErrStreamDeleted, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Did not receive expected message")
	}

	req := server.GetSubscribeRequests()[0]
	require.Equal(t, "foo", req.Stream)
	require.Equal(t, int32(0), req.Partition)
	require.Equal(t, proto.StartPosition_TIMESTAMP, req.StartPosition)
	require.Equal(t, timestamp.UnixNano(), req.StartTimestamp)
	require.False(t, req.ReadISRReplica)
}

func TestSubscribePartitionPaused(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	metadataResp := &proto.FetchMetadataResponse{
		Brokers: []*proto.Broker{{
			Id:   "a",
			Host: "localhost",
			Port: int32(port),
		}},
		Metadata: []*proto.StreamMetadata{{
			Name:    "foo",
			Subject: "foo",
			Partitions: map[int32]*proto.PartitionMetadata{
				0: {
					Id:       0,
					Leader:   "a",
					Replicas: []string{"a"},
					Isr:      []string{"a"},
				},
			},
		}},
	}
	server.SetupMockFetchMetadataResponse(metadataResp)

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	server.SetupMockSubscribeAsyncError(status.Error(codes.FailedPrecondition, "partition paused"))

	ch := make(chan error)
	err = client.Subscribe(context.Background(), "foo", func(msg *Message, err error) {
		ch <- err
	})
	require.NoError(t, err)

	select {
	case err := <-ch:
		require.Equal(t, ErrPartitionPaused, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Did not receive expected message")
	}

	req := server.GetSubscribeRequests()[0]
	require.Equal(t, "foo", req.Stream)
	require.Equal(t, int32(0), req.Partition)
	require.Equal(t, proto.StartPosition_NEW_ONLY, req.StartPosition)
	require.False(t, req.ReadISRReplica)
}

func TestSubscribeServerUnavailableRetry(t *testing.T) {
	server := newMockServer()
	server.SetAutoClearError()

	defer server.Stop(t)
	port := server.Start(t)

	metadataResp := &proto.FetchMetadataResponse{
		Brokers: []*proto.Broker{{
			Id:   "a",
			Host: "localhost",
			Port: int32(port),
		}},
		Metadata: []*proto.StreamMetadata{{
			Name:    "foo",
			Subject: "foo",
			Partitions: map[int32]*proto.PartitionMetadata{
				0: {
					Id:       0,
					Leader:   "a",
					Replicas: []string{"a"},
					Isr:      []string{"a"},
				},
			},
		}},
	}
	server.SetupMockFetchMetadataResponse(metadataResp)

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	server.Stop(t)
	server = newMockServer()

	server.SetAutoClearError()

	defer server.Stop(t)
	server.SetupMockFetchMetadataResponse(metadataResp)
	timestamp := time.Now().UnixNano()
	messages := []*proto.Message{
		{
			Offset:        0,
			Key:           []byte("key"),
			Value:         []byte("value"),
			Timestamp:     timestamp,
			Stream:        "foo",
			Partition:     0,
			Subject:       "foo",
			Headers:       map[string][]byte{"foo": []byte("bar")},
			AckInbox:      "ack",
			CorrelationId: "123",
			AckPolicy:     proto.AckPolicy_ALL,
		},
	}

	server.SetupMockSubscribeAsyncError(status.Error(codes.Unavailable, "temporarily unavailable"))
	server.SetupMockSubscribeMessages(messages)

	go func() {
		time.Sleep(50 * time.Millisecond)
		server.StartOnPort(t, port)
	}()

	ch := make(chan *Message)
	err = client.Subscribe(context.Background(), "foo", func(msg *Message, err error) {
		require.NoError(t, err)
		ch <- msg
	}, StartAtLatestReceived())
	require.NoError(t, err)

	select {
	case msg := <-ch:
		require.Equal(t, int64(0), msg.Offset())
		require.Equal(t, []byte("key"), msg.Key())
		require.Equal(t, []byte("value"), msg.Value())
		require.Equal(t, time.Unix(0, timestamp), msg.Timestamp())
		require.Equal(t, "foo", msg.Subject())
		require.Equal(t, map[string][]byte{"foo": []byte("bar")}, msg.Headers())
	case <-time.After(2 * time.Second):
		t.Fatal("Did not receive expected message")
	}

	req := server.GetSubscribeRequests()[0]
	require.Equal(t, "foo", req.Stream)
	require.Equal(t, int32(0), req.Partition)
	require.Equal(t, proto.StartPosition_LATEST, req.StartPosition)
	require.False(t, req.ReadISRReplica)
}

func TestSubscribeInvalidPartition(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	err = client.Subscribe(context.Background(), "foo", func(msg *Message, err error) {}, Partition(-1))
	require.Error(t, err)

	require.Len(t, server.GetSubscribeRequests(), 0)
}

func TestPublish(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	expectedAck := &proto.Ack{
		Stream:           "foo",
		PartitionSubject: "foo",
		MsgSubject:       "foo",
		Offset:           0,
		AckInbox:         "ack",
		AckPolicy:        proto.AckPolicy_LEADER,
	}

	server.SetupMockPublishAsyncResponse(&proto.PublishResponse{Ack: expectedAck})

	ack, err := client.Publish(context.Background(), "foo", []byte("hello"))
	require.NoError(t, err)
	require.Equal(t, expectedAck.Stream, ack.Stream())
	require.Equal(t, expectedAck.PartitionSubject, ack.PartitionSubject())
	require.Equal(t, expectedAck.MsgSubject, ack.MessageSubject())
	require.Equal(t, expectedAck.Offset, ack.Offset())
	require.Equal(t, expectedAck.AckInbox, ack.AckInbox())
	require.Equal(t, expectedAck.CorrelationId, ack.CorrelationID())
	require.Equal(t, AckPolicy(expectedAck.AckPolicy), ack.AckPolicy())

	req := server.GetPublishAsyncRequests()[0]
	require.Equal(t, []byte(nil), req.Key)
	require.Equal(t, []byte("hello"), req.Value)
	require.Equal(t, "foo", req.Stream)
	require.Equal(t, int32(0), req.Partition)
	require.Equal(t, map[string][]byte(nil), req.Headers)
	require.Equal(t, "", req.AckInbox)
	require.NotEqual(t, "", req.CorrelationId)
	require.Equal(t, proto.AckPolicy_LEADER, req.AckPolicy)
}

func TestPublishAckPolicyNone(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	_, err = client.Publish(context.Background(), "foo", []byte("hello"), AckPolicyNone())
	require.NoError(t, err)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(server.GetPublishAsyncRequests()) == 0 {
			continue
		}
		break
	}

	req := server.GetPublishAsyncRequests()[0]
	require.Equal(t, []byte(nil), req.Key)
	require.Equal(t, []byte("hello"), req.Value)
	require.Equal(t, "foo", req.Stream)
	require.Equal(t, int32(0), req.Partition)
	require.Equal(t, map[string][]byte(nil), req.Headers)
	require.Equal(t, "", req.AckInbox)
	require.NotEqual(t, "", req.CorrelationId)
	require.Equal(t, proto.AckPolicy_NONE, req.AckPolicy)
}

func TestPublishAckTimeout(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)},
		AckWaitTime(time.Nanosecond))
	require.NoError(t, err)
	defer client.Close()

	_, err = client.Publish(context.Background(), "foo", []byte("hello"))
	require.Equal(t, ErrAckTimeout, err)
}

func TestPublishAsync(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	expectedAck := &proto.Ack{
		Stream:           "foo",
		PartitionSubject: "foo",
		MsgSubject:       "foo",
		Offset:           0,
		AckInbox:         "ack",
		AckPolicy:        proto.AckPolicy_LEADER,
	}

	server.SetupMockPublishAsyncResponse(&proto.PublishResponse{Ack: expectedAck})

	ackC := make(chan *Ack)
	err = client.PublishAsync(context.Background(), "foo", []byte("hello"),
		func(ack *Ack, err error) {
			require.NoError(t, err)
			ackC <- ack
		},
	)
	require.NoError(t, err)

	select {
	case ack := <-ackC:
		require.Equal(t, expectedAck.Stream, ack.Stream())
		require.Equal(t, expectedAck.PartitionSubject, ack.PartitionSubject())
		require.Equal(t, expectedAck.MsgSubject, ack.MessageSubject())
		require.Equal(t, expectedAck.Offset, ack.Offset())
		require.Equal(t, expectedAck.AckInbox, ack.AckInbox())
		require.Equal(t, expectedAck.CorrelationId, ack.CorrelationID())
		require.Equal(t, AckPolicy(expectedAck.AckPolicy), ack.AckPolicy())
	case <-time.After(time.Second):
		t.Fatal("Did not receive expected ack")
	}

	req := server.GetPublishAsyncRequests()[0]
	require.Equal(t, []byte(nil), req.Key)
	require.Equal(t, []byte("hello"), req.Value)
	require.Equal(t, "foo", req.Stream)
	require.Equal(t, int32(0), req.Partition)
	require.Equal(t, map[string][]byte(nil), req.Headers)
	require.Equal(t, "", req.AckInbox)
	require.NotEqual(t, "", req.CorrelationId)
	require.Equal(t, proto.AckPolicy_LEADER, req.AckPolicy)
}

func TestPublishAsyncAckTimeout(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	errorC := make(chan error)
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	err = client.PublishAsync(ctx, "foo", []byte("hello"),
		func(ack *Ack, err error) {
			errorC <- err
		},
	)
	require.NoError(t, err)

	select {
	case err := <-errorC:
		require.Equal(t, ErrAckTimeout, err)
	case <-time.After(time.Second):
		t.Fatal("Did not receive expected error")
	}
}

func TestPublishAsyncPartitionNotFound(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	server.SetupMockPublishAsyncResponse(&proto.PublishResponse{
		AsyncError: &proto.PublishAsyncError{
			Code:    proto.PublishAsyncError_NOT_FOUND,
			Message: "partition not found",
		},
	})

	errorC := make(chan error)
	err = client.PublishAsync(context.Background(), "foo", []byte("hello"),
		func(ack *Ack, err error) {
			errorC <- err
		})
	require.NoError(t, err)

	select {
	case err := <-errorC:
		require.Equal(t, ErrNoSuchPartition, err)
	case <-time.After(time.Second):
		t.Fatal("Did not receive expected error")
	}
}

func TestPublishAsyncReadonlyPartition(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	server.SetupMockPublishAsyncResponse(&proto.PublishResponse{
		AsyncError: &proto.PublishAsyncError{
			Code:    proto.PublishAsyncError_READONLY,
			Message: "partition is readonly",
		},
	})

	errorC := make(chan error)
	err = client.PublishAsync(context.Background(), "foo", []byte("hello"),
		func(ack *Ack, err error) {
			errorC <- err
		})
	require.NoError(t, err)

	select {
	case err := <-errorC:
		require.Equal(t, ErrReadonlyPartition, err)
	case <-time.After(time.Second):
		t.Fatal("Did not receive expected error")
	}
}

func TestPublishAsyncInternalError(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	server.SetupMockPublishAsyncResponse(&proto.PublishResponse{
		AsyncError: &proto.PublishAsyncError{
			Code:    proto.PublishAsyncError_UNKNOWN,
			Message: "internal error",
		},
	})

	errorC := make(chan error)
	err = client.PublishAsync(context.Background(), "foo", []byte("hello"),
		func(ack *Ack, err error) {
			errorC <- err
		})
	require.NoError(t, err)

	select {
	case err := <-errorC:
		require.Error(t, err)
		require.Equal(t, "internal error", err.Error())
	case <-time.After(time.Second):
		t.Fatal("Did not receive expected error")
	}
}

func TestPublishToPartition(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	expectedAck := &proto.Ack{
		Stream:           "foo",
		PartitionSubject: "foo.1",
		MsgSubject:       "foo.1",
		Offset:           0,
		AckInbox:         "ack",
		CorrelationId:    "123",
		AckPolicy:        proto.AckPolicy_ALL,
	}

	server.SetupMockPublishAsyncResponse(&proto.PublishResponse{Ack: expectedAck})

	ack, err := client.Publish(context.Background(), "foo", []byte("hello"),
		ToPartition(1), Key([]byte("key")), AckPolicyAll(), Header("foo", []byte("bar")))

	require.NoError(t, err)
	require.Equal(t, expectedAck.Stream, ack.Stream())
	require.Equal(t, expectedAck.PartitionSubject, ack.PartitionSubject())
	require.Equal(t, expectedAck.MsgSubject, ack.MessageSubject())
	require.Equal(t, expectedAck.Offset, ack.Offset())
	require.Equal(t, expectedAck.AckInbox, ack.AckInbox())
	require.Equal(t, expectedAck.CorrelationId, ack.CorrelationID())
	require.Equal(t, AckPolicy(expectedAck.AckPolicy), ack.AckPolicy())

	req := server.GetPublishAsyncRequests()[0]
	require.Equal(t, []byte("key"), req.Key)
	require.Equal(t, []byte("hello"), req.Value)
	require.Equal(t, "foo", req.Stream)
	require.Equal(t, int32(1), req.Partition)
	require.Equal(t, map[string][]byte(nil), req.Headers)
	require.Equal(t, "", req.AckInbox)
	require.NotEqual(t, "", req.CorrelationId)
	require.Equal(t, proto.AckPolicy_ALL, req.AckPolicy)
}

func TestPublishRoundRobin(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	metadataResp := &proto.FetchMetadataResponse{
		Brokers: []*proto.Broker{{
			Id:   "a",
			Host: "localhost",
			Port: int32(port),
		}},
		Metadata: []*proto.StreamMetadata{{
			Name:    "foo",
			Subject: "foo",
			Partitions: map[int32]*proto.PartitionMetadata{
				0: {
					Id:       0,
					Leader:   "a",
					Replicas: []string{"a"},
					Isr:      []string{"a"},
				},
				1: {
					Id:       1,
					Leader:   "a",
					Replicas: []string{"a"},
					Isr:      []string{"a"},
				},
			},
		}},
	}
	server.SetupMockFetchMetadataResponse(metadataResp)

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	server.SetupMockPublishAsyncResponse(&proto.PublishResponse{
		Ack: &proto.Ack{},
	})
	_, err = client.Publish(context.Background(), "foo", []byte("hello"),
		PartitionByRoundRobin())
	require.NoError(t, err)

	server.SetupMockPublishToSubjectResponse(&proto.PublishResponse{
		Ack: &proto.Ack{},
	})
	_, err = client.Publish(context.Background(), "foo", []byte("hello"),
		PartitionByRoundRobin())
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		req := server.GetPublishAsyncRequests()[i]
		require.Equal(t, []byte(nil), req.Key)
		require.Equal(t, []byte("hello"), req.Value)
		require.Equal(t, "foo", req.Stream)
		require.Equal(t, int32(i), req.Partition)
		require.Equal(t, map[string][]byte(nil), req.Headers)
		require.Equal(t, "", req.AckInbox)
		require.NotEqual(t, "", req.CorrelationId)
		require.Equal(t, proto.AckPolicy_LEADER, req.AckPolicy)
	}
}

func TestPublishToSubject(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	expectedAck := &proto.Ack{
		Stream:           "foo",
		PartitionSubject: "foo",
		MsgSubject:       "foo",
		Offset:           0,
		AckInbox:         "ack",
		CorrelationId:    "123",
		AckPolicy:        proto.AckPolicy_LEADER,
	}

	server.SetupMockPublishToSubjectResponse(&proto.PublishToSubjectResponse{Ack: expectedAck})

	ack, err := client.PublishToSubject(context.Background(), "foo", []byte("hello"), Key([]byte("key")))
	require.NoError(t, err)
	require.Equal(t, expectedAck.Stream, ack.Stream())
	require.Equal(t, expectedAck.PartitionSubject, ack.PartitionSubject())
	require.Equal(t, expectedAck.MsgSubject, ack.MessageSubject())
	require.Equal(t, expectedAck.Offset, ack.Offset())
	require.Equal(t, expectedAck.AckInbox, ack.AckInbox())
	require.Equal(t, expectedAck.CorrelationId, ack.CorrelationID())
	require.Equal(t, AckPolicy(expectedAck.AckPolicy), ack.AckPolicy())

	req := server.GetPublishToSubjectRequests()[0]
	require.Equal(t, []byte("key"), req.Key)
	require.Equal(t, []byte("hello"), req.Value)
	require.Equal(t, "foo", req.Subject)
	require.Equal(t, map[string][]byte(nil), req.Headers)
	require.Equal(t, "", req.AckInbox)
	require.Equal(t, "", req.CorrelationId)
	require.Equal(t, proto.AckPolicy_LEADER, req.AckPolicy)
}

func TestPublishToSubjectAckTimeout(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	server.SetupMockPublishToSubjectError(status.Error(codes.DeadlineExceeded, "deadline exceeded"))

	_, err = client.PublishToSubject(context.Background(), "foo", []byte("hello"))
	require.Equal(t, ErrAckTimeout, err)
}

func TestFetchMetadata(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	metadataResp := &proto.FetchMetadataResponse{
		Brokers: []*proto.Broker{{
			Id:   "a",
			Host: "localhost",
			Port: int32(port),
		}},
		Metadata: []*proto.StreamMetadata{{
			Name:    "foo",
			Subject: "foo-subject",
			Partitions: map[int32]*proto.PartitionMetadata{
				0: {
					Id:       0,
					Leader:   "a",
					Replicas: []string{"a"},
					Isr:      []string{"a"},
				},
			},
		}},
	}
	server.SetupMockFetchMetadataResponse(metadataResp)

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	metadata, err := client.FetchMetadata(context.Background())
	require.NoError(t, err)
	require.Len(t, metadata.Brokers(), 1)
	broker := metadata.Brokers()[0]
	require.Equal(t, "a", broker.ID())
	require.Equal(t, "localhost", broker.Host())
	require.Equal(t, int32(port), broker.Port())
	require.Equal(t, fmt.Sprintf("localhost:%d", port), broker.Addr())

	stream := metadata.GetStream("foo")
	require.NotNil(t, stream)
	require.Equal(t, "foo-subject", stream.Subject())
	require.Equal(t, "foo", stream.Name())
	require.Len(t, stream.Partitions(), 1)
	partition := stream.GetPartition(0)
	require.NotNil(t, partition)
	require.Equal(t, int32(0), partition.ID())
	require.Equal(t, []*BrokerInfo{broker}, partition.Replicas())
	require.Equal(t, []*BrokerInfo{broker}, partition.ISR())
	require.Equal(t, broker, partition.Leader())
	require.Equal(t, false, partition.Paused())

	req := server.GetFetchMetadataRequests()[0]
	require.Equal(t, []string(nil), req.Streams)
}

func TestSubscribeDisconnectError(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	metadataResp := &proto.FetchMetadataResponse{
		Brokers: []*proto.Broker{{
			Id:   "a",
			Host: "localhost",
			Port: int32(port),
		}},
		Metadata: []*proto.StreamMetadata{{
			Name:    "foo",
			Subject: "foo",
			Partitions: map[int32]*proto.PartitionMetadata{
				0: {
					Id:       0,
					Leader:   "a",
					Replicas: []string{"a"},
					Isr:      []string{"a"},
				},
			},
		}},
	}
	server.SetupMockFetchMetadataResponse(metadataResp)

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)}, ResubscribeWaitTime(0))
	require.NoError(t, err)

	ch := make(chan error)
	err = client.Subscribe(context.Background(), "foo", func(msg *Message, err error) {
		ch <- err
	})

	server.Stop(t)

	select {
	case err := <-ch:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected error")
	}
}

func TestResubscribeFail(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	metadataResp := &proto.FetchMetadataResponse{
		Brokers: []*proto.Broker{{
			Id:   "a",
			Host: "localhost",
			Port: int32(port),
		}},
		Metadata: []*proto.StreamMetadata{{
			Name:    "foo",
			Subject: "foo",
			Partitions: map[int32]*proto.PartitionMetadata{
				0: {
					Id:       0,
					Leader:   "a",
					Replicas: []string{"a"},
					Isr:      []string{"a"},
				},
			},
		}},
	}
	server.SetupMockFetchMetadataResponse(metadataResp)

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)},
		ResubscribeWaitTime(time.Millisecond))

	ch := make(chan error)
	err = client.Subscribe(context.Background(), "foo", func(msg *Message, err error) {
		ch <- err
	})
	require.NoError(t, err)

	server.Stop(t)

	select {
	case err := <-ch:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected error")
	}
}

func TestConnectionContextDeadlineExceeded(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	// Use a context with an already exceeded deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()

	_, err := ConnectCtx(ctx, []string{fmt.Sprintf("localhost:%d", port)})
	require.Equal(t, context.DeadlineExceeded, err)
}

func TestStreamOptionsNewRequest(t *testing.T) {
	var (
		retentionMaxBytes             = int64(1024)
		retentionMaxMessages          = int64(10)
		retentionMaxAge               = time.Hour
		cleanerInterval               = time.Minute
		segmentMaxBytes               = int64(512)
		segmentMaxAge                 = time.Minute
		compactMaxGoroutines          = int32(5)
		compactEnabled                = true
		autoPauseTime                 = time.Minute
		autoPauseDisableIfSubscribers = true
		minISR                        = 2
	)
	options := []StreamOption{
		Group("foo"),
		ReplicationFactor(2),
		Partitions(3),
		RetentionMaxBytes(retentionMaxBytes),
		RetentionMaxMessages(retentionMaxMessages),
		RetentionMaxAge(retentionMaxAge),
		CleanerInterval(cleanerInterval),
		SegmentMaxBytes(segmentMaxBytes),
		SegmentMaxAge(segmentMaxAge),
		CompactMaxGoroutines(compactMaxGoroutines),
		CompactEnabled(compactEnabled),
		AutoPauseTime(autoPauseTime),
		AutoPauseDisableIfSubscribers(autoPauseDisableIfSubscribers),
		MinISR(minISR),
	}

	opts := &StreamOptions{}
	for _, opt := range options {
		require.NoError(t, opt(opts))
	}

	req := opts.newRequest("foo", "bar")

	require.Equal(t, "foo", req.Group)
	require.Equal(t, int32(2), req.ReplicationFactor)
	require.Equal(t, int32(3), req.Partitions)
	require.Equal(t, retentionMaxBytes, req.RetentionMaxBytes.Value)
	require.Equal(t, retentionMaxMessages, req.RetentionMaxMessages.Value)
	require.Equal(t, retentionMaxAge.Milliseconds(), req.RetentionMaxAge.Value)
	require.Equal(t, cleanerInterval.Milliseconds(), req.CleanerInterval.Value)
	require.Equal(t, segmentMaxBytes, req.SegmentMaxBytes.Value)
	require.Equal(t, segmentMaxAge.Milliseconds(), req.SegmentMaxAge.Value)
	require.Equal(t, compactMaxGoroutines, req.CompactMaxGoroutines.Value)
	require.Equal(t, compactEnabled, req.CompactEnabled.Value)
	require.Equal(t, autoPauseTime.Milliseconds(), req.AutoPauseTime.Value)
	require.Equal(t, autoPauseDisableIfSubscribers, req.AutoPauseDisableIfSubscribers.Value)
	require.Equal(t, int32(minISR), req.MinIsr.Value)
}

func TestSetCursor(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	metadataResp := &proto.FetchMetadataResponse{
		Brokers: []*proto.Broker{{
			Id:   "a",
			Host: "localhost",
			Port: int32(port),
		}},
		Metadata: []*proto.StreamMetadata{{
			Name:    "__cursors",
			Subject: "__cursors",
			Partitions: map[int32]*proto.PartitionMetadata{
				0: {
					Id:       0,
					Leader:   "a",
					Replicas: []string{"a"},
					Isr:      []string{"a"},
				},
			},
		}},
	}
	server.SetupMockFetchMetadataResponse(metadataResp)
	server.SetupMockSetCursorResponse(new(proto.SetCursorResponse))

	err = client.SetCursor(context.Background(), "foo", "bar", 1, 5)
	require.NoError(t, err)

	reqs := server.GetSetCursorRequests()
	require.Len(t, reqs, 1)
	req := reqs[0]
	require.Equal(t, "foo", req.CursorId)
	require.Equal(t, "bar", req.Stream)
	require.Equal(t, int32(1), req.Partition)
	require.Equal(t, int64(5), req.Offset)
}

func TestSetCursorNotLeader(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	metadataResp := &proto.FetchMetadataResponse{
		Brokers: []*proto.Broker{{
			Id:   "a",
			Host: "localhost",
			Port: int32(port),
		}},
		Metadata: []*proto.StreamMetadata{{
			Name:    "__cursors",
			Subject: "__cursors",
			Partitions: map[int32]*proto.PartitionMetadata{
				0: {
					Id:       0,
					Leader:   "a",
					Replicas: []string{"a"},
					Isr:      []string{"a"},
				},
			},
		}},
	}

	server.SetupMockFetchMetadataResponse(metadataResp)
	server.SetupMockSetCursorError(status.Error(codes.FailedPrecondition, "server is not partition leader"))

	err = client.SetCursor(context.Background(), "foo", "bar", 2, 5)
	require.Error(t, err)
}

func TestFetchCursor(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	metadataResp := &proto.FetchMetadataResponse{
		Brokers: []*proto.Broker{{
			Id:   "a",
			Host: "localhost",
			Port: int32(port),
		}},
		Metadata: []*proto.StreamMetadata{{
			Name:    "__cursors",
			Subject: "__cursors",
			Partitions: map[int32]*proto.PartitionMetadata{
				0: {
					Id:       0,
					Leader:   "a",
					Replicas: []string{"a"},
					Isr:      []string{"a"},
				},
			},
		}},
	}

	resp := &proto.FetchCursorResponse{Offset: 11}
	server.SetupMockFetchMetadataResponse(metadataResp)
	server.SetupMockFetchCursorRequestsResponse(resp)

	offset, err := client.FetchCursor(context.Background(), "foo", "bar", 1)
	require.NoError(t, err)
	require.Equal(t, int64(11), offset)

	reqs := server.GetFetchCursorRequests()
	require.Len(t, reqs, 1)
	req := reqs[0]
	require.Equal(t, "foo", req.CursorId)
	require.Equal(t, "bar", req.Stream)
	require.Equal(t, int32(1), req.Partition)
}

func TestFetchCursorNotLeader(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	metadataResp := &proto.FetchMetadataResponse{
		Brokers: []*proto.Broker{{
			Id:   "a",
			Host: "localhost",
			Port: int32(port),
		}},
		Metadata: []*proto.StreamMetadata{{
			Name:    "__cursors",
			Subject: "__cursors",
			Partitions: map[int32]*proto.PartitionMetadata{
				0: {
					Id:       0,
					Leader:   "a",
					Replicas: []string{"a"},
					Isr:      []string{"a"},
				},
			},
		}},
	}

	server.SetupMockFetchMetadataResponse(metadataResp)
	server.SetupMockFetchCursorError(status.Error(codes.FailedPrecondition, "server is not partition leader"))

	_, err = client.FetchCursor(context.Background(), "foo", "bar", 2)
	require.Error(t, err)
}

func TestFetchPartitionMetadata(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)
	metadataResp := &proto.FetchMetadataResponse{
		Brokers: []*proto.Broker{{
			Id:   "a",
			Host: "localhost",
			Port: int32(port),
		}},
		Metadata: []*proto.StreamMetadata{{
			Name:    "foo",
			Subject: "foo",
			Partitions: map[int32]*proto.PartitionMetadata{
				0: {
					Id:       0,
					Leader:   "a",
					Replicas: []string{"a"},
					Isr:      []string{"a"},
				},
			},
		}},
	}
	server.SetupMockFetchMetadataResponse(metadataResp)

	partitionMetadataResp := &proto.FetchPartitionMetadataResponse{
		Metadata: &proto.PartitionMetadata{
			Id:            0,
			Leader:        "a",
			Replicas:      []string{"a"},
			Isr:           []string{"a"},
			HighWatermark: 100,
			NewestOffset:  105,
			MessagesReceivedTimestamps: &proto.PartitionEventTimestamps{
				FirstTimestamp:  110,
				LatestTimestamp: 115,
			},
			PauseTimestamps: &proto.PartitionEventTimestamps{
				FirstTimestamp:  120,
				LatestTimestamp: 125,
			},
			ReadonlyTimestamps: &proto.PartitionEventTimestamps{
				FirstTimestamp:  130,
				LatestTimestamp: 135,
			},
		},
	}
	server.SetupMockFetchPartitionMetadataResponse(partitionMetadataResp)

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	resp, err := client.FetchPartitionMetadata(context.Background(), "foo", 0)
	require.NoError(t, err)
	require.Equal(t, int64(100), resp.HighWatermark())
	require.Equal(t, int64(105), resp.NewestOffset())
	require.Equal(t, time.Unix(0, 110), resp.MessagesReceivedTimestamps().FirstTime())
	require.Equal(t, time.Unix(0, 115), resp.MessagesReceivedTimestamps().LatestTime())
	require.Equal(t, time.Unix(0, 120), resp.PauseTimestamps().FirstTime())
	require.Equal(t, time.Unix(0, 125), resp.PauseTimestamps().LatestTime())
	require.Equal(t, time.Unix(0, 130), resp.ReadonlyTimestamps().FirstTime())
	require.Equal(t, time.Unix(0, 135), resp.ReadonlyTimestamps().LatestTime())

	// Expect broker info exists for leader, isr and replicas
	broker := &BrokerInfo{id: "a",
		host: "localhost",
		port: int32(port)}

	expectedISR := []*BrokerInfo{broker}
	expectedReplicas := []*BrokerInfo{broker}
	require.Equal(t, broker, resp.Leader())
	require.Equal(t, expectedISR, resp.ISR())
	require.Equal(t, expectedReplicas, resp.Replicas())
}

func TestFetchPartitionMetadataNotLeader(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	metadataResp := &proto.FetchMetadataResponse{
		Brokers: []*proto.Broker{{
			Id:   "a",
			Host: "localhost",
			Port: int32(port),
		}},
		Metadata: []*proto.StreamMetadata{{
			Name:    "foo",
			Subject: "foo",
			Partitions: map[int32]*proto.PartitionMetadata{
				0: {
					Id:       0,
					Leader:   "a",
					Replicas: []string{"a"},
					Isr:      []string{"a"},
				},
			},
		}},
	}

	server.SetupMockFetchMetadataResponse(metadataResp)
	server.SetupMockFetchPartitionMetadataError(status.Error(codes.FailedPrecondition, "server is not partition leader"))
	_, err = client.FetchPartitionMetadata(context.Background(), "foo", 0)
	require.Error(t, err)
}

func ExampleConnect() {
	addr := "localhost:9292"
	client, err := Connect([]string{addr})
	if err != nil {
		panic(err)
	}
	defer client.Close()
}

func ExampleClient_createStream() {
	// Connect to Liftbridge.
	addr := "localhost:9292"
	client, err := Connect([]string{addr})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Create stream with a single partition.
	if err := client.CreateStream(context.Background(), "foo", "foo-stream"); err != nil {
		panic(err)
	}

	// Create stream with three partitions.
	if err := client.CreateStream(context.Background(), "bar", "bar-stream", Partitions(3)); err != nil {
		panic(err)
	}
}

func ExampleClient_subscribe() {
	// Connect to Liftbridge.
	addr := "localhost:9292"
	client, err := Connect([]string{addr})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Subscribe to base stream partition.
	if err := client.Subscribe(context.Background(), "foo-stream", func(msg *Message, err error) {
		if err != nil {
			panic(err)
		}
		fmt.Println(msg.Offset(), string(msg.Value()))
	}); err != nil {
		panic(err)
	}

	// Subscribe to a specific stream partition.
	ctx := context.Background()
	if err := client.Subscribe(ctx, "bar-stream", func(msg *Message, err error) {
		if err != nil {
			panic(err)
		}
		fmt.Println(msg.Offset(), string(msg.Value()))
	}, Partition(1)); err != nil {
		panic(err)
	}

	<-ctx.Done()
}

func ExampleClient_publish() {
	// Connect to Liftbridge.
	addr := "localhost:9292"
	client, err := Connect([]string{addr})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Publish message to base stream partition.
	if _, err := client.Publish(context.Background(), "foo-stream", []byte("hello")); err != nil {
		panic(err)
	}

	// Publish message to stream partition based on key.
	if _, err := client.Publish(context.Background(), "bar-stream", []byte("hello"),
		Key([]byte("key")), PartitionByKey(),
	); err != nil {
		panic(err)
	}
}

func ExampleClient_publishToSubject() {
	// Connect to Liftbridge.
	addr := "localhost:9292"
	client, err := Connect([]string{addr})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Publish message directly to NATS subject.
	if _, err := client.PublishToSubject(context.Background(), "foo.bar", []byte("hello")); err != nil {
		panic(err)
	}
}

func ExampleNewMessage() {
	// Create NATS connection.
	conn, err := nats.GetDefaultOptions().Connect()
	if err != nil {
		panic(err)
	}
	defer conn.Flush()
	defer conn.Close()

	// Publish simple message.
	msg := NewMessage([]byte("value"))
	if err := conn.Publish("foo", msg); err != nil {
		panic(err)
	}

	// Publish message with options.
	msg = NewMessage([]byte("value"),
		Key([]byte("key")),
		AckPolicyAll(),
		AckInbox("ack"),
		CorrelationID("123"),
	)
	if err := conn.Publish("foo", msg); err != nil {
		panic(err)
	}
}

func ExampleUnmarshalAck() {
	// Create NATS connection.
	conn, err := nats.GetDefaultOptions().Connect()
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// Setup ack inbox.
	ackInbox := "acks"
	acked := make(chan struct{})
	_, err = conn.Subscribe(ackInbox, func(m *nats.Msg) {
		ack, err := UnmarshalAck(m.Data)
		if err != nil {
			panic(err)
		}
		fmt.Println("ack:", ack.Stream(), ack.Offset(), ack.MessageSubject())
		close(acked)
	})
	if err != nil {
		panic(err)
	}

	// Publish message.
	msg := NewMessage([]byte("value"), Key([]byte("key")), AckInbox(ackInbox))
	if err := conn.Publish("foo", msg); err != nil {
		panic(err)
	}

	<-acked
}

func marshalAck(t *testing.T, ack *proto.Ack) []byte {
	data, err := marshalEnvelope(ack, msgTypeAck)
	require.NoError(t, err)
	return data
}
