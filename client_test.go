package liftbridge

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	natsdTest "github.com/nats-io/nats-server/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	proto "github.com/liftbridge-io/liftbridge-api/go"
	"github.com/liftbridge-io/liftbridge/server"
)

type message struct {
	Key    []byte
	Value  []byte
	Offset int64
}

func assertMsg(t *testing.T, expected *message, msg Message) {
	require.Equal(t, expected.Offset, msg.Offset())
	require.Equal(t, expected.Key, msg.Key())
	require.Equal(t, expected.Value, msg.Value())
}

func getPartitionLeader(t *testing.T, timeout time.Duration, name string, partitionID int32,
	client *client, servers map[*server.Config]*server.Server) (*server.Server, *server.Config) {
	var (
		leaderID string
		deadline = time.Now().Add(timeout)
	)
	for time.Now().Before(deadline) {
		metadata, err := client.FetchMetadata(context.Background())
		require.NoError(t, err)
		stream := metadata.GetStream(name)
		if stream == nil {
			time.Sleep(15 * time.Millisecond)
			continue
		}
		partition := stream.GetPartition(partitionID)
		if partition == nil {
			time.Sleep(15 * time.Millisecond)
			continue
		}
		leader := partition.Leader()
		if leader == nil {
			time.Sleep(15 * time.Millisecond)
			continue
		}
		leaderID = leader.ID()
		if leaderID == "" {
			time.Sleep(15 * time.Millisecond)
			continue
		}
		for config, s := range servers {
			if config.Clustering.ServerID == leaderID {
				return s, config
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	return nil, nil
}

func TestUnmarshalAck(t *testing.T) {
	ack := &proto.Ack{
		Stream:     "bar",
		MsgSubject: "foo",
		Offset:     1,
		AckInbox:   "acks",
	}
	data, err := ack.Marshal()
	require.NoError(t, err)
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
		key      = []byte("foo")
		value    = []byte("bar")
		ackInbox = "acks"
	)
	msg := NewMessage(value,
		Key(key),
		AckInbox(ackInbox),
		AckPolicyNone(),
		CorrelationID("123"),
	)
	actual, ok := UnmarshalMessage(msg)
	require.True(t, ok)
	require.Equal(t, key, actual.Key())
	require.Equal(t, value, actual.Value())
	require.Equal(t, ackInbox, actual.AckInbox())
	require.Equal(t, AckPolicy(proto.AckPolicy_NONE), actual.AckPolicy())
	require.Equal(t, "123", actual.CorrelationID())
}

func TestUnmarshalMessageError(t *testing.T) {
	_, ok := UnmarshalMessage(nil)
	require.False(t, ok)

	_, ok = UnmarshalMessage([]byte("blahh"))
	require.False(t, ok)

	buf := make([]byte, 8)
	copy(buf, envelopeMagicNumber)
	copy(buf[envelopeMagicNumberLen:], []byte("blah"))
	_, ok = UnmarshalMessage(buf)
	require.False(t, ok)

	// CRC flag set with no CRC present.
	msg := NewMessage([]byte("hello"))
	msg[6] = setBit(msg[6], 0)
	_, ok = UnmarshalMessage(msg)
	require.False(t, ok)

	// CRC flag set with invalid CRC.
	msg = NewMessage([]byte("hello"))
	msg[6] = setBit(msg[6], 0)
	buf = make([]byte, len(msg)+4)
	copy(buf, msg[:8])
	buf[8] = byte(32)
	copy(buf[12:], msg[8:])
	buf[5] = byte(12)
	_, ok = UnmarshalMessage(buf)
	require.False(t, ok)

	// Unknown protocol version.
	msg = NewMessage([]byte("hello"))
	msg[4] = 0x01
	_, ok = UnmarshalMessage(msg)
	require.False(t, ok)
}

func TestConnectNoAddrs(t *testing.T) {
	_, err := Connect(nil)
	require.Error(t, err)
}

func TestClientSubscribe(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	config := getTestConfig("a", true, 5050)
	s := runServerWithConfig(t, config)
	defer s.Stop()

	client, err := Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s)

	require.NoError(t, client.CreateStream(context.Background(), "foo", "bar"))

	count := 5
	expected := make([]*message, count)
	for i := 0; i < count; i++ {
		expected[i] = &message{
			Key:    []byte("test"),
			Value:  []byte(strconv.Itoa(i)),
			Offset: int64(i),
		}
	}

	for i := 0; i < count; i++ {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := client.Publish(ctx, "bar", expected[i].Value, Key(expected[i].Key))
		require.NoError(t, err)
	}

	// Subscribe from the beginning.
	recv := 0
	ch1 := make(chan struct{})
	ch2 := make(chan struct{})
	err = client.Subscribe(context.Background(), "bar", func(msg Message, err error) {
		require.NoError(t, err)
		expect := expected[recv]
		assertMsg(t, expect, msg)
		recv++
		if recv == count {
			close(ch1)
			return
		}
		if recv == 2*count {
			close(ch2)
			return
		}
	}, StartAtEarliestReceived())
	require.NoError(t, err)

	// Wait to read back publishes messages.
	select {
	case <-ch1:
	case <-time.After(10 * time.Second):
		t.Fatal("Did not receive all expected messages")
	}

	// Publish some more.
	for i := 0; i < count; i++ {
		expected = append(expected, &message{
			Key:    []byte("blah"),
			Value:  []byte(strconv.Itoa(i + count)),
			Offset: int64(i + count),
		})
	}

	for i := 0; i < count; i++ {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := client.Publish(ctx, "bar", expected[i+count].Value,
			Key(expected[i+count].Key))
		require.NoError(t, err)
	}

	// Wait to read the new messages.
	select {
	case <-ch2:
	case <-time.After(10 * time.Second):
		t.Fatal("Did not receive all expected messages")
	}
}

// Ensure an error isn't returned on the stream when the client is closed.
func TestClientCloseNoError(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	config := getTestConfig("a", true, 5050)
	s := runServerWithConfig(t, config)
	defer s.Stop()

	client, err := Connect([]string{"localhost:5050"})
	require.NoError(t, err)

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s)

	require.NoError(t, client.CreateStream(context.Background(), "foo", "bar"))

	err = client.Subscribe(context.Background(), "bar", func(msg Message, err error) {
		require.NoError(t, err)
	})
	require.NoError(t, err)
	client.Close()
}

// Ensure an error is returned on the stream when the client is disconnected
// and reconnect is disabled.
func TestClientDisconnectError(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	config := getTestConfig("a", true, 5050)
	s := runServerWithConfig(t, config)
	defer s.Stop()

	client, err := Connect([]string{"localhost:5050"}, ResubscribeWaitTime(0))
	require.NoError(t, err)

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s)

	require.NoError(t, client.CreateStream(context.Background(), "foo", "bar"))

	ch := make(chan error)
	err = client.Subscribe(context.Background(), "bar", func(msg Message, err error) {
		ch <- err
	})

	s.Stop()

	select {
	case err := <-ch:
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Did not receive expected error")
	}
}

// Ensure if the the stream leader for a subscription fails and the client is
// unable to re-establish the subscription, an error is returned on it.
func TestClientResubscribeFail(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	config := getTestConfig("a", true, 5050)
	s := runServerWithConfig(t, config)
	defer s.Stop()

	client, err := Connect([]string{"localhost:5050"}, ResubscribeWaitTime(time.Millisecond))
	require.NoError(t, err)
	defer client.Close()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s)

	require.NoError(t, client.CreateStream(context.Background(), "foo", "bar"))

	ch := make(chan error)
	err = client.Subscribe(context.Background(), "bar", func(msg Message, err error) {
		ch <- err
	})
	require.NoError(t, err)

	s.Stop()

	select {
	case err := <-ch:
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Did not receive expected error")
	}
}

// Ensure messages sent with the publish API are received on a stream and an
// ack is received when an AckPolicy and timeout are configured.
func TestClientPublishAck(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	config := getTestConfig("a", true, 5050)
	s := runServerWithConfig(t, config)
	defer s.Stop()

	client, err := Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s)

	require.NoError(t, client.CreateStream(context.Background(), "foo", "bar"))

	count := 5
	expected := make([]*message, count)
	for i := 0; i < count; i++ {
		expected[i] = &message{
			Key:    []byte("test"),
			Value:  []byte(strconv.Itoa(i)),
			Offset: int64(i),
		}
	}

	for i := 0; i < count; i++ {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		ack, err := client.Publish(ctx, "bar", expected[i].Value,
			Key(expected[i].Key), AckPolicyLeader())
		require.NoError(t, err)
		require.NotNil(t, ack)
		require.Equal(t, "bar", ack.Stream())
	}

	// Subscribe from the beginning.
	recv := 0
	ch := make(chan struct{})
	err = client.Subscribe(context.Background(), "bar", func(msg Message, err error) {
		require.NoError(t, err)
		expect := expected[recv]
		assertMsg(t, expect, msg)
		recv++
		if recv == count {
			close(ch)
			return
		}
	}, StartAtEarliestReceived())
	require.NoError(t, err)

	// Wait to read back published messages.
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatal("Did not receive all expected messages")
	}
}

// Ensure messages sent with the publish API are received on a stream and a nil
// ack is returned when no AckPolicy is configured.
func TestClientPublishNoAck(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	config := getTestConfig("a", true, 5050)
	s := runServerWithConfig(t, config)
	defer s.Stop()

	client, err := Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s)

	require.NoError(t, client.CreateStream(context.Background(), "foo", "bar"))

	count := 5
	expected := make([]*message, count)
	for i := 0; i < count; i++ {
		expected[i] = &message{
			Key:    []byte("test"),
			Value:  []byte(strconv.Itoa(i)),
			Offset: int64(i),
		}
	}

	for i := 0; i < count; i++ {
		ack, err := client.Publish(context.Background(), "bar", expected[i].Value,
			Key(expected[i].Key))
		require.NoError(t, err)
		require.Nil(t, ack)
	}

	// Subscribe from the beginning.
	recv := 0
	ch := make(chan struct{})
	err = client.Subscribe(context.Background(), "bar", func(msg Message, err error) {
		require.NoError(t, err)
		expect := expected[recv]
		assertMsg(t, expect, msg)
		recv++
		if recv == count {
			close(ch)
			return
		}
	}, StartAtEarliestReceived())
	require.NoError(t, err)

	// Wait to read back published messages.
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatal("Did not receive all expected messages")
	}
}

// Ensure headers are set correctly on messages.
func TestClientPublishHeaders(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	config := getTestConfig("a", true, 5050)
	s := runServerWithConfig(t, config)
	defer s.Stop()

	client, err := Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s)

	require.NoError(t, client.CreateStream(context.Background(), "foo", "bar"))

	ack, err := client.Publish(context.Background(), "bar",
		[]byte("hello"),
		Header("a", []byte("header")),
		Headers(map[string][]byte{"some": []byte("more")}))
	require.NoError(t, err)
	require.Nil(t, ack)

	// Subscribe from the beginning.
	ch := make(chan Message)
	err = client.Subscribe(context.Background(), "bar", func(msg Message, err error) {
		require.NoError(t, err)
		ch <- msg
	}, StartAtEarliestReceived())
	require.NoError(t, err)

	// Wait to read back published messages.
	select {
	case msg := <-ch:
		require.Len(t, msg.Headers(), 2)
		for name, value := range msg.Headers() {
			if name == "a" {
				require.Equal(t, []byte("header"), value)
			}
			if name == "some" {
				require.Equal(t, []byte("more"), value)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive all expected messages")
	}
}

// Ensure when partitions are specified the stream has the correct number of
// partitions.
func TestPartitioning(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	config := getTestConfig("a", true, 5050)
	s := runServerWithConfig(t, config)
	defer s.Stop()

	conn, err := Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer conn.Close()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s)

	require.NoError(t, conn.CreateStream(context.Background(), "foo", "bar", Partitions(3)))

	metadata, err := conn.FetchMetadata(context.Background())
	require.NoError(t, err)
	stream := metadata.GetStream("bar")
	require.NotNil(t, stream)
	require.Len(t, stream.Partitions(), 3)
}

// Ensure ToPartition publishes a message to the given partition and Subscribe
// reads from the correct partition.
func TestPublishToPartition(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	config := getTestConfig("a", true, 5050)
	s := runServerWithConfig(t, config)
	defer s.Stop()

	client, err := Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s)

	var (
		subject = "foo"
		name    = "bar"
	)
	require.NoError(t, client.CreateStream(context.Background(), subject, name, Partitions(3)))

	_, err = client.Publish(context.Background(), name, []byte("hello"), ToPartition(1))
	require.NoError(t, err)

	recv := make(chan Message)
	err = client.Subscribe(context.Background(), name, func(msg Message, err error) {
		require.NoError(t, err)
		recv <- msg
	}, Partition(1), StartAtEarliestReceived())
	require.NoError(t, err)

	select {
	case msg := <-recv:
		require.Equal(t, []byte("hello"), msg.Value())
	case <-time.After(5 * time.Second):
		require.Fail(t, "Did not receive expected message")
	}
}

// Ensure PartitionByRoundRobin publishes messages evenly across partitions.
func TestPublishPartitionByRoundRobin(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	config := getTestConfig("a", true, 5050)
	s := runServerWithConfig(t, config)
	defer s.Stop()

	client, err := Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s)

	var (
		subject = "foo"
		name    = "bar"
	)
	require.NoError(t, client.CreateStream(context.Background(), subject, name, Partitions(3)))

	for i := 0; i < 3; i++ {
		_, err = client.Publish(context.Background(), name, []byte(strconv.Itoa(i)), PartitionByRoundRobin())
		require.NoError(t, err)
	}

	for i := 0; i < 3; i++ {
		recv := make(chan Message)
		err = client.Subscribe(context.Background(), name, func(msg Message, err error) {
			require.NoError(t, err)
			recv <- msg
		}, Partition(int32(i)), StartAtEarliestReceived())
		require.NoError(t, err)

		select {
		case msg := <-recv:
			require.Equal(t, []byte(strconv.Itoa(i)), msg.Value())
		case <-time.After(5 * time.Second):
			require.Fail(t, "Did not receive expected message")
		}
	}
}

// Ensure PartitionByKey publishes messages to partitions based on a hash of
// the message key.
func TestPublishPartitionByKey(t *testing.T) {
	defer cleanupStorage(t)
	oldHasher := hasher
	defer func() {
		hasher = oldHasher
	}()
	hash := uint32(0)
	hasher = func(data []byte) uint32 {
		ret := hash
		hash++
		return ret
	}

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	config := getTestConfig("a", true, 5050)
	s := runServerWithConfig(t, config)
	defer s.Stop()

	client, err := Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s)

	var (
		subject = "foo"
		name    = "bar"
	)
	require.NoError(t, client.CreateStream(context.Background(), subject, name, Partitions(3)))

	for i := 0; i < 3; i++ {
		_, err = client.Publish(context.Background(), name, []byte(strconv.Itoa(i)),
			Key([]byte(strconv.Itoa(i))),
			PartitionByKey(),
		)
		require.NoError(t, err)
	}

	msgs := map[string]struct{}{
		"0": {},
		"1": {},
		"2": {},
	}

	for i := 0; i < 3; i++ {
		recv := make(chan Message)
		err = client.Subscribe(context.Background(), name, func(msg Message, err error) {
			require.NoError(t, err)
			recv <- msg
		}, Partition(int32(i)), StartAtEarliestReceived())
		require.NoError(t, err)

		select {
		case msg := <-recv:
			_, ok := msgs[string(msg.Value())]
			require.True(t, ok)
			delete(msgs, string(msg.Value()))
		case <-time.After(5 * time.Second):
			require.Fail(t, "Did not receive expected message")
		}
	}
}

// Ensure PublishToSubject publishes directly to a NATS subject and all streams
// attached to the subject receive the message.
func TestPublishToSubject(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	config := getTestConfig("a", true, 5050)
	s := runServerWithConfig(t, config)
	defer s.Stop()

	client, err := Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s)

	var (
		subject = "foo"
		stream1 = "stream1"
		stream2 = "stream2"
		streams = []string{stream1, stream2}
	)
	require.NoError(t, client.CreateStream(context.Background(), subject, stream1))
	require.NoError(t, client.CreateStream(context.Background(), subject, stream2))

	_, err = client.PublishToSubject(context.Background(), subject, []byte("hello"))
	require.NoError(t, err)

	// Ensure both streams received the message.
	for _, stream := range streams {
		recv := make(chan Message)
		err = client.Subscribe(context.Background(), stream, func(msg Message, err error) {
			require.NoError(t, err)
			recv <- msg
		}, StartAtEarliestReceived())
		require.NoError(t, err)

		select {
		case <-recv:
		case <-time.After(5 * time.Second):
			require.Fail(t, "Did not receive expected message")
		}
	}
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
	if err := client.Subscribe(context.Background(), "foo-stream", func(msg Message, err error) {
		if err != nil {
			panic(err)
		}
		fmt.Println(msg.Offset(), string(msg.Value()))
	}); err != nil {
		panic(err)
	}

	// Subscribe to a specific stream partition.
	ctx := context.Background()
	if err := client.Subscribe(ctx, "bar-stream", func(msg Message, err error) {
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
