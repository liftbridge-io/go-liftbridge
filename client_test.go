package liftbridge

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/liftbridge-io/go-liftbridge/liftbridge-grpc"
	"github.com/liftbridge-io/liftbridge/server"
	natsdTest "github.com/nats-io/gnatsd/test"
	"github.com/nats-io/go-nats"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

type message struct {
	Key    []byte
	Value  []byte
	Offset int64
}

func assertMsg(t *testing.T, expected *message, msg *proto.Message) {
	require.Equal(t, expected.Offset, msg.Offset)
	require.Equal(t, expected.Key, msg.Key)
	require.Equal(t, expected.Value, msg.Value)
}

func getStreamLeader(t *testing.T, timeout time.Duration, subject, name string,
	client *client, servers map[*server.Config]*server.Server) (*server.Server, *server.Config) {
	var (
		leaderID string
		deadline = time.Now().Add(timeout)
	)
	for time.Now().Before(deadline) {
		metadata, err := client.updateMetadata()
		require.NoError(t, err)
		for _, meta := range metadata.Metadata {
			if meta.Stream.Subject == subject && meta.Stream.Name == name {
				leaderID = meta.Leader
			}
		}
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
		StreamSubject: "foo",
		StreamName:    "bar",
		MsgSubject:    "foo",
		Offset:        1,
		AckInbox:      "acks",
	}
	data, err := ack.Marshal()
	require.NoError(t, err)
	actual, err := UnmarshalAck(data)
	require.NoError(t, err)
	require.Equal(t, ack, actual)
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
	msg := NewMessage(value, Key(key), AckInbox(ackInbox))
	actual, ok := UnmarshalMessage(msg)
	require.True(t, ok)
	require.Equal(t, key, actual.Key)
	require.Equal(t, value, actual.Value)
	require.Equal(t, ackInbox, actual.AckInbox)
}

func TestUnmarshalMessageError(t *testing.T) {
	_, ok := UnmarshalMessage(nil)
	require.False(t, ok)

	_, ok = UnmarshalMessage([]byte("blahh"))
	require.False(t, ok)

	_, ok = UnmarshalMessage([]byte("LIFTblah"))
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
	time.Sleep(2 * time.Second)

	require.NoError(t, client.CreateStream(context.Background(), "foo", "bar"))

	nc, err := nats.GetDefaultOptions().Connect()
	require.NoError(t, err)
	defer nc.Close()

	ackInbox := "acks"
	acked := 0
	count := 5
	acksCh1 := make(chan struct{})
	acksCh2 := make(chan struct{})
	_, err = nc.Subscribe(ackInbox, func(m *nats.Msg) {
		_, err := UnmarshalAck(m.Data)
		require.NoError(t, err)
		acked++
		if acked == count {
			close(acksCh1)
		}
		if acked == 2*count {
			close(acksCh2)
		}
	})
	require.NoError(t, err)

	expected := make([]*message, count)
	for i := 0; i < count; i++ {
		expected[i] = &message{
			Key:    []byte("test"),
			Value:  []byte(strconv.Itoa(i)),
			Offset: int64(i),
		}
	}

	for i := 0; i < count; i++ {
		err = nc.Publish("foo", NewMessage(expected[i].Value,
			Key(expected[i].Key), AckInbox(ackInbox)))
		require.NoError(t, err)
	}

	// Ensure all acks were received.
	select {
	case <-acksCh1:
	case <-time.After(10 * time.Second):
		t.Fatal("Did not receive all expected acks")
	}

	// Subscribe from the beginning.
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	recv := 0
	ch1 := make(chan struct{})
	ch2 := make(chan struct{})
	err = client.Subscribe(ctx, "foo", "bar", func(msg *proto.Message, err error) {
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
			cancel()
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
		err = nc.Publish("foo", NewMessage(expected[i+count].Value,
			Key(expected[i+count].Key), AckInbox(ackInbox)))
		require.NoError(t, err)
	}

	// Ensure all acks were received.
	select {
	case <-acksCh2:
	case <-time.After(10 * time.Second):
		t.Fatal("Did not receive all expected acks")
	}

	// Wait to read the new messages.
	select {
	case <-ch2:
	case <-time.After(10 * time.Second):
		t.Fatal("Did not receive all expected messages")
	}
}

// Ensure the client resubscribes to the stream if the stream leader fails
// over.
func TestClientResubscribe(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	config1 := getTestConfig("a", true, 5050)
	config1.Clustering.ReplicaMaxLeaderTimeout = 1100 * time.Millisecond
	config1.Clustering.ReplicaFetchTimeout = time.Second
	config1.Clustering.ReplicaMaxLagTime = 2 * time.Second
	s1 := runServerWithConfig(t, config1)
	defer s1.Stop()

	config2 := getTestConfig("b", false, 5051)
	config2.Clustering.ReplicaMaxLeaderTimeout = 1100 * time.Millisecond
	config2.Clustering.ReplicaFetchTimeout = time.Second
	config2.Clustering.ReplicaMaxLagTime = 2 * time.Second
	s2 := runServerWithConfig(t, config2)
	defer s2.Stop()

	config3 := getTestConfig("c", false, 5052)
	config3.Clustering.ReplicaMaxLeaderTimeout = 1100 * time.Millisecond
	config3.Clustering.ReplicaFetchTimeout = time.Second
	config3.Clustering.ReplicaMaxLagTime = 2 * time.Second
	s3 := runServerWithConfig(t, config3)
	defer s3.Stop()

	servers := map[*server.Config]*server.Server{
		config1: s1,
		config2: s2,
		config3: s3,
	}

	c, err := Connect([]string{"localhost:5050", "localhost:5051", "localhost:5052"})
	require.NoError(t, err)
	defer c.Close()
	time.Sleep(2 * time.Second)

	subject := "foo"
	name := "bar"
	require.NoError(t, c.CreateStream(context.Background(), subject, name, ReplicationFactor(3)))

	nc, err := nats.GetDefaultOptions().Connect()
	require.NoError(t, err)
	defer nc.Close()

	ackInbox := "acks"
	acked := 0
	count := 5
	acksCh1 := make(chan struct{})
	acksCh2 := make(chan struct{})
	_, err = nc.Subscribe(ackInbox, func(m *nats.Msg) {
		_, err := UnmarshalAck(m.Data)
		require.NoError(t, err)
		acked++
		if acked == count {
			close(acksCh1)
		}
		if acked == 2*count {
			close(acksCh2)
		}
	})
	require.NoError(t, err)

	expected := make([]*message, count)
	for i := 0; i < count; i++ {
		expected[i] = &message{
			Key:    []byte("test"),
			Value:  []byte(strconv.Itoa(i)),
			Offset: int64(i),
		}
	}

	for i := 0; i < count; i++ {
		err = nc.Publish("foo", NewMessage(expected[i].Value,
			Key(expected[i].Key),
			AckInbox(ackInbox),
			AckPolicyAll(),
		))
		require.NoError(t, err)
	}

	// Ensure all acks were received.
	select {
	case <-acksCh1:
	case <-time.After(10 * time.Second):
		t.Fatal("Did not receive all expected acks")
	}

	// Subscribe from the beginning.
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	recv := 0
	ch1 := make(chan struct{})
	ch2 := make(chan struct{})
	err = c.Subscribe(ctx, "foo", "bar", func(msg *proto.Message, err error) {
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
			cancel()
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

	// TODO: This is needed until Liftbridge issue #38 is fixed.
	time.Sleep(time.Second)

	// Kill the stream leader.
	leader, leaderConfig := getStreamLeader(t, 10*time.Second, subject, name, c.(*client), servers)
	leader.Stop()

	// Wait for new leader to be elected.
	delete(servers, leaderConfig)
	_, leaderConfig = getStreamLeader(t, 10*time.Second, subject, name, c.(*client), servers)

	// Publish some more.
	for i := 0; i < count; i++ {
		expected = append(expected, &message{
			Key:    []byte("blah"),
			Value:  []byte(strconv.Itoa(i + count)),
			Offset: int64(i + count),
		})
	}

	for i := 0; i < count; i++ {
		err = nc.Publish("foo", NewMessage(expected[i+count].Value,
			Key(expected[i+count].Key),
			AckInbox(ackInbox),
			AckPolicyAll(),
		))
		require.NoError(t, err)
	}

	// Ensure all acks were received.
	select {
	case <-acksCh2:
	case <-time.After(10 * time.Second):
		t.Fatal("Did not receive all expected acks")
	}

	// Wait to read the new messages.
	select {
	case <-ch2:
	case <-time.After(10 * time.Second):
		t.Fatal("Did not receive all expected messages")
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
	time.Sleep(2 * time.Second)

	require.NoError(t, client.CreateStream(context.Background(), "foo", "bar"))

	ch := make(chan error)
	err = client.Subscribe(context.Background(), "foo", "bar", func(msg *proto.Message, err error) {
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
	if err := client.CreateStream(context.Background(), "foo", "foo-stream"); err != nil {
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

	// Subscribe to stream.
	ctx := context.Background()
	if err := client.Subscribe(ctx, "bar", "bar-stream", func(msg *proto.Message, err error) {
		if err != nil {
			panic(err)
		}
		fmt.Println(msg.Offset, string(msg.Value))
	}); err != nil {
		panic(err)
	}

	<-ctx.Done()
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
		fmt.Println("ack:", ack.StreamSubject, ack.StreamName, ack.Offset, ack.MsgSubject)
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
