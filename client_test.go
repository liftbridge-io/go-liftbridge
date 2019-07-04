package liftbridge

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/liftbridge-io/liftbridge/server"
	natsdTest "github.com/nats-io/nats-server/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"

	"github.com/liftbridge-io/go-liftbridge/liftbridge-grpc"
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
	msg := NewMessage(value,
		Key(key),
		AckInbox(ackInbox),
		AckPolicyNone(),
		CorrelationID("123"),
	)
	actual, ok := UnmarshalMessage(msg)
	require.True(t, ok)
	require.Equal(t, key, actual.Key)
	require.Equal(t, value, actual.Value)
	require.Equal(t, ackInbox, actual.AckInbox)
	require.Equal(t, proto.AckPolicy_NONE, actual.AckPolicy)
	require.Equal(t, "123", actual.CorrelationId)
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
		_, err := client.Publish(ctx, "foo", expected[i].Value, Key(expected[i].Key))
		require.NoError(t, err)
	}

	// Subscribe from the beginning.
	recv := 0
	ch1 := make(chan struct{})
	ch2 := make(chan struct{})
	err = client.Subscribe(context.Background(), "foo", "bar", func(msg *proto.Message, err error) {
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
		_, err := client.Publish(ctx, "foo", expected[i+count].Value,
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

	err = client.Subscribe(context.Background(), "foo", "bar", func(msg *proto.Message, err error) {
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
	err = client.Subscribe(context.Background(), "foo", "bar", func(msg *proto.Message, err error) {
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

	// Wait for leader to be elected.
	getMetadataLeader(t, 10*time.Second, s1, s2, s3)

	subject := "foo"
	name := "bar"
	require.NoError(t, c.CreateStream(context.Background(), subject, name, ReplicationFactor(3)))

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
		_, err := c.Publish(ctx, "foo", expected[i].Value,
			Key(expected[i].Key), AckPolicyAll())
		require.NoError(t, err)
	}

	// Subscribe from the beginning.
	recv := 0
	ch1 := make(chan struct{})
	ch2 := make(chan struct{})
	err = c.Subscribe(context.Background(), "foo", "bar", func(msg *proto.Message, err error) {
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

	// Wait to read back published messages.
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
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := c.Publish(ctx, "foo", expected[i+count].Value,
			Key(expected[i+count].Key), AckPolicyAll())
		require.NoError(t, err)
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

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s)

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
		ack, err := client.Publish(ctx, "foo", expected[i].Value,
			Key(expected[i].Key), AckPolicyLeader())
		require.NoError(t, err)
		require.NotNil(t, ack)
		require.Equal(t, "foo", ack.StreamSubject)
		require.Equal(t, "bar", ack.StreamName)
	}

	// Subscribe from the beginning.
	recv := 0
	ch := make(chan struct{})
	err = client.Subscribe(context.Background(), "foo", "bar", func(msg *proto.Message, err error) {
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
		ack, err := client.Publish(context.Background(), "foo", expected[i].Value,
			Key(expected[i].Key))
		require.NoError(t, err)
		require.Nil(t, ack)
	}

	// Subscribe from the beginning.
	recv := 0
	ch := make(chan struct{})
	err = client.Subscribe(context.Background(), "foo", "bar", func(msg *proto.Message, err error) {
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

	ack, err := client.Publish(context.Background(), "foo",
		[]byte("hello"),
		Header("a", []byte("header")),
		Headers(map[string][]byte{"some": []byte("more")}))
	require.NoError(t, err)
	require.Nil(t, ack)

	// Subscribe from the beginning.
	ch := make(chan *proto.Message)
	err = client.Subscribe(context.Background(), "foo", "bar", func(msg *proto.Message, err error) {
		require.NoError(t, err)
		ch <- msg
	}, StartAtEarliestReceived())
	require.NoError(t, err)

	// Wait to read back published messages.
	select {
	case msg := <-ch:
		require.Len(t, msg.Headers, 2)
		for name, value := range msg.Headers {
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
