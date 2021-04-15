# go-liftbridge [![CircleCI](https://circleci.com/gh/liftbridge-io/go-liftbridge.svg?style=svg)](https://circleci.com/gh/liftbridge-io/go-liftbridge) [![GoDoc](https://godoc.org/github.com/liftbridge-io/go-liftbridge/v2?status.svg)](https://godoc.org/github.com/liftbridge-io/go-liftbridge/v2)

Go client for [Liftbridge](https://github.com/liftbridge-io/liftbridge), a
system that provides lightweight, fault-tolerant message streams for
[NATS](https://nats.io).

Liftbridge provides the following high-level features:

- Log-based API for NATS
- Replicated for fault-tolerance
- Horizontally scalable
- Wildcard subscription support
- At-least-once delivery support and message replay
- Message key-value support
- Log compaction by key

## Installation

```
$ go get github.com/liftbridge-io/go-liftbridge/v2
```

## Basic Usage

```go
package main

import (
	"fmt"

	lift "github.com/liftbridge-io/go-liftbridge/v2"
	"golang.org/x/net/context"
)

func main() {
	// Create Liftbridge client.
	addrs := []string{"localhost:9292", "localhost:9293", "localhost:9294"}
	client, err := lift.Connect(addrs)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Create a stream attached to the NATS subject "foo".
	var (
        	subject = "foo"
        	name    = "foo-stream"
	)
	if err := client.CreateStream(context.Background(), subject, name); err != nil {
		if err != lift.ErrStreamExists {
			panic(err)
		}
	}
	
	// Publish a message to "foo".
	if _, err := client.Publish(context.Background(), name, []byte("hello")); err != nil {
		panic(err)
	}

	// Subscribe to the stream starting from the beginning.
	ctx := context.Background()
	if err := client.Subscribe(ctx, name, func(msg *lift.Message, err error) {
		if err != nil {
			panic(err)
		}
		fmt.Println(msg.Offset(), string(msg.Value()))
	}, lift.StartAtEarliestReceived()); err != nil {
		panic(err)
	}

	<-ctx.Done()
}
```

### Create Stream

[Streams](https://github.com/liftbridge-io/liftbridge/blob/master/documentation/concepts.md#stream)
are a durable message log attached to a NATS subject. They record messages
published to the subject for consumption.

Streams have a few key properties: a subject, which is the corresponding NATS
subject, a name, which is a human-readable identifier for the stream, and a
replication factor, which is the number of nodes the stream should be
replicated to for redundancy.  Optionally, there is a group which is the name
of a load-balance group for the stream to join. When there are multiple streams
in the same group, messages will be balanced among them.

```go
// Create a stream attached to the NATS subject "foo.*" that is replicated to
// all the brokers in the cluster. ErrStreamExists is returned if a stream with
// the given name already exists.
client.CreateStream(context.Background(), "foo.*", "my-stream", lift.MaxReplication())

We can also configure different properties of the stream with options, which
allow overriding the server settings.

// Create a stream and set the rentention to 134217728 bytes (128MB) and enable
// stream compaction.
client.CreateStream(context.Background(), subject, name,
    lift.RetentionMaxBytes(134217728), lift.CompactEnabled(true))
```

Also, the client has the possibility to create a stream with enforced Optimistic Concurrency Control.
This will create a stream and enable Optimistic Concurrency Control on that specific stream.


```go
// Create a stream with Optimistic Concurrency Control enabled
err = client.CreateStream(context.Background(), "foo", stream, lift.OptimisticConcurrencyControl(true))

```
### Subscription Start/Replay Options

[Subscriptions](https://github.com/liftbridge-io/liftbridge/blob/master/documentation/concepts.md#subscription)
are how Liftbridge streams are consumed. Clients can choose where to start
consuming messages from in a stream. This is controlled using options passed to
Subscribe. Client can also choose to subscribe from partition's leader (by default)
or from a random ISR replica

```go
// Subscribe starting with new messages only.
client.Subscribe(ctx, name, func(msg *lift.Message, err error) {
	fmt.Println(msg.Offset(), string(msg.Value()))
})

// Subscribe starting with the most recently published value.
client.Subscribe(ctx, name, func(msg *lift.Message, err error) {
	fmt.Println(msg.Offset(), string(msg.Value()))
}, lift.StartAtLatestReceived())

// Subscribe starting with the oldest published value.
client.Subscribe(ctx, name, func(msg *lift.Message, err error) {
	fmt.Println(msg.Offset(), string(msg.Value()))
}, lift.StartAtEarliestReceived())

// Subscribe starting at a specific offset.
client.Subscribe(ctx, name, func(msg *lift.Message, err error) {
	fmt.Println(msg.Offset(), string(msg.Value()))
}, lift.StartAtOffset(42))

// Subscribe starting at a specific time.
client.Subscribe(ctx, name, func(msg *lift.Message, err error) {
	fmt.Println(msg.Offset(), string(msg.Value()))
}, lift.StartAtTime(time.Now()))

// Subscribe starting at a specific amount of time in the past.
client.Subscribe(ctx, name, func(msg *lift.Message, err error) {
	fmt.Println(msg.Offset(), string(msg.Value()))
}, lift.StartAtTimeDelta(time.Minute))

// Subscribe to a random ISR replica
// this helps reduce the work load
// for partition leader
client.Subscribe(ctx, name, func(msg *lift.Message, err error) {
	fmt.Println(msg.Offset(), string(msg.Value()))
}, lift.ReadISRReplica())
```

### Publishing

There are two publish APIs provided to make it easy to write messages to
streams, `Publish` and `PublishToSubject`. These include a number of options
for decorating messages with metadata like a message key and headers as well as
configuring acking behavior from the server.

`Publish` sends a message to a Liftbridge stream. The stream partition that
gets published to is determined by the provided partition or `Partitioner`
strategy passed through `MessageOptions`, if any. If a partition or
`Partitioner` is not provided, it will publish to the base partition (partition
0). This partition determines the underlying NATS subject that gets published
to. To publish directly to a specific NATS subject, use the low-level
`PublishToSubject` API described below.

Keys are used by Liftbridge's log compaction. When enabled, Liftbridge streams
will retain only the last message for a given key.

```go
// Publish a message with a key and header set.
client.Publish(context.Background(), "foo-stream", []byte("hello"),
	lift.Key([]byte("key"),
	lift.Header("foo", []byte("bar")),
)
```

An `AckPolicy` tells the server when to send an ack. If a deadline is provided
to `Publish`, it will block up to this amount of time waiting for the ack.

```go
ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
client.Publish(ctx, "foo-stream", []byte("hello"),
	lift.AckPolicyAll(), // Wait for all stream replicas to get the message
)

ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
client.Publish(ctx, "foo-stream", []byte("hello"),
	lift.AckPolicyLeader(), // Wait for just the stream leader to get the message
)

client.Publish(context.Background(), "foo-stream", []byte("hello"),
	lift.AckPolicyNone(), // Don't send an ack
)
```

Also, on specific streams where Optimistic Concurrency Control is enabled,
the client has to publish a message with `ExpectedOffset`. The `ExpectedOffset` is the
offset that should be on the stream's partition if the message is published successfully.

*Note*: as the error of concurrency is piggybacked via the `Ack`, so `AckPolicy` has to be set
to use this feature.

*Note*: by default, Liftbridge concurrency control considers the `ExpectedOffset` value -1 to be the signil
to indicate default behavior: next offset, i.e: simply skip the concurrency control verification. By default,
to provide compatibility, the client should make sure that this value is always set to -1 when concurrency control is not used.

```golang
	// Publish Async with expected offset
	err = client.PublishAsync(context.Background(), "foo", []byte("hello"),
		func(ack *lift.Ack, err error) {
			errorC <- err
		},
		lift.AckPolicyLeader(),
		// Correct offset is 0 (first message)
		lift.SetExpectedOffset(0),
	)
	require.NoError(t, err)

```

`PublishToSubject` sends a message directly to the provided NATS subject. Note
that because this publishes directly to a subject, there may be multiple (or
no) streams that receive the message. As a result, `MessageOptions` related to
partitioning will be ignored. To publish at the stream/partition level, use the
high-level `Publish` API described above.

```go
// Publish a message directly to a NATS subject.
client.PublishToSubject(context.Background(), "foo.bar", []byte("hello"))
```

Alternatively, messages can be [published directly to
NATS](#publishing-directly-with-nats) using a NATS client and Liftbridge
helpers detailed below.

#### Publishing Directly with NATS

Since Liftbridge is an extension of [NATS](https://github.com/nats-io/gnatsd),
a [NATS client](https://github.com/nats-io/nats.go) can also be used to publish
messages. This means existing NATS publishers do not need any changes for
messages to be consumed in Liftbridge.

```go
package main

import "github.com/nats-io/go-nats"

func main() {
	// Connect to NATS.
	nc, _ := nats.Connect(nats.DefaultURL)

	// Publish a message.
	nc.Publish("foo.bar", []byte("Hello, world!")) 
	nc.Flush()
}
```

As shown with the publish APIs above, Liftbridge allows publishers to add
metadata to messages, including a key, ack inbox, correlation ID, and ack
policy. The message key can be used for stream compaction in Liftbridge.
Acks are used to guarantee Liftbridge has recorded a message to ensure
at-least-once delivery. The ack inbox determines a NATS subject to publish
an acknowledgement to once Liftbridge has committed the message. The
correlation id is used to correlate an ack back to the original message.
The ack policy determines when Liftbridge acknowledges the message:
when the stream leader has stored the message, when all replicas have stored
it, or no ack at all.

This additional metadata is sent using a message envelope which is a
[protobuf](https://github.com/liftbridge-io/liftbridge-api). The publish APIs
handle this for you, but this client library also provides helper APIs to make
it easy to create envelopes and deal with acks yourself using a NATS client
directly.

```go
var (
	ackInbox = "foo.acks"
	cid      = "some-random-id"
)

// Create a message envelope to publish.
msg := lift.NewMessage([]byte("Hello, world!"),
	lift.Key([]byte("foo")), // Key to set on the message
	lift.AckInbox(ackInbox), // Send ack to this NATS subject
	lift.AckPolicyAll(),     // Send ack once message is fully replicated
	lift.CorrelationID(cid), // Set the ID which will be sent on the ack
)

// Setup a NATS subscription for acks.
sub, _ := nc.SubscribeSync(ackInbox)

// Publish the message.
nc.Publish("foo.bar", msg)

// Wait for ack from Liftbridge.
resp, _ := sub.NextMsg(5*time.Second)
ack, _ := lift.UnmarshalAck(resp.Data)
if ack.CorrelationID() == cid {
	fmt.Println("message acked!")
}
```

### Partitioning

Liftbridge streams are partitioned to allow for increased parallelism. By
default, a stream consists of a single partition, but the number of
partitions can be configured when the stream is created.

#### Creating Partitioned Streams

```go
// Create stream with three partitions.
client.CreateStream(context.Background(), "bar", "bar-stream", lift.Partitions(3))
```

Each partition maps to a NATS subject derived from the base stream subject. For
example, the partitions for a stream with three partitions attached to the
subject "bar" map to the NATS subjects "bar", "bar.1", and "bar.2",
respectively.

#### Publishing to Stream Partitions

By default, clients will publish to the base partition, but this can be
configured by providing a `Partitioner`.

```go
// Publish to partition based on message key hash.
client.Publish(context.Background(), "bar-stream", []byte("hello"),
	lift.Key([]byte("key")),
	lift.PartitionByKey(),
)

// Publish to partitions in a round-robin fashion.
client.Publish(context.Background(), "bar-stream", []byte("hello"),
	lift.Key([]byte("key")),
	lift.PartitionByRoundRobin(),
)

// Publish to a specific partition.
client.Publish(context.Background(), "bar-stream", []byte("hello"),
	lift.Key([]byte("key")),
	lift.ToPartition(1),
)

// Publish directly to a partition NATS subject.
client.PublishToSubject(context.Background(), "bar.1", []byte("hello"),
	lift.Key([]byte("key")))
```

A custom `Partitioner` implementation can also be provided to `Publish`.
`PublishToSubject` will ignore any partition-related `MessageOption`.

#### Subscribing to Stream Partitions

Like publishing, clients will subscribe to the base partition by default.
However, a specific partition to consume from can be specified at subscribe
time.

```go
// Subscribe to a specific partition.
client.Subscribe(ctx, "bar-stream", func(msg *lift.Message, err error) {
	fmt.Println(msg.Offset, string(msg.Value))
}, lift.Partition(1))
```
