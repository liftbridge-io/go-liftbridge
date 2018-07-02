package main

import (
	"fmt"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/tylertreat/go-liftbridge"
	"golang.org/x/net/context"
)

const (
	msgSize = 10
	numMsgs = 100000
)

func main() {
	addrs := []string{"localhost:9292", "localhost:9293", "localhost:9294"}
	client, err := liftbridge.Connect(addrs...)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	stream := liftbridge.StreamInfo{
		Subject:           "bar",
		Name:              "bar-stream",
		ReplicationFactor: 3,
	}
	if err := client.CreateStream(context.Background(), stream); err != nil {
		if err != liftbridge.ErrStreamExists {
			panic(err)
		}
	}

	conn, err := nats.DefaultOptions.Connect()
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	ackInbox := "ack"
	acked := 0
	ch := make(chan struct{})

	sub, err := conn.Subscribe(ackInbox, func(m *nats.Msg) {
		acked++
		if acked >= numMsgs {
			ch <- struct{}{}
		}
	})
	if err != nil {
		panic(err)
	}
	sub.SetPendingLimits(-1, -1)

	msg := make([]byte, msgSize)

	start := time.Now()
	for i := 0; i < numMsgs; i++ {
		m := liftbridge.NewEnvelope(nil, msg, ackInbox)
		if err := conn.Publish("bar", m); err != nil {
			panic(err)
		}
	}

	<-ch
	elapsed := time.Since(start)
	fmt.Printf("Elapsed: %s, Msgs: %d, Msgs/sec: %f\n", elapsed, numMsgs, numMsgs/elapsed.Seconds())
}
