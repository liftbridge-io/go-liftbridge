package main

import (
	"fmt"
	"strconv"
	"sync"

	lift "github.com/liftbridge-io/go-liftbridge"
	"github.com/nats-io/go-nats"
	"golang.org/x/net/context"
)

const count = 5

func main() {
	if err := createStream(); err != nil {
		panic(err)
	}
	conn, err := nats.DefaultOptions.Connect()
	if err != nil {
		panic(err)
	}
	defer conn.Flush()
	defer conn.Close()

	ackInbox := "acks"
	var wg sync.WaitGroup

	sub, err := conn.Subscribe(ackInbox, func(m *nats.Msg) {
		ack, err := lift.UnmarshalAck(m.Data)
		if err != nil {
			panic(err)
		}
		fmt.Println("ack:", ack.StreamSubject, ack.StreamName, ack.Offset, ack.MsgSubject)
		wg.Done()
	})
	if err != nil {
		panic(err)
	}
	defer sub.Unsubscribe()

	wg.Add(count)
	fmt.Println("publishing")
	for i := 0; i < count; i++ {
		m := lift.NewMessage([]byte(strconv.FormatInt(int64(i), 10)),
			lift.Key([]byte("test")), lift.AckInbox(ackInbox))
		if err := conn.Publish("bar", m); err != nil {
			panic(err)
		}
	}
	fmt.Println("done publishing")

	wg.Wait()
}

func createStream() error {
	addr := "localhost:9292"
	client, err := lift.Connect([]string{addr})
	if err != nil {
		return err
	}
	defer client.Close()
	if err := client.CreateStream(context.Background(), "bar", "bar-stream"); err != nil {
		if err != lift.ErrStreamExists {
			return err
		}
	}
	fmt.Println("created stream foo-stream")
	return nil
}
