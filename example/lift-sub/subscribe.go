package main

import (
	"fmt"
	"time"

	"golang.org/x/net/context"

	lift "github.com/liftbridge-io/go-liftbridge"
	"github.com/liftbridge-io/go-liftbridge/liftbridge-grpc"
)

func main() {
	addr := "localhost:9292"
	client, err := lift.Connect([]string{addr, "localhost:9293"})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	if err := client.CreateStream(
		context.Background(), "bar", "bar-stream",
		lift.MaxReplication(),
	); err != nil {
		if err != lift.ErrStreamExists {
			panic(err)
		}
	} else {
		fmt.Println("created stream bar-stream")
	}

	ctx := context.Background()
	if err := client.Subscribe(ctx, "bar", "bar-stream", func(msg *proto.Message, err error) {
		if err != nil {
			panic(err)
		}
		fmt.Println(time.Unix(0, msg.Timestamp), msg.Offset, string(msg.Key), string(msg.Value))
	}, lift.StartAtEarliestReceived()); err != nil {
		panic(err)
	}

	<-ctx.Done()
}
