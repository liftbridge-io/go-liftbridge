package main

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/tylertreat/go-liftbridge"
	"github.com/tylertreat/go-liftbridge/liftbridge-grpc"
)

func main() {
	addr := "localhost:9292"
	client, err := liftbridge.Connect([]string{addr})
	if err != nil {
		panic(err)
	}
	defer client.Close()
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
