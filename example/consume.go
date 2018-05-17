package main

import (
	"fmt"

	"github.com/tylertreat/go-liftbridge"
	"golang.org/x/net/context"
)

func main() {
	addr := "localhost:9292"
	client, err := liftbridge.Connect(addr)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	ctx := context.Background()
	stream, err := client.ConsumeStream(ctx, "foo", "foo-stream", 0)
	if err != nil {
		panic(err)
	}
	for {
		msg, err := stream.Recv()
		if err != nil {
			panic(err)
		}
		fmt.Println(msg.Offset, string(msg.Value))
	}
}
