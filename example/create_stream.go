package main

import (
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
	stream := liftbridge.StreamInfo{
		Subject:           "foo",
		Name:              "foo-stream",
		ReplicationFactor: 1,
	}
	if err := client.CreateStream(context.Background(), stream); err != nil {
		panic(err)
	}
	println("created stream foo-stream")
}
