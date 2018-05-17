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
	if err := client.CreateStream(context.Background(), "foo", "foo-stream", 3); err != nil {
		panic(err)
	}
	println("created stream foo-stream")
}
