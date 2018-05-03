package main

import (
	"github.com/tylertreat/go-jetbridge"
	"golang.org/x/net/context"
)

func main() {
	addr := "localhost:9292"
	client, err := jetbridge.Connect(addr)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	if err := client.CreateStream(context.Background(), "foo", "foo-stream", 1); err != nil {
		panic(err)
	}
	println("created stream foo-stream")
}
