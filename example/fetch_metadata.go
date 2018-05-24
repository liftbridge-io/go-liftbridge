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
	resp, err := client.FetchMetadata(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v\n", resp)
}
