package main

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	lift "github.com/liftbridge-io/go-liftbridge"
)

const count = 5

var keys = [][]byte{[]byte("foo"), []byte("bar"), []byte("baz"), []byte("qux")}

func main() {
	addr := "localhost:9292"
	client, err := lift.Connect([]string{addr})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	if err := client.CreateStream(
		context.Background(), "bar", "bar-stream",
		lift.MaxReplication(),
		lift.Partitions(5),
	); err != nil {
		if err != lift.ErrStreamExists {
			panic(err)
		}
	} else {
		fmt.Println("created stream bar-stream")
	}

	fmt.Println("publishing")
	for i := 0; i < count; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := client.Publish(ctx, "bar",
			[]byte(strconv.FormatInt(int64(i), 10)),
			lift.Key(keys[rand.Intn(len(keys))]),
			lift.PartitionByKey(),
			lift.AckPolicyAll(),
		); err != nil {
			panic(err)
		}
	}
	fmt.Println("done publishing")
}
