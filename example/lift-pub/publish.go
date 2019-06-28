package main

import (
	"fmt"
	"strconv"
	"time"

	lift "github.com/liftbridge-io/go-liftbridge"
	"golang.org/x/net/context"
)

const count = 5

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
	); err != nil {
		if err != lift.ErrStreamExists {
			panic(err)
		}
	} else {
		fmt.Println("created stream bar-stream")
	}

	fmt.Println("publishing")
	for i := 0; i < count; i++ {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		if err := client.Publish(ctx, "bar",
			[]byte(strconv.FormatInt(int64(i), 10)),
			lift.Key([]byte("test")), lift.AckPolicyAll(),
		); err != nil {
			panic(err)
		}
	}
	fmt.Println("done publishing")
}
