module github.com/liftbridge-io/go-liftbridge

go 1.13

require (
	github.com/liftbridge-io/liftbridge v0.0.0-20191106173932-5d89c897d5b0
	github.com/liftbridge-io/liftbridge-api v0.0.0-20190910222614-5694b15f251d
	github.com/nats-io/nats-server v1.4.1
	github.com/nats-io/nats.go v1.9.1
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.4.0
	google.golang.org/grpc v1.25.0
	honnef.co/go/tools v0.0.0-20190523083050-ea95bdfd59fc // indirect
)

replace github.com/golang/lint => golang.org/x/lint v0.0.0-20190409202823-959b441ac422
