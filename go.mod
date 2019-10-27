module github.com/liftbridge-io/go-liftbridge

go 1.13

require (
	github.com/golang/protobuf v1.3.2
	github.com/liftbridge-io/liftbridge v0.0.0-20190831212313-ad1b5f9c2b17
	github.com/liftbridge-io/liftbridge-grpc v0.0.0-20190829220806-66e3ee4b7943
	github.com/nats-io/nats-server v1.4.1
	github.com/nats-io/nats.go v1.8.1
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.3.0
	google.golang.org/grpc v1.24.0
)

//replace github.com/golang/lint => golang.org/x/lint v0.0.0-20190409202823-959b441ac422
