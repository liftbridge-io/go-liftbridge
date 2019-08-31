module github.com/liftbridge-io/go-liftbridge

go 1.12

require (
	github.com/kr/pretty v0.1.0 // indirect
	github.com/liftbridge-io/liftbridge v0.0.0-20190831212313-ad1b5f9c2b17
	github.com/liftbridge-io/liftbridge-grpc v0.0.0-20190829220806-66e3ee4b7943
	github.com/nats-io/go-nats v1.7.2 // indirect
	github.com/nats-io/nats-server v1.4.1
	github.com/nats-io/nats.go v1.8.1
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.3.0
	golang.org/x/net v0.0.0-20190628185345-da137c7871d7
	google.golang.org/grpc v1.22.0
)

replace github.com/golang/lint => golang.org/x/lint v0.0.0-20190409202823-959b441ac422
