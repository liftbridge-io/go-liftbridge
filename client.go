//go:generate protoc --gofast_out=plugins=grpc:. ./proto/api.proto

package jetbridge

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/tylertreat/go-jetbridge/proto"
)

var (
	envelopeCookie    = []byte("jetb")
	envelopeCookieLen = len(envelopeCookie)
)

type Stream interface {
	Recv() (*proto.Message, error)
}

type Client interface {
	Close() error
	CreateStream(ctx context.Context, subject, name string, replicationFactor int32) error
	ConsumeStream(ctx context.Context, subject, name string, offset int64) (Stream, error)
}

func NewEnvelope(key, value []byte, ackInbox string) []byte {
	msg := &proto.Message{
		Key:      key,
		Value:    value,
		AckInbox: ackInbox,
	}
	m, err := msg.Marshal()
	if err != nil {
		panic(err)
	}
	buf := make([]byte, envelopeCookieLen+len(m))
	copy(buf[0:], envelopeCookie)
	copy(buf[envelopeCookieLen:], m)
	return buf
}

func UnmarshalAck(data []byte) (*proto.Ack, error) {
	var (
		ack = &proto.Ack{}
		err = ack.Unmarshal(data)
	)
	return ack, err
}

type client struct {
	apiClient proto.APIClient
	conn      *grpc.ClientConn
}

func Connect(addr string) (Client, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	client := &client{
		conn:      conn,
		apiClient: proto.NewAPIClient(conn),
	}
	return client, nil
}

func (c *client) Close() error {
	return c.conn.Close()
}

func (c *client) CreateStream(ctx context.Context, subject, name string, replicationFactor int32) error {
	req := &proto.CreateStreamRequest{
		Subject:           subject,
		Name:              name,
		ReplicationFactor: replicationFactor,
	}
	_, err := c.apiClient.CreateStream(ctx, req)
	return err
}

func (c *client) ConsumeStream(ctx context.Context, subject, name string, offset int64) (Stream, error) {
	req := &proto.ConsumeStreamRequest{
		Subject: subject,
		Name:    name,
		Offset:  offset,
	}
	return c.apiClient.ConsumeStream(ctx, req)
}
