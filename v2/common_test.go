package liftbridge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"time"

	proto "github.com/liftbridge-io/liftbridge-api/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Used by both testing.B and testing.T so need to use
// a common interface: tLogger
type tLogger interface {
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

func stackFatalf(t tLogger, f string, args ...interface{}) {
	lines := make([]string, 0, 32)
	msg := fmt.Sprintf(f, args...)
	lines = append(lines, msg)

	// Generate the Stack of callers:
	for i := 1; true; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		msg := fmt.Sprintf("%d - %s:%d", i, file, line)
		lines = append(lines, msg)
	}

	t.Fatalf("%s", strings.Join(lines, "\n"))
}

type mockServer struct {
	*grpc.Server
	*mockAPI
	listener net.Listener
	stopped  chan struct{}
}

func newMockServer() *mockServer {
	server := grpc.NewServer()
	api := newMockAPI()
	proto.RegisterAPIServer(server, api)
	return &mockServer{
		Server:  server,
		mockAPI: api,
	}
}

func (m *mockServer) Start(t require.TestingT) int {
	return m.startOnPort(t, 0)
}

func (m *mockServer) StartOnPort(t require.TestingT, port int) {
	m.startOnPort(t, port)
}

func (m *mockServer) startOnPort(t require.TestingT, port int) int {
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	require.NoError(t, err)
	go func() {
		require.NoError(t, m.Serve(l))
	}()
	m.listener = l
	m.stopped = make(chan struct{})

	var (
		addr     = l.Addr()
		deadline = time.Now().Add(5 * time.Second)
		conn     net.Conn
	)
	for time.Now().Before(deadline) {
		conn, err = net.Dial("tcp", addr.String())
		if err == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if conn == nil {
		require.Fail(t, "Could not establish connection to mock server")
	}
	require.NoError(t, conn.Close())

	return addr.(*net.TCPAddr).Port
}

func (m *mockServer) Stop(t require.TestingT) {
	select {
	case <-m.stopped:
		return
	default:
	}
	close(m.stopped)
	m.Server.Stop()
}

type mockAPI struct {
	mu                        sync.Mutex
	createStreamRequests      []*proto.CreateStreamRequest
	deleteStreamRequests      []*proto.DeleteStreamRequest
	pauseStreamRequests       []*proto.PauseStreamRequest
	setStreamReadonlyRequests []*proto.SetStreamReadonlyRequest
	subscribeRequests         []*proto.SubscribeRequest
	fetchMetadataRequests     []*proto.FetchMetadataRequest
	publishAsyncRequests      []*proto.PublishRequest
	publishToSubjectRequests  []*proto.PublishToSubjectRequest
	setCursorRequests         []*proto.SetCursorRequest
	fetchCursorRequests       []*proto.FetchCursorRequest
	responses                 []interface{}
	messages                  []*proto.Message
	createStreamErr           error
	deleteStreamErr           error
	pauseStreamErr            error
	setStreamReadonlyErr      error
	subscribeErr              error
	subscribeAsyncErr         error
	fetchMetadataErr          error
	publishErr                error
	publishAsyncErr           error
	publishToSubjectErr       error
	setCursorErr              error
	fetchCursorErr            error
}

func newMockAPI() *mockAPI {
	return &mockAPI{
		createStreamRequests:      []*proto.CreateStreamRequest{},
		deleteStreamRequests:      []*proto.DeleteStreamRequest{},
		pauseStreamRequests:       []*proto.PauseStreamRequest{},
		setStreamReadonlyRequests: []*proto.SetStreamReadonlyRequest{},
		subscribeRequests:         []*proto.SubscribeRequest{},
		fetchMetadataRequests:     []*proto.FetchMetadataRequest{},
		publishAsyncRequests:      []*proto.PublishRequest{},
		publishToSubjectRequests:  []*proto.PublishToSubjectRequest{},
		setCursorRequests:         []*proto.SetCursorRequest{},
		fetchCursorRequests:       []*proto.FetchCursorRequest{},
	}
}

func (m *mockAPI) SetupMockResponse(responses ...interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.responses = responses
}

func (m *mockAPI) SetupMockCreateStreamError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.createStreamErr = err
}

func (m *mockAPI) SetupMockDeleteStreamError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteStreamErr = err
}

func (m *mockAPI) SetupMockPauseStreamError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pauseStreamErr = err
}

func (m *mockAPI) SetupMockSetStreamReadonlyError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setStreamReadonlyErr = err
}

func (m *mockAPI) SetupMockSubscribeMessages(messages []*proto.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = messages
}

func (m *mockAPI) SetupMockFetchMetadataError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fetchMetadataErr = err
}

func (m *mockAPI) SetupMockPublishError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishErr = err
}

func (m *mockAPI) SetupMockPublishAsyncError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishAsyncErr = err
}

func (m *mockAPI) SetupMockPublishToSubjectError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishToSubjectErr = err
}

func (m *mockAPI) SetupMockSubscribeError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subscribeErr = err
}

func (m *mockAPI) SetupMockSubscribeAsyncError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subscribeAsyncErr = err
}

func (m *mockAPI) SetupMockSetCursorError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setCursorErr = err
}

func (m *mockAPI) SetupMockFetchCursorError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fetchCursorErr = err
}

func (m *mockAPI) GetCreateStreamRequests() []*proto.CreateStreamRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.createStreamRequests
}

func (m *mockAPI) GetDeleteStreamRequests() []*proto.DeleteStreamRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.deleteStreamRequests
}

func (m *mockAPI) GetPauseStreamRequests() []*proto.PauseStreamRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pauseStreamRequests
}

func (m *mockAPI) GetSetStreamReadonlyRequests() []*proto.SetStreamReadonlyRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.setStreamReadonlyRequests
}

func (m *mockAPI) GetSubscribeRequests() []*proto.SubscribeRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.subscribeRequests
}

func (m *mockAPI) GetPublishAsyncRequests() []*proto.PublishRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.publishAsyncRequests
}

func (m *mockAPI) GetPublishToSubjectRequests() []*proto.PublishToSubjectRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.publishToSubjectRequests
}

func (m *mockAPI) GetSetCursorRequests() []*proto.SetCursorRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.setCursorRequests
}

func (m *mockAPI) GetFetchCursorRequests() []*proto.FetchCursorRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.fetchCursorRequests
}

func (m *mockAPI) GetFetchMetadataRequests() []*proto.FetchMetadataRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.fetchMetadataRequests
}

func (m *mockAPI) getResponse() interface{} {
	if len(m.responses) == 0 {
		return nil
	}
	resp := m.responses[0]
	m.responses = m.responses[1:]
	return resp
}

func (m *mockAPI) CreateStream(ctx context.Context, in *proto.CreateStreamRequest) (*proto.CreateStreamResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.createStreamRequests = append(m.createStreamRequests, in)
	if m.createStreamErr != nil {
		err := m.createStreamErr
		m.createStreamErr = nil
		return nil, err
	}
	resp := m.getResponse()
	return resp.(*proto.CreateStreamResponse), nil
}

func (m *mockAPI) DeleteStream(ctx context.Context, in *proto.DeleteStreamRequest) (*proto.DeleteStreamResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteStreamRequests = append(m.deleteStreamRequests, in)
	if m.deleteStreamErr != nil {
		err := m.deleteStreamErr
		m.deleteStreamErr = nil
		return nil, err
	}
	resp := m.getResponse()
	return resp.(*proto.DeleteStreamResponse), nil
}

func (m *mockAPI) PauseStream(ctx context.Context, in *proto.PauseStreamRequest) (*proto.PauseStreamResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pauseStreamRequests = append(m.pauseStreamRequests, in)
	if m.pauseStreamErr != nil {
		err := m.pauseStreamErr
		m.pauseStreamErr = nil
		return nil, err
	}
	resp := m.getResponse()
	return resp.(*proto.PauseStreamResponse), nil
}

func (m *mockAPI) SetStreamReadonly(ctx context.Context, in *proto.SetStreamReadonlyRequest) (*proto.SetStreamReadonlyResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setStreamReadonlyRequests = append(m.setStreamReadonlyRequests, in)
	if m.setStreamReadonlyErr != nil {
		err := m.setStreamReadonlyErr
		m.setStreamReadonlyErr = nil
		return nil, err
	}
	resp := m.getResponse()
	return resp.(*proto.SetStreamReadonlyResponse), nil
}

func (m *mockAPI) Subscribe(in *proto.SubscribeRequest, server proto.API_SubscribeServer) error {
	m.mu.Lock()
	m.subscribeRequests = append(m.subscribeRequests, in)
	if m.subscribeErr != nil {
		err := m.subscribeErr
		m.subscribeErr = nil
		m.mu.Unlock()
		return err
	}
	server.Send(new(proto.Message))
	if m.subscribeAsyncErr != nil {
		err := m.subscribeAsyncErr
		m.subscribeAsyncErr = nil
		m.mu.Unlock()
		return err
	}
	for _, msg := range m.messages {
		server.Send(msg)
	}
	m.mu.Unlock()
	<-server.Context().Done()
	return nil
}

func (m *mockAPI) FetchMetadata(ctx context.Context, in *proto.FetchMetadataRequest) (*proto.FetchMetadataResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fetchMetadataRequests = append(m.fetchMetadataRequests, in)
	if m.fetchMetadataErr != nil {
		err := m.fetchMetadataErr
		m.fetchMetadataErr = nil
		return nil, err
	}
	resp := m.getResponse()
	return resp.(*proto.FetchMetadataResponse), nil
}

func (m *mockAPI) FetchPartitionMetadata(ctx context.Context, in *proto.FetchPartitionMetadataRequest) (*proto.FetchPartitionMetadataResponse, error) {
	return nil, errors.New("not implemented")
}

func (m *mockAPI) Publish(ctx context.Context, in *proto.PublishRequest) (*proto.PublishResponse, error) {
	panic("Publish is deprecated")
}

func (m *mockAPI) PublishAsync(stream proto.API_PublishAsyncServer) error {
	m.mu.Lock()
	if m.publishAsyncErr != nil {
		err := m.publishAsyncErr
		m.publishAsyncErr = nil
		m.mu.Unlock()
		return err
	}
	m.mu.Unlock()
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF || status.Code(err) == codes.Canceled {
				return nil
			}
			return err
		}
		m.mu.Lock()
		m.publishAsyncRequests = append(m.publishAsyncRequests, req)
		m.mu.Unlock()

		if req.AckPolicy != proto.AckPolicy_NONE {
			respIface := m.getResponse()
			if respIface == nil {
				continue
			}
			resp := respIface.(*proto.PublishResponse)
			if resp.Ack != nil {
				resp.Ack.CorrelationId = req.CorrelationId
			}
			resp.CorrelationId = req.CorrelationId
			stream.Send(resp)
		}
	}
}

func (m *mockAPI) PublishToSubject(ctx context.Context, in *proto.PublishToSubjectRequest) (*proto.PublishToSubjectResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishToSubjectRequests = append(m.publishToSubjectRequests, in)
	if m.publishToSubjectErr != nil {
		err := m.publishToSubjectErr
		m.publishToSubjectErr = nil
		return nil, err
	}
	resp := m.getResponse()
	return resp.(*proto.PublishToSubjectResponse), nil
}

func (m *mockAPI) SetCursor(ctx context.Context, in *proto.SetCursorRequest) (*proto.SetCursorResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setCursorRequests = append(m.setCursorRequests, in)
	if m.setCursorErr != nil {
		err := m.setCursorErr
		m.setCursorErr = nil
		return nil, err
	}
	resp := m.getResponse()
	return resp.(*proto.SetCursorResponse), nil
}

func (m *mockAPI) FetchCursor(ctx context.Context, in *proto.FetchCursorRequest) (*proto.FetchCursorResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fetchCursorRequests = append(m.fetchCursorRequests, in)
	if m.fetchCursorErr != nil {
		err := m.fetchCursorErr
		m.fetchCursorErr = nil
		return nil, err
	}
	resp := m.getResponse()
	return resp.(*proto.FetchCursorResponse), nil
}
