package liftbridge

import (
	"context"
	"fmt"
	"testing"
	"time"

	proto "github.com/liftbridge-io/liftbridge-api/go"
	"github.com/stretchr/testify/require"
)

// Ensure normal consumer lifecycle works as expected.
func TestConsumerSubscribe(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	groupID := "my-group"
	consumerID := "my-consumer"

	// Mock FetchMetadata to return info for streams foo and __cursors.
	metadataResp := &proto.FetchMetadataResponse{
		Brokers: []*proto.Broker{{
			Id:   "a",
			Host: "localhost",
			Port: int32(port),
		}},
		StreamMetadata: []*proto.StreamMetadata{
			{
				Name:    "foo",
				Subject: "foo",
				Partitions: map[int32]*proto.PartitionMetadata{
					0: {
						Id:       0,
						Leader:   "a",
						Replicas: []string{"a"},
						Isr:      []string{"a"},
					},
				},
			},
			{
				Name:    "__cursors",
				Subject: "__cursors",
				Partitions: map[int32]*proto.PartitionMetadata{
					0: {
						Id:       0,
						Leader:   "a",
						Replicas: []string{"a"},
						Isr:      []string{"a"},
					},
				},
			},
		},
		GroupMetadata: []*proto.ConsumerGroupMetadata{{
			GroupId:     groupID,
			Coordinator: "a",
			Epoch:       42,
		}},
	}
	server.SetupMockFetchMetadataResponse(metadataResp)

	// Setup mocks to successfully join consumer group and receive an
	// assignment for stream foo.
	joinResp := &proto.JoinConsumerGroupResponse{
		Coordinator:        "a",
		Epoch:              42,
		ConsumerTimeout:    int64(time.Minute),
		CoordinatorTimeout: int64(time.Minute),
	}
	server.SetupMockJoinConsumerGroupResponse(joinResp)

	fetchResp := &proto.FetchConsumerGroupAssignmentsResponse{
		Epoch: 42,
		Assignments: []*proto.PartitionAssignment{
			{Stream: "foo", Partitions: []int32{0}},
		},
	}
	server.SetupMockFetchConsumerGroupAssignmentsResponse(fetchResp)

	// Mock FetchCursor to return the consumer group's cursor for the assigned
	// partition in foo.
	fetchCursorResp := &proto.FetchCursorResponse{Offset: 2}
	server.SetupMockFetchCursorRequestsResponse(fetchCursorResp)

	// Mock Subscribe to return a message for the assigned partition in foo.
	messages := []*proto.Message{
		{
			Offset:    3,
			Stream:    "foo",
			Partition: 0,
			Subject:   "foo",
		},
	}
	server.SetupMockSubscribeMessages(messages)

	// Mock SetCursor.
	server.SetupMockSetCursorResponse(&proto.SetCursorResponse{})

	// Mock LeaveConsumerGroup to successfully remove consumer.
	server.SetupMockLeaveConsumerGroupResponse(
		&proto.LeaveConsumerGroupResponse{},
	)

	// Create consumer and subscribe.
	cons, err := client.CreateConsumer(groupID, ConsumerID(consumerID))
	require.NoError(t, err)

	ch := make(chan *Message)
	err = cons.Subscribe(context.Background(), []string{"foo", "bar"},
		func(msg *Message, err error) {
			require.NoError(t, err)
			ch <- msg
		})
	require.NoError(t, err)

	// Validate we receive the expected message.
	select {
	case msg := <-ch:
		require.Equal(t, int64(3), msg.Offset())
		require.Equal(t, "foo", msg.Stream())
		require.Equal(t, int32(0), msg.Partition())
	case <-time.After(2 * time.Second):
		t.Fatal("Did not receive expected message")
	}

	// Validate mock calls.
	joinReqs := server.GetJoinConsumerGroupRequests()
	require.Len(t, joinReqs, 1)
	require.Equal(t, consumerID, joinReqs[0].ConsumerId)
	require.Equal(t, groupID, joinReqs[0].GroupId)
	require.Equal(t, []string{"foo", "bar"}, joinReqs[0].Streams)

	fetchCursorReqs := server.GetFetchCursorRequests()
	require.Len(t, fetchCursorReqs, 1)
	require.Equal(t, fmt.Sprintf("__group:%s", groupID),
		fetchCursorReqs[0].CursorId)
	require.Equal(t, int32(0), fetchCursorReqs[0].Partition)
	require.Equal(t, "foo", fetchCursorReqs[0].Stream)

	subReqs := server.GetSubscribeRequests()
	require.Len(t, subReqs, 1)
	require.Equal(t, consumerID, subReqs[0].Consumer.ConsumerId)
	require.Equal(t, groupID, subReqs[0].Consumer.GroupId)
	require.Equal(t, uint64(42), subReqs[0].Consumer.GroupEpoch)
	require.Equal(t, "foo", subReqs[0].Stream)
	require.Equal(t, int32(0), subReqs[0].Partition)

	// Leave the group.
	require.NoError(t, cons.Close())

	// Validate LeaveConsumerGroup was called and the cursor was set.
	leaveReqs := server.GetLeaveConsumerGroupRequests()
	require.Len(t, leaveReqs, 1)
	require.Equal(t, consumerID, leaveReqs[0].ConsumerId)
	require.Equal(t, groupID, leaveReqs[0].GroupId)

	setCursorReqs := server.GetSetCursorRequests()
	require.Len(t, setCursorReqs, 1)
	require.Equal(t, fmt.Sprintf("__group:%s", groupID),
		setCursorReqs[0].CursorId)
	require.Equal(t, int64(3), setCursorReqs[0].Offset)
	require.Equal(t, "foo", setCursorReqs[0].Stream)
	require.Equal(t, int32(0), setCursorReqs[0].Partition)

}

// Ensure if a cursor does not exist for the consumer group and AutoOffsetNone
// is used, an error is returned to the Subscribe handler.
func TestConsumerAutoOffsetNone(t *testing.T) {
	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	groupID := "my-group"

	// Mock FetchMetadata to return info for streams foo and __cursors.
	metadataResp := &proto.FetchMetadataResponse{
		Brokers: []*proto.Broker{{
			Id:   "a",
			Host: "localhost",
			Port: int32(port),
		}},
		StreamMetadata: []*proto.StreamMetadata{
			{
				Name:    "foo",
				Subject: "foo",
				Partitions: map[int32]*proto.PartitionMetadata{
					0: {
						Id:       0,
						Leader:   "a",
						Replicas: []string{"a"},
						Isr:      []string{"a"},
					},
				},
			},
			{
				Name:    "__cursors",
				Subject: "__cursors",
				Partitions: map[int32]*proto.PartitionMetadata{
					0: {
						Id:       0,
						Leader:   "a",
						Replicas: []string{"a"},
						Isr:      []string{"a"},
					},
				},
			},
		},
		GroupMetadata: []*proto.ConsumerGroupMetadata{{
			GroupId:     groupID,
			Coordinator: "a",
			Epoch:       42,
		}},
	}
	server.SetupMockFetchMetadataResponse(metadataResp)

	// Setup mocks to successfully join consumer group and receive an
	// assignment for stream foo.
	joinResp := &proto.JoinConsumerGroupResponse{
		Coordinator:        "a",
		Epoch:              42,
		ConsumerTimeout:    int64(time.Minute),
		CoordinatorTimeout: int64(time.Minute),
	}
	server.SetupMockJoinConsumerGroupResponse(joinResp)

	fetchResp := &proto.FetchConsumerGroupAssignmentsResponse{
		Epoch: 42,
		Assignments: []*proto.PartitionAssignment{
			{Stream: "foo", Partitions: []int32{0}},
		},
	}
	server.SetupMockFetchConsumerGroupAssignmentsResponse(fetchResp)

	// Mock FetchCursor to return -1.
	fetchCursorResp := &proto.FetchCursorResponse{Offset: -1}
	server.SetupMockFetchCursorRequestsResponse(fetchCursorResp)

	// Create consumer and subscribe.
	cons, err := client.CreateConsumer(groupID, AutoOffsetNone())
	require.NoError(t, err)

	ch := make(chan error)
	err = cons.Subscribe(context.Background(), []string{"foo", "bar"},
		func(msg *Message, err error) {
			if err != nil {
				ch <- err
			}
		})
	require.NoError(t, err)

	// Validate we receive the expected error.
	select {
	case err := <-ch:
		require.Error(t, err)
		expected := "ConsumerError: no previous consumer group offset found " +
			"for partition 0 of stream foo"
		require.Equal(t, expected, err.Error())
	case <-time.After(2 * time.Second):
		t.Fatal("Did not receive expected error")
	}

	// Validate that Subscribe was not actually called.
	subReqs := server.GetSubscribeRequests()
	require.Len(t, subReqs, 0)
}

// Ensure if a cursor does not exist for the consumer group the correct
// AutoOffset is used for Subscribe calls.
func TestConsumerAutoOffsets(t *testing.T) {
	testConsumerAutoOffset(t, proto.StartPosition_EARLIEST, AutoOffsetEarliest())
	testConsumerAutoOffset(t, proto.StartPosition_LATEST, AutoOffsetLatest())
	testConsumerAutoOffset(t, proto.StartPosition_NEW_ONLY)
}

func testConsumerAutoOffset(t *testing.T, expectedStartPos proto.StartPosition,
	options ...ConsumerOption) {

	server := newMockServer()
	defer server.Stop(t)
	port := server.Start(t)

	server.SetupMockFetchMetadataResponse(new(proto.FetchMetadataResponse))

	client, err := Connect([]string{fmt.Sprintf("localhost:%d", port)})
	require.NoError(t, err)
	defer client.Close()

	groupID := "my-group"

	// Mock FetchMetadata to return info for streams foo and __cursors.
	metadataResp := &proto.FetchMetadataResponse{
		Brokers: []*proto.Broker{{
			Id:   "a",
			Host: "localhost",
			Port: int32(port),
		}},
		StreamMetadata: []*proto.StreamMetadata{
			{
				Name:    "foo",
				Subject: "foo",
				Partitions: map[int32]*proto.PartitionMetadata{
					0: {
						Id:       0,
						Leader:   "a",
						Replicas: []string{"a"},
						Isr:      []string{"a"},
					},
				},
			},
			{
				Name:    "__cursors",
				Subject: "__cursors",
				Partitions: map[int32]*proto.PartitionMetadata{
					0: {
						Id:       0,
						Leader:   "a",
						Replicas: []string{"a"},
						Isr:      []string{"a"},
					},
				},
			},
		},
		GroupMetadata: []*proto.ConsumerGroupMetadata{{
			GroupId:     groupID,
			Coordinator: "a",
			Epoch:       42,
		}},
	}
	server.SetupMockFetchMetadataResponse(metadataResp)

	// Setup mocks to successfully join consumer group and receive an
	// assignment for stream foo.
	joinResp := &proto.JoinConsumerGroupResponse{
		Coordinator:        "a",
		Epoch:              42,
		ConsumerTimeout:    int64(time.Minute),
		CoordinatorTimeout: int64(time.Minute),
	}
	server.SetupMockJoinConsumerGroupResponse(joinResp)

	fetchResp := &proto.FetchConsumerGroupAssignmentsResponse{
		Epoch: 42,
		Assignments: []*proto.PartitionAssignment{
			{Stream: "foo", Partitions: []int32{0}},
		},
	}
	server.SetupMockFetchConsumerGroupAssignmentsResponse(fetchResp)

	// Mock FetchCursor to return -1.
	fetchCursorResp := &proto.FetchCursorResponse{Offset: -1}
	server.SetupMockFetchCursorRequestsResponse(fetchCursorResp)

	// Mock Subscribe to return a message for the assigned partition in foo.
	messages := []*proto.Message{
		{
			Offset:    3,
			Stream:    "foo",
			Partition: 0,
			Subject:   "foo",
		},
	}
	server.SetupMockSubscribeMessages(messages)

	options = append(options, AutoCheckpoint(0))

	// Create consumer and subscribe.
	cons, err := client.CreateConsumer(groupID, options...)
	require.NoError(t, err)

	ch := make(chan *Message)
	err = cons.Subscribe(context.Background(), []string{"foo", "bar"},
		func(msg *Message, err error) {
			require.NoError(t, err)
			ch <- msg
		})
	require.NoError(t, err)

	// Validate we receive the expected message.
	select {
	case msg := <-ch:
		require.Equal(t, int64(3), msg.Offset())
		require.Equal(t, "foo", msg.Stream())
		require.Equal(t, int32(0), msg.Partition())
	case <-time.After(2 * time.Second):
		t.Fatal("Did not receive expected message")
	}

	// Validate that Subscribe was called using correct start position.
	subReqs := server.GetSubscribeRequests()
	require.Len(t, subReqs, 1)
	require.Equal(t, expectedStartPos, subReqs[0].StartPosition)
}
