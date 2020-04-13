package liftbridge

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Ensure that if the stream doesn't exist, keyPartitioner's Partition returns
// 0.
func TestKeyPartitionerNoPartitions(t *testing.T) {
	partitioner := new(keyPartitioner)

	partition := partitioner.Partition("foo", []byte("bar"), []byte("baz"), new(Metadata))

	require.Equal(t, int32(0), partition)
}

// Ensure keyPartitioner's Partition always returns the same partition for the
// same key.
func TestKeyPartitioner(t *testing.T) {
	partitioner := new(keyPartitioner)
	streams := map[string]*StreamInfo{
		"foo": {
			partitions: map[int32]*PartitionInfo{
				0: new(PartitionInfo),
				1: new(PartitionInfo),
			},
		},
	}
	metadata := &Metadata{streams: streams}

	partition := partitioner.Partition("foo", []byte("foobarbazqux"), []byte("1"), metadata)
	require.Equal(t, int32(1), partition)

	partition = partitioner.Partition("foo", []byte("foobarbazqux"), []byte("2"), metadata)
	require.Equal(t, int32(1), partition)

	partition = partitioner.Partition("foo", []byte("blah"), []byte("3"), metadata)
	require.Equal(t, int32(0), partition)

	partition = partitioner.Partition("foo", []byte("blah"), []byte("4"), metadata)
	require.Equal(t, int32(0), partition)
}
