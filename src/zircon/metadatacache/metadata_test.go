package metadatacache

import (
	"github.com/stretchr/testify/assert"
	//	"math/rand"
	"testing"
	"zircon/apis"
	"zircon/chunkserver"
	"zircon/etcd"
	"zircon/rpc"
)

func TestSingleCache(t *testing.T) {
	// Start etcd
	etcds, _ := etcd.PrepareSubscribeForTesting(t)
	etcd1, _ := etcds("mc1")

	conn := rpc.NewConnectionCache()

	cs, _, csT := chunkserver.NewTestChunkserver(t, conn)
	defer csT()

	teardown, _, err := rpc.PublishChunkserver(cs, ":0")
	assert.NoError(t, err)
	defer teardown(true)

	cache, err := NewCache(conn, etcd1)
	assert.NoError(t, err)

	// Try reading an entry that doesn't exist
	_, _, err = cache.ReadEntry(0)
	assert.Error(t, err)

	// Now create an entry and read it unitialized
	chunk1, err := cache.NewEntry()
	assert.NoError(t, err)
	_, _, err = cache.ReadEntry(chunk1)
	assert.NoError(t, err)
	// assert.Equal(t, entry, defaultEntry())

	// Create another entry and check that it comes from the same block
	chunk2, err := cache.NewEntry()
	assert.NotEqual(t, chunk1, chunk2)
	assert.Equal(t, chunkToBlockID(chunk1), chunkToBlockID(chunk2))

	// Update the first entry
	entry1 := apis.MetadataEntry{
		Version:  1,
		Replicas: []apis.ServerID{0},
	}

	_, err = cache.UpdateEntry(chunk1, entry1)
	assert.NoError(t, err)
	readEntry1, _, err := cache.ReadEntry(chunk1)
	assert.Equal(t, entry1, readEntry1)

	// Update the second entry
	entry2 := apis.MetadataEntry{
		Version:  2,
		Replicas: []apis.ServerID{1},
	}

	_, err = cache.UpdateEntry(chunk2, entry2)
	assert.NoError(t, err)
	readEntry2, _, err := cache.ReadEntry(chunk2)
	assert.Equal(t, entry2, readEntry2)

	// Delete this entry and check that it is deleted
	_, err = cache.DeleteEntry(chunk1)
	assert.NoError(t, err)
	_, _, err = cache.ReadEntry(chunk1)
	assert.Error(t, err)

	// Check that entry 2 still exists
	readEntry2, _, err = cache.ReadEntry(chunk2)
	assert.Equal(t, entry2, readEntry2)

	// Clear the cache, check that entry 2 can still be read
	// cache.reset()
	readEntry2, _, err = cache.ReadEntry(chunk2)
	assert.Equal(t, entry2, readEntry2)

	// TODO Test for failure if previousEntry doesn't match
}

func TestMultipleClients(t *testing.T) {
}

func TestTwoCaches(t *testing.T) {
	etcds, _ := etcd.PrepareSubscribeForTesting(t)
	etcd1, _ := etcds("mc1")
	etcd2, _ := etcds("mc2")
	conn1 := rpc.NewConnectionCache()
	conn2 := rpc.NewConnectionCache()

	cache1, err := NewCache(conn1, etcd1)
	assert.NoError(t, err)
	cache2, err := NewCache(conn2, etcd2)
	assert.NoError(t, err)

	chunk1, err := cache1.NewEntry()
	assert.NoError(t, err)
	chunk2, err := cache2.NewEntry()
	assert.NoError(t, err)
	assert.NotEqual(t, chunk1, chunk2)
	assert.NotEqual(t, chunkToBlockID(chunk1), chunkToBlockID(chunk2))

	// Update the first entry
	entry1 := apis.MetadataEntry{
		Version:  1,
		Replicas: []apis.ServerID{0},
	}

	_, err = cache1.UpdateEntry(chunk1, entry1)
	assert.NoError(t, err)
	readEntry1, _, err := cache1.ReadEntry(chunk1)
	assert.Equal(t, entry1, readEntry1)

	// Update the second entry
	entry2 := apis.MetadataEntry{
		Version:  2,
		Replicas: []apis.ServerID{1},
	}

	_, err = cache2.UpdateEntry(chunk2, entry2)
	assert.NoError(t, err)
	readEntry2, _, err := cache2.ReadEntry(chunk2)
	assert.Equal(t, entry2, readEntry2)

	// Cache1 reading Chunk2 should error and direct it to Cache2
	readEntry2, _, err = cache1.ReadEntry(chunk2)
	assert.Error(t, err)
	// assert.Equal(t, owner, server2)

	// Clearing the cache and waiting should free up the lease
	// cache2.reset()
	// wait
	readEntry2, _, err = cache1.ReadEntry(chunk2)
	assert.NoError(t, err)
	assert.Equal(t, entry2, readEntry2)
}

/*
func TestAggresiveAllocationAndRelease(t *testing.T) {
	etcds, _ := etcd.PrepareSubscribeForTesting(t)
	etcd1, _ := etcds("mc1")
	conn := rpc.NewConnectionCache()
	cache, err := NewCache(conn, etcd1)
	assert.NoError(t, err)

	allocations := make(map[int]apis.ChunkNum)
	numAllocations := 9999
	for i := 0; i < numAllocations; i++ {
		chunkNum, err := cache.NewEntry()
		assert.NoError(t, err)
		allocations[i] = chunkNum
	}

	for i := 0; i < numAllocations/3; i++ {
		chunkNum, _ := allocations[rand.Int()%numAllocations]
		_, err := cache.DeleteEntry(chunkNum)
		assert.NoError(t, err)
	}

	for i := numAllocations; i < 2*numAllocations; i++ {
		chunkNum, err := cache.NewEntry()
		assert.NoError(t, err)
		allocations[i] = chunkNum
	}
}
*/
