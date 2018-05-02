package control

import (
	"testing"
	"zircon/apis"
	"zircon/rpc"
	"github.com/stretchr/testify/assert"
	"fmt"
	"zircon/chunkserver/test"
	"zircon/frontend"
	"zircon/etcd"
	"zircon/util"
	"strconv"
	"math/rand"
	"time"
)

// Prepares three chunkservers (cs0-cs2) and one frontend server (fe0)
func PrepareLocalCluster(t *testing.T) (rpccache rpc.ConnectionCache, fe apis.Frontend, teardown func()) {
	cache := &MockCache {
		Frontends: map[apis.ServerAddress]apis.Frontend{},
	}
	cs0, teardown1 := test.NewTestChunkserver(t, cache)
	cs1, teardown2 := test.NewTestChunkserver(t, cache)
	cs2, teardown3 := test.NewTestChunkserver(t, cache)
	cache.Chunkservers = map[apis.ServerAddress]apis.Chunkserver {
		"cs0": cs0,
		"cs1": cs1,
		"cs2": cs2,
	}
	etcds, teardown4 := etcd.PrepareSubscribe(t)
	etcd0, teardown5 := etcds("fe0")
	fe, err := frontend.ConstructFrontendOnNetwork("fe0", etcd0, cache)
	assert.NoError(t, err)
	return cache, fe, func() {
		teardown5()
		teardown4()
		teardown3()
		teardown2()
		teardown1()
	}
}

func PrepareSimpleClient(t *testing.T) (apis.Client, func()) {
	cache, fe, teardown := PrepareLocalCluster(t)
	client, err := ConstructClient(fe, cache)
	assert.NoError(t, err)
	return client, func() {
		client.Close()
		teardown()
	}
}

// Tests the ability for a single client to properly interact with a cluster, and
// perform a simple series of new, read, write, and delete operations, including
// correct error handling.
func TestSimpleClientReadWrite(t *testing.T) {
	client, teardown := PrepareSimpleClient(t)
	defer teardown()

	cn, err := client.New()
	assert.NoError(t, err)

	_, _, err = client.Read(cn, 0, 1)
	assert.Error(t, err)

	ver, err := client.Write(cn, 0, apis.AnyVersion, []byte("hello, world!"))
	assert.NoError(t, err)
	assert.True(t, ver > 0)

	data, ver2, err := client.Read(cn, 0, apis.MaxChunkSize)
	assert.NoError(t, err)
	assert.Equal(t, ver, ver2)
	assert.Equal(t, "hello, world!", string(util.StripTrailingZeroes(data)))

	ver3, err := client.Write(cn, 7, ver2, []byte("home!"))
	assert.NoError(t, err)
	assert.True(t, ver3 > ver2)

	ver5, err := client.Write(cn, 7, ver2, []byte("earth..."))
	assert.Error(t, err)
	assert.Equal(t, ver3, ver5) // make sure it returns the correct new version after staleness failure

	data, ver4, err := client.Read(cn, 0, apis.MaxChunkSize)
	assert.NoError(t, err)
	assert.Equal(t, ver3, ver4)
	assert.Equal(t, "hello, home!!", string(util.StripTrailingZeroes(data)))

	assert.Error(t, client.Delete(cn, ver2))

	data, ver6, err := client.Read(cn, 0, apis.MaxChunkSize)
	assert.NoError(t, err)
	assert.Equal(t, ver4, ver6)
	assert.Equal(t, "hello, home!!", string(util.StripTrailingZeroes(data)))

	assert.NoError(t, client.Delete(cn, ver6))

	_, _, err = client.Read(cn, 0, apis.MaxChunkSize)
	assert.Error(t, err)
}

// Tests that error checking works properly for reads and writes that exceed the maximum chunk size
func TestMaxSizeChecking(t *testing.T) {
	client, teardown := PrepareSimpleClient(t)
	defer teardown()

	cn, err := client.New()
	assert.NoError(t, err)

	data := make([]byte, apis.MaxChunkSize-1)
	data[len(data) - 1] = 'a'
	ver, err := client.Write(cn, 2, apis.AnyVersion, data)
	assert.Error(t, err)
	assert.Equal(t, 0, ver)

	// make sure that the failed write didn't actually succeed
	_, _, err = client.Read(cn, 2, 5)
	assert.Error(t, err)

	ver, err = client.Write(cn, 1, apis.AnyVersion, data)
	assert.NoError(t, err)
	assert.True(t, ver > 0)

	// confirm write succeeded this time
	rdata, ver2, err := client.Read(cn, 0, apis.MaxChunkSize)
	assert.NoError(t, err)
	assert.Equal(t, ver, ver2)
	assert.Equal(t, apis.MaxChunkSize, len(rdata))
	assert.Equal(t, byte('a'), rdata[apis.MaxChunkSize - 1])
	assert.Empty(t, util.StripTrailingZeroes(rdata[:apis.MaxChunkSize - 1]))

	// attempt out-of-bounds read
	_, _, err = client.Read(cn, 1, apis.MaxChunkSize)
	assert.Error(t, err)
}

// Tests the ability for multiple clients to safely clobber each others' changes to a shared block of data.
func TestConflictingClients(t *testing.T) {
	cache, fe, teardown := PrepareLocalCluster(t)
	defer teardown()

	var chunk apis.ChunkNum

	func () {
		setupClient, err := ConstructClient(fe, cache)
		assert.NoError(t, err)
		defer setupClient.Close()
		chunk, err = setupClient.New()
		assert.NoError(t, err)
		_, err = setupClient.Write(chunk, 0, apis.AnyVersion, []byte("0"))
		assert.NoError(t, err)
	}()

	complete := make(chan struct{subtotal int; count int})
	count := 10

	finishAt := time.Now().Add(time.Second)
	for i := 0; i < count; i++ {
		go func(clientId int) {
			subtotal := 0
			subcount := 0
			ok := false
			defer func() {
				if ok {
					complete <- struct {subtotal int;count int}{subtotal, subcount}
				} else {
					complete <- struct {subtotal int;count int}{0, 0}
				}
			}()

			client, err := ConstructClient(fe, cache)
			assert.NoError(t, err)
			defer client.Close()

			for time.Now().Before(finishAt) {
				nextAddition := rand.Intn(10000) - 100
				subtotal += nextAddition

				for {
					num, ver, err := client.Read(chunk, 0, 128)
					assert.NoError(t, err)
					numnum, err := strconv.Atoi(string(util.StripTrailingZeroes(num)))
					newValue := nextAddition + numnum

					newData := make([]byte, 128)
					copy(newData, []byte(strconv.Itoa(newValue)))
					newver, err := client.Write(chunk, 0, ver, newData)
					assert.True(t, newver > ver)
					if err == nil {
						break
					}
				}

				subcount++
			}

			ok = true
		}(i)
	}

	finalSum := 0
	finalCount := 0
	for i := 0; i < count; i++ {
		subtotal := <-complete
		// should be able to process at least one contended request per millisecond on average
		assert.True(t, subtotal.count >= 50, "not enough requests processed: %d/50", subtotal.count)
		assert.NotEqual(t, 0, subtotal.subtotal)
		finalCount += subtotal.count
		finalSum += subtotal.subtotal
	}
	assert.True(t, finalCount >= 1000, "not enough requests processed: %d/1000", finalCount)

	checkSum := func () int {
		teardownClient, err := ConstructClient(fe, cache)
		assert.NoError(t, err)
		defer teardownClient.Close()
		contents, _, err := teardownClient.Read(chunk, 0, 128)
		assert.NoError(t, err)
		result, err := strconv.Atoi(string(util.StripTrailingZeroes(contents)))
		assert.NoError(t, err)
		return result
	}

	assert.Equal(t, finalSum, checkSum)
}

// Tests the ability of many parallel clients to independently perform lots of operations on their own blocks.
func TestParallelClients(t *testing.T) {
	panic("TODO")
}

// Tests the ability for deleted chunks to be fully cleaned up
func TestDeletion(t *testing.T) {
	t.Fatal("unimplemented: additional tests")
}

// Tests the ability of a series of clients to invoke New() and then close their connections, and have all of the extra
// new chunks be safely cleaned up.
func TestIncompleteRemoval(t *testing.T) {
	t.Fatal("unimplemented: additional tests")
}

// Tests that linearizability applies to a large mess of operations
func TestLinearizable(t *testing.T) {
	t.Fatal("unimplemented: additional tests")
}

type MockCache struct {
	Frontends    map[apis.ServerAddress]apis.Frontend
	Chunkservers map[apis.ServerAddress]apis.Chunkserver
}

func (mc *MockCache) SubscribeChunkserver(address apis.ServerAddress) (apis.Chunkserver, error) {
	cs, found := mc.Chunkservers[address]
	if found {
		return cs, nil
	} else {
		return nil, fmt.Errorf("no such chunkserver: %s", address)
	}
}

func (mc *MockCache) SubscribeFrontend(address apis.ServerAddress) (apis.Frontend, error) {
	fe, found := mc.Frontends[address]
	if found {
		return fe, nil
	} else {
		return nil, fmt.Errorf("no such frontend: %s", address)
	}
}

func (mc *MockCache) CloseAll() {
	// don't bother doing anything
}
