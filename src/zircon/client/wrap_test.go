package client

import (
	"testing"
	"zircon/apis"
	"zircon/frontend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"zircon/util"
	"zircon/rpc"
	"zircon/chunkserver"
	"zircon/etcd"
)

type MultiTeardown struct {
	teardowns []func()
}

func (m *MultiTeardown) Teardown() {
	for _, teardown := range m.teardowns {
		teardown()
	}
}

func (m *MultiTeardown) Add(teardowns ...func()) {
	m.teardowns = append(m.teardowns, teardowns...)
}

// NOTE: Only simple tests are provided here. Everything else should either go into control/client_test.go or should be
// end-to-end tests that cover more than just this subsystem.

// Prepares three chunkservers (cs0-cs2) and one frontend server (fe0)
func PrepareNetworkedCluster(t *testing.T) (fe apis.Client, teardown func()) {
	cache := rpc.NewConnectionCache()
	teardowns := &MultiTeardown{}
	cs0, _, teardown1 := chunkserver.NewTestChunkserver(t, cache)
	cs1, _, teardown2 := chunkserver.NewTestChunkserver(t, cache)
	cs2, _, teardown3 := chunkserver.NewTestChunkserver(t, cache)
	teardowns.Add(teardown1, teardown2, teardown3)
	teardown4, cs0addr, err := rpc.PublishChunkserver(cs0, "127.0.0.1:0")
	assert.NoError(t, err)
	teardowns.Add(func() { teardown4(true) })
	teardown5, cs1addr, err := rpc.PublishChunkserver(cs1, "127.0.0.1:0")
	assert.NoError(t, err)
	teardowns.Add(func() { teardown5(true) })
	teardown6, cs2addr, err := rpc.PublishChunkserver(cs2, "127.0.0.1:0")
	assert.NoError(t, err)
	teardowns.Add(func() { teardown6(true) })

	// TODO
	assert.Fail(t, "we need to do something with these addresses: %s, %s, %s", string(cs0addr), string(cs1addr), string(cs2addr))

	etcds, teardown7 := etcd.PrepareSubscribeForTesting(t)
	teardowns.Add(teardown7)

	config := Configuration{}

	for _, name := range []apis.ServerName{ "fe0", "fe1", "fe2" } {
		etcdn, teardown8 := etcds(name)

		fen, err := frontend.ConstructFrontendOnNetwork(etcdn, cache)
		assert.NoError(t, err)
		teardown9, address, err := rpc.PublishFrontend(fen, "127.0.0.1:0")
		assert.NoError(t, err)

		assert.NoError(t, etcdn.UpdateAddress(address))
		teardowns.Add(teardown8, func () {
			teardown9(true)
		})

		config.FrontendAddresses = append(config.FrontendAddresses, address)
	}

	client, err := ConfigureNetworkedClient(config)
	require.NoError(t, err)

	return client, teardowns.Teardown
}

// Tests the ability for a single client to properly interact with a cluster, and
// perform a simple series of new, read, write, and delete operations, including
// correct error handling.
func TestSimpleClientReadWrite(t *testing.T) {
	client, teardown := PrepareNetworkedCluster(t)
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
