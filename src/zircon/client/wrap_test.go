package client

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"zircon/apis"
	"zircon/chunkserver"
	"zircon/etcd"
	"zircon/frontend"
	"zircon/rpc"
	"zircon/util"
	"zircon/metadatacache"
)

// NOTE: Only simple tests are provided here. Everything else should either go into control/client_test.go or should be
// end-to-end tests that cover more than just this subsystem.

// Prepares three chunkservers (cs0-cs2) and one frontend server (fe0)
func PrepareNetworkedCluster(t *testing.T) (fe apis.Client, teardown func()) {
	cache := rpc.NewConnectionCache()
	teardowns := &util.MultiTeardown{}

	etcds, teardown7 := etcd.PrepareSubscribeForTesting(t)
	teardowns.Add(teardown7)

	for _, name := range []apis.ServerName{"cs0", "cs1", "cs2"} {
		cs, _, teardown1 := chunkserver.NewTestChunkserver(t, cache)
		teardowns.Add(teardown1)

		teardown4, csaddr, err := rpc.PublishChunkserver(cs, "127.0.0.1:0")
		assert.NoError(t, err)
		teardowns.Add(func() { teardown4(true) })

		etcdif, teardown := etcds(name)
		teardowns.Add(teardown)
		etcdif.UpdateAddress(csaddr, apis.CHUNKSERVER)
	}

	config := Configuration{}

	for _, name := range []apis.ServerName{"fe0", "fe1", "fe2"} {
		etcdn, teardown8 := etcds(name)

		fen, err := frontend.ConstructFrontend(etcdn, cache)
		assert.NoError(t, err)
		teardown9, address, err := rpc.PublishFrontend(fen, "127.0.0.1:0")
		assert.NoError(t, err)
		teardowns.Add(teardown8, func() {
			teardown9(true)
		})

		assert.NoError(t, etcdn.UpdateAddress(address, apis.FRONTEND))

		mdc, err := metadatacache.NewCache(cache, etcdn)
		assert.NoError(t, err)
		teardown10, mdcaddress, err := rpc.PublishMetadataCache(mdc, "127.0.0.1:0")
		assert.NoError(t, err)
		teardowns.Add(func() { teardown10(true) })

		assert.NoError(t, etcdn.UpdateAddress(mdcaddress, apis.METADATACACHE))

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

	data, ver, err := client.Read(cn, 0, 1)
	assert.NoError(t, err)
	assert.Equal(t, apis.Version(0), ver)
	assert.Equal(t, []byte{0}, data)

	ver, err = client.Write(cn, 0, apis.AnyVersion, []byte("hello, world!"))
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
