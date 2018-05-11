package integration

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"zircon/apis"
	"zircon/chunkserver"
	"zircon/client"
	"zircon/etcd"
	"zircon/frontend"
	"zircon/metadatacache"
	"zircon/rpc"
	"zircon/services"
	"zircon/util"
)

// Prepares three chunkservers (cs0-cs2) and one frontend server (fe0)
// TODO Copy-pasted this from client, do something about that
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

	config := client.Configuration{}

	for _, name := range []apis.ServerName{"fe0"} {
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

		// Setup services
		_, err = services.StartServices(etcdn, mdc, cache)
		assert.NoError(t, err)
	}

	clientH, err := client.ConfigureNetworkedClient(config)
	require.NoError(t, err)

	return clientH, teardowns.Teardown
}
