package client

import (
	"testing"
	"zircon/apis"
	"zircon/chunkserver"
	"github.com/stretchr/testify/assert"
	"zircon/frontend"
	"zircon/metadatacache"
	"zircon/rpc"
	"zircon/util"
	"zircon/etcd"
	"fmt"
	"math/rand"
)

// Prepares three chunkservers (cs0-cs2) and one frontend server (fe0)
func PrepareNetworkedCluster(t *testing.T) (cliConfig Configuration, newEtcd func() apis.EtcdInterface, teardown func()) {
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

	newEtcd = func() apis.EtcdInterface {
		iface, teardown := etcds(apis.ServerName(fmt.Sprintf("etcd-%d", rand.Uint64())))
		teardowns.Add(teardown)
		return iface
	}

	return config, newEtcd, teardowns.Teardown
}