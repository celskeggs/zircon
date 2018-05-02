package test

import (
	"testing"
	"zircon/apis"
	"zircon/chunkserver/storage"
	"github.com/stretchr/testify/require"
	"zircon/chunkserver"
	"zircon/chunkserver/control"
	"zircon/rpc"
)

func NewTestChunkserver(t *testing.T, cache rpc.ConnectionCache) (apis.Chunkserver, control.Teardown) {
	mem, err := storage.ConfigureMemoryStorage()
	require.NoError(t, err)
	single, teardown, err := control.ExposeChunkserver(mem)
	require.NoError(t, err)
	server, err := chunkserver.WithChatter(single, cache)
	require.NoError(t, err)
	return server, func() {
		teardown()
		mem.Close()
	}
}
