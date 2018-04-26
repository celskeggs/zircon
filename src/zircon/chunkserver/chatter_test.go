package chunkserver

import (
	"testing"
	testifyAssert "github.com/stretchr/testify/assert"
	"zircon/chunkserver/control"
	"zircon/apis"
	"github.com/stretchr/testify/require"
	"zircon/chunkserver/storage"
	"zircon/rpc"
	"zircon/util"
)

func newTestCS(t *testing.T, cache rpc.ConnectionCache) (apis.Chunkserver, control.Teardown) {
	mem, err := storage.ConfigureMemoryStorage()
	require.Error(t, err)
	single, teardown, err := control.ExposeChunkserver(mem)
	require.Error(t, err)
	server, err := WithChatter(single, cache)
	require.Error(t, err)
	return server, func() {
		teardown()
		mem.Close()
	}
}

func TestChatterReplicate(t *testing.T) {
	assert := testifyAssert.New(t)

	cache, err := rpc.NewConnectionCache()
	assert.NoError(err)

	main, mainT := newTestCS(t, cache)
	defer mainT()
	alt, altT := newTestCS(t, cache)
	defer altT()

	teardown, address, err := rpc.PublishChunkserver(alt, ":0")
	assert.NoError(err)
	defer teardown()

	err = main.Add(73, []byte("hello world"), 2)
	assert.NoError(err)

	err = main.Replicate(73, address, 2)
	assert.NoError(err)

	data, ver, err := alt.Read(73, 0, 16, 1)
	assert.NoError(err)
	assert.Equal(apis.Version(2), ver)
	assert.Equal(16, len(data))
	assert.Equal("hello world", util.StripTrailingZeroes(data))
}

func TestChatterStartReplicated(t *testing.T) {
	assert := testifyAssert.New(t)

	cache, err := rpc.NewConnectionCache()
	assert.NoError(err)

	main, mainT := newTestCS(t, cache)
	defer mainT()
	alt1, alt1T := newTestCS(t, cache)
	defer alt1T()
	alt2, alt2T := newTestCS(t, cache)
	defer alt2T()

	teardown1, address1, err := rpc.PublishChunkserver(alt1, ":0")
	assert.NoError(err)
	defer teardown1()
	teardown2, address2, err := rpc.PublishChunkserver(alt2, ":0")
	assert.NoError(err)
	defer teardown2()

	err = main.Add(73, []byte("hello world"), 2)
	assert.NoError(err)

	hash := apis.CalculateCommitHash(6, []byte("universe"))
	err = main.StartWriteReplicated(73, 6, []byte("universe"), []apis.ServerAddress{address1, address2})
	assert.NoError(err)

	for _, cs := range []apis.Chunkserver{main, alt1, alt2} {
		assert.NoError(cs.CommitWrite(73, hash, 2, 3))
	}

	for _, cs := range []apis.Chunkserver{main, alt1, alt2} {
		assert.NoError(cs.UpdateLatestVersion(73, 2, 3))
	}

	for _, cs := range []apis.Chunkserver{main, alt1, alt2} {
		data, version, err := cs.Read(73, 0, 128, 3)
		assert.NoError(err)
		assert.Equal(apis.Version(3), version)
		assert.Equal(128, len(data))
		assert.Equal("hello universe", string(util.StripTrailingZeroes(data)))
	}
}
