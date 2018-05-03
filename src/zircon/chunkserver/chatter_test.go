package chunkserver

import (
	testifyAssert "github.com/stretchr/testify/assert"
	"testing"
	"zircon/apis"
	"zircon/rpc"
	"zircon/util"
)

func TestChatterReplicate(t *testing.T) {
	assert := testifyAssert.New(t)

	cache := rpc.NewConnectionCache()

	main, _, mainT := NewTestChunkserver(t, cache)
	defer mainT()
	alt, _, altT := NewTestChunkserver(t, cache)
	defer altT()

	teardown, address, err := rpc.PublishChunkserver(alt, ":0")
	assert.NoError(err)
	defer teardown(true)

	err = main.Add(73, []byte("hello world"), 2)
	assert.NoError(err)

	err = main.Replicate(73, address, 2)
	assert.NoError(err)

	data, ver, err := alt.Read(73, 0, 16, 1)
	assert.NoError(err)
	assert.Equal(apis.Version(2), ver)
	assert.Equal(16, len(data))
	assert.Equal("hello world", string(util.StripTrailingZeroes(data)))
}

func TestChatterStartReplicated(t *testing.T) {
	assert := testifyAssert.New(t)

	cache := rpc.NewConnectionCache()

	main, _, mainT := NewTestChunkserver(t, cache)
	defer mainT()
	alt1, _, alt1T := NewTestChunkserver(t, cache)
	defer alt1T()
	alt2, _, alt2T := NewTestChunkserver(t, cache)
	defer alt2T()

	teardown1, address1, err := rpc.PublishChunkserver(alt1, ":0")
	assert.NoError(err)
	defer teardown1(true)
	teardown2, address2, err := rpc.PublishChunkserver(alt2, ":0")
	assert.NoError(err)
	defer teardown2(true)

	err = main.Add(73, []byte("hello world"), 2)
	assert.NoError(err)
	err = alt1.Add(73, []byte("hello world"), 2)
	assert.NoError(err)
	err = alt2.Add(73, []byte("hello world"), 2)
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
