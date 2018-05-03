package rpc

import (
	"testing"
	"zircon/apis"
	"github.com/stretchr/testify/assert"
	"zircon/apis/mocks"
	"errors"
)

func beginMetadataCacheTest(t *testing.T) (*mocks.MetadataCache, func(), apis.MetadataCache) {
	cache := NewConnectionCache()
	mocked := new(mocks.MetadataCache)

	teardown, address, err := PublishMetadataCache(mocked, ":0")
	assert.NoError(t, err)

	server, err := cache.SubscribeMetadataCache(address)
	assert.NoError(t, err)

	return mocked, func() {
		mocked.AssertExpectations(t)

		teardown(true)
		cache.CloseAll()
	}, server
}

func TestMetadataCache_NewEntry_Succeed(t *testing.T) {
	mocked, teardown, server := beginMetadataCacheTest(t)
	defer teardown()

	mocked.On("NewEntry").Return(apis.ChunkNum(555), nil)

	chunk, err := server.NewEntry()
	assert.NoError(t, err)
	assert.Equal(t, apis.ChunkNum(555), chunk)
}

func TestMetadataCache_NewEntry_Error(t *testing.T) {
	mocked, teardown, server := beginMetadataCacheTest(t)
	defer teardown()

	mocked.On("NewEntry").Return(apis.ChunkNum(0), errors.New("metadatacache error 1"))

	_, err := server.NewEntry()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "metadatacache error 1")
}

func TestMetadataCache_ReadEntry(t *testing.T) {
	mocked, teardown, server := beginMetadataCacheTest(t)
	defer teardown()

	mocked.On("ReadEntry", apis.ChunkNum(556)).Return(apis.MetadataEntry{
		Version: 900,
		Replicas: []apis.ServerID{ 0, 1, 555555 },
	}, nil)
	mocked.On("ReadEntry", apis.ChunkNum(0)).Return(apis.MetadataEntry{}, errors.New("metadatacache error 2"))

	version, err := server.ReadEntry(556)
	assert.NoError(t, err)
	assert.Equal(t, apis.MetadataEntry{
		Version: 900,
		Replicas: []apis.ServerID{ 0, 1, 555555 },
	}, version)

	_, err = server.ReadEntry(0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "metadatacache error 2")
}

func TestMetadataCache_UpdateEntry(t *testing.T) {
	mocked, teardown, server := beginMetadataCacheTest(t)
	defer teardown()

	mocked.On("UpdateEntry", apis.ChunkNum(557), apis.MetadataEntry{
		Version: 901,
		Replicas: []apis.ServerID{ 5, 88, 71 },
	}).Return(nil)
	mocked.On("UpdateEntry", apis.ChunkNum(0), apis.MetadataEntry{
		Replicas: []apis.ServerID{},
	}).Return(errors.New("metadatacache error 3"))

	err := server.UpdateEntry(557, apis.MetadataEntry{
		Version: 901,
		Replicas: []apis.ServerID{ 5, 88, 71 },
	})
	assert.NoError(t, err)

	err = server.UpdateEntry(0, apis.MetadataEntry{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "metadatacache error 3")
}

func TestMetadataCache_DeleteEntry(t *testing.T) {
	mocked, teardown, server := beginMetadataCacheTest(t)
	defer teardown()

	mocked.On("DeleteEntry", apis.ChunkNum(558)).Return(nil)
	mocked.On("DeleteEntry", apis.ChunkNum(0)).Return(errors.New("metadatacache error 4"))

	err := server.DeleteEntry(558)
	assert.NoError(t, err)

	err = server.DeleteEntry(0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "metadatacache error 4")
}
