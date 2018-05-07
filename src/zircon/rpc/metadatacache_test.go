package rpc

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
	"zircon/apis"
	"zircon/apis/mocks"
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
		MostRecentVersion:   900,
		LastConsumedVersion: 910,
		Replicas:            []apis.ServerID{0, 1, 555555},
	}, apis.ServerName(""), nil)
	mocked.On("ReadEntry", apis.ChunkNum(1)).Return(apis.MetadataEntry{}, apis.ServerName("owner"), errors.New("metadatacache error 2a"))
	mocked.On("ReadEntry", apis.ChunkNum(0)).Return(apis.MetadataEntry{}, apis.ServerName(""), errors.New("metadatacache error 2b"))

	version, _, err := server.ReadEntry(556)
	assert.NoError(t, err)
	assert.Equal(t, apis.MetadataEntry{
		MostRecentVersion:   900,
		LastConsumedVersion: 910,
		Replicas:            []apis.ServerID{0, 1, 555555},
	}, version)

	_, owner, err := server.ReadEntry(1)
	assert.Error(t, err)
	assert.Equal(t, apis.ServerName("owner"), owner)
	assert.Contains(t, err.Error(), "metadatacache error 2a")

	_, owner, err = server.ReadEntry(0)
	assert.Error(t, err)
	assert.Equal(t, apis.ServerName(""), owner)
	assert.Contains(t, err.Error(), "metadatacache error 2b")
}

func TestMetadataCache_UpdateEntry(t *testing.T) {
	mocked, teardown, server := beginMetadataCacheTest(t)
	defer teardown()

	mocked.On("UpdateEntry", apis.ChunkNum(557),
		apis.MetadataEntry{
			Replicas: []apis.ServerID{},
		},
		apis.MetadataEntry{
			MostRecentVersion:   901,
			LastConsumedVersion: 911,
			Replicas:            []apis.ServerID{5, 88, 71},
		}).Return(apis.ServerName(""), nil)
	mocked.On("UpdateEntry", apis.ChunkNum(0),
		apis.MetadataEntry{
			Replicas: []apis.ServerID{},
		},
		apis.MetadataEntry{
			Replicas: []apis.ServerID{},
		}).Return(apis.ServerName("test.mit.edu"), errors.New("metadatacache error 3a"))
	mocked.On("UpdateEntry", apis.ChunkNum(1),
		apis.MetadataEntry{
			Replicas: []apis.ServerID{},
		},
		apis.MetadataEntry{
			Replicas: []apis.ServerID{},
		}).Return(apis.ServerName(""), errors.New("metadatacache error 3b"))

	_, err := server.UpdateEntry(557, apis.MetadataEntry{},
		apis.MetadataEntry{
			MostRecentVersion:   901,
			LastConsumedVersion: 911,
			Replicas:            []apis.ServerID{5, 88, 71},
		})
	assert.NoError(t, err)

	owner, err := server.UpdateEntry(0, apis.MetadataEntry{}, apis.MetadataEntry{})
	assert.Error(t, err)
	assert.Equal(t, apis.ServerName("test.mit.edu"), owner)
	assert.Contains(t, err.Error(), "metadatacache error 3a")

	owner, err = server.UpdateEntry(1, apis.MetadataEntry{}, apis.MetadataEntry{})
	assert.Error(t, err)
	assert.Equal(t, apis.ServerName(""), owner)
	assert.Contains(t, err.Error(), "metadatacache error 3b")
}

func TestMetadataCache_DeleteEntry(t *testing.T) {
	mocked, teardown, server := beginMetadataCacheTest(t)
	defer teardown()

	mocked.On("DeleteEntry", apis.ChunkNum(558), apis.MetadataEntry{
		MostRecentVersion: 902,
		LastConsumedVersion: 912,
		Replicas: []apis.ServerID{59, 1, 91},
	}).Return(apis.ServerName(""), nil)
	mocked.On("DeleteEntry", apis.ChunkNum(2), apis.MetadataEntry{
		Replicas: []apis.ServerID{},
	}).Return(apis.ServerName("abc.example.com"), errors.New("metadatacache error 4a"))
	mocked.On("DeleteEntry", apis.ChunkNum(0), apis.MetadataEntry{
		Replicas: []apis.ServerID{},
	}).Return(apis.ServerName(""), errors.New("metadatacache error 4b"))

	_, err := server.DeleteEntry(558, apis.MetadataEntry{
		MostRecentVersion: 902,
		LastConsumedVersion: 912,
		Replicas: []apis.ServerID{59, 1, 91},
	})
	assert.NoError(t, err)

	owner, err := server.DeleteEntry(2, apis.MetadataEntry{})
	assert.Error(t, err)
	assert.Equal(t, apis.ServerName("abc.example.com"), owner)
	assert.Contains(t, err.Error(), "metadatacache error 4a")

	owner, err = server.DeleteEntry(0, apis.MetadataEntry{})
	assert.Error(t, err)
	assert.Equal(t, apis.ServerName(""), owner)
	assert.Contains(t, err.Error(), "metadatacache error 4b")
}
