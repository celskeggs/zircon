package rpc

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
	"zircon/apis"
	"zircon/apis/mocks"
)

func beginFrontendTest(t *testing.T) (*mocks.Frontend, func(), apis.Frontend) {
	cache := NewConnectionCache()
	mocked := new(mocks.Frontend)

	teardown, address, err := PublishFrontend(mocked, ":0")
	assert.NoError(t, err)

	server, err := cache.SubscribeFrontend(address)
	assert.NoError(t, err)

	return mocked, func() {
		mocked.AssertExpectations(t)

		teardown(true)
		cache.CloseAll()
	}, server
}

func TestFrontend_ReadMetadataEntry(t *testing.T) {
	mocked, teardown, server := beginFrontendTest(t)
	defer teardown()

	mocked.On("ReadMetadataEntry", apis.ChunkNum(166)).Return(apis.Version(885), []apis.ServerAddress{"test1.mit.edu", "test2.mit.edu", "test3.mit.edu"}, nil)
	mocked.On("ReadMetadataEntry", apis.ChunkNum(0)).Return(apis.Version(0), nil, errors.New("frontend error 1"))

	version, address, err := server.ReadMetadataEntry(166)
	assert.NoError(t, err)
	assert.Equal(t, apis.Version(885), version)
	assert.Equal(t, []apis.ServerAddress{"test1.mit.edu", "test2.mit.edu", "test3.mit.edu"}, address)

	_, _, err = server.ReadMetadataEntry(0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "frontend error 1")
}

func TestFrontend_CommitWrite(t *testing.T) {
	mocked, teardown, server := beginFrontendTest(t)
	defer teardown()

	mocked.On("CommitWrite", apis.ChunkNum(167), apis.Version(886), apis.CommitHash("potatoes and bacon")).Return(apis.Version(888), nil)
	mocked.On("CommitWrite", apis.ChunkNum(0), apis.Version(0), apis.CommitHash("")).Return(apis.Version(0), errors.New("frontend error 2"))

	version, err := server.CommitWrite(167, 886, "potatoes and bacon")
	assert.NoError(t, err)
	assert.Equal(t, apis.Version(888), version)

	_, err = server.CommitWrite(0, 0, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "frontend error 2")
}

func TestFrontend_New_Succeed(t *testing.T) {
	mocked, teardown, server := beginFrontendTest(t)
	defer teardown()

	mocked.On("New").Return(apis.ChunkNum(168), nil)

	chunk, err := server.New()
	assert.NoError(t, err)
	assert.Equal(t, apis.ChunkNum(168), chunk)
}

func TestFrontend_New_Error(t *testing.T) {
	mocked, teardown, server := beginFrontendTest(t)
	defer teardown()

	mocked.On("New").Return(apis.ChunkNum(0), errors.New("frontend error 3"))

	_, err := server.New()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "frontend error 3")
}

func TestFrontend_Delete(t *testing.T) {
	mocked, teardown, server := beginFrontendTest(t)
	defer teardown()

	mocked.On("Delete", apis.ChunkNum(169), apis.Version(889)).Return(nil)
	mocked.On("Delete", apis.ChunkNum(0), apis.Version(0)).Return(errors.New("frontend error 4"))

	err := server.Delete(169, 889)
	assert.NoError(t, err)

	err = server.Delete(0, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "frontend error 4")
}
