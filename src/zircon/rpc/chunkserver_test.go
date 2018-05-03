package rpc

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
	"zircon/apis"
	"zircon/apis/mocks"
)

func beginTest(t *testing.T) (*mocks.Chunkserver, func(), apis.Chunkserver) {
	cache := NewConnectionCache()

	mocked := new(mocks.Chunkserver)

	teardown, address, err := PublishChunkserver(mocked, ":0")
	assert.NoError(t, err)

	server, err := cache.SubscribeChunkserver(address)
	assert.NoError(t, err)

	return mocked, func() {
		mocked.AssertExpectations(t)

		teardown(true)
		cache.CloseAll()
	}, server
}

func TestChunkserver_StartWriteReplicated(t *testing.T) {
	mocked, teardown, server := beginTest(t)
	defer teardown()

	mocked.On("StartWriteReplicated", 73, 55, []byte("this is a hello\000 world!!\n"),
		[]apis.ServerAddress{"abc", "def", "ghi.mit.edu"}).Return(nil)
	mocked.On("StartWriteReplicated", 0, 0, []byte("|||"),
		[]apis.ServerAddress{}).Return(errors.New("hello world 01"))

	err := server.StartWriteReplicated(73, 55, []byte("this is a hello\000 world!!\n"),
		[]apis.ServerAddress{"abc", "def", "ghi.mit.edu"})
	assert.NoError(t, err)

	err = server.StartWriteReplicated(0, 0, []byte("|||"), []apis.ServerAddress{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "hello world 01")
}

func TestChunkserver_Replicate(t *testing.T) {
	mocked, teardown, server := beginTest(t)
	defer teardown()

	mocked.On("Replicate", 74, "jkl.mit.edu", 56).Return(nil)
	mocked.On("Replicate", 0, "", 0).Return(errors.New("hello world 02"))

	assert.NoError(t, server.Replicate(74, "jkl.mit.edu", 56))

	err := server.Replicate(0, "", 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "hello world 02")
}

func TestChunkserver_Read(t *testing.T) {
	mocked, teardown, server := beginTest(t)
	defer teardown()

	mocked.On("Read", 75, 57, 58, 59).Return("testy testy", 60, nil)
	mocked.On("Read", 0, 0, 0, 0).Return("", 6, errors.New("hello world 03"))

	data, ver, err := server.Read(75, 57, 58, 59)
	assert.NoError(t, err)
	assert.Equal(t, "testy testy", string(data))
	assert.Equal(t, apis.Version(60), ver)

	_, ver, err = server.Read(0, 0, 0, 0)
	assert.Error(t, err)
	assert.Equal(t, apis.Version(6), ver)
	assert.Contains(t, err.Error(), "hello world 03")
}

func TestChunkserver_StartWrite(t *testing.T) {
	mocked, teardown, server := beginTest(t)
	defer teardown()

	mocked.On("StartWrite", 76, 61, []byte("phenomenologist")).Return(nil)
	mocked.On("StartWrite", 0, 0, []byte(nil)).Return(errors.New("hello world 04"))

	assert.NoError(t, server.StartWrite(76, 61, []byte("phenomenologist")))

	err := server.StartWrite(0, 0, []byte{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "hello world 04")
}

func TestChunkserver_CommitWrite(t *testing.T) {
	mocked, teardown, server := beginTest(t)
	defer teardown()

	mocked.On("CommitWrite", 77, "this is my hash", 62, 63).Return(nil)
	mocked.On("CommitWrite", 0, "", 0, 0).Return(errors.New("hello world 05"))

	assert.NoError(t, server.CommitWrite(77, "this is my hash", 62, 63))

	err := server.CommitWrite(0, "", 0, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "hello world 05")
}

func TestChunkserver_UpdateLatestVersion(t *testing.T) {
	mocked, teardown, server := beginTest(t)
	defer teardown()

	mocked.On("UpdateLatestVersion", 78, 64, 65).Return(nil)
	mocked.On("UpdateLatestVersion", 0, 0, 0).Return(errors.New("hello world 06"))

	assert.NoError(t, server.UpdateLatestVersion(78, 64, 65))

	err := server.UpdateLatestVersion(0, 0, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "hello world 06")
}

func TestChunkserver_Add(t *testing.T) {
	mocked, teardown, server := beginTest(t)
	defer teardown()

	mocked.On("Add", 79, []byte("quest"), 66).Return(nil)
	mocked.On("Add", 0, []byte(nil), 0).Return(errors.New("hello world 07"))

	assert.NoError(t, server.Add(79, []byte("quest"), 66))

	err := server.Add(0, []byte{}, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "hello world 07")
}

func TestChunkserver_Delete(t *testing.T) {
	mocked, teardown, server := beginTest(t)
	defer teardown()

	mocked.On("Delete", 80, 67).Return(nil)
	mocked.On("Delete", 0, 0).Return(errors.New("hello world 08"))

	assert.NoError(t, server.Delete(80, 67))

	err := server.Delete(0, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "hello world 08")
}

func TestChunkserver_ListAllChunks_Pass(t *testing.T) {
	mocked, teardown, server := beginTest(t)
	defer teardown()

	mocked.On("ListAllChunks").Return([]struct {
		Chunk   apis.ChunkNum
		Version apis.Version
	}{
		{81, 68}, {82, 69},
	}, nil)

	chunks, err := server.ListAllChunks()
	assert.NoError(t, err)
	assert.Equal(t, []struct {
		Chunk   apis.ChunkNum
		Version apis.Version
	}{
		{81, 68}, {82, 69},
	}, chunks)
}

func TestChunkserver_ListAllChunks_Fail(t *testing.T) {
	mocked, teardown, server := beginTest(t)
	defer teardown()

	mocked.On("ListAllChunks").Return([]struct {
		Chunk   apis.ChunkNum
		Version apis.Version
	}{},
		errors.New("hello world 09"))

	chunks, err := server.ListAllChunks()
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "hello world 09")
	}
	assert.Empty(t, chunks)
}
