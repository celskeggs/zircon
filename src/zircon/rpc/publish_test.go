package rpc

import (
	"testing"
	testifyAssert "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"zircon/apis"
	"errors"
)

func beginTest(t *testing.T) (*testifyAssert.Assertions, *MockChunkserver, func(), apis.Chunkserver) {
	assert := testifyAssert.New(t)

	cache, err := NewConnectionCache()
	assert.NoError(err)

	mocked := new(MockChunkserver)

	teardown, address, err := PublishChunkserver(mocked, ":0")
	assert.NoError(err)

	server, err := cache.SubscribeChunkserver(address)
	assert.NoError(err)

	return assert, mocked, func () {
		mocked.AssertExpectations(t)

		teardown()
		cache.CloseAll()
	}, server
}

func TestChunkserver_StartWriteReplicated(t *testing.T) {
	assert, mocked, teardown, server := beginTest(t)
	defer teardown()

	mocked.On("StartWriteReplicated", 73, 55, []byte("this is a hello\000 world!!\n"),
		[]apis.ServerAddress{"abc", "def", "ghi.mit.edu"}).Return(nil)
	mocked.On("StartWriteReplicated", 0, 0, []byte("|||"),
		[]apis.ServerAddress{}).Return(errors.New("hello world 01"))

	err := server.StartWriteReplicated(73, 55, []byte("this is a hello\000 world!!\n"),
		[]apis.ServerAddress{"abc", "def", "ghi.mit.edu"})
	assert.NoError(err)

	err = server.StartWriteReplicated(0, 0, []byte("|||"), []apis.ServerAddress{})
	assert.Error(err)
	assert.Contains(err.Error(), "hello world 01")
}

func TestChunkserver_Replicate(t *testing.T) {
	assert, mocked, teardown, server := beginTest(t)
	defer teardown()

	mocked.On("Replicate", 74, "jkl.mit.edu", 56).Return(nil)
	mocked.On("Replicate", 0, "", 0).Return(errors.New("hello world 02"))

	assert.NoError(server.Replicate(74, "jkl.mit.edu", 56))

	err := server.Replicate(0, "", 0)
	assert.Error(err)
	assert.Contains(err.Error(), "hello world 02")
}

func TestChunkserver_Read(t *testing.T) {
	assert, mocked, teardown, server := beginTest(t)
	defer teardown()

	mocked.On("Read", 75, 57, 58, 59).Return("testy testy", 60, nil)
	mocked.On("Read", 0, 0, 0, 0).Return("", 6, errors.New("hello world 03"))

	data, ver, err := server.Read(75, 57, 58, 59)
	assert.NoError(err)
	assert.Equal("testy testy", string(data))
	assert.Equal(apis.Version(60), ver)

	_, ver, err = server.Read(0, 0, 0, 0)
	assert.Error(err)
	assert.Equal(apis.Version(6), ver)
	assert.Contains(err.Error(), "hello world 03")
}

func TestChunkserver_StartWrite(t *testing.T) {
	assert, mocked, teardown, server := beginTest(t)
	defer teardown()

	mocked.On("StartWrite", 76, 61, []byte("phenomenologist")).Return(nil)
	mocked.On("StartWrite", 0, 0, []byte(nil)).Return(errors.New("hello world 04"))

	assert.NoError(server.StartWrite(76, 61, []byte("phenomenologist")))

	err := server.StartWrite(0, 0, []byte{})
	assert.Error(err)
	assert.Contains(err.Error(), "hello world 04")
}

func TestChunkserver_CommitWrite(t *testing.T) {
	assert, mocked, teardown, server := beginTest(t)
	defer teardown()

	mocked.On("CommitWrite", 77, "this is my hash", 62, 63).Return(nil)
	mocked.On("CommitWrite", 0, "", 0, 0).Return(errors.New("hello world 05"))

	assert.NoError(server.CommitWrite(77, "this is my hash", 62, 63))

	err := server.CommitWrite(0, "", 0, 0)
	assert.Error(err)
	assert.Contains(err.Error(), "hello world 05")
}

func TestChunkserver_UpdateLatestVersion(t *testing.T) {
	assert, mocked, teardown, server := beginTest(t)
	defer teardown()

	mocked.On("UpdateLatestVersion", 78, 64, 65).Return(nil)
	mocked.On("UpdateLatestVersion", 0, 0, 0).Return(errors.New("hello world 06"))

	assert.NoError(server.UpdateLatestVersion(78, 64, 65))

	err := server.UpdateLatestVersion(0, 0, 0)
	assert.Error(err)
	assert.Contains(err.Error(), "hello world 06")
}

func TestChunkserver_Add(t *testing.T) {
	assert, mocked, teardown, server := beginTest(t)
	defer teardown()

	mocked.On("Add", 79, []byte("quest"), 66).Return(nil)
	mocked.On("Add", 0, []byte(nil), 0).Return(errors.New("hello world 07"))

	assert.NoError(server.Add(79, []byte("quest"), 66))

	err := server.Add(0, []byte{}, 0)
	assert.Error(err)
	assert.Contains(err.Error(), "hello world 07")
}

func TestChunkserver_Delete(t *testing.T) {
	assert, mocked, teardown, server := beginTest(t)
	defer teardown()

	mocked.On("Delete", 80, 67).Return(nil)
	mocked.On("Delete", 0, 0).Return(errors.New("hello world 08"))

	assert.NoError(server.Delete(80, 67)) // TODO: purposefully wrong to test tests

	err := server.Delete(0, 0)
	assert.Error(err)
	assert.Contains(err.Error(), "hello world 08")
}

func TestChunkserver_ListAllChunks_Pass(t *testing.T) {
	assert, mocked, teardown, server := beginTest(t)
	defer teardown()

	mocked.On("ListAllChunks").Return([]struct {
		Chunk   apis.ChunkNum;
		Version apis.Version
	}{
		{81, 68}, {82, 69},
	}, nil)

	chunks, err := server.ListAllChunks()
	assert.NoError(err)
	assert.Equal([]struct {
		Chunk   apis.ChunkNum;
		Version apis.Version
	}{
		{81, 68}, {82, 69},
	}, chunks)
}

func TestChunkserver_ListAllChunks_Fail(t *testing.T) {
	assert, mocked, teardown, server := beginTest(t)
	defer teardown()

	mocked.On("ListAllChunks").Return([]struct{Chunk apis.ChunkNum; Version apis.Version}{},
		errors.New("hello world 09"))

	chunks, err := server.ListAllChunks()
	if assert.Error(err) {
		assert.Contains(err.Error(), "hello world 09")
	}
	assert.Empty(chunks)
}

type MockChunkserver struct {
	mock.Mock
}

func (o *MockChunkserver) StartWriteReplicated(chunk apis.ChunkNum, offset apis.Offset, data []byte, replicas []apis.ServerAddress) (error) {
	args := o.Called(int(chunk), int(offset), data, replicas)
	return args.Error(0)
}

func (o *MockChunkserver) Replicate(chunk apis.ChunkNum, serverAddress apis.ServerAddress, version apis.Version) (error) {
	args := o.Called(int(chunk), string(serverAddress), int(version))
	return args.Error(0)
}

func (o *MockChunkserver) Read(chunk apis.ChunkNum, offset apis.Offset, length apis.Length, minimum apis.Version) ([]byte, apis.Version, error) {
	args := o.Called(int(chunk), int(offset), int(length), int(minimum))
	return []byte(args.String(0)), apis.Version(args.Int(1)), args.Error(2)
}

func (o *MockChunkserver) StartWrite(chunk apis.ChunkNum, offset apis.Offset, data []byte) (error) {
	args := o.Called(int(chunk), int(offset), data)
	return args.Error(0)
}

func (o *MockChunkserver) CommitWrite(chunk apis.ChunkNum, hash apis.CommitHash, oldVersion apis.Version, newVersion apis.Version) (error) {
	args := o.Called(int(chunk), string(hash), int(oldVersion), int(newVersion))
	return args.Error(0)
}

func (o *MockChunkserver) UpdateLatestVersion(chunk apis.ChunkNum, oldVersion apis.Version, newVersion apis.Version) error {
	args := o.Called(int(chunk), int(oldVersion), int(newVersion))
	return args.Error(0)
}

func (o *MockChunkserver) Add(chunk apis.ChunkNum, initialData []byte, initialVersion apis.Version) (error) {
	args := o.Called(int(chunk), initialData, int(initialVersion))
	return args.Error(0)
}

func (o *MockChunkserver) Delete(chunk apis.ChunkNum, version apis.Version) (error) {
	args := o.Called(int(chunk), int(version))
	return args.Error(0)
}

func (o *MockChunkserver) ListAllChunks() ([]struct{ Chunk apis.ChunkNum; Version apis.Version }, error) {
	args := o.Called()
	return args.Get(0).([]struct{ Chunk apis.ChunkNum; Version apis.Version }), args.Error(1)
}
