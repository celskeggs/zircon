package test

import (
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
	"zircon/chunkserver/storage"
)

func TestMemoryStorage(t *testing.T) {
	mem, err := storage.ConfigureMemoryStorage()
	require.NoError(t, err)
	openStorage := func() storage.ChunkStorage {
		return mem
	}
	closeStorage := func(_ storage.ChunkStorage) {} // do nothing; memory would be wiped.
	resetStorage := func() {
		mem.Close()
		newmem, err := storage.ConfigureMemoryStorage()
		require.NoError(t, err)
		mem = newmem
	}
	TestChunkStorage(openStorage, closeStorage, resetStorage, t)
	TestVersionStorage(openStorage, closeStorage, resetStorage, t)
}

func TestFilesystemStorage(t *testing.T) {
	dir, err := ioutil.TempDir("", "filesystem-test-")
	require.NoError(t, err)
	defer func() {
		err := os.RemoveAll(dir)
		if err != nil {
			t.Log("failed to clean up:", err)
		}
	}()
	working := dir + "/test"
	require.NoError(t, os.Mkdir(working, 0755))
	openStorage := func() storage.ChunkStorage {
		cs, err := storage.ConfigureFilesystemStorage(dir)
		require.NoError(t, err)
		return cs
	}
	closeStorage := func(storage storage.ChunkStorage) {
		storage.Close()
	}
	resetStorage := func() {
		require.NoError(t, os.RemoveAll(working))
		require.NoError(t, os.Mkdir(working, 0755))
	}
	TestChunkStorage(openStorage, closeStorage, resetStorage, t)
	TestVersionStorage(openStorage, closeStorage, resetStorage, t)
}

/*
func TestBlockStorage(t *testing.T) {
	// TODO once we figure out how to make test block devices
	openStorage := func() storage.ChunkStorage {

	}
	closeStorage := func(storage storage.ChunkStorage) {
		storage.Close()
	}
	resetStorage := func() {

	}
	TestChunkStorage(openStorage, resetStorage, t)
	TestVersionStorage(openStorage, resetStorage, t)
}
*/
