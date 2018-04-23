package test

import (
	"testing"
	"zircon/chunkserver/storage"
	"os"
	"io/ioutil"
	"github.com/stretchr/testify/require"
)

func TestMemoryStorage(t *testing.T) {
	mem, err := storage.ConfigureMemoryStorage()
	require.NoError(t, err)
	openStorage := func() storage.ChunkStorage {
		return mem
	}
	resetStorage := func() {
		newmem, err := storage.ConfigureMemoryStorage()
		require.NoError(t, err)
		mem = newmem
	}
	TestChunkStorage(openStorage, resetStorage, t)
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
	resetStorage := func() {
		require.NoError(t, os.RemoveAll(working))
		require.NoError(t, os.Mkdir(working, 0755))
	}
	TestChunkStorage(openStorage, resetStorage, t)
}

/*
func TestBlockStorage(t *testing.T) {
	// TODO once we figure out how to make test block devices
	openStorage := func() storage.ChunkStorage {

	}
	resetStorage := func() {

	}
	TestChunkStorage(openStorage, resetStorage, t)
}
*/
